use kabudachi_core::protocol::ids::{IdGenerator, TaskId, TaskRunId};
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::scheduler::Event;
use kabudachi_core::task_record::RecordKey;
use kabudachi_core::time::{Clock, Instant};

use super::answers::{Deciding, HeldAnswer, Local};

/// Lets time act on the scheduler if its deadline has come (delays
/// released, pending tasks expired, finished tasks forgotten), writes what
/// that changed, and returns its next deadline. Nothing waits on those
/// writes: no one asked for them.
pub(super) fn catch_up_if_due<C: Clock, I: IdGenerator>(
    deciding: &mut Deciding<'_, C, I>,
    now: Instant,
) -> Option<Instant> {
    if deciding
        .scheduler
        .next_deadline()
        .is_some_and(|due| due <= now)
    {
        deciding.scheduler.catch_up();
        deciding.write();
    }
    deciding.scheduler.next_deadline()
}

/// The events the scheduler raises name no client of their own, so they are
/// logged and dropped rather than kept for ever, except a composite that
/// ended, which is held until its records are stored and then told to its
/// client, and a cancel of a running run: it is held until the cancel is stored,
/// then left in `local` for the run's worker to be told. So is a run stored
/// as cancelled, perhaps by an earlier leader, that its worker reported it
/// still holds. A task with a client that ended is told to the client the
/// same way: once what ended it is stored. The scheduler hands those over
/// only while it leads, so what a rebuild concluded waits until every
/// revision the rebuild republished is stored. Taking the events ends the
/// scheduler's call, which can publish the revisions the call made, so they
/// are written first; nothing else waits on those writes.
pub(super) fn drain_events<C: Clock, I: IdGenerator>(
    deciding: &mut Deciding<'_, C, I>,
    local: &mut Local,
) {
    let events = deciding.scheduler.take_events();
    deciding.write();
    for event in events {
        if let Event::CompositeFinished(finished) = &event
            && let Some(client) = finished.client.clone()
        {
            // Told only once the composite's terminal records are stored: a
            // client that settles on this must never see a later leader
            // undo it.
            let held = HeldAnswer::Finished {
                client,
                finished: finished.into(),
            };
            let record = RecordKey::Composite(finished.composite_id.clone());
            deciding.hold_unwritten(held, Some(record), local);
            continue;
        }
        hold_for_worker(deciding, &event, local);
        tracing::debug!(?event, "scheduler event with no client to tell");
    }
    for held in deciding.scheduler.take_held_cancels() {
        let answer = HeldAnswer::CancelledRun {
            worker: held.worker,
            run: held.task_run_id,
        };
        deciding.hold_unwritten(answer, Some(RecordKey::Task(held.task_id)), local);
    }
    for conclusion in deciding.scheduler.take_conclusions() {
        let task = conclusion.outcome.task_id.clone();
        deciding.hold_unwritten(HeldAnswer::Concluded(conclusion), Some(RecordKey::Task(task)), local);
    }
}

/// Holds what `event` leaves a worker to be told once it is stored. A
/// running run that was cancelled is stopped where it runs, once the
/// cancel is stored: told to its worker in its next ack, or, for a run of
/// this worker's own, to its executor. Telling it earlier could stop a
/// body whose cancel the next leader never sees.
// One arm per event a worker or client must hear of.
#[allow(clippy::single_match)]
fn hold_for_worker<C: Clock, I: IdGenerator>(
    deciding: &mut Deciding<'_, C, I>,
    event: &Event,
    local: &mut Local,
) {
    match event {
        Event::Cancelled {
            task_id,
            task_run_id,
            was_running: true,
        } => {
            hold_cancelled_run(deciding, task_id, task_run_id, local);
        }
        _ => {}
    }
}

fn hold_cancelled_run<C: Clock, I: IdGenerator>(
    deciding: &mut Deciding<'_, C, I>,
    task: &TaskId,
    run: &TaskRunId,
    local: &mut Local,
) {
    let Some(worker) = deciding
        .scheduler
        .task_run(run)
        .and_then(|held| held.selected_worker())
    else {
        return;
    };
    deciding.hold_unwritten(
        HeldAnswer::CancelledRun {
            worker,
            run: run.clone(),
        },
        Some(RecordKey::Task(task.clone())),
        local,
    );
}
