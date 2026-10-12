use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::ids::IdGenerator;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::scheduler::{Event, Scheduler};
use kabudachi_core::task_record::{LeaderRecords, RecordOutbox};
use kabudachi_core::time::{Clock, Instant};

use super::answers::{HeldAnswer, Local, send_answer};
use super::stepper::NetRecords;
use crate::messenger::Net;

/// Lets time act on `scheduler` if its deadline has come (delays released,
/// pending tasks expired, finished tasks forgotten), writes what that
/// changed, and returns its next deadline. Nothing waits on those writes: no
/// one asked for them.
pub(super) fn catch_up_if_due<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    records: &mut LeaderRecords<HeldAnswer>,
    ports: &mut NetRecords<'_>,
    now: Instant,
) -> Option<Instant> {
    if scheduler.next_deadline().is_some_and(|due| due <= now) {
        scheduler.catch_up();
        records.write(node, scheduler, ports);
    }
    scheduler.next_deadline()
}

/// No client lives on a worker of a networked shard yet, so the events the
/// scheduler raises for one are logged and dropped rather than kept for ever,
/// except a cancel of a running run: it is held until the cancel is stored,
/// then left in `local` for the run's worker to be told. So is a run stored
/// as cancelled, perhaps by an earlier leader, that its worker reported it
/// still holds. Taking the events ends the scheduler's call, which can
/// publish the revisions the call made, so they are written first; nothing
/// else waits on those writes.
pub(super) fn drain_events<C: Clock, I: IdGenerator>(
    node: &WorkerNode<C>,
    scheduler: &mut Scheduler<C, I, RecordOutbox>,
    records: &mut LeaderRecords<HeldAnswer>,
    ports: &mut NetRecords<'_>,
    net: &Net,
    local: &mut Local,
) {
    let events = scheduler.take_events();
    records.write(node, scheduler, ports);
    for event in events {
        // A running run that was cancelled is stopped where it runs, once
        // the cancel is stored: told to its worker in its next ack, or, for
        // a run of this worker's own, to its executor. Telling it earlier
        // could stop a body whose cancel the next leader never sees.
        if let Event::Cancelled {
            task_id,
            task_run_id,
            was_running: true,
        } = &event
            && let Some(worker) = scheduler
                .task_run(task_run_id)
                .and_then(|run| run.selected_worker())
        {
            let held = HeldAnswer::CancelledRun {
                worker,
                run: task_run_id.clone(),
            };
            if let Some(settled) = records.hold(held, Vec::new(), Some(task_id)) {
                send_answer(net, settled, local);
            }
        }
        tracing::debug!(?event, "scheduler event with no client to tell");
    }
    for held in scheduler.take_held_cancels() {
        let answer = HeldAnswer::CancelledRun {
            worker: held.worker,
            run: held.task_run_id,
        };
        if let Some(settled) = records.hold(answer, Vec::new(), Some(&held.task_id)) {
            send_answer(net, settled, local);
        }
    }
}
