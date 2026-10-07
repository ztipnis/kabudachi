//! A node that is its shard's only worker and only holder answers its own
//! reconciliation from its own store.

use std::collections::BTreeSet;

use super::wire::held_key;
use super::{ReconcileRound, ReconcileTerm, ReportPage, ReportedRun, ReportedState};
use crate::protocol::generated;
use crate::protocol::ids::{IdGenerator, WorkerId};
use crate::scheduler::{Claim, ReconcileRefused, Scheduler};
use crate::task_record::LocalRecords;
use crate::time::{Clock, Duration, Instant};

/// The lone node `me` took office: it answers its own reconciliation from
/// its store (every record is certain, and every run the store says it holds
/// is still held, since the executor runs in this process) and rebuilds
/// `scheduler`, which republishes into the same store before this returns.
/// Returns the workers the store says hold runs, which the node must watch
/// as lost workers.
pub fn reconcile_alone<C: Clock, I: IdGenerator>(
    scheduler: &mut Scheduler<C, I, LocalRecords<C>>,
    me: &WorkerId,
    now: Instant,
    office: ReconcileTerm,
) -> Result<BTreeSet<WorkerId>, ReconcileRefused> {
    let records: Vec<generated::TaskRecord> =
        scheduler.observer_mut().records().iter().cloned().collect();
    let mut page = ReportPage {
        last: true,
        ..ReportPage::default()
    };
    for record in &records {
        // Cannot fail: the local store only holds records this scheduler
        // wrote, and each has its version, task and task id (a put of any
        // other is refused as malformed), and a well formed input digest.
        page.keys
            .push(held_key(record).expect("a stored record is well formed"));
        let Some(run) = record.runs.last() else {
            continue;
        };
        let state = match generated::TaskRunState::try_from(run.state) {
            Ok(generated::TaskRunState::Claimed) => ReportedState::Claimed,
            Ok(generated::TaskRunState::Running) => ReportedState::Running,
            _ => continue,
        };
        if run
            .selected_worker
            .as_ref()
            .map(|worker| WorkerId::from(worker.clone()))
            .as_ref()
            != Some(me)
        {
            continue;
        }
        let (Some(task), Some(identity)) = (record.task.clone(), run.identity.as_ref()) else {
            continue;
        };
        let Some(task_run_id) = identity.task_run_id.clone() else {
            continue;
        };
        page.runs.push(ReportedRun {
            claim: Claim {
                task,
                task_run_id: task_run_id.into(),
                attempt_number: identity.attempt_number,
                chain: Vec::new(),
            },
            state,
        });
    }
    let mut round = ReconcileRound::new(office, [me.clone()], now, Duration::from_millis(0));
    round.page(me, page, now);
    for record in records {
        round.fetched(record);
    }
    let rebuilt = scheduler.reconcile(round.take_settled(|worker| worker == me))?;
    // The store settles each write as it is made. A republish it refused
    // would leave a record of an earlier term standing, which nothing here
    // can retry. None is refused: the store holds only what this one node
    // wrote at earlier terms, so each republish is a newer version of what it
    // holds, and none is malformed.
    assert!(
        scheduler.observer_mut().take_settled().iter().all(|(_, kept)| *kept),
        "the lone node's own store keeps every record it republishes"
    );
    Ok(rebuilt.silent_holders)
}
