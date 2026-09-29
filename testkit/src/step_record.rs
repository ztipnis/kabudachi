//! One node's step as a harness saw it, and the invariants the harnesses
//! assert over a run's records. The core simulator and the net tests that
//! drive real workers both record steps this way, so both check leadership
//! exclusivity through the same code.

use kabudachi_core::configuration::Generation;
use kabudachi_core::election::{Input, Output, Step, WorkerNode};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd};
use kabudachi_core::time::{Clock, Instant};

/// One step of one node: the input it handled, the outputs it produced, and
/// what the node reported of itself once the step was done.
///
/// `at` and the lease end of any grant in `outputs` are instants of one
/// timeline that every record of a run shares (see [`grant_intervals`]).
#[derive(Debug, Clone, PartialEq)]
pub struct StepRecord {
    pub at: Instant,
    pub node: WorkerId,
    /// The input the step handled; `None` for a step no single input caused
    /// (the one a node starts or joins with).
    pub input: Option<Input>,
    pub state: WorkerState,
    pub term: u64,
    pub recovery_epoch: u64,
    pub admission: Option<Generation>,
    pub prior_admission: Option<Generation>,
    pub leader: Option<(WorkerId, u64)>,
    pub outputs: Vec<Output>,
}

impl StepRecord {
    /// The record of `step`, which `node` has just taken on `input`, at `at`.
    pub fn of<C: Clock>(
        node: &WorkerNode<C>,
        input: Option<&Input>,
        step: &Step,
        at: Instant,
    ) -> Self {
        StepRecord {
            at,
            node: node.id().clone(),
            input: input.cloned(),
            state: node.state(),
            term: node.term(),
            recovery_epoch: node.recovery_epoch(),
            admission: node.admission(),
            prior_admission: node.prior_admission(),
            leader: node.known_leader(),
            outputs: step.outputs.clone(),
        }
    }

    /// Whether the step reported a grant or its withdrawal.
    pub fn reports_grant(&self) -> bool {
        self.outputs
            .iter()
            .any(|output| matches!(output, Output::Grant(_)))
    }
}

/// A stretch of time one node held a leadership grant: from the record that
/// reported it until the earlier of its lease end and the node's next grant
/// report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantInterval {
    pub node: WorkerId,
    /// The index, in the records it was worked out from, of the record that
    /// reported the grant.
    pub record: usize,
    pub from: Instant,
    /// `None` for an unbounded grant the node has not reported since.
    pub until: Option<Instant>,
    start: Moment,
    end: Option<Moment>,
}

/// A point in a run's order of events: an instant and, among the steps at
/// that instant, the index of one (`None` before every step there, where a
/// lease ending at that instant ends).
type Moment = (Instant, Option<usize>);

/// Every stretch of time a node held a grant, in the order of the records
/// that reported them (see [`GrantInterval`]). A grant whose lease had
/// ended by the time it was reported covers nothing and is left out.
///
/// `records` must be in the order the steps happened, and hold every step
/// that reported a grant; their `at` and every lease end must be instants
/// of one clock. A node that stalls or stops reports nothing more, so its
/// grant ends at its lease end, as its scheduler's does; one steps at the
/// same instant are ordered by their place in `records`.
pub fn grant_intervals(records: &[StepRecord]) -> Vec<GrantInterval> {
    let mut intervals: Vec<GrantInterval> = Vec::new();
    for (index, record) in records.iter().enumerate() {
        let now: Moment = (record.at, Some(index));
        for output in &record.outputs {
            let Output::Grant(grant) = output else {
                continue;
            };
            if let Some(previous) = intervals
                .iter_mut()
                .rev()
                .find(|interval| interval.node == record.node)
                && previous.end.is_none_or(|end| now < end)
            {
                previous.end = Some(now);
                previous.until = Some(record.at);
            }
            if let Some(LeadershipGrant { valid_until, .. }) = grant {
                let lease_end = match valid_until {
                    LeaseEnd::Unbounded => None,
                    LeaseEnd::At(end) => Some((*end, None)),
                };
                intervals.push(GrantInterval {
                    node: record.node.clone(),
                    record: index,
                    from: record.at,
                    until: lease_end.map(|(end, _)| end),
                    start: now,
                    end: lease_end,
                });
            }
        }
    }
    intervals.retain(|interval| interval.end.is_none_or(|end| interval.start < end));
    intervals
}

/// Panics, naming both grants, if two nodes held a leadership grant at once
/// (see [`first_grant_overlap`]).
pub fn assert_at_most_one_leader(records: &[StepRecord]) {
    if let Some((first, second)) = first_grant_overlap(records) {
        panic!(
            "two nodes held a leadership grant at once: {:?} from {:?} and {:?} from {:?}",
            first.node, first.at, second.node, second.at
        );
    }
}

/// The records that reported the first pair of grants, in the order the
/// grants began, that belonged to different nodes and overlapped (see
/// [`grant_intervals`]); `None` if no two did.
pub fn first_grant_overlap(records: &[StepRecord]) -> Option<(StepRecord, StepRecord)> {
    // `grant_intervals` lists intervals in the order they start, so every
    // `b` after `a` starts no earlier: it reaches back into `a` exactly
    // when it starts before `a` ends, and once one does not, none after it
    // does.
    let intervals = grant_intervals(records);
    for (i, a) in intervals.iter().enumerate() {
        for b in &intervals[i + 1..] {
            if a.end.is_some_and(|end| b.start >= end) {
                break;
            }
            if a.node != b.node {
                return Some((records[a.record].clone(), records[b.record].clone()));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(valid_until: LeaseEnd) -> Option<LeadershipGrant> {
        Some(LeadershipGrant {
            term: 1,
            recovery_epoch: 0,
            valid_until,
        })
    }

    fn reports(node: &str, at: u64, grant: Option<LeadershipGrant>) -> StepRecord {
        StepRecord {
            at: Instant::at(at),
            node: WorkerId::new(node),
            input: None,
            state: WorkerState::Leader,
            term: 1,
            recovery_epoch: 0,
            admission: None,
            prior_admission: None,
            leader: None,
            outputs: vec![Output::Grant(grant)],
        }
    }

    #[test]
    fn two_overlapping_grants_are_reported() {
        let records = [
            reports("a", 10, grant(LeaseEnd::Unbounded)),
            reports("b", 20, grant(LeaseEnd::Unbounded)),
            reports("a", 30, None),
        ];

        let (first, second) = first_grant_overlap(&records).expect("the grants overlapped");

        assert_eq!(
            (first.node, second.node),
            (WorkerId::new("a"), WorkerId::new("b"))
        );
    }

    #[test]
    fn a_grant_withdrawn_before_the_next_begins_is_no_overlap() {
        let records = [
            reports("a", 10, grant(LeaseEnd::Unbounded)),
            reports("a", 20, None),
            reports("b", 20, grant(LeaseEnd::Unbounded)),
        ];

        assert_eq!(first_grant_overlap(&records), None);
    }

    #[test]
    fn a_node_renewing_its_grant_is_no_overlap() {
        let records = [
            reports("a", 10, grant(LeaseEnd::At(Instant::at(30)))),
            reports("a", 20, grant(LeaseEnd::At(Instant::at(40)))),
        ];

        assert_eq!(first_grant_overlap(&records), None);
    }

    // A node that stalls or stops never reports its grant withdrawn; its
    // lease still ends, and another may lead from then on, not before.
    #[test]
    fn a_lease_end_before_the_next_report_closes_the_interval() {
        let lease_end = LeaseEnd::At(Instant::at(15));
        let after_it = [
            reports("a", 10, grant(lease_end)),
            reports("b", 15, grant(LeaseEnd::Unbounded)),
        ];
        let before_it = [
            reports("a", 10, grant(lease_end)),
            reports("b", 14, grant(LeaseEnd::Unbounded)),
        ];

        assert_eq!(first_grant_overlap(&after_it), None);
        assert!(first_grant_overlap(&before_it).is_some());
    }
}
