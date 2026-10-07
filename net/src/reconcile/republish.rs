//! Writing every record of a rebuild again at the new leader's term.

use std::collections::VecDeque;

use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::task_record::{Settlement, Write, WriteOrder};
use kabudachi_core::time::{Duration, Instant};

use crate::messenger::{PlacedWrite, WriteOutcome};

/// The most republished records written at once.
pub(crate) const REPUBLISH_IN_FLIGHT: usize = 64;

/// Writes every republished record, at most [`REPUBLISH_IN_FLIGHT`] at a
/// time, and writes again any that was not stored, once `retry_after` has
/// passed, until every one is stored. A record that names its successor is
/// written only after the successor's is stored (see [`WriteOrder`]).
pub(crate) struct Republish {
    retry_after: Duration,
    /// Not yet issued, in the order they must be.
    queued: VecDeque<PlacedWrite>,
    /// Issued, outcome not yet seen.
    in_flight: Vec<PlacedWrite>,
    /// Not stored, to be issued again at the instant.
    retries: Vec<(Instant, PlacedWrite)>,
    /// Held behind the write of their successor.
    behind: Vec<PlacedWrite>,
    order: WriteOrder,
}

fn is_write_of(placed: &PlacedWrite, write: &Write) -> bool {
    Write::of(&placed.record) == *write
}

impl Republish {
    /// Republishes `writes`, in publication order.
    pub(crate) fn new(writes: Vec<PlacedWrite>, retry_after: Duration) -> Self {
        let mut order = WriteOrder::default();
        let admitted = order.admit(writes.iter().map(|placed| placed.record.clone()).collect());
        // `admit` keeps the order of what it returns, so what it leaves out is
        // what was held back.
        let mut admitted = admitted.iter().map(Write::of).peekable();
        let (mut queued, mut behind) = (VecDeque::new(), Vec::new());
        for placed in writes {
            if admitted.peek() == Some(&Write::of(&placed.record)) {
                admitted.next();
                queued.push_back(placed);
            } else {
                behind.push(placed);
            }
        }
        Republish {
            retry_after,
            queued,
            in_flight: Vec::new(),
            retries: Vec::new(),
            behind,
            order,
        }
    }

    /// The writes to issue now: those whose retry is due, then the queued, up
    /// to the number that may be in flight.
    pub(crate) fn due(&mut self, now: Instant) -> Vec<PlacedWrite> {
        let (due, later): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.retries).into_iter().partition(|(at, _)| *at <= now);
        self.retries = later;
        self.queued.extend(due.into_iter().map(|(_, placed)| placed));
        let room = REPUBLISH_IN_FLIGHT.saturating_sub(self.in_flight.len());
        let issued: Vec<PlacedWrite> = self.queued.drain(..room.min(self.queued.len())).collect();
        self.in_flight.extend(issued.iter().cloned());
        issued
    }

    /// `outcome` arrived at `now`. Whether it was one of this republish's.
    pub(crate) fn settled(&mut self, outcome: &WriteOutcome, now: Instant) -> bool {
        let Some(at) = self
            .in_flight
            .iter()
            .position(|placed| is_write_of(placed, &outcome.write))
        else {
            return false;
        };
        let placed = self.in_flight.swap_remove(at);
        match self.order.settled(&outcome.write, outcome.stored) {
            Settlement::Release(records) => {
                for record in records {
                    self.release(&record);
                }
            }
            Settlement::Refuse(refused) => {
                // The records held behind the refused write still wait for it:
                // it is written again, and they follow once it is stored.
                let held: Vec<TaskRecord> = self
                    .behind
                    .iter()
                    .filter(|held| refused.iter().any(|write| is_write_of(held, write)))
                    .map(|held| held.record.clone())
                    .collect();
                if !held.is_empty() {
                    let mut again = vec![placed.record.clone()];
                    again.extend(held);
                    // Only to register the order again; the writes are issued from
                    // `queued` and `behind`, so what `admit` returns is not used.
                    self.order.admit(again);
                }
            }
        }
        if !outcome.stored {
            tracing::error!(
                task = outcome.write.task_id.as_str(),
                "a republished record was not stored: writing it again"
            );
            self.retries.push((now + self.retry_after, placed));
        }
        true
    }

    /// The voters changed: `replace` places every write not yet stored on
    /// them, and each refused one is written again now, not after its delay.
    pub(crate) fn re_place(&mut self, mut replace: impl FnMut(&mut PlacedWrite), now: Instant) {
        let waiting = self.queued.iter_mut().chain(&mut self.behind).chain(&mut self.in_flight);
        for placed in waiting {
            replace(placed);
        }
        for (at, placed) in &mut self.retries {
            replace(placed);
            *at = now;
        }
    }

    fn release(&mut self, record: &TaskRecord) {
        let write = Write::of(record);
        if let Some(at) = self.behind.iter().position(|held| is_write_of(held, &write)) {
            self.queued.push_back(self.behind.remove(at));
        }
    }

    /// Whether every record is stored.
    pub(crate) fn is_done(&self) -> bool {
        self.queued.is_empty()
            && self.in_flight.is_empty()
            && self.retries.is_empty()
            && self.behind.is_empty()
    }

    /// When the earliest write not stored is due to be written again.
    pub(crate) fn wake_at(&self) -> Option<Instant> {
        self.retries.iter().map(|(at, _)| *at).min()
    }
}

#[cfg(test)]
mod tests {
    use kabudachi_core::coordination_authority::RecoveryEpoch;
    use kabudachi_core::protocol::generated::{CoalescingLink, Task};
    use kabudachi_core::protocol::ids::TaskId;
    use kabudachi_core::task_record::RecordVersion;

    use super::*;

    fn at(ticks: u64) -> Instant {
        Instant::at(ticks)
    }

    fn record(task: &str, superseded_by: Option<&str>) -> TaskRecord {
        TaskRecord {
            version: Some(
                RecordVersion {
                    recovery_epoch: RecoveryEpoch::new(0, 0),
                    leader_term: 2,
                    revision: 1,
                }
                .into(),
            ),
            task: Some(Task {
                task_id: Some(TaskId::new(task).into()),
                ..Task::default()
            }),
            link: superseded_by.map(|newer| CoalescingLink {
                superseded_by: Some(TaskId::new(newer).into()),
                absorbed: Vec::new(),
            }),
            ..TaskRecord::default()
        }
    }

    fn placed(task: &str) -> PlacedWrite {
        PlacedWrite {
            record: record(task, None),
            quorum: 2,
        }
    }

    fn outcome(placed: &PlacedWrite, stored: bool) -> WriteOutcome {
        WriteOutcome {
            write: Write::of(&placed.record),
            stored,
        }
    }

    fn named(writes: &[PlacedWrite]) -> Vec<String> {
        writes
            .iter()
            .map(|placed| Write::of(&placed.record).task_id.as_str().to_owned())
            .collect()
    }

    #[test]
    fn only_the_cap_is_in_flight_and_the_rest_follow_as_writes_are_stored() {
        let total = REPUBLISH_IN_FLIGHT + 3;
        let writes: Vec<PlacedWrite> = (0..total).map(|n| placed(&format!("task-{n:03}"))).collect();
        let mut republish = Republish::new(writes, Duration::from_millis(10));

        let first = republish.due(at(0));
        assert_eq!(first.len(), REPUBLISH_IN_FLIGHT);
        assert!(republish.due(at(0)).is_empty(), "nothing more is issued at the cap");

        for issued in &first[..2] {
            assert!(republish.settled(&outcome(issued, true), at(1)));
        }
        let second = republish.due(at(1));
        assert_eq!(named(&second), ["task-064", "task-065"]);
        assert!(!republish.is_done());

        for issued in first[2..].iter().chain(&second) {
            republish.settled(&outcome(issued, true), at(2));
        }
        let last = republish.due(at(2));
        assert_eq!(named(&last), ["task-066"]);
        republish.settled(&outcome(&last[0], true), at(3));
        assert!(republish.is_done());
    }

    #[test]
    fn a_refused_write_is_written_again_after_the_delay_until_it_is_stored() {
        let mut republish = Republish::new(vec![placed("task-a")], Duration::from_millis(10));
        let issued = republish.due(at(0));
        assert!(!republish.settled(&outcome(&placed("task-other"), true), at(1)), "not its write");

        republish.settled(&outcome(&issued[0], false), at(5));

        assert!(republish.due(at(14)).is_empty(), "not before the delay has passed");
        assert_eq!(republish.wake_at(), Some(at(15)));
        let again = republish.due(at(15));
        assert_eq!(named(&again), ["task-a"]);
        assert!(!republish.is_done());
        republish.settled(&outcome(&again[0], true), at(16));
        assert!(republish.is_done());
        assert_eq!(republish.wake_at(), None);
    }

    #[test]
    fn writes_not_yet_stored_are_placed_again_and_written_at_once() {
        let mut republish = Republish::new(
            vec![placed("task-a"), placed("task-b"), placed("task-c")],
            Duration::from_millis(1_000),
        );
        let issued = republish.due(at(0));
        republish.settled(&outcome(&issued[0], true), at(1));
        republish.settled(&outcome(&issued[1], false), at(1));

        republish.re_place(|write| write.quorum = 7, at(2));

        let again = republish.due(at(2));
        assert_eq!(named(&again), ["task-b"], "the refused write does not wait out its delay");
        assert_eq!(again[0].quorum, 7);
        republish.settled(&outcome(&again[0], true), at(3));
        let last = republish.settled(&outcome(&issued[2], false), at(3));
        assert!(last);
        let retried = republish.due(at(1_003));
        assert_eq!(named(&retried), ["task-c"]);
        assert_eq!(retried[0].quorum, 7, "a write still in flight is placed again too");
    }

    #[test]
    fn a_generation_naming_its_successor_is_written_only_once_the_successor_is_stored() {
        let newer = placed("task-new");
        let older = PlacedWrite {
            record: record("task-old", Some("task-new")),
            quorum: 2,
        };
        let mut republish = Republish::new(vec![newer, older], Duration::from_millis(10));

        let first = republish.due(at(0));
        assert_eq!(named(&first), ["task-new"], "the older generation waits");
        // The successor is refused and written again; the older one still waits.
        republish.settled(&outcome(&first[0], false), at(1));
        let second = republish.due(at(11));
        assert_eq!(named(&second), ["task-new"]);
        assert!(republish.due(at(11)).is_empty());

        republish.settled(&outcome(&second[0], true), at(12));

        let last = republish.due(at(12));
        assert_eq!(named(&last), ["task-old"]);
        republish.settled(&outcome(&last[0], true), at(13));
        assert!(republish.is_done());
    }
}
