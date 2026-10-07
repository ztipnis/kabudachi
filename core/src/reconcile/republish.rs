//! Writing every record of a rebuild again at the new leader's term.

use std::collections::VecDeque;

use crate::protocol::generated::TaskRecord;
use crate::task_record::{PlacedWrite, Settlement, Write, WriteOrder, WriteOutcome};
use crate::time::{Duration, Instant};

/// The most republished records written at once.
const REPUBLISH_IN_FLIGHT: usize = 64;

/// Writes every republished record, a bounded number at a time
/// (`REPUBLISH_IN_FLIGHT`), and writes again any that was not stored, once
/// `retry_after` has passed, until every one is stored. A record that names
/// its successor is written only after the successor's is stored (see
/// [`WriteOrder`]).
pub struct Republish {
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
    pub fn new(writes: Vec<PlacedWrite>, retry_after: Duration) -> Self {
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
    pub fn due(&mut self, now: Instant) -> Vec<PlacedWrite> {
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
    pub fn settled(&mut self, outcome: &WriteOutcome, now: Instant) -> bool {
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
    pub fn re_place(&mut self, mut replace: impl FnMut(&mut PlacedWrite), now: Instant) {
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
    pub fn is_done(&self) -> bool {
        self.queued.is_empty()
            && self.in_flight.is_empty()
            && self.retries.is_empty()
            && self.behind.is_empty()
    }

    /// When the earliest write not stored is due to be written again.
    pub fn wake_at(&self) -> Option<Instant> {
        self.retries.iter().map(|(at, _)| *at).min()
    }
}
