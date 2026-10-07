//! Where tasks wait to start: the queue, the schedule of delayed tasks, and
//! the expiries, kept consistent with each other in one place.

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::ids::TaskId;
use crate::time::Instant;

/// Where tasks wait to be claimed: queued in the order they started waiting,
/// or scheduled until their delay passes; and, until first claimed, by when
/// they expire. A task is in exactly one of the queue and the schedule while
/// it waits, and in the expiries exactly while it waits with an expiry.
#[derive(Debug, Default)]
pub(super) struct WaitingRoom {
    places: BTreeMap<TaskId, Place>,
    queue: BTreeMap<u64, TaskId>,
    scheduled: BTreeMap<(Instant, u64), TaskId>,
    expiries: BTreeSet<(Instant, TaskId)>,
    /// Orders the queue, and tasks scheduled for the same instant, by when
    /// they started waiting.
    next_ticket: u64,
}

#[derive(Debug, Clone, Copy)]
struct Place {
    spot: Spot,
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
enum Spot {
    Queued(u64),
    Scheduled(Instant, u64),
}

impl WaitingRoom {
    /// A new task starts waiting: scheduled until `not_before` if given,
    /// queued otherwise, and with `expires_at` it expires then unless claimed.
    pub(super) fn admit(
        &mut self,
        task: &TaskId,
        not_before: Option<Instant>,
        expires_at: Option<Instant>,
    ) {
        let ticket = self.take_ticket();
        let spot = match not_before {
            Some(due) => {
                self.scheduled.insert((due, ticket), task.clone());
                Spot::Scheduled(due, ticket)
            }
            None => {
                self.queue.insert(ticket, task.clone());
                Spot::Queued(ticket)
            }
        };
        if let Some(expires_at) = expires_at {
            self.expiries.insert((expires_at, task.clone()));
        }
        self.places.insert(task.clone(), Place { spot, expires_at });
    }

    /// Every task stops waiting.
    pub(super) fn clear(&mut self) {
        *self = WaitingRoom::default();
    }

    /// The next attempt of a task that was claimed (a retry or a replay)
    /// waits again at the back of the queue, never with an expiry: expiry is
    /// only about starting.
    pub(super) fn requeue(&mut self, task: &TaskId) {
        let ticket = self.take_ticket();
        self.queue.insert(ticket, task.clone());
        self.places.insert(
            task.clone(),
            Place {
                spot: Spot::Queued(ticket),
                expires_at: None,
            },
        );
    }

    /// `task` was claimed: it stops waiting, and its expiry goes with it.
    pub(super) fn claimed(&mut self, task: &TaskId) {
        self.leave(task);
    }

    /// `task` stops waiting unclaimed (cancelled or superseded). A task that
    /// is not waiting is left alone.
    pub(super) fn leave(&mut self, task: &TaskId) {
        let Some(place) = self.places.remove(task) else {
            return;
        };
        match place.spot {
            Spot::Queued(ticket) => {
                self.queue.remove(&ticket);
            }
            Spot::Scheduled(due, ticket) => {
                self.scheduled.remove(&(due, ticket));
            }
        }
        if let Some(expires_at) = place.expires_at {
            self.expiries.remove(&(expires_at, task.clone()));
        }
    }

    /// Removes and returns every waiting task whose expiry is due by `now`,
    /// earliest first.
    pub(super) fn take_expired(&mut self, now: Instant) -> Vec<TaskId> {
        let mut expired = Vec::new();
        while let Some((due, _)) = self.expiries.first() {
            if *due > now {
                break;
            }
            let (_, task) = self.expiries.pop_first().expect("just seen");
            self.leave(&task);
            expired.push(task);
        }
        expired
    }

    /// Moves every scheduled task due by `now` to the back of the queue,
    /// earliest due first (ties in the order they started waiting), and
    /// returns them.
    pub(super) fn release_due(&mut self, now: Instant) -> Vec<TaskId> {
        let mut released = Vec::new();
        while let Some(((due, _), _)) = self.scheduled.first_key_value() {
            if *due > now {
                break;
            }
            let (_, task) = self.scheduled.pop_first().expect("just seen");
            let ticket = self.take_ticket();
            self.queue.insert(ticket, task.clone());
            self.places
                .get_mut(&task)
                .expect("a scheduled task has a place")
                .spot = Spot::Queued(ticket);
            released.push(task);
        }
        released
    }

    /// Queued tasks after queue position `after`, oldest first, with their
    /// positions.
    pub(super) fn queued_after(
        &self,
        after: Option<u64>,
    ) -> impl Iterator<Item = (u64, &TaskId)> {
        let rest = match after {
            Some(position) => self.queue.range(position + 1..),
            None => self.queue.range(..),
        };
        rest.map(|(position, task)| (*position, task))
    }

    /// The earliest delay or expiry.
    pub(super) fn next_deadline(&self) -> Option<Instant> {
        let delay = self.scheduled.first_key_value().map(|((due, _), _)| *due);
        let expiry = self.expiries.first().map(|(at, _)| *at);
        [delay, expiry].into_iter().flatten().min()
    }

    /// How many tasks are queued, for the notifications' `Counts::pending`.
    pub(super) fn queued_len(&self) -> usize {
        self.queue.len()
    }

    fn take_ticket(&mut self) -> u64 {
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        ticket
    }
}
