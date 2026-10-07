//! A node's standing with its coordination authority: when it renews its registration, when it must
//! fence itself for having failed to, and, while it leads, when it renews
//! its recovery fence and until when that fence lets it act.
//!
//! Everything is measured on the node's own monotonic clock, from the
//! instant it asked for a registration or a fence: the authority starts the
//! TTL no earlier than that, so a registration the node counts as lapsing
//! at `asked + TTL` lapses no earlier at the authority. A drift share is
//! taken off so the node gives up first even if its clock runs a little
//! slower than the authority's.
//!
//! A node with no authority has no lease at all: it never
//! registers, never fences itself and never needs a fence to lead.

use crate::coordination_authority::RecoveryEpoch;
use crate::election::authority::{AuthorityTimings, less_drift};
use crate::election::standing::{EpochOrder, order};
use crate::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub(crate) struct AuthorityLease {
    ttl: Duration,
    /// The node's `ElectionTimings::clock_drift_divisor`.
    drift_divisor: u64,
    /// Until when the node counts itself registered: its orphan deadline.
    registered_until: Instant,
    next_registration_at: Instant,
    /// While the node leads: its recovery fence.
    fence: Option<Fence>,
}

#[derive(Debug, Clone, Copy)]
struct Fence {
    /// Until when the fence lets the node act; `None` until first acquired.
    valid_until: Option<Instant>,
    /// The recovery epoch of the leader's office: the epoch the node led from
    /// the moment it took office, before any fence was granted, and the epoch
    /// of any grant since (the authority grants a fence only at the epoch it
    /// holds). It outlasts the grant's validity: a lapsed fence keeps it.
    /// Cleared by `need_fence`, which starts a fresh fence, and by
    /// `drop_fence`; `took_office` sets it again after either.
    office_epoch: Option<RecoveryEpoch>,
    next_attempt_at: Instant,
}

impl AuthorityLease {
    /// The lease of a node that starts at `now`. It counts `now` as a
    /// registration, so a node that never reaches its authority fences
    /// itself one TTL (less drift) later, as one that stopped renewing
    /// would, and asks to register at once.
    pub(crate) fn starting_at(timings: AuthorityTimings, drift_divisor: u64, now: Instant) -> Self {
        AuthorityLease {
            ttl: timings.ttl,
            drift_divisor,
            registered_until: now + less_drift(timings.ttl, drift_divisor),
            next_registration_at: now,
            fence: None,
        }
    }

    /// How long a call to the authority may go unanswered before the node
    /// counts it lost: the TTL it expects a registration to last, the time
    /// after which the node would have fenced itself had it stopped renewing.
    pub(crate) fn call_timeout(&self) -> Duration {
        self.ttl
    }

    /// How often the node renews its registration and its fence: a third of
    /// the TTL, so two renewals can fail before either lapses.
    pub(crate) fn renewal_interval(&self) -> Duration {
        Duration::from_ticks((self.ttl.as_ticks() / 3).max(1))
    }

    /// Until when a registration or fence asked for at `sent_at` and
    /// granted for `granted` lasts, as far as the node may count on it.
    fn lasts_until(&self, sent_at: Instant, granted: Duration) -> Instant {
        sent_at + less_drift(granted.min(self.ttl), self.drift_divisor)
    }

    pub(crate) fn registration_due(&self, now: Instant) -> bool {
        now >= self.next_registration_at
    }

    /// The node asked to register at `now`: the next renewal is an interval
    /// later, whether or not this one succeeds.
    pub(crate) fn registration_asked(&mut self, now: Instant) {
        self.next_registration_at = now + self.renewal_interval();
    }

    /// A registration asked for at `sent_at` succeeded for `granted`.
    pub(crate) fn registered(&mut self, sent_at: Instant, granted: Duration) {
        self.registered_until = self
            .registered_until
            .max(self.lasts_until(sent_at, granted));
    }

    pub(crate) fn is_registered(&self, now: Instant) -> bool {
        now < self.registered_until
    }

    /// Starts over at `now`, as at [`Self::starting_at`], keeping the TTL:
    /// for a node that has just joined its shard, or whose registration was
    /// asked for before it was built.
    pub(crate) fn restart_at(&mut self, now: Instant) {
        *self = AuthorityLease::starting_at(AuthorityTimings { ttl: self.ttl }, self.drift_divisor, now);
    }

    /// The node needs a fence from now on (it has won, or is waiting out
    /// the fence to lead): it holds none yet and asks at once.
    pub(crate) fn need_fence(&mut self, now: Instant) {
        self.fence = Some(Fence {
            valid_until: None,
            office_epoch: None,
            next_attempt_at: now,
        });
    }

    /// The node took office leading `epoch`: it needs a fence unless it holds
    /// a live one, and it leads `epoch` from now on, with or without a grant.
    pub(crate) fn took_office(&mut self, epoch: Option<RecoveryEpoch>, now: Instant) {
        if self.fence_valid_until().is_none() {
            self.need_fence(now);
        }
        if let Some(fence) = self.fence.as_mut() {
            fence.office_epoch = epoch;
        }
    }

    /// The node no longer leads or stands: it gives up its fence.
    pub(crate) fn drop_fence(&mut self) {
        self.fence = None;
    }

    pub(crate) fn fence_due(&self, now: Instant) -> bool {
        self.fence.is_some_and(|fence| now >= fence.next_attempt_at)
    }

    /// The node asked for its fence at `now`: the next attempt is a renewal
    /// interval later unless a reply says otherwise.
    pub(crate) fn fence_asked(&mut self, now: Instant) {
        let interval = self.renewal_interval();
        if let Some(fence) = self.fence.as_mut() {
            fence.next_attempt_at = now + interval;
        }
    }

    /// A fence at `epoch`, asked for at `sent_at`, was granted for `granted`.
    pub(crate) fn fence_acquired(
        &mut self,
        epoch: RecoveryEpoch,
        sent_at: Instant,
        granted: Duration,
    ) {
        let until = self.lasts_until(sent_at, granted);
        if let Some(fence) = self.fence.as_mut() {
            fence.office_epoch = Some(epoch);
            fence.valid_until = Some(fence.valid_until.map_or(until, |valid| valid.max(until)));
        }
    }

    /// Asks for the fence again at `at`, sooner than the renewal interval
    /// would: once another holder's fence has run out, or at once after
    /// republishing the epoch.
    pub(crate) fn retry_fence_at(&mut self, at: Instant) {
        if let Some(fence) = self.fence.as_mut() {
            fence.next_attempt_at = at;
        }
    }

    /// Until when the node's fence lets it act; `None` while it holds none.
    pub(crate) fn fence_valid_until(&self) -> Option<Instant> {
        self.fence.and_then(|fence| fence.valid_until)
    }

    /// The recovery epoch of the office the node took, held with or without a
    /// fence grant, valid or lapsed; `None` once it gave the fence up or
    /// before it took office.
    pub(crate) fn office_epoch(&self) -> Option<RecoveryEpoch> {
        self.fence.and_then(|fence| fence.office_epoch)
    }

    /// The earliest instant at which this lease wants something done: a
    /// registration or fence attempt, or, unless `fenced` already, fencing
    /// the node itself.
    pub(crate) fn next_deadline(&self, fenced: bool) -> Instant {
        let mut next = self.next_registration_at;
        if !fenced {
            next = next.min(self.registered_until);
        }
        if let Some(fence) = self.fence {
            next = next.min(fence.next_attempt_at);
        }
        next
    }
}

/// What a fenced node does once it can reach its authority again and has
/// read the shard's recovery epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reconnect {
    /// The epoch is the node's own, lineage included, or a later one of the
    /// node's own lineage: the outage hit it alone, or every worker alike,
    /// and nothing replaced it; a recovery that kept the lineage and raised
    /// the number; or a swap whose reply was lost moved the authority on,
    /// with or without a live leader at the later epoch. It carries on as it
    /// was, and its next read of the epoch decides whether it stands beside a
    /// later one: beside one with a live leader, an ack or a refused census
    /// then moves the node.
    Resume,
    /// The epoch is another, and not a later one of the node's lineage: it
    /// differs in lineage, founded afresh after the authority lost the
    /// node's own (whatever the numbers, which a flush resets); or, lower in
    /// the same lineage, put back by a leader that republished it after a
    /// flush while the node had moved past it, which it can no longer
    /// recover from, so it follows that leader. It discards its state and
    /// joins again as a pending member, of that epoch or a later one.
    Rejoin(RecoveryEpoch),
    /// The epoch is missing: the authority lost data and nobody has
    /// republished it yet. The node stays fenced and asks again later.
    StayFenced,
}

impl Reconnect {
    /// `own_epoch` is `None` for a node that never learned its epoch's
    /// lineage, which cannot tell its own epoch from another and so never
    /// resumes.
    pub(crate) fn decide(
        own_epoch: Option<RecoveryEpoch>,
        authority_epoch: Option<RecoveryEpoch>,
    ) -> Self {
        match (own_epoch, authority_epoch) {
            (_, None) => Reconnect::StayFenced,
            // Never resumes: without a lineage it cannot tell its own epoch.
            (None, Some(epoch)) => Reconnect::Rejoin(epoch),
            (Some(own), Some(epoch)) => match order(&own, epoch.into()) {
                EpochOrder::Mine => Reconnect::Resume,
                EpochOrder::Later if own.lineage == epoch.lineage => Reconnect::Resume,
                _ => Reconnect::Rejoin(epoch),
            },
        }
    }
}
