use std::collections::{BTreeMap, BTreeSet};

use crate::configuration::{Generation, Roster};
use crate::protocol::ids::WorkerId;
use crate::time::{Duration, Instant};

use super::{Duties, Heard, LeaderOffice};

/// When the leader last heard from, and last had an ack confirmed by, each
/// worker it leads, and which of them it has reported silent or lost.
#[derive(Default)]
pub(super) struct Liveness {
    /// When it last heard from each worker it has not yet reported lost.
    last_heard: BTreeMap<WorkerId, Instant>,
    /// When each counted member last confirmed a recent ack of this leader;
    /// a member not in it counts from when it was first heard.
    confirmed_at: BTreeMap<WorkerId, Instant>,
    /// The counted members reported lost for confirming no ack while their
    /// heartbeats kept arriving, to take out one at a time (see
    /// [`super::LeaderOffice::remove_one_unconfirming`]).
    unconfirming: BTreeSet<WorkerId>,
    /// The workers reported lost and not heard since: one that confirmed no
    /// ack stays here until it confirms one, its heartbeats notwithstanding.
    lost: BTreeSet<WorkerId>,
    /// The workers reported silent and not heard since, each with the
    /// instant from which the reconnect timeouts of the runs it holds count
    /// (see [`Self::silence_changes`]).
    reported_silence: BTreeMap<WorkerId, Instant>,
}

impl Liveness {
    /// Hears each of `workers` at `now`.
    pub(super) fn hearing(workers: impl Iterator<Item = WorkerId>, now: Instant) -> Self {
        Liveness {
            last_heard: workers.map(|worker| (worker, now)).collect(),
            ..Liveness::default()
        }
    }

    /// A heartbeat from `from`, a member the configuration counts when
    /// `is_member`, told the leader `heard` at `duties.now`.
    pub(super) fn heard(
        &mut self,
        from: &WorkerId,
        heard: &Heard,
        is_member: bool,
        duties: &Duties,
    ) {
        self.last_heard.insert(from.clone(), duties.now);
        // A counted member confirms when its heartbeat echoes an ack sent
        // within a suspicion timeout, as `confirming_lately` reads a promised
        // joiner's. One that keeps heartbeating but confirms none, a one-way
        // link say, is removed once it has gone a loss timeout without (see
        // [`Self::lost_by`]).
        if is_member {
            self.confirmed_at.entry(from.clone()).or_insert(duties.now);
            if let Heard::Confirmed { sent_at, .. } = heard
                && sent_at.as_ticks() + duties.timings.suspect_timeout.as_ticks()
                    >= duties.now.as_ticks()
            {
                self.confirmed_at.insert(from.clone(), duties.now);
                self.unconfirming.remove(from);
            }
        }
        if !self.unconfirming.contains(from) {
            self.lost.remove(from);
        }
    }

    /// Starts counting from `now` toward reporting each of `workers` but `me`
    /// lost, unless it already counts one: a worker heard from keeps its own
    /// time.
    pub(super) fn watch(&mut self, workers: BTreeSet<WorkerId>, me: &WorkerId, now: Instant) {
        for worker in workers.into_iter().filter(|worker| worker != me) {
            self.last_heard.entry(worker).or_insert(now);
        }
    }

    /// `workers` answered this office's rebuild at `now`: each is heard, as
    /// by a heartbeat, so it counts as silent again only from now, and a
    /// silence reported for it is forgotten. A counted member that still
    /// confirms no ack stays lost, and is silent again a suspicion timeout
    /// after the first heartbeat heard from it since it was reported lost,
    /// or after this answer if none has been heard since (see
    /// [`Self::silent_from`]); it is reported silent at once if that has
    /// passed.
    pub(super) fn heard_answers(
        &mut self,
        workers: BTreeSet<WorkerId>,
        me: &WorkerId,
        now: Instant,
    ) {
        for worker in workers.into_iter().filter(|worker| worker != me) {
            self.last_heard.insert(worker.clone(), now);
            if !self.unconfirming.contains(&worker) {
                self.lost.remove(&worker);
            }
            self.reported_silence.remove(&worker);
        }
    }

    /// When the office next has a worker to report lost: `lost_after`
    /// after it last heard from each, or, for a counted member whose
    /// heartbeats still arrive (heard within `still_heard` of `now`), after
    /// it last confirmed an ack if that comes first.
    pub(super) fn next_lost_at(
        &self,
        roster: &Roster,
        now: Instant,
        lost_after: Duration,
        still_heard: Duration,
    ) -> Option<Instant> {
        self.last_heard
            .iter()
            .map(|(worker, heard)| {
                let silent = *heard + lost_after;
                match self.confirmed_at.get(worker) {
                    Some(confirmed)
                        if roster.members().contains_key(worker) && now < *heard + still_heard =>
                    {
                        silent.min(*confirmed + lost_after)
                    }
                    _ => silent,
                }
            })
            .min()
    }

    /// When `worker`'s runs' reconnect timeouts start to count: a suspicion
    /// timeout after it was last heard or, for a counted member whose
    /// heartbeats still arrive (one heard within `suspect_timeout` of `now`)
    /// but which confirms no ack, after it last confirmed one, if that is
    /// earlier, as [`Self::lost_by`] counts. `None` for a worker not tracked.
    fn silent_from(
        &self,
        roster: &Roster,
        worker: &WorkerId,
        now: Instant,
        suspect_timeout: Duration,
    ) -> Option<Instant> {
        let heard = *self.last_heard.get(worker)?;
        let since = match self.confirmed_at.get(worker) {
            Some(confirmed)
                if roster.members().contains_key(worker) && now < heard + suspect_timeout =>
            {
                heard.min(*confirmed)
            }
            _ => heard,
        };
        Some(since + suspect_timeout)
    }

    /// The silences to report at `now`, each once: a worker newly silent,
    /// with the instant its runs' reconnect timeouts count from (see
    /// [`Self::silent_from`]), and `None` for one reported silent and heard
    /// since. A worker reported lost and not heard since keeps its silence,
    /// as does a lost member that still confirms no ack: nothing has ended
    /// either silence. Such a member whose answer to a rebuild ended its
    /// silence (see [`Self::heard_answers`]) is reported silent again. A
    /// silence once reported keeps its instant until it ends, so a replay it
    /// times never moves earlier.
    pub(super) fn silence_changes(
        &mut self,
        roster: &Roster,
        now: Instant,
        suspect_timeout: Duration,
    ) -> Vec<(WorkerId, Option<Instant>)> {
        let mut changes = Vec::new();
        for worker in self
            .last_heard
            .keys()
            .filter(|worker| self.may_fall_silent(worker))
        {
            let silent = self
                .silent_from(roster, worker, now, suspect_timeout)
                .filter(|from| *from <= now);
            // A lost member's heartbeats do not end its silence: only a
            // confirmation, which takes it out of `lost`, or an answer does.
            let ends = !self.lost.contains(worker);
            match (silent, self.reported_silence.contains_key(worker)) {
                (Some(from), false) => changes.push((worker.clone(), Some(from))),
                (None, true) if ends => changes.push((worker.clone(), None)),
                _ => {}
            }
        }
        for (worker, from) in &changes {
            match from {
                Some(from) => {
                    self.reported_silence.insert(worker.clone(), *from);
                }
                None => {
                    self.reported_silence.remove(worker);
                }
            }
        }
        changes
    }

    /// When the next worker not yet reported silent falls silent, while
    /// that is ahead of `now`.
    pub(super) fn next_silent_at(
        &self,
        roster: &Roster,
        now: Instant,
        suspect_timeout: Duration,
    ) -> Option<Instant> {
        self.last_heard
            .keys()
            .filter(|worker| {
                self.may_fall_silent(worker) && !self.reported_silence.contains_key(*worker)
            })
            .filter_map(|worker| self.silent_from(roster, worker, now, suspect_timeout))
            .filter(|from| *from > now)
            .min()
    }

    /// Whether `worker` can be reported silent: it is not lost, or it is a
    /// lost member still heard but confirming no ack, whose silence its
    /// answer to a rebuild can end while it stays lost.
    fn may_fall_silent(&self, worker: &WorkerId) -> bool {
        !self.lost.contains(worker) || self.unconfirming.contains(worker)
    }

    /// The workers not heard from for `lost_after` by `now`, each reported
    /// once. A counted member whose heartbeats still arrive (one heard
    /// within `still_heard`) but which confirmed no recent ack for
    /// `lost_after` is reported lost too, and queued for removal (see
    /// [`LeaderOffice::remove_one_unconfirming`]) so admissions stop waiting
    /// on it. A silent member is only reported.
    pub(super) fn lost_by(
        &mut self,
        roster: &Roster,
        now: Instant,
        lost_after: Duration,
        still_heard: Duration,
    ) -> Vec<WorkerId> {
        let mut lost: Vec<WorkerId> = self
            .last_heard
            .iter()
            .filter(|(_, heard)| now >= **heard + lost_after)
            .map(|(worker, _)| worker.clone())
            .collect();
        let queued_before = self.unconfirming.clone();
        let unconfirming: Vec<WorkerId> = self
            .last_heard
            .iter()
            .filter(|(worker, heard)| {
                roster.members().contains_key(*worker)
                    && now < **heard + still_heard
                    && self
                        .confirmed_at
                        .get(*worker)
                        .is_some_and(|confirmed| now >= *confirmed + lost_after)
            })
            .map(|(worker, _)| worker.clone())
            .collect();
        self.unconfirming.extend(unconfirming.iter().cloned());
        lost.extend(unconfirming);
        for worker in &lost {
            self.last_heard.remove(worker);
            self.confirmed_at.remove(worker);
            self.lost.insert(worker.clone());
        }
        // A voter already reported for confirming no ack is reported again
        // only after it confirmed one in between, which unqueues it.
        lost.retain(|worker| !queued_before.contains(worker));
        lost
    }

    /// Whether `worker` was reported lost and not heard since.
    pub(super) fn is_lost(&self, worker: &WorkerId) -> bool {
        self.lost.contains(worker)
    }

    /// The counted members reported lost for confirming no ack, queued for
    /// removal.
    pub(super) fn unconfirming(&self) -> &BTreeSet<WorkerId> {
        &self.unconfirming
    }

    /// Takes the first queued unconfirming member off the queue.
    pub(super) fn pop_unconfirming(&mut self) -> Option<WorkerId> {
        self.unconfirming.pop_first()
    }

    /// Drops what is kept about workers that are no longer `members`.
    pub(super) fn forget_non_members(&mut self, members: &BTreeMap<WorkerId, Generation>) {
        self.confirmed_at
            .retain(|worker, _| members.contains_key(worker));
        self.unconfirming
            .retain(|worker| members.contains_key(worker));
        self.lost.retain(|worker| members.contains_key(worker));
    }
}

impl LeaderOffice {
    /// See [`Liveness::watch`].
    pub(crate) fn watch(&mut self, workers: BTreeSet<WorkerId>, me: &WorkerId, now: Instant) {
        self.liveness.watch(workers, me, now);
    }

    /// See [`Liveness::heard_answers`].
    pub(crate) fn heard_answers(
        &mut self,
        workers: BTreeSet<WorkerId>,
        me: &WorkerId,
        now: Instant,
    ) {
        self.liveness.heard_answers(workers, me, now);
    }

    /// See [`Liveness::next_lost_at`].
    pub(crate) fn next_lost_at(
        &self,
        now: Instant,
        lost_after: Duration,
        still_heard: Duration,
    ) -> Option<Instant> {
        self.liveness
            .next_lost_at(&self.roster, now, lost_after, still_heard)
    }

    /// See [`Liveness::silence_changes`].
    pub(crate) fn silence_changes(
        &mut self,
        now: Instant,
        suspect_timeout: Duration,
    ) -> Vec<(WorkerId, Option<Instant>)> {
        self.liveness
            .silence_changes(&self.roster, now, suspect_timeout)
    }

    /// See [`Liveness::next_silent_at`].
    pub(crate) fn next_silent_at(
        &self,
        now: Instant,
        suspect_timeout: Duration,
    ) -> Option<Instant> {
        self.liveness
            .next_silent_at(&self.roster, now, suspect_timeout)
    }

    /// See [`Liveness::lost_by`].
    pub(crate) fn lost_by(
        &mut self,
        now: Instant,
        lost_after: Duration,
        still_heard: Duration,
    ) -> Vec<WorkerId> {
        self.liveness
            .lost_by(&self.roster, now, lost_after, still_heard)
    }
}
