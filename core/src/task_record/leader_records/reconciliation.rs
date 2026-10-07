//! What a new leader decides from its shard's answers while it reconciles.

use crate::election::WorkerNode;
use crate::protocol::generated::TaskRecord;
use crate::protocol::ids::WorkerId;
use crate::reconcile::{Rebuild, ReconcileRound, ReconcileTerm, Republish};
use crate::task_record::{PlacedWrite, WriteOutcome};
use crate::time::{Clock, Duration, Instant};

use super::RecordPorts;

/// What the driver is to do next for a reconciliation.
pub enum Progress {
    /// Nothing yet.
    Waiting,
    /// The round may stop: rebuild the scheduler from this.
    Rebuild(Rebuild),
    /// The rebuilt scheduler's records are to be placed on the voters and
    /// written again: an earlier attempt could not place them all.
    Place(Vec<TaskRecord>),
    /// The voters changed while records are still to be stored: place them
    /// on the new voters (see [`OfficeReconciliation::re_place`]).
    RePlace,
    /// Every republished record is stored: the node may lead.
    Republished(ReconcileTerm),
    /// What was learnt since, for the leading scheduler to adopt.
    Learnt(Rebuild),
}

/// What a reconciliation could not finish, to be done again.
pub enum Stuck {
    /// The scheduler did not take the rebuild.
    Rebuild(Rebuild),
    /// These records could not all be placed on the voters.
    Place(Vec<TaskRecord>),
}

/// Work that failed and is tried again when the voters change and, failing
/// that, once per `every`.
struct Retry<T> {
    work: Option<T>,
    voters: Vec<WorkerId>,
    at: Instant,
    every: Duration,
}

impl<T> Retry<T> {
    fn new(every: Duration) -> Self {
        Retry {
            work: None,
            voters: Vec::new(),
            at: Instant::at(0),
            every,
        }
    }

    /// Keeps `work` to try again, given the voters it failed with.
    fn hold(&mut self, work: T, mut voters: Vec<WorkerId>, now: Instant) {
        voters.sort();
        self.work = Some(work);
        self.voters = voters;
        self.at = now + self.every;
    }

    /// The work to try again, if the voters changed or the time has come.
    fn take_if_due(&mut self, voters: &[WorkerId], now: Instant) -> Option<T> {
        let mut voters = voters.to_vec();
        voters.sort();
        if self.work.is_some() && (voters != self.voters || now >= self.at) {
            self.work.take()
        } else {
            None
        }
    }

    fn wake_at(&self) -> Option<Instant> {
        self.work.as_ref().map(|_| self.at)
    }
}

/// One office's reconciliation, as far as it decides: the round of answers,
/// when it may stop, the republish of the rebuilt records at the office's
/// term, work that failed and is tried again, and, once the node leads, what
/// late answers teach. It asks no one: its driver asks the shard and feeds
/// the round (see [`Self::round_mut`]), and says when something new arrived
/// ([`Self::heard`]).
pub struct OfficeReconciliation {
    round: ReconcileRound,
    republish: Option<Republish>,
    /// The voters, sorted, the republish was last placed on.
    republish_voters: Vec<WorkerId>,
    rebuilt: bool,
    /// The rebuild or placement that failed, tried again.
    stuck: Retry<Stuck>,
    /// When `progress` last ran: what falls due after it is woken for.
    progressed_at: Instant,
    led: bool,
    /// Whether something arrived that the round has not yet handed over.
    news: bool,
}

impl OfficeReconciliation {
    /// Starts reconciling `office` with `reconcilees` to ask, at `now`. It may
    /// stop early only once `grace` (one suspicion timeout) has passed, and
    /// tries stuck work again after `grace`.
    pub fn new(
        office: ReconcileTerm,
        reconcilees: impl IntoIterator<Item = WorkerId>,
        now: Instant,
        grace: Duration,
    ) -> Self {
        OfficeReconciliation {
            round: ReconcileRound::new(office, reconcilees, now, grace),
            republish: None,
            republish_voters: Vec::new(),
            rebuilt: false,
            stuck: Retry::new(grace),
            progressed_at: now,
            led: false,
            news: false,
        }
    }

    /// The office this reconciles.
    pub fn office(&self) -> ReconcileTerm {
        self.round.term()
    }

    /// The round of answers.
    pub fn round(&self) -> &ReconcileRound {
        &self.round
    }

    /// The round of answers, for the driver to feed what it was told.
    pub fn round_mut(&mut self) -> &mut ReconcileRound {
        &mut self.round
    }

    /// Something arrived that the round has not handed over yet.
    pub fn heard(&mut self) {
        self.news = true;
    }

    /// Whether every republished record was stored and the node told it may
    /// lead.
    pub fn leads(&self) -> bool {
        self.led
    }

    /// When [`Self::progress`] last ran: a wake at or before it is not due.
    pub fn progressed_at(&self) -> Instant {
        self.progressed_at
    }

    /// What to do now. `leading` is whether the scheduler leads;
    /// `lookups_done` whether no record lookup is in flight or owed. Writes
    /// the republished records that fell due through `ports`. What late
    /// answers teach is handed over only while the scheduler leads, and is
    /// kept until then.
    pub fn progress<C: Clock>(
        &mut self,
        node: &WorkerNode<C>,
        leading: bool,
        lookups_done: bool,
        now: Instant,
        ports: &mut impl RecordPorts,
    ) -> Progress {
        self.progressed_at = now;

        // A republish that cannot reach a quorum is not given up: this leader
        // keeps office, writes each record again, and places the writes anew
        // whenever the voters change. That is accepted because a node holds
        // office only while its lease holds, and holding the lease means a
        // quorum of voters has been heard from lately. When the lease ends the
        // node leaves office (`NoQuorum`), the driver drops this
        // reconciliation. Writes that fell due before the node stepped may
        // still go out; a holder refuses them against a newer term.
        if let Some(republish) = self.republish.as_mut() {
            let due = republish.due(now);
            if !due.is_empty() {
                ports.write(due);
            }
            if republish.is_done() && !self.led {
                self.led = true;
                return Progress::Republished(self.round.term());
            }
            let mut voters = node.placeable_voters();
            voters.sort();
            if !republish.is_done() && voters != self.republish_voters {
                self.republish_voters = voters;
                return Progress::RePlace;
            }
        }
        if let Some(stuck) = self.stuck.take_if_due(&node.placeable_voters(), now) {
            return match stuck {
                Stuck::Rebuild(rebuild) => Progress::Rebuild(rebuild),
                Stuck::Place(records) => Progress::Place(records),
            };
        }
        if !self.rebuilt {
            let answered = node.voters_answered(&self.round.answered());
            if self.round.may_finish(answered, now) && lookups_done {
                self.rebuilt = true;
                self.news = false;
                return Progress::Rebuild(self.round.take_settled(|worker| node.is_member(worker)));
            }
            return Progress::Waiting;
        }
        if self.led && self.news && leading {
            self.news = false;
            let learnt = self.round.take_settled(|worker| node.is_member(worker));
            if !learnt.records.is_empty() || !learnt.reports.is_empty() {
                return Progress::Learnt(learnt);
            }
        }
        Progress::Waiting
    }

    /// Takes back what the scheduler could not adopt because it did not lead:
    /// it is offered again, with what is learnt meanwhile, once it does.
    pub fn give_back(&mut self, learnt: Rebuild) {
        self.round.give_back(learnt);
        self.news = true;
    }

    /// Keeps what could not be finished, to be done again when the voters
    /// change or after a suspicion timeout.
    pub fn stuck(&mut self, work: Stuck, voters: Vec<WorkerId>, now: Instant) {
        self.stuck.hold(work, voters, now);
    }

    /// The rebuild ran: write these republished records, each again after
    /// `retry_after` if it was not stored.
    pub fn republishing(
        &mut self,
        writes: Vec<PlacedWrite>,
        retry_after: Duration,
        mut voters: Vec<WorkerId>,
    ) {
        voters.sort();
        self.republish_voters = voters;
        self.republish = Some(Republish::new(writes, retry_after));
    }

    /// The voters changed: places every republished write not yet stored on
    /// them, and writes each refused one again now.
    pub fn re_place(&mut self, replace: impl FnMut(&mut PlacedWrite), now: Instant) {
        if let Some(republish) = self.republish.as_mut() {
            republish.re_place(replace, now);
        }
    }

    /// Takes a write outcome that belongs to the republish; whether it did.
    pub fn settle(&mut self, outcome: &WriteOutcome, now: Instant) -> bool {
        self.republish
            .as_mut()
            .is_some_and(|republish| republish.settled(outcome, now))
    }

    /// The end of the grace, a republished write or stuck work to try again;
    /// only what falls due after [`Self::progressed_at`]. What fell due since
    /// `progress` last ran is due now, so no wake is lost to the time
    /// `progress` took; `progress` moves its own time on, so this does not
    /// spin.
    pub fn wake_at(&self) -> Option<Instant> {
        let grace = (!self.rebuilt).then(|| self.round.grace_ends_at());
        [
            grace,
            self.republish.as_ref().and_then(Republish::wake_at),
            self.stuck.wake_at(),
        ]
        .into_iter()
        .flatten()
        .filter(|at| *at > self.progressed_at)
        .min()
    }
}
