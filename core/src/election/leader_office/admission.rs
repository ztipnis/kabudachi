use std::collections::BTreeSet;

use crate::protocol::ids::WorkerId;
use crate::time::Instant;

use super::{Duties, LeaderOffice};

/// What the round of promises standing calls for.
enum NextAdmission {
    /// Every worker promised is waiting: start the batch that admits them.
    BeginBatch,
    /// The round stands: wait for its workers.
    Wait,
    /// No round stands, or it must be replaced: promise afresh.
    PromiseAgain,
}

/// The instant `ticks` before `now`, or the start of time.
fn ticks_before(now: Instant, ticks: u64) -> Instant {
    Instant::at(now.as_ticks().saturating_sub(ticks))
}

impl LeaderOffice {
    /// Commits the joint configuration this office leads once a majority of
    /// each side holds it (see [`super::Roster::commit_if_confirmed`]): it then
    /// leads the new side alone, with each member's admission generation
    /// there, its own included, and no prior admission generation any more.
    /// Whether it committed.
    pub(super) fn commit_when_due(&mut self, me: &WorkerId) -> bool {
        self.apply_pending_removals();
        let committed = self.roster.commit_if_confirmed(me, self.term);
        self.note_voters();
        committed
    }

    /// [`Self::commit_when_due`], then the admission of joiners that waited
    /// out the change, if it committed.
    pub(super) fn commit_if_confirmed(&mut self, duties: &Duties) {
        if self.commit_when_due(duties.me) {
            self.admit_waiting_joiners(duties);
        }
    }

    /// Advances the admission of workers waiting to join, which takes two
    /// phases: the leader first promises each worker that has confirmed one of
    /// its acks recently enough to leave it a lease worth having (see
    /// [`super::Lease::admissible`]; sent within the last two heartbeat
    /// intervals, or no earlier than the lease's quorum-contact time) its
    /// admission at a generation of its term (see
    /// [`super::Roster::promise_admission`]); and once every one has said it
    /// holds the promise, starts the batch (see [`super::Roster::begin_batch`])
    /// that admits exactly those workers there. So each joiner holds its
    /// admission, which a quorum counts at the batch's generation, before the
    /// batch exists, and a leader lost before any joiner heard of the batch
    /// leaves survivors that can still count them. A round with a worker that
    /// has yet to confirm its promise and has not confirmed an ack for a
    /// suspicion timeout, or whose workers are no longer all admissible when
    /// every one holds its promise, is replaced by a new round, since a batch
    /// must admit exactly those promised. A worker that drained after its last
    /// confirmation, its SELF_REMOVE not yet here, may be promised: the same as
    /// one that is admitted and then drains, which the removal handles in turn.
    /// The leader itself, if its configuration does not count it, joins the
    /// batch too. Nothing starts while the configuration is joint: joiners wait
    /// for the commit, which calls this again.
    ///
    /// Nor does anything start until every member the configuration counts
    /// has said it holds it. A member that missed the acks after a commit
    /// and is left a generation behind by the next batch is refused by those
    /// that hold the newer one, and if the leader is then lost it may be
    /// needed for a quorum nobody can reach. A member that goes silent
    /// therefore blocks admissions until it is replaced by other means. One
    /// whose heartbeats keep arriving but which confirms none of this
    /// leader's acks blocks them only until a suspicion timeout and a
    /// reconnect timeout have passed since it last confirmed one: the leader
    /// then removes it, one voter per configuration change, once every other
    /// voter holds the committed configuration (see
    /// [`Self::remove_one_unconfirming`]). It may rejoin as a pending joiner
    /// once its link heals.
    pub(super) fn admit_waiting_joiners(&mut self, duties: &Duties) {
        self.advance_admission(duties);
        self.note_voters();
    }

    /// The body of [`Self::admit_waiting_joiners`].
    fn advance_admission(&mut self, duties: &Duties) {
        self.apply_pending_removals();
        if self.remove_one_unconfirming(duties) {
            return;
        }
        if self.roster.configuration().is_joint() || !self.roster.is_held_by_every_voter(duties.me)
        {
            return;
        }
        let waiting = self.admissible_joiners(duties);
        let leader = self.roster.is_admissible(duties.me).then_some(duties.me);
        match self.next_admission(&waiting, duties) {
            NextAdmission::BeginBatch => {
                self.roster.begin_batch(leader, self.term);
            }
            NextAdmission::Wait => {}
            NextAdmission::PromiseAgain => {
                if !self.roster.promise_admission(&waiting, self.term) {
                    self.roster.begin_batch(leader, self.term);
                }
            }
        }
    }

    /// The workers waiting to join that have confirmed an ack recently enough
    /// to leave the leader a lease worth having.
    fn admissible_joiners(&self, duties: &Duties) -> BTreeSet<WorkerId> {
        // A joiner heartbeats every heartbeat interval, echoing the ack that
        // answered its previous heartbeat: two intervals cover that ack's
        // age, and network delays within one.
        let recent_since =
            ticks_before(duties.now, 2 * duties.timings.heartbeat_interval.as_ticks());
        let roster = &self.roster;
        duties
            .lease
            .admissible(
                roster
                    .pending()
                    .iter()
                    .chain(roster.members().keys())
                    .filter(|worker| roster.is_admissible(worker)),
                duties.me,
                roster,
                duties.timings,
                recent_since,
            )
            .into_iter()
            .cloned()
            .collect()
    }

    /// What the standing round of promises calls for, with `waiting` the
    /// workers admissible now.
    fn next_admission(&self, waiting: &BTreeSet<WorkerId>, duties: &Duties) -> NextAdmission {
        let roster = &self.roster;
        // A round stands while every promised worker that has yet to confirm
        // its promise still confirms the leader's acks, if not recently
        // enough for a batch then within a suspicion timeout: a worker that
        // heartbeats on but confirms nothing new would hold every later
        // joiner back for good, so a new round replaces the round. Hearing a
        // heartbeat is no such confirmation, which an echo of an old ack
        // gives as well as a fresh one.
        let promised: Vec<(&WorkerId, bool)> = roster.promised_workers().collect();
        let confirming_lately: BTreeSet<&WorkerId> = duties
            .lease
            .admissible(
                promised.iter().map(|(worker, _)| *worker),
                duties.me,
                roster,
                duties.timings,
                ticks_before(duties.now, duties.timings.suspect_timeout.as_ticks()),
            )
            .into_iter()
            .collect();
        if promised.is_empty() {
            return NextAdmission::PromiseAgain;
        }
        let confirming =
            |worker: &WorkerId| waiting.contains(worker) || confirming_lately.contains(worker);
        if roster.is_every_promise_held() {
            // The batch's new side must not cost the leader its lease, so
            // each worker it takes, one that confirmed its promise before
            // included, must still be one a lease is worth leaving it
            // with. A promised worker not waiting at this moment but
            // still confirming is between two confirmations: the next
            // heartbeat brings it back, whereas a new round would make
            // every joiner confirm again.
            if promised.iter().all(|(worker, _)| waiting.contains(*worker)) {
                return NextAdmission::BeginBatch;
            }
            if promised.iter().all(|(worker, _)| confirming(worker)) {
                return NextAdmission::Wait;
            }
        } else if promised
            .iter()
            .all(|(worker, held)| *held || confirming(worker))
        {
            return NextAdmission::Wait;
        }
        NextAdmission::PromiseAgain
    }

    /// Takes one queued unconfirming member out of the configuration if it
    /// may (see [`super::RemovalGuard::may_remove_one`]). One voter per
    /// change, so each moves from n voters to n - 1 and the majorities of the
    /// two share a voter. Whether it removed one: the shrunk generation is
    /// then unheld, which holds the next removal and every admission back
    /// until the remaining voters echo it.
    fn remove_one_unconfirming(&mut self, duties: &Duties) -> bool {
        self.removal.drop_settled(&self.roster, duties.me);
        if !self
            .removal
            .may_remove_one(&self.roster, self.liveness.unconfirming(), duties.me)
        {
            return false;
        }
        while let Some(worker) = self.liveness.pop_unconfirming() {
            if self.roster.members().contains_key(&worker) {
                let last_echoed = self.roster.held_generation_of(&worker);
                self.roster.remove_lost(&worker, self.term);
                self.removal.record(worker, last_echoed);
                self.forget_non_members();
                self.note_voters();
                return true;
            }
        }
        false
    }
}
