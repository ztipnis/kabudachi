use crate::configuration::Roster;
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::checked::Checked;
use crate::protocol::ids::{TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{LeaderHeartbeatAck, WorkerHeartbeat, election_message};
use crate::protocol::worker_state::WorkerState;
use crate::queues::Subscription;
use crate::reconcile::ReconcileTerm;
use crate::time::{Clock, Instant};

use super::super::EpochOrder;
use super::super::leader_office::{AckContent, Duties, Heard, LeaderOffice, MemberReport};
use super::super::{Output, WorkerNode};

/// What `heartbeat` says of its sender beyond its liveness.
fn member_report(heartbeat: &Checked<WorkerHeartbeat>) -> MemberReport {
    MemberReport {
        routing_crawled: heartbeat.routing_crawled,
        admission: heartbeat.admission_generation(),
        runs_compaction: heartbeat.runs_compaction,
        available_capacity: heartbeat.available_capacity,
        queues: Subscription::from_names(&heartbeat.queues),
    }
}

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Answers a follower's heartbeat with one ack to its sender, and records
    /// which of this leader's acks the heartbeat confirms. Only a `Leader`
    /// answers, and only a heartbeat from its own shard at its own recovery
    /// epoch or an earlier one: a worker left on an earlier epoch adopts this
    /// leader's from the ack, but what it echoes confirms nothing here. Any
    /// heartbeat also tells the leader its sender is alive (see
    /// [`Self::report_lost_workers`]). A sender its roster does not hold is
    /// added as a pending joiner, so its acks name it pending, until the leader
    /// promises it admission and then a batch admits it (see
    /// [`LeaderOffice::take_heartbeat`]); the ack answering this heartbeat
    /// already carries either. Every sender gets an ack, though only members'
    /// confirmations count towards the lease, except one whose heartbeat names
    /// a later term of this leader's epoch: this leader steps down instead, and
    /// neither acks it nor records it as heard.
    ///
    /// A confirmation counts only for an ack of this leader's own term, sent
    /// no later than now: anything else names no ack this leader has sent in
    /// this term. A heartbeat that confirms one also says which
    /// configuration its sender holds, which counts toward committing a
    /// joint configuration this leader leads when it is exactly that one
    /// (see [`Roster::commit_if_confirmed`]). Granting a vote in a later term
    /// clears neither `leader` nor the newest accepted ack, so a worker that
    /// went on to vote there can still echo this leader's term-matching ack
    /// and help commit its configuration; safety rests on the exact-
    /// generation rule, not on that worker's vote. The term fence above only
    /// keeps echoes of other leaderships' acks from counting here.
    pub(super) fn on_heartbeat(&mut self, from: WorkerId, heartbeat: &Checked<WorkerHeartbeat>) {
        let Some(same_epoch) = self.heartbeat_epoch(heartbeat) else {
            return;
        };
        // A heartbeat naming a later term than this leader's: its sender
        // voted, or heard of a vote, in that term. Some roll call of that term found
        // a returning quorum of stale voters, so this leader is all but
        // deposed, and the sender, whose floor is above this term, can never
        // follow it. A heartbeat from an earlier epoch names a term of
        // another count, and deposes no one.
        if same_epoch && heartbeat.term_seen > self.term {
            self.standing.saw_term(heartbeat.term_seen);
            self.step_down_if_outpaced();
            return;
        }

        let now = self.clock.now();
        let heard = self.take_confirmation(&from, heartbeat, same_epoch, now);
        if let Some(office) = self.office.as_mut() {
            let duties = Duties {
                me: &self.my_id,
                lease: &self.lease,
                timings: &self.timings,
                now,
            };
            office.take_heartbeat(from.clone(), heard, member_report(heartbeat), &duties);
        }
        self.drain_once_free();
        if !self.holds_office() {
            return;
        }
        self.answer_heartbeat(from, heartbeat, now);
    }

    /// Whether this node, in office, takes `heartbeat`: one from its own
    /// shard, at its own recovery epoch or an earlier one. `Some(true)` at its
    /// own epoch.
    fn heartbeat_epoch(&self, heartbeat: &Checked<WorkerHeartbeat>) -> Option<bool> {
        let seen = RecoveryEpoch::new(
            heartbeat.recovery_epoch_seen,
            heartbeat.recovery_epoch_lineage,
        );
        let epoch = self.standing.order(seen);
        if !self.holds_office() || heartbeat.shard_id() != self.shard_id {
            return None;
        }
        match epoch? {
            EpochOrder::Later => None,
            order => Some(order == EpochOrder::Mine),
        }
    }

    /// What `heartbeat` from `from` tells this leader at `now`: a confirmation
    /// of one of its acks of its own term, sent no later than now, which it
    /// records with its lease, when `same_epoch`; that its sender is alive
    /// otherwise.
    fn take_confirmation(
        &mut self,
        from: &WorkerId,
        heartbeat: &Checked<WorkerHeartbeat>,
        same_epoch: bool,
        now: Instant,
    ) -> Heard {
        if same_epoch
            && let Some(echo) = heartbeat.newest_accepted_ack
            && echo.term == self.term
            && echo.send_token <= now.as_ticks()
        {
            self.lease
                .confirm(from.clone(), Instant::at(echo.send_token));
            return Heard::Confirmed {
                sent_at: Instant::at(echo.send_token),
                held: heartbeat.configuration_generation(),
            };
        }
        Heard::Alive
    }

    /// Reports the runs `from` holds and acks its heartbeat, vouching for it
    /// while this leader holds a grant.
    fn answer_heartbeat(
        &mut self,
        from: WorkerId,
        heartbeat: &Checked<WorkerHeartbeat>,
        now: Instant,
    ) {
        self.outputs.push(Output::RunsHeard {
            worker: from.clone(),
            digest: heartbeat.active_task_runs_digest.clone(),
        });
        // Only a leader that holds a grant vouches for when it heard the
        // sender: no rival can win until that grant ends (see
        // `Output::AbortDeadline`). The office applied removals pending, as
        // they can end the grant.
        let heartbeat_token = self
            .lease
            .holds_grant_at(self.office().as_ref(), &self.timings, now)
            .then_some(heartbeat.send_token);
        self.send_ack(from, heartbeat_token);
    }

    /// Sends `to` an ack naming this leader, its term, recovery epoch and
    /// configuration, and `to`'s admission generation in its roster (none
    /// for a pending joiner or a worker the roster does not hold), with the
    /// instant it is sent as the token a heartbeat echoes back, and, when it
    /// answers a heartbeat, that heartbeat's own token echoed. Removals
    /// pending take effect first (see [`LeaderOffice::ack_for`]); a
    /// caller that vouches with `heartbeat_token` applies them before it
    /// decides whether it still holds a grant.
    pub(super) fn send_ack(&mut self, to: WorkerId, heartbeat_token: Option<u64>) {
        let Some(office) = self.office.as_mut() else {
            return;
        };
        let content = office.ack_for(&to);
        self.send_ack_with(to, content, heartbeat_token);
    }

    /// Sends `to` an ack carrying `content` from this leader.
    pub(super) fn send_ack_with(
        &mut self,
        to: WorkerId,
        content: AckContent,
        heartbeat_token: Option<u64>,
    ) {
        let cancelled_runs = self
            .cancelled_runs
            .next_batch_for(&to)
            .into_iter()
            .map(Into::into)
            .collect();
        let ack = LeaderHeartbeatAck {
            shard_id: Some(self.shard_id.clone().into()),
            leader_id: Some(self.my_id.clone().into()),
            recovery_epoch: self.standing.epoch_number(),
            term: self.term,
            configuration: Some((&content.configuration).into()),
            recipient_admission: content.recipient_admission.map(Into::into),
            recipient_prior_admission: content.recipient_prior_admission.map(Into::into),
            send_token: self.clock.now().as_ticks(),
            heartbeat_token,
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
            cancelled_runs,
        };
        self.send(to, election_message::Payload::HeartbeatAck(ack));
    }

    /// Acks every connected peer once, unasked, so each learns that this
    /// node now leads. A follower heartbeats only a leader it knows of, and
    /// the election certificate does not name it one: it reaches only the
    /// roll call's respondents, and tells them what they founded. A peer
    /// outside this leader's roster, a voter that missed the winning roll
    /// call say, is acked too: its ack names no admission generation, so it
    /// keeps its own, older than the founded configuration's base, and is
    /// no voter of the new side (it can still be a voter of the old side,
    /// under the founded joint configuration, until the commit). A worker
    /// this misses hears from the leader once a
    /// connection to it opens (see [`Self::on_peer_connected`]).
    fn announce_leadership(&mut self) {
        let Some(office) = self.office.as_mut() else {
            return;
        };
        for (peer, content) in office.announce(&self.connected) {
            self.send_ack_with(peer, content, None);
        }
    }

    /// Takes office as leader of `roster` in this node's current term, in
    /// `LeaderReconciling`: holds its configuration and its own admission
    /// generations there, commits it at once if it alone is a majority of
    /// each side, starts its quorum-contact lease and its watch over the
    /// workers it leads, records itself as its own leader in place of any it
    /// followed before, asks its driver to reconcile (see
    /// [`Output::Reconcile`]), and announces itself to every connected peer
    /// (see [`Self::announce_leadership`]), unless a drain kept from before
    /// stops it first. With an authority it needs a fence to act; one it
    /// already holds is kept. Every election duty runs from here; only the
    /// scheduler's grant waits for [`Input::Reconciled`](crate::election::Input::Reconciled).
    pub(super) fn take_office(&mut self, roster: Roster) {
        let now = self.clock.now();
        // A leader never acks itself, so nothing else raises its own
        // `highest_term_seen` to the term it won.
        self.standing.saw_term(self.term);
        let duties = Duties {
            me: &self.my_id,
            lease: &self.lease,
            timings: &self.timings,
            now,
        };
        self.office = Some(LeaderOffice::take(roster, self.term, &duties));

        self.lease.won(now);
        if let Some(authority) = self.authority.as_mut() {
            authority.took_office(self.standing.epoch(), now);
        }
        // The leader it followed before is replaced. Should this node lose
        // its quorum and go back to electing, it must not heartbeat that
        // leader again.
        self.leader = Some((self.my_id.clone(), self.term));
        self.newest_accepted_ack = None;
        self.next_heartbeat = None;
        self.transition_to(WorkerState::LeaderReconciling);
        // A drain kept from before may have drained the node already.
        if self.state == WorkerState::LeaderReconciling {
            if let Some(office) = self.office_term() {
                self.outputs.push(Output::Reconcile(office));
            }
            self.announce_leadership();
        }
    }

    /// Its scheduler reconciled for `office`: the node leads.
    pub(super) fn on_reconciled(&mut self, office: ReconcileTerm) {
        if self.state == WorkerState::LeaderReconciling && self.office_term() == Some(office) {
            self.transition_to(WorkerState::Leader);
        }
    }

    /// While it holds office: `worker` holds `run`, which is stored as
    /// cancelled, by this leader or an earlier one. Every ack to `worker`
    /// lists it until [`Self::forget_cancelled`].
    pub fn tell_cancelled(&mut self, worker: WorkerId, run: TaskRunId) {
        if self.holds_office() {
            self.cancelled_runs.tell(worker, run);
        }
    }

    /// `worker`'s heartbeats show it holds no run this leader told it was
    /// cancelled: its acks stop listing them.
    pub fn forget_cancelled(&mut self, worker: &WorkerId) {
        self.cancelled_runs.forget(worker);
    }
}
