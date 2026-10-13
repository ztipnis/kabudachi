use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::checked::Checked;
use crate::protocol::digest::Digest;
use crate::protocol::ids::{ShardId, TaskRunId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{AckEcho, LeaderHeartbeatAck, WorkerHeartbeat, election_message};
use crate::protocol::worker_state::WorkerState;
use crate::queues::Subscription;
use crate::time::{Clock, Instant};

use super::super::EpochOrder;
use super::super::drain::DrainRequest;
use super::super::authority_standing::AuthorityStanding;
use super::super::standing::AckChange;
use super::super::{Output, WorkerNode};

/// The recovery epoch an accepted ack comes from, and whether it is later
/// than this node's own.
struct AckedEpoch {
    heard: RecoveryEpoch,
    later: bool,
}

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Processes a leader heartbeat acknowledgement. Acks for another shard,
    /// an older recovery epoch, or a term of this node's own epoch below its
    /// floor (see [`Self::ack_floor`]) are ignored without touching any state.
    ///
    /// An ack from a later recovery epoch means the shard was recovered
    /// through the authority: the node adopts that epoch (see
    /// [`ShardStanding::accept_ack`](crate::election::standing::ShardStanding::accept_ack)) and steps down from any term it holds
    /// or contests, whatever the terms, which two epochs do not order. A
    /// pending joiner that a stale JOIN pointer left on the old epoch finds
    /// its way the same way. A leader of the authority's epoch (see
    /// [`Self::leads_its_office_epoch`]) ignores an ack of another
    /// lineage's epoch whatever its number, even after its fence lapsed. An
    /// accepted ack refreshes leader contact, records its leader as
    /// [`Self::known_leader`] and the ack itself for this node's heartbeats
    /// to echo, and returns a node in `LeaderSuspect`, `RollCall` or
    /// `NoQuorum` to `Active` because a leader is reachable again, whatever
    /// term its roll call contests. A `Candidate` or `Leader` of a term
    /// earlier than the ack's steps down to `Active` under the ack's leader;
    /// one of the ack's own term keeps it.
    ///
    /// An ack that says its leader is handing leadership over changes less:
    /// that leader is alive but leads no longer, so the ack refreshes no
    /// contact, returns no node seeking a leader to `Active` and makes the
    /// node forget no roll call; it records the leader, and the node no
    /// longer counts that leader as valid against a roll call.
    ///
    /// It also carries the leader's configuration and this node's admission
    /// generations in the leader's roster, which this node adopts as
    /// [`ShardStanding::accept_ack`](crate::election::standing::ShardStanding::accept_ack) says. An ack that names no admission
    /// generation (the leader holds this node as pending, or not at all)
    /// leaves this node's own as they were. A node that adopts a newer
    /// configuration heartbeats its leader at once, so its echo of it
    /// reaches the leader without waiting out a heartbeat interval. A node
    /// that accepts an ack, in whatever state, forgets the roll calls it
    /// answered or made above the ack's term, short of those it voted in (see
    /// `ElectionRound::follow_leader_of`): the live leader disproved the
    /// suspicion they rested on, and that includes a node still `Active` whose
    /// stale contact no tick had yet acted on.
    pub(super) fn on_leader_ack(&mut self, ack: &Checked<LeaderHeartbeatAck>) {
        let Some(AckedEpoch { heard, later }) = self.acked_epoch(ack) else {
            return;
        };
        let outpaced = self
            .term_in_play()
            .is_some_and(|term| later || ack.term > term);
        if outpaced {
            self.leave_office();
        }
        let change = self.standing.accept_ack(
            heard,
            ack.term,
            ack.configuration(),
            ack.recipient_admission(),
            ack.recipient_prior_admission(),
        );
        if change.epoch_moved {
            self.forget_election_state();
        }
        self.follow_ack(ack, change, outpaced);
    }

    /// The epoch of `ack` when this node accepts acks of it at all: none for
    /// another shard, an older epoch, another lineage's epoch while this node
    /// leads the authority's, or a term of its own epoch below its ack floor.
    fn acked_epoch(&self, ack: &Checked<LeaderHeartbeatAck>) -> Option<AckedEpoch> {
        // A node back in `Bootstrapping` rejoins through JOIN alone: an ack
        // from a leader of the epoch it left would take it back past the
        // floor it rejoins at, and one validating the pointer it took is no
        // member yet.
        if !self.is_joined_to(&ack.shard_id()) {
            return None;
        }
        let heard = RecoveryEpoch::new(ack.recovery_epoch, ack.recovery_epoch_lineage);
        // Never having joined a shard is `Bootstrapping`, returned above.
        let later = match self.standing.order(heard)? {
            EpochOrder::Stale => return None,
            EpochOrder::Later => true,
            EpochOrder::Mine => false,
        };
        // A leader with an authority leads the epoch it took office at, which
        // no other lineage's epoch displaces, a higher number or lineage included.
        let refused = if later {
            self.leads_its_office_epoch(heard.lineage)
        } else {
            ack.term < self.ack_floor()
        };
        (!refused).then_some(AckedEpoch { heard, later })
    }

    /// Whether this node is a member of `shard`: it has joined (it is neither
    /// `Bootstrapping` nor `Joining`) and `shard` is its own.
    pub(super) fn is_joined_to(&self, shard: &ShardId) -> bool {
        !matches!(
            self.state,
            WorkerState::Bootstrapping | WorkerState::Joining
        ) && *shard == self.shard_id
    }

    /// Follows the leader whose `ack` this node accepted, the ack having
    /// changed its standing as `change` says; `outpaced` when the ack outran
    /// a term it held or contested.
    fn follow_ack(&mut self, ack: &Checked<LeaderHeartbeatAck>, change: AckChange, outpaced: bool) {
        let now = self.clock.now();
        // A leader handing over is alive but leads no longer: its ack neither
        // refreshes contact nor ends a suspicion or a roll call (its
        // successor's included), and the calls above its term are kept.
        self.leader_draining = ack.leader_draining;
        if !ack.leader_draining {
            self.last_leader_contact = now;
            self.round.end_no_quorum_streak();
        }
        self.record_ack(ack, now);
        if change.generation_changed || change.admission_changed {
            // Heartbeat at once: the echo of the new generation is what
            // commits it (see `Roster::commit_if_confirmed`), and the
            // admission held is what confirms a promise (see
            // `Roster::promise_admission`).
            self.next_heartbeat = None;
        }
        if outpaced || (!ack.leader_draining && self.seeks_a_leader()) {
            self.round.stop();
            self.drop_recovery();
            self.transition_to(WorkerState::Active);
        }
        // Whatever this node's state: see the forgetting described at
        // `on_leader_ack`.
        if !ack.leader_draining {
            self.round.follow_leader_of(ack.term);
        }
        // A node that handed its leadership over leaves once the leader that
        // took over vouches for hearing it: that leader holds a grant, so it
        // has reconciled with this node's runs and can place its records.
        if self.drain_request.hands_over()
            && self.state == WorkerState::Active
            && ack.heartbeat_token.is_some()
            && ack.leader_id() != self.my_id
        {
            self.drain_request = DrainRequest::NotAsked;
            self.drain();
        }
    }

    /// Records `ack`, accepted at `now`: its vouching token for the lease, its
    /// leader, the runs it says were cancelled and the echo the next heartbeat
    /// carries.
    fn record_ack(&mut self, ack: &Checked<LeaderHeartbeatAck>, now: Instant) {
        // A token from a later instant than this node's own clock reads was
        // never sent by this node, and proves nothing.
        self.lease.acked(
            ack.heartbeat_token
                .filter(|token| *token <= now.as_ticks())
                .map(Instant::at),
        );
        self.leader = Some((ack.leader_id(), ack.term));
        if !ack.cancelled_runs.is_empty() {
            let runs = ack
                .cancelled_runs
                .iter()
                .cloned()
                .map(TaskRunId::from)
                .collect();
            self.outputs.push(Output::RunsCancelled(runs));
        }
        self.newest_accepted_ack = Some(AckEcho {
            term: ack.term,
            send_token: ack.send_token,
        });
    }

    /// Whether this node seeks a leader: `LeaderSuspect`, `RollCall` or `NoQuorum`.
    fn seeks_a_leader(&self) -> bool {
        matches!(
            self.state,
            WorkerState::LeaderSuspect | WorkerState::RollCall | WorkerState::NoQuorum
        )
    }

    /// Whether this node leads an epoch of a lineage other than `lineage`.
    /// It leads the epoch it took office at from the moment it took office,
    /// before any grant. Once the authority grants the fence, that epoch is the
    /// one the authority holds (it grants only at the epoch it holds). The node
    /// keeps that standing after the fence lapses, by time or by a flush of the
    /// authority, until it leaves leadership. An ack of a later epoch of its
    /// own lineage ends it too, by taking the node out of office. A node with
    /// no authority has none.
    fn leads_its_office_epoch(&self, lineage: u64) -> bool {
        self.holds_office()
            && self
                .authority
                .as_ref()
                .and_then(AuthorityStanding::office_epoch)
                .is_some_and(|led| led.lineage != lineage)
    }

    /// The earliest term whose leader's acks this node accepts: the highest
    /// term it has seen, or, while it stands or leads, its own term if
    /// later. A candidate never follows an earlier term's leader while its
    /// candidacy can still win; once that lapses unwon, it can.
    pub(super) fn ack_floor(&self) -> u64 {
        // A leader's own term is already its term seen (see
        // `Self::take_office`); the arm only keeps that from resting on it.
        match self.state {
            WorkerState::Candidate | WorkerState::LeaderReconciling | WorkerState::Leader => {
                self.standing.highest_term_seen().max(self.term)
            }
            _ => self.standing.highest_term_seen(),
        }
    }

    /// Sends this node's leader a heartbeat once a heartbeat
    /// interval has passed since the last one. A leader this node was not
    /// already heartbeating hears from it at once, so a leader it has just
    /// learned of does not wait a whole interval for its first heartbeat.
    pub(super) fn heartbeat_leader_if_due(&mut self) {
        let Some(leader) = self.leader_to_heartbeat() else {
            self.next_heartbeat = None;
            return;
        };
        let now = self.clock.now();
        let due = match &self.next_heartbeat {
            Some((heartbeating, at)) => *heartbeating != leader || *at <= now,
            None => true,
        };
        if !due {
            return;
        }

        let heartbeat = self.heartbeat(now);
        self.send(
            leader.clone(),
            election_message::Payload::Heartbeat(heartbeat),
        );
        self.next_heartbeat = Some((leader, now + self.timings.heartbeat_interval));
    }

    /// The heartbeat this node sends its leader at `now`.
    fn heartbeat(&self, now: Instant) -> WorkerHeartbeat {
        WorkerHeartbeat {
            worker_id: Some(self.my_id.clone().into()),
            incarnation_id: Some(self.incarnation_id.clone().into()),
            recovery_epoch_seen: self.standing.epoch_number(),
            recovery_epoch_lineage: self.standing.epoch().map_or(0, |epoch| epoch.lineage),
            term_seen: self.standing.highest_term_seen(),
            available_capacity: self.report.available_capacity,
            active_task_runs_digest: self.report.active_runs_digest.clone(),
            shard_id: Some(self.shard_id.clone().into()),
            newest_accepted_ack: self.newest_accepted_ack,
            configuration_generation: self
                .standing
                .configuration()
                .map(|configuration| configuration.generation().into()),
            send_token: now.as_ticks(),
            routing_crawled: self.routing_crawled(),
            admission_generation: self.standing.admission().map(Into::into),
            runs_compaction: self.report.runs_compaction,
            queues: self.report.queues.names(),
        }
    }

    /// The leader this node heartbeats: the one it records, while it is
    /// `Active`, `LeaderSuspect`, `RollCall` or `NoQuorum`, unless that is
    /// itself.
    fn leader_to_heartbeat(&self) -> Option<WorkerId> {
        if !matches!(
            self.state,
            WorkerState::Active
                | WorkerState::LeaderSuspect
                | WorkerState::RollCall
                | WorkerState::NoQuorum
        ) {
            return None;
        }
        let (leader, _) = self.leader.as_ref()?;
        (*leader != self.my_id).then(|| leader.clone())
    }

    /// The digest of the runs this worker holds, which every heartbeat it
    /// sends from now on carries (see `reconcile::active_runs_digest`).
    pub fn set_active_runs_digest(&mut self, digest: Digest) {
        self.report.active_runs_digest = digest.value().to_vec();
    }

    /// Whether this worker runs compaction, which every heartbeat it sends
    /// from now on says.
    pub fn set_runs_compaction(&mut self, runs: bool) {
        self.report.runs_compaction = runs;
    }

    /// The places this worker has free to run tasks in, which every
    /// heartbeat it sends from now on says.
    pub fn set_available_capacity(&mut self, places: u32) {
        self.report.available_capacity = places;
    }

    /// The queues this worker takes work from, which every heartbeat it sends
    /// from now on says.
    pub fn set_queues(&mut self, queues: Subscription) {
        self.report.queues = queues;
    }
}
