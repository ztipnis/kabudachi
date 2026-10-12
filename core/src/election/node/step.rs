use crate::protocol::checked::{CheckedMessage, CheckedPayload};
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{ElectionMessage, election_message};
use crate::protocol::worker_state::WorkerState;
use crate::time::{Clock, Duration, Instant};

use super::super::{Input, Output, RejoinCheck, Step, WorkerNode, earliest};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Handles one input and returns what the node asks its driver to do,
    /// with the next instant at which a `Tick` can change anything.
    ///
    /// Whatever the input, a node with an authority whose registration has
    /// lapsed first fences itself (see [`Self::orphan_if_unregistered`]),
    /// and a leader whose quorum-contact lease has run out first gives up
    /// leading (see [`Self::lose_quorum_if_its_lease_ended`]): a node paused
    /// past either must not act on the inputs held while it was paused
    /// before its next `Tick`. A paused leader would otherwise still lead,
    /// and, finding the authority flushed, republish its old epoch over one
    /// the shard recovered to meanwhile.
    pub fn step(&mut self, input: Input) -> Step {
        let orphaned = self.orphan_if_unregistered();
        self.lose_quorum_if_its_lease_ended();
        match input {
            Input::Tick if orphaned => {}
            Input::Tick => self.tick(),
            Input::Message { from, message } => self.on_message(from, message),
            // A connection to itself is no peer: a leader would ack itself.
            Input::PeerConnected(peer) if peer != self.my_id => self.on_peer_connected(peer),
            Input::PeerConnected(_) => {}
            Input::PeerDisconnected(peer) => {
                self.connected.remove(&peer);
            }
            Input::Authority(reply) => self.on_authority_reply(reply),
            Input::JoinAnswer(pointer) => self.join(&pointer),
            Input::AuthorityEpochAsked(token) => self.note_epoch_read_asked(token),
            Input::AuthorityEpochRead { token, held } => self.on_epoch_read(token, held),
            Input::Drain => self.request_drain(),
            Input::RoutingCrawled => self.crawled_at_admission = self.admission(),
            Input::Reconciled(office) => self.on_reconciled(office),
            Input::WatchWorkers {
                silent_holders,
                answered,
            } => self.on_watch_workers(silent_holders, answered),
        }
        self.finish_step()
    }

    /// Hands back everything this step produced, with the next deadline.
    /// Whatever the input, a node with an authority first asks for any
    /// registration or fence that has come due (see
    /// [`Self::ask_authority_if_due`]), a follower heartbeats its leader if
    /// that has come due (see [`Self::heartbeat_leader_if_due`]), and a
    /// leader reports its grant if the step changed it.
    pub(in crate::election) fn finish_step(&mut self) -> Step {
        self.ask_authority_if_due();
        self.heartbeat_leader_if_due();
        self.report_silences();
        self.report_lease_changes();
        Step {
            outputs: std::mem::take(&mut self.outputs),
            next_deadline: self.next_deadline(),
        }
    }

    /// The earliest instant at which a `Tick` can change anything.
    ///
    /// A `Tick` at the instant this returns must move the node into another
    /// state or make this return a later instant or `None`: a driver ticks
    /// the node while its deadline has come, without letting time pass in
    /// between (as `net`'s `run_driver` does), and would otherwise never
    /// stop. `election_step_test`'s
    /// `a_tick_at_the_deadline_moves_a_node_on_in_every_state_that_reports_one`
    /// checks it.
    ///
    /// - `Active`: the first instant past its jittered suspicion timeout
    ///   (see [`Self::jittered_suspect_timeout`]), or its next heartbeat to
    ///   its leader if that comes first.
    /// - `LeaderSuspect` and `NoQuorum`: when it may start a roll call (see
    ///   [`Self::next_roll_call_due`]), or its next heartbeat to its leader
    ///   if that comes first; `None` for a node with neither, a joiner with
    ///   no configuration and no leader to heartbeat.
    /// - `RollCall`: its roll call's deadline, or its next heartbeat to its
    ///   leader if that comes first.
    /// - `Candidate`: its vote's deadline.
    /// - `LeaderReconciling` and `Leader`: when it goes `NoQuorum` unless
    ///   more confirmations arrive (never, for a leader that alone is a
    ///   quorum), or when it next has a worker to report silent or lost,
    ///   whichever comes first.
    /// - Every other state: `None`.
    ///
    /// With an authority, also the lease's next registration or fence
    /// attempt and, until it is `Fenced`, when it fences itself, in every
    /// state in which it keeps a registration. In every state, also when its
    /// contact floor goes stale, while that is still ahead (see
    /// [`Output::AbortDeadline`]): a `Tick` then reports the deadline.
    pub(in crate::election) fn next_deadline(&self) -> Option<Instant> {
        let lease_deadline = self
            .authority
            .as_ref()
            .and_then(|authority| authority.next_deadline(self.state));
        earliest(
            earliest(self.election_deadline(), lease_deadline),
            self.lease.next_deadline(&self.timings, self.clock.now()),
        )
    }

    /// What [`Self::next_deadline`] reports, leaving the authority lease
    /// aside.
    pub(in crate::election) fn election_deadline(&self) -> Option<Instant> {
        let next_heartbeat = self.next_heartbeat.as_ref().map(|(_, at)| *at);
        match self.state {
            WorkerState::Active => {
                let suspicion = self.last_leader_contact
                    + self.jittered_suspect_timeout()
                    + Duration::from_ticks(1);
                Some(next_heartbeat.map_or(suspicion, |at| at.min(suspicion)))
            }
            WorkerState::LeaderSuspect | WorkerState::NoQuorum => {
                earliest(next_heartbeat, self.next_roll_call_due())
            }
            WorkerState::RollCall => earliest(next_heartbeat, self.round.next_deadline()),
            WorkerState::Candidate => self.round.next_deadline(),
            WorkerState::LeaderReconciling | WorkerState::Leader => self.office_deadline(),
            _ => None,
        }
    }

    /// What a node in office waits for: going `NoQuorum` unless more
    /// confirmations arrive, its next worker to report lost or silent, and a
    /// kept drain's wake-up, whichever comes first.
    pub(in crate::election) fn office_deadline(&self) -> Option<Instant> {
        let office = self.office.as_ref();
        let no_quorum = office.and_then(|office| {
            self.lease
                .no_quorum_at(&self.my_id, office.roster(), &self.timings)
        });
        let lost = office.and_then(|office| {
            office.next_lost_at(
                self.clock.now(),
                self.lost_after(),
                self.timings.suspect_timeout,
            )
        });
        let silent = office.and_then(|office| {
            office.next_silent_at(self.clock.now(), self.timings.suspect_timeout)
        });
        earliest(
            earliest(no_quorum, lost),
            earliest(self.drain_request.wakes_at(), silent),
        )
    }

    /// Moves to `next` and reports the move. Leaving `Leader` first reports
    /// that the node holds no grant (see [`Output::Grant`]), and leaving the
    /// states that lead or stand to lead gives up any fence. Reaching
    /// `Active`, `LeaderReconciling` or `Leader` applies a drain kept from an
    /// earlier request at once (see [`Input::Drain`]).
    pub(in crate::election) fn transition_to(&mut self, next: WorkerState) {
        // Every caller moves along an edge of the transition table, checked
        // by the state it guards on first; no input can reach an illegal
        // edge, so one here is a bug in this module.
        assert!(
            self.state.can_transition_to(next),
            "illegal election state transition {:?} -> {next:?}",
            self.state
        );
        // Reconciling to leading keeps the office; every other edge out of
        // either state gives it up.
        if self.holds_office() && next != WorkerState::Leader {
            // Losing office mid-wait keeps the request: the node drains
            // when it next reaches `Active`, `LeaderReconciling` or `Leader`.
            self.drain_request.office_lost();
            self.lease.withdraw_grant(self.clock.now());
            self.outputs.push(Output::Grant(None));
            self.leave_office();
        }
        if let Some(authority) = self.authority.as_mut() {
            authority.state_changed(next);
        }
        // Only a rejoining node keeps a read it awaits or a pointer it holds.
        if !matches!(next, WorkerState::Bootstrapping | WorkerState::Joining) {
            self.rejoin = RejoinCheck::default();
        }
        self.state = next;
        self.outputs.push(Output::StateChanged(next));

        if self.drain_request.take_kept_for(next) {
            self.request_drain();
        }
    }

    /// Hands the office back to the standing, if this node holds one: the
    /// standing then follows the configuration the roster led, as it stands
    /// (see [`LeaderOffice::hand_back`]). Call it before anything that writes
    /// the standing while this node still leads, so what it writes is not
    /// overwritten by the configuration this node led.
    pub(in crate::election) fn leave_office(&mut self) {
        if let Some(office) = self.office.take() {
            office.hand_back(&mut self.standing, &self.my_id);
        }
        // A new office tells only what it cancelled itself.
        self.cancelled_runs.clear();
    }

    pub(in crate::election) fn send(&mut self, to: WorkerId, payload: election_message::Payload) {
        self.outputs.push(Output::Send {
            to,
            message: ElectionMessage {
                payload: Some(payload),
            },
        });
    }

    pub(in crate::election) fn publish(&mut self, payload: election_message::Payload) {
        self.outputs.push(Output::Publish {
            message: ElectionMessage {
                payload: Some(payload),
            },
        });
    }

    /// Dispatches an inbound message from `from` to its handler.
    ///
    /// Nodes that are `Draining`, `Stopped` or `Fenced` take no part in
    /// elections and drop everything. Every message is honoured only when
    /// its sender is the worker it names as sending it: the leader of a
    /// heartbeat ack, the follower of a heartbeat, the initiator of a roll
    /// call, the responder of a reply, the candidate of a vote request, the
    /// voter of a vote grant, the rejecter of a refusal, the departing
    /// worker of a self-remove and the leader of an election certificate.
    /// Anything else is dropped.
    pub(in crate::election) fn on_message(&mut self, from: WorkerId, message: CheckedMessage) {
        if matches!(
            self.state,
            WorkerState::Draining | WorkerState::Stopped | WorkerState::Fenced
        ) {
            return;
        }

        match message.into_payload() {
            Some(CheckedPayload::Heartbeat(heartbeat)) if heartbeat.worker_id() == from => {
                self.on_heartbeat(from, &heartbeat);
            }
            Some(CheckedPayload::HeartbeatAck(ack)) if ack.leader_id() == from => {
                self.on_leader_ack(&ack);
            }
            Some(CheckedPayload::RollCall(call)) if call.initiator_id() == from => {
                let now = self.clock.now();
                self.decide(|round, view| round.on_roll_call(view, from, &call, now));
            }
            Some(CheckedPayload::RollCallReply(reply)) if reply.responder_id() == from => {
                self.decide(|round, view| round.on_roll_call_reply(view, from, &reply));
            }
            Some(CheckedPayload::VoteRequest(req)) if req.candidate_id() == from => {
                self.decide(|round, view| round.on_vote_request(view, from, &req));
            }
            Some(CheckedPayload::VoteGrant(grant)) if grant.voter_id() == from => {
                self.decide(|round, view| round.on_vote_grant(view, from, &grant));
            }
            Some(CheckedPayload::ElectionReject(reject)) if reject.rejecter_id() == from => {
                self.on_election_reject(&reject);
            }
            Some(CheckedPayload::SelfRemove(msg)) if msg.worker_id() == from => {
                self.on_self_remove(&from, &msg);
            }
            Some(CheckedPayload::ElectionCertificate(certificate))
                if certificate.leader_id() == from =>
            {
                self.on_election_certificate(&from, &certificate);
            }
            _ => {}
        }
    }

    /// Records a connection to `peer`. A leader also acks any peer it has
    /// newly connected to, as it did every peer connected when it won (see
    /// [`Self::announce_leadership`]): one that missed that announcement,
    /// cut off at the win or joining since (a restarted process joins under
    /// a new `WorkerId`), would otherwise never learn whom to heartbeat.
    pub(in crate::election) fn on_peer_connected(&mut self, peer: WorkerId) {
        let newly_connected = self.connected.insert(peer.clone());
        if newly_connected && self.holds_office() {
            self.send_ack(peer, None);
        }
    }
}
