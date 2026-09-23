//! The worker-side election state machine (README §10-§14): leader liveness,
//! ring roll call, candidate selection, voting, graceful draining and forced
//! recovery through the coordination authority.
//!
//! Known gaps (see README §27 for the phase plan): leaders and candidates do
//! not step down on seeing a higher term, `RollCall`/`Candidate` have no
//! timeout or retry, `NoQuorum` is only exited through forced recovery, and
//! there is no recovery fencing. Peer liveness comes from
//! [`PeerMessenger::reachable_peers`] rather than observed heartbeats.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use crate::coordination_authority::CoordinationAuthority;
use crate::hashing::{Field, HashFunction};
use crate::membership::MembershipView;
use crate::protocol::generated;
use crate::protocol::ids::{IncarnationId, ShardId, WorkerId};
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{
    ElectionCertificate, ElectionMessage, LeaderHeartbeatAck, RollCall, RollCallObservation,
    SelfRemove, VoteGrant, VoteReject, VoteRejectReason, VoteRequest, election_message,
};
use crate::protocol::worker_state::WorkerState;
use crate::time::{Clock, Duration, Instant};
use crate::transport::PeerMessenger;

pub struct WorkerNode<C, M, V, A>
where
    C: Clock,
    M: PeerMessenger,
    V: MembershipView,
    A: CoordinationAuthority,
{
    my_id: WorkerId,
    incarnation_id: IncarnationId,
    shard_id: ShardId,
    state: WorkerState,
    recovery_epoch: u64,
    highest_term_seen: u64,
    last_leader_contact: Instant,
    suspect_timeout: Duration,
    clock: C,
    transport: M,
    membership: V,
    authority: A,
    hash_function: HashFunction,
    /// Roll calls already processed by this node. Grows for the life of the
    /// node; bounding it is deferred.
    seen_roll_calls: BTreeSet<String>,
    /// Makes this node's own roll-call IDs (`"{my_id}-{seq}"`) unique.
    next_roll_call_seq: u64,
    /// The term this node is contesting or holds. Meaningful from `Candidate`
    /// onward.
    term: u64,
    /// term -> candidate this node voted for. One entry per term is what
    /// enforces "at most one vote per term".
    voted_for: BTreeMap<u64, WorkerId>,
    /// Electorate members that granted this node's current candidacy,
    /// starting with the implicit self-vote.
    votes_received: BTreeSet<WorkerId>,
}

/// The deterministic priority of `candidate` in the election for `term`
/// (README §12.5); the highest among eligible workers wins. Workers using the
/// same `hash_function` compute the same winner on any build.
pub fn candidate_priority(
    hash_function: &HashFunction,
    shard_id: &ShardId,
    recovery_epoch: u64,
    term: u64,
    candidate: &WorkerId,
) -> u64 {
    hash_function.hash_to_u64(&[
        Field::Text(shard_id.as_str()),
        Field::Text(candidate.as_str()),
        Field::Number(recovery_epoch),
        Field::Number(term),
    ])
}

impl<C, M, V, A> WorkerNode<C, M, V, A>
where
    C: Clock,
    M: PeerMessenger,
    V: MembershipView,
    A: CoordinationAuthority,
{
    /// Constructs a node in `WorkerState::Active` with a full membership
    /// known up front. For a node that must discover its membership first,
    /// see [`Self::bootstrapping`]. The leader-contact timer starts now so a
    /// new node isn't immediately suspicious.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        my_id: WorkerId,
        incarnation_id: IncarnationId,
        shard_id: ShardId,
        clock: C,
        transport: M,
        membership: V,
        authority: A,
        suspect_timeout: Duration,
    ) -> Self {
        Self::with_initial_state(
            WorkerState::Active,
            my_id,
            incarnation_id,
            shard_id,
            clock,
            transport,
            membership,
            authority,
            suspect_timeout,
        )
    }

    /// Constructs a node in `WorkerState::Bootstrapping` (README §27 Phase 2
    /// bootstrap join protocol): for a fresh node that has not yet discovered
    /// the shard's current membership, rather than being statically
    /// pre-configured with it like [`Self::new`]. `membership` is typically
    /// empty; call [`Self::finish_joining`] once something outside `core`
    /// (`net`'s `/kabudachi/join/1` handshake) has resolved a membership list
    /// to drive `Bootstrapping -> Joining -> Active`.
    ///
    /// `tick()` no-ops in both `Bootstrapping` and `Joining` (see its doc):
    /// nothing here times out a stalled join, by design — that is left to
    /// whatever drives the join handshake itself, not this state machine.
    #[allow(clippy::too_many_arguments)]
    pub fn bootstrapping(
        my_id: WorkerId,
        incarnation_id: IncarnationId,
        shard_id: ShardId,
        clock: C,
        transport: M,
        membership: V,
        authority: A,
        suspect_timeout: Duration,
    ) -> Self {
        Self::with_initial_state(
            WorkerState::Bootstrapping,
            my_id,
            incarnation_id,
            shard_id,
            clock,
            transport,
            membership,
            authority,
            suspect_timeout,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_initial_state(
        state: WorkerState,
        my_id: WorkerId,
        incarnation_id: IncarnationId,
        shard_id: ShardId,
        clock: C,
        transport: M,
        membership: V,
        authority: A,
        suspect_timeout: Duration,
    ) -> Self {
        let last_leader_contact = clock.now();
        WorkerNode {
            my_id,
            incarnation_id,
            shard_id,
            state,
            recovery_epoch: 0,
            highest_term_seen: 0,
            last_leader_contact,
            suspect_timeout,
            clock,
            transport,
            membership,
            authority,
            hash_function: HashFunction::default(),
            seen_roll_calls: BTreeSet::new(),
            next_roll_call_seq: 0,
            term: 0,
            voted_for: BTreeMap::new(),
            votes_received: BTreeSet::new(),
        }
    }

    /// Replaces the hash function used to rank candidates. Every worker in
    /// the shard must use the same one, or they will disagree on the winner.
    pub fn with_hash_function(mut self, hash_function: HashFunction) -> Self {
        self.hash_function = hash_function;
        self
    }

    pub fn state(&self) -> WorkerState {
        self.state
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    /// This node's recovery epoch, bumped only by a successful
    /// [`Self::attempt_forced_recovery`].
    pub fn recovery_epoch(&self) -> u64 {
        self.recovery_epoch
    }

    /// The current effective electorate (README §11), as seen by this node's
    /// [`MembershipView`]. Exposed so a driver outside `core` (e.g. `net`'s
    /// `/kabudachi/join/1` request responder) can compose a bootstrap join
    /// response without reaching into `core`'s private fields — `core` knows
    /// the electorate but nothing about network addresses, and `net` is the
    /// reverse, so composing a full `JOIN_RESPONSE` needs both sides
    /// deliberately, in the driver, not in either one alone.
    pub fn electorate(&self) -> BTreeSet<WorkerId> {
        self.membership.effective_electorate()
    }

    /// Processes a leader heartbeat acknowledgement (README §12.2
    /// `on_leader_ack`). Acks for another shard, a different recovery epoch or
    /// an older term are ignored without touching any state.
    ///
    /// Differs from the README pseudocode, which ignores only older epochs: a
    /// node cannot yet adopt a newer epoch, so following a newer-epoch leader
    /// would leave it acting on its old epoch under that leader. An accepted ack
    /// refreshes leader contact, and returns a node in `RollCall` to `Active`
    /// because the leader is reachable again.
    pub fn on_leader_ack(&mut self, ack: &LeaderHeartbeatAck) {
        if ack.shard_id() != self.shard_id {
            return;
        }
        if ack.recovery_epoch != self.recovery_epoch {
            return;
        }
        if ack.term < self.highest_term_seen {
            return;
        }

        self.highest_term_seen = self.highest_term_seen.max(ack.term);
        self.last_leader_contact = self.clock.now();

        if self.state == WorkerState::RollCall {
            self.state = WorkerState::Active;
        }
    }

    /// Runs one periodic step of the state machine.
    ///
    /// - `Active`: moves to `LeaderSuspect` once no leader ack has arrived
    ///   within the suspicion timeout (README §12.2).
    /// - `LeaderSuspect`: starts a roll call. Each transition takes its own
    ///   call, so a single tick never goes from `Active` to `RollCall`.
    /// - `Leader`: acknowledges every reachable electorate member (README
    ///   §12.1), then moves to `NoQuorum` if fewer than a majority of the
    ///   electorate, counting itself, are reachable.
    ///
    /// Every other state is a no-op — deliberately so for `Bootstrapping` and
    /// `Joining` (README §27 Phase 2 bootstrap join): nothing times out a
    /// stalled join here, since the transition out of those states happens
    /// once via [`Self::finish_joining`], driven by something outside `core`
    /// that resolves a membership list, not by a per-tick check.
    pub fn tick(&mut self) {
        match self.state {
            WorkerState::Active => {
                if self.clock.now() - self.last_leader_contact > self.suspect_timeout {
                    self.state = WorkerState::LeaderSuspect;
                }
            }
            WorkerState::LeaderSuspect => self.begin_roll_call(),
            WorkerState::Leader => self.tick_as_leader(),
            _ => {}
        }
    }

    fn tick_as_leader(&mut self) {
        let electorate = self.membership.effective_electorate();
        let quorum = electorate.len() / 2 + 1;
        let reachable_electorate: BTreeSet<WorkerId> = self
            .transport
            .reachable_peers(self.my_id.clone())
            .intersection(&electorate)
            .cloned()
            .collect();

        let ack = LeaderHeartbeatAck {
            shard_id: Some(self.shard_id.clone().into()),
            leader_id: Some(self.my_id.clone().into()),
            recovery_epoch: self.recovery_epoch,
            term: self.term,
            membership_generation: self.membership.membership_generation(),
        };
        self.transport.broadcast(
            self.my_id.clone(),
            reachable_electorate.clone(),
            ElectionMessage {
                payload: Some(election_message::Payload::HeartbeatAck(ack)),
            },
        );

        let visible = 1 + reachable_electorate.len(); // +1 for self.
        if visible < quorum {
            self.state = WorkerState::NoQuorum;
        }
    }

    /// Starts a fresh roll call (README §12.4). The state becomes `RollCall`
    /// first so a single-member electorate can go straight on to `Candidate`.
    fn begin_roll_call(&mut self) {
        let roll_call_id = format!("{}-{}", self.my_id.as_str(), self.next_roll_call_seq);
        self.next_roll_call_seq += 1;

        let call = RollCall {
            roll_call_id: roll_call_id.clone(),
            shard_id: Some(self.shard_id.clone().into()),
            recovery_epoch: self.recovery_epoch,
            membership_generation: self.membership.membership_generation(),
            membership_digest: self.membership.membership_digest().to_vec(),
            highest_term_seen: self.highest_term_seen,
            initiator_id: Some(self.my_id.clone().into()),
            responses: vec![],
        };

        self.seen_roll_calls.insert(roll_call_id);
        self.state = WorkerState::RollCall;
        self.process_roll_call(call);
    }

    /// Completes the bootstrap join handshake (README §27 Phase 2): adopts
    /// `members` (plus this node's own id — [`Self::electorate`]'s doc notes
    /// every other node's `effective_electorate` already includes itself, so
    /// a joining node's adopted electorate must too, for quorum/ring math to
    /// treat it consistently) and drives `Bootstrapping -> Joining ->
    /// Active`. There is no direct `Bootstrapping -> Active` edge in
    /// [`WorkerState::can_transition_to`], so this goes through `Joining`
    /// explicitly, mirroring how [`Self::maybe_win_election`] passes through
    /// `LeaderReconciling` on its way to `Leader`.
    ///
    /// A no-op outside `Bootstrapping`: a node constructed via [`Self::new`]
    /// (already `Active`) or one that already finished joining has nothing
    /// left to join.
    ///
    /// The wire handshake that produces `members` — dialing seed addresses,
    /// sending `JOIN_REQUEST`, taking the first `JOIN_RESPONSE` — is entirely
    /// `net`'s concern (a separate `/kabudachi/join/1` request_response
    /// protocol, not routed through `ElectionMessage`/`on_message`); this
    /// method only performs the resulting state transition, the same way
    /// `on_message` itself never touches the network.
    pub fn finish_joining(&mut self, mut members: BTreeSet<WorkerId>) {
        if self.state != WorkerState::Bootstrapping {
            return;
        }
        members.insert(self.my_id.clone());

        self.state = WorkerState::Joining;
        self.membership.rebuild(members);
        // A freshly joined node hasn't heard from a leader yet; start the
        // suspicion clock now so it isn't judged suspect the instant it
        // ticks — the same reasoning `Self::new`'s doc gives for a freshly
        // constructed node.
        self.last_leader_contact = self.clock.now();
        self.state = WorkerState::Active;
    }

    /// Dispatches an inbound message from `from` to its handler.
    ///
    /// Nodes that are `Draining`, `Stopped` or `Fenced` take no part in
    /// elections and drop everything. A heartbeat ack is only honoured when
    /// the sender is the leader it names, and the same holds for a vote request
    /// (the candidate), a vote grant (the voter) and a self-remove (the
    /// departing worker): a message that names a different worker than the one
    /// that sent it is dropped. Received election certificates are not used yet
    /// and are ignored.
    pub fn on_message(&mut self, from: WorkerId, msg: ElectionMessage) {
        if matches!(
            self.state,
            WorkerState::Draining | WorkerState::Stopped | WorkerState::Fenced
        ) {
            return;
        }

        match msg.payload {
            Some(election_message::Payload::HeartbeatAck(ack)) => {
                if ack.leader_id() == from {
                    self.on_leader_ack(&ack);
                }
            }
            Some(election_message::Payload::RollCall(call)) => self.on_roll_call(call),
            Some(election_message::Payload::VoteRequest(req)) if req.candidate_id() == from => {
                self.on_vote_request(&req);
            }
            Some(election_message::Payload::VoteGrant(grant)) if grant.voter_id() == from => {
                self.on_vote_grant(&grant);
            }
            Some(election_message::Payload::VoteReject(reject)) => self.on_vote_reject(&reject),
            Some(election_message::Payload::SelfRemove(msg)) if msg.worker_id() == from => {
                self.on_self_remove(&msg);
            }
            _ => {}
        }
    }

    /// Gracefully shuts this node down (README §12.3, §18): announces
    /// `SelfRemove` to every reachable peer, removes itself from its own
    /// electorate, and ends in `Stopped`.
    ///
    /// Only `Active` and `Leader` nodes can drain; from any other state this
    /// does nothing.
    pub fn begin_drain(&mut self) {
        if !self.state.can_transition_to(WorkerState::Draining) {
            return;
        }
        self.state = WorkerState::Draining;

        let msg = SelfRemove {
            worker_id: Some(self.my_id.clone().into()),
            incarnation_id: Some(self.incarnation_id.clone().into()),
            shard_id: Some(self.shard_id.clone().into()),
            membership_generation: self.membership.membership_generation(),
        };
        self.membership.apply_self_remove(&msg);
        self.transport.broadcast(
            self.my_id.clone(),
            self.transport.reachable_peers(self.my_id.clone()),
            ElectionMessage {
                payload: Some(election_message::Payload::SelfRemove(msg)),
            },
        );

        // There is no outstanding work to wait for yet, so draining finishes
        // immediately. The transition table has no direct `Active -> Stopped`
        // edge, hence the two assignments.
        self.state = WorkerState::Stopped;
    }

    /// Removes another node from this node's electorate (README §12.3). A
    /// candidate also discards the removed worker's vote, and wins at once if
    /// the smaller electorate's quorum is already met by the votes left.
    pub fn on_self_remove(&mut self, msg: &SelfRemove) {
        if msg.shard_id() != self.shard_id {
            return;
        }
        self.membership.apply_self_remove(msg);

        if self.state == WorkerState::Candidate {
            let electorate = self.membership.effective_electorate();
            self.votes_received
                .retain(|voter| electorate.contains(voter));
            self.maybe_win_election();
        }
    }

    /// Handles an inbound roll call (README §12.4). A roll call already seen
    /// is dropped; otherwise this node adds its observation and either
    /// becomes a candidate or forwards the call.
    ///
    /// A call for another shard or recovery epoch is dropped. So is a call
    /// whose own origin-time `highest_term_seen` is already behind what this
    /// node knows: this node has observed a term the call's originator had
    /// not yet accounted for, so letting the call keep circulating would let
    /// its contested term be recomputed from state the call's origin never
    /// agreed to (see `choose_candidate`'s doc for the incoherent-escalation
    /// bug this prevents).
    pub fn on_roll_call(&mut self, call: RollCall) {
        if call.shard_id() != self.shard_id || call.recovery_epoch != self.recovery_epoch {
            return;
        }
        if !self.seen_roll_calls.insert(call.roll_call_id.clone()) {
            return;
        }
        if self.highest_term_seen > call.highest_term_seen {
            return;
        }
        self.process_roll_call(call);
    }

    /// Adds this node's observation to `call`, then either becomes `Candidate`
    /// (a majority responded, this node is the winner and is in `RollCall`) or
    /// forwards the call so another node can act on the same result.
    fn process_roll_call(&mut self, mut call: RollCall) {
        call.responses.push(self.my_observation());

        let electorate = self.membership.effective_electorate();
        let quorum = electorate.len() / 2 + 1;
        let observations = Self::electorate_observations(&call.responses, &electorate);

        if observations.len() >= quorum {
            let (winner, next_term) = self.choose_candidate(call.highest_term_seen, &observations);
            if winner == self.my_id && self.state == WorkerState::RollCall {
                self.state = WorkerState::Candidate;
                self.term = next_term;
                // A leader never acks itself, so nothing else raises its own
                // `highest_term_seen` to the term it now contests.
                self.highest_term_seen = self.highest_term_seen.max(next_term);
                // A candidate counts its own vote without messaging itself.
                self.votes_received = BTreeSet::from([self.my_id.clone()]);
                self.send_vote_requests(observations.keys());
                // A single-member electorate is already won by the self-vote.
                self.maybe_win_election();
                return;
            }
        }

        self.forward_to_next_reachable_neighbor(call);
    }

    /// One observation per distinct electorate member: duplicates and
    /// non-members never count towards quorum or candidacy.
    fn electorate_observations<'a>(
        responses: &'a [RollCallObservation],
        electorate: &BTreeSet<WorkerId>,
    ) -> BTreeMap<WorkerId, &'a RollCallObservation> {
        let mut observations = BTreeMap::new();
        for response in responses {
            let worker_id = response.worker_id();
            if electorate.contains(&worker_id) {
                observations.entry(worker_id).or_insert(response);
            }
        }
        observations
    }

    fn my_observation(&self) -> RollCallObservation {
        RollCallObservation {
            worker_id: Some(self.my_id.clone().into()),
            state: generated::WorkerState::from(self.state) as i32,
            highest_term_seen: self.highest_term_seen,
            current_leader_seen: None,
            leader_contact_age_ticks: (self.clock.now() - self.last_leader_contact).as_ticks(),
        }
    }

    /// Sends `call` to the first reachable one of this node's ring successors.
    /// If none is reachable the call stops here; the same election can still
    /// be reached through another node's copy.
    fn forward_to_next_reachable_neighbor(&self, call: RollCall) {
        let successors = self.membership.ring_successors(self.my_id.clone());
        let reachable = self.transport.reachable_peers(self.my_id.clone());

        for successor in successors {
            if reachable.contains(&successor) {
                self.transport.send(
                    self.my_id.clone(),
                    successor,
                    ElectionMessage {
                        payload: Some(election_message::Payload::RollCall(call)),
                    },
                );
                return;
            }
        }
    }

    /// Picks the winner of the next election from `observations` (README
    /// §12.5) and the term it is contested in: one past `call_highest_term_seen`
    /// — the roll call's own `highest_term_seen`, fixed once at its origin
    /// (`begin_roll_call`) and never mutated as the call is forwarded. This
    /// deliberately does *not* derive the term from `observations`' own
    /// `highest_term_seen` values: those are stamped with whatever a visited
    /// node's local state happened to be at the moment it responded, which
    /// can be bumped mid-flight by that node granting a vote for a
    /// completely unrelated candidacy. Deriving the term from that would let
    /// the same stale, still-circulating call retarget itself to a
    /// different, higher term purely as an artifact of which nodes it
    /// happened to pass through and when — an incoherent candidacy nobody
    /// actually contested, which could win independently and produce a
    /// second, live `Leader` (split brain). The highest [`candidate_priority`]
    /// wins; an exact tie goes to the lower `WorkerId`.
    fn choose_candidate(
        &self,
        call_highest_term_seen: u64,
        observations: &BTreeMap<WorkerId, &RollCallObservation>,
    ) -> (WorkerId, u64) {
        let next_term = call_highest_term_seen + 1;

        let winner = observations
            .keys()
            .min_by_key(|candidate| {
                let priority = candidate_priority(
                    &self.hash_function,
                    &self.shard_id,
                    self.recovery_epoch,
                    next_term,
                    candidate,
                );
                (Reverse(priority), *candidate)
            })
            .expect("process_roll_call only chooses once observations reach a non-empty quorum");
        (winner.clone(), next_term)
    }

    /// Asks every other worker in `voters` for its vote in this node's
    /// current term. `roll_call_digest` stays empty: no handler checks it yet.
    fn send_vote_requests<'a>(&self, voters: impl IntoIterator<Item = &'a WorkerId>) {
        let request = VoteRequest {
            shard_id: Some(self.shard_id.clone().into()),
            recovery_epoch: self.recovery_epoch,
            term: self.term,
            candidate_id: Some(self.my_id.clone().into()),
            membership_generation: self.membership.membership_generation(),
            membership_digest: self.membership.membership_digest().to_vec(),
            roll_call_digest: Vec::new(),
        };

        for worker_id in voters {
            if *worker_id != self.my_id {
                self.transport.send(
                    self.my_id.clone(),
                    worker_id.clone(),
                    ElectionMessage {
                        payload: Some(election_message::Payload::VoteRequest(request.clone())),
                    },
                );
            }
        }
    }

    /// Handles a vote request (README §12.6 `on_vote_request`). A request for
    /// another shard, or from a candidate outside the electorate, is ignored; every other refusal is answered with a
    /// `VoteReject` carrying the reason.
    ///
    /// Differences from the README pseudocode:
    /// - `LeaderSuspect` and `RollCall` nodes may vote, not only `Active`
    ///   ones, since roll call is what leads to the vote.
    /// - "already voted" is checked before "stale term". Granting a vote
    ///   raises `highest_term_seen` to the request's term, so with the
    ///   README's order a repeat request for that term would always report
    ///   "stale term" and "already voted" could never be reached.
    pub fn on_vote_request(&mut self, req: &VoteRequest) {
        if req.shard_id() != self.shard_id {
            return;
        }
        // A non-member must not be able to use up this node's vote for a term.
        if !self
            .membership
            .effective_electorate()
            .contains(&req.candidate_id())
        {
            return;
        }

        let eligible_voter = matches!(
            self.state,
            WorkerState::Active | WorkerState::LeaderSuspect | WorkerState::RollCall
        );
        if !eligible_voter {
            self.send_vote_reject(req, VoteRejectReason::NotVoter);
            return;
        }
        if req.recovery_epoch != self.recovery_epoch {
            self.send_vote_reject(req, VoteRejectReason::WrongRecoveryEpoch);
            return;
        }
        if self.voted_for.contains_key(&req.term) {
            // Applies even when the request comes from the candidate already
            // voted for.
            self.send_vote_reject(req, VoteRejectReason::AlreadyVoted);
            return;
        }
        if req.term <= self.highest_term_seen {
            self.send_vote_reject(req, VoteRejectReason::StaleTerm);
            return;
        }
        if self.current_leader_still_valid() {
            self.send_vote_reject(req, VoteRejectReason::LeaderStillValid);
            return;
        }

        self.highest_term_seen = req.term;
        self.voted_for.insert(req.term, req.candidate_id());
        self.transport.send(
            self.my_id.clone(),
            req.candidate_id(),
            ElectionMessage {
                payload: Some(election_message::Payload::VoteGrant(VoteGrant {
                    shard_id: Some(self.shard_id.clone().into()),
                    recovery_epoch: self.recovery_epoch,
                    term: req.term,
                    candidate_id: Some(req.candidate_id().into()),
                    voter_id: Some(self.my_id.clone().into()),
                })),
            },
        );
    }

    fn send_vote_reject(&self, req: &VoteRequest, reason: VoteRejectReason) {
        self.transport.send(
            self.my_id.clone(),
            req.candidate_id(),
            ElectionMessage {
                payload: Some(election_message::Payload::VoteReject(VoteReject {
                    shard_id: Some(self.shard_id.clone().into()),
                    recovery_epoch: self.recovery_epoch,
                    term: req.term,
                    candidate_id: Some(req.candidate_id().into()),
                    voter_id: Some(self.my_id.clone().into()),
                    reason: reason as i32,
                })),
            },
        );
    }

    fn current_leader_still_valid(&self) -> bool {
        self.clock.now() - self.last_leader_contact <= self.suspect_timeout
    }

    /// Records a vote for this node's candidacy, then checks whether it has
    /// won. Ignored unless this node is a `Candidate` and the grant is for
    /// its own candidacy: same term, shard and recovery epoch, addressed to
    /// this node, and cast by a member of the current electorate.
    pub fn on_vote_grant(&mut self, grant: &VoteGrant) {
        if self.state != WorkerState::Candidate || grant.term != self.term {
            return;
        }
        if grant.shard_id() != self.shard_id
            || grant.recovery_epoch != self.recovery_epoch
            || grant.candidate_id() != self.my_id
        {
            return;
        }
        let voter = grant.voter_id();
        if !self.membership.effective_electorate().contains(&voter) {
            return;
        }

        self.votes_received.insert(voter);
        self.maybe_win_election();
    }

    /// Deliberately does nothing: a candidate either collects enough grants
    /// or the attempt does not converge and a later suspicion cycle retries.
    pub fn on_vote_reject(&mut self, _reject: &VoteReject) {}

    /// Once a majority of the electorate has granted this candidacy,
    /// broadcasts an `ElectionCertificate` (README §12.6) to the granting
    /// voters and becomes `Leader`. Leader reconciliation (README §13) needs
    /// task data that does not exist yet, so `LeaderReconciling` is passed
    /// through immediately; the transition table has no direct
    /// `Candidate -> Leader` edge.
    fn maybe_win_election(&mut self) {
        let quorum = self.membership.effective_electorate().len() / 2 + 1;
        if self.votes_received.len() < quorum {
            return;
        }

        let certificate = ElectionCertificate {
            shard_id: Some(self.shard_id.clone().into()),
            recovery_epoch: self.recovery_epoch,
            term: self.term,
            leader_id: Some(self.my_id.clone().into()),
            granting_voters: self
                .votes_received
                .iter()
                .cloned()
                .map(Into::into)
                .collect(),
            membership_generation: self.membership.membership_generation(),
        };
        self.transport.broadcast(
            self.my_id.clone(),
            self.votes_received.clone(),
            ElectionMessage {
                payload: Some(election_message::Payload::ElectionCertificate(certificate)),
            },
        );

        self.state = WorkerState::LeaderReconciling;
        self.state = WorkerState::Leader;
    }

    /// Forced recovery of a `NoQuorum` node through the coordination authority
    /// (README §14.3); a no-op in any other state.
    ///
    /// It intersects the authority's view with the peers this node can reach.
    /// If that is empty, the authority is unavailable, the authority's epoch is
    /// behind this node's own, or the authority rejects the compare-and-swap,
    /// the node stays in `NoQuorum` and a later call retries.
    /// Otherwise it adopts the new epoch and the reachable set as its
    /// electorate and starts a roll call.
    pub fn attempt_forced_recovery(&mut self) {
        if self.state != WorkerState::NoQuorum {
            return;
        }

        let Ok(authority_view) = self.authority.discover_workers(&self.shard_id) else {
            return;
        };

        let locally_reachable = self.transport.reachable_peers(self.my_id.clone());
        let mut reachable: BTreeSet<WorkerId> = authority_view
            .intersection(&locally_reachable)
            .cloned()
            .collect();

        // Checked before adding this node: a lone node with no confirmed
        // peer must not "recover" a shard on its own say-so.
        if reachable.is_empty() {
            return;
        }
        reachable.insert(self.my_id.clone());

        let Ok(old_epoch) = self.authority.read_recovery_epoch(&self.shard_id) else {
            return;
        };
        // An authority behind this node (wiped or restored from an old backup)
        // would hand back an epoch this node has already used; adopting it
        // would let peers still on the higher epoch be mistaken for stale.
        if old_epoch < self.recovery_epoch {
            return;
        }
        let Ok(new_epoch) =
            self.authority
                .force_reconfigure(&self.shard_id, old_epoch, reachable.clone())
        else {
            return;
        };

        self.recovery_epoch = new_epoch;
        self.membership.rebuild(reachable);
        self.begin_roll_call();
    }
}
