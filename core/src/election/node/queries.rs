use super::super::JoinFloor;
use super::super::{ElectionTimings, StopReason, WorkerNode, led_or_followed};
use crate::configuration::{Admission, Configuration, Generation};
use crate::protocol::ids::{ShardId, WorkerId};
use crate::protocol::messages::JoinResponse;
use crate::protocol::worker_state::WorkerState;
use crate::time::{Clock, Instant};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    pub fn state(&self) -> WorkerState {
        self.state
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    /// This node's clock's reading now: the time base of every instant it
    /// reports, such as a grant's lease end.
    pub fn now(&self) -> Instant {
        self.clock.now()
    }

    /// The timers this node runs its election on.
    pub fn timings(&self) -> ElectionTimings {
        self.timings
    }

    /// The id this node was started under (see [`Identity::id`]).
    pub fn id(&self) -> &WorkerId {
        &self.my_id
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    /// The highest term this node has seen: raised by a vote it granted, an
    /// ack it accepted, a refusal, a certificate or a heartbeat naming a
    /// later term, the leader a JOIN pointed it at, winning an election, and
    /// standing through the authority path. Never by a roll call it
    /// answered, nor by standing as a candidate in an election it has not
    /// won.
    pub fn highest_term_seen(&self) -> u64 {
        self.standing.highest_term_seen()
    }

    /// This node's recovery epoch: its configuration's when built with one
    /// ([`Self::start`]), taken from the leader a JOIN pointed it at
    /// ([`Input::JoinAnswer`]), moved on by an authority-path recovery of
    /// its own, and adopted from the ack of a leader of a later epoch.
    pub fn recovery_epoch(&self) -> u64 {
        self.standing.epoch_number()
    }

    /// The floor this node takes JOIN pointers against: its recovery epoch,
    /// or none before it has joined a shard. A driver searching for a leader
    /// for it takes the node's current floor for each pass, as the node's
    /// floor moves with the epoch reads it is told of.
    pub fn join_floor(&self) -> JoinFloor {
        self.standing.join_floor()
    }

    /// The lineage of this node's recovery epoch (see [`RecoveryEpoch`]),
    /// learned with the epoch; `None` until the node has joined a shard.
    pub fn recovery_lineage(&self) -> Option<u64> {
        self.standing.epoch().map(|epoch| epoch.lineage)
    }

    /// Why this node stopped; `None` unless it is `Stopped`.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    /// The configuration this node knows: the one it was built with or the
    /// newest a leader's ack or election certificate has carried since.
    /// `None` for a joiner that has accepted neither yet.
    pub fn configuration(&self) -> Option<&Configuration> {
        self.led_or_followed_configuration()
    }

    /// Whether this worker may start a TaskRun now: only once it could abort
    /// the run in time if it lost its leader's ear, which needs a leader to
    /// have vouched for hearing it (an ack echoing one of its heartbeats), or
    /// a grant of its own that ends, or a grant no rival can outlast. Before
    /// then no abort deadline could be named for the run (see
    /// [`Output::AbortDeadline`]).
    pub fn has_contact_floor(&self) -> bool {
        self.lease.has_contact_floor()
    }

    /// Whether this node has completed a routing crawl since it was admitted
    /// at the admission generation it holds now. Its heartbeats carry it.
    pub fn routing_crawled(&self) -> bool {
        self.admission().is_some() && self.crawled_at_admission == self.admission()
    }

    /// The generation at which this node was admitted or promised admission
    /// as a voter; `None` for a pending member. A promise outruns the
    /// configuration that will count it, so this alone does not make the
    /// node a voter of the configuration it holds.
    pub fn admission(&self) -> Option<Generation> {
        self.counted_admission().current
    }

    /// The admission generation this node held before the election that
    /// founded the joint configuration it holds admitted it; `None` once
    /// that configuration is committed, and for any other configuration.
    pub fn prior_admission(&self) -> Option<Generation> {
        self.counted_admission().prior
    }

    /// Whether this node has no admission generation: it joined through a
    /// JOIN and neither a leader's ack nor an election certificate has
    /// admitted or promised it admission since. It claims work, and it answers roll calls and
    /// grants votes as a new voter, but no quorum counts it.
    pub fn is_pending_member(&self) -> bool {
        self.counted_admission().current.is_none()
    }

    /// The leader this node would point a joining worker at, with the term
    /// that leader was elected in: itself while `Leader`, and while `Active`
    /// the leader whose heartbeat ack it last accepted, or that a JOIN
    /// pointed it at if no ack has arrived since.
    ///
    /// A node with no configuration (a joiner that has accepted no ack yet)
    /// also keeps naming that leader while `LeaderSuspect`: it cannot elect a
    /// replacement (it starts no roll call), it keeps heartbeating that
    /// leader until an ack returns it to `Active`, and dropping its leader
    /// would leave every joiner it answers with nowhere to go. If that leader
    /// has since gone, the pointer costs a joiner time but nothing worse: the
    /// joiner cannot connect to the named leader, so it passes the answer
    /// over and keeps asking its seeds.
    ///
    /// `None` in every other state: a node that suspects its leader or is
    /// electing a new one has no leader it can vouch for. `None` too once
    /// this node has seen a term later than the named leader's (by granting
    /// a vote in it, say): a newer leader may lead by then.
    pub fn known_leader(&self) -> Option<(WorkerId, u64)> {
        let named = match self.state {
            WorkerState::LeaderReconciling | WorkerState::Leader => {
                Some((self.my_id.clone(), self.term))
            }
            WorkerState::Active => self.leader.clone(),
            WorkerState::LeaderSuspect if self.led_or_followed_configuration().is_none() => {
                self.leader.clone()
            }
            _ => None,
        };
        named.filter(|(_, term)| *term >= self.standing.highest_term_seen())
    }

    /// The `JOIN_RESPONSE` this node hands a joiner right now: the leader
    /// [`Self::known_leader`] names (itself while `Leader`), with that
    /// leader's term, at `leader_addr`, and this node's recovery epoch and
    /// its lineage. `None` when it knows no leader.
    ///
    /// `leader_addr` is the named leader's address, which the node does not
    /// know: its driver resolves it (see `known_leader` for whom to resolve).
    pub fn join_response(&self, leader_addr: String) -> Option<JoinResponse> {
        let (leader_id, term) = self.known_leader()?;
        Some(JoinResponse {
            leader_id: Some(leader_id.into()),
            leader_multiaddr: leader_addr,
            term,
            recovery_epoch: self.standing.epoch_number(),
            recovery_epoch_lineage: self.recovery_lineage().unwrap_or_default(),
            shard_id: Some(self.shard_id.clone().into()),
        })
    }

    /// The configuration this node leads while it leads, and the one it
    /// follows otherwise.
    pub(in crate::election) fn led_or_followed_configuration(&self) -> Option<&Configuration> {
        led_or_followed(&self.office, &self.standing)
    }

    /// The admission generations a quorum counts this node by: those of the
    /// roster it leads while it leads, and those it follows otherwise.
    pub(in crate::election) fn counted_admission(&self) -> Admission {
        match &self.office {
            Some(office) => Admission {
                current: office.admission_of(&self.my_id),
                prior: office.prior_admission_of(&self.my_id),
            },
            None => self.standing.counted_admission(),
        }
    }
}
