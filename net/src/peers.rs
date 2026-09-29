//! What `crate::messenger`'s swarm task knows about the peers around it:
//! which are connected, the address of record for each, this node's own
//! address, which peers it is redialing, which share its shard's gossip
//! topic and mesh, and how much traffic it has carried. The swarm task owns
//! [`Peers`] outright and is the only code that changes it; a `Net` reads it
//! by asking the task (see `Net::diagnostics`), except for this node's own
//! address, which the task publishes on a `watch` channel because a node's
//! driver needs it without waiting (to register itself, say).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use libp2p::{Multiaddr, PeerId, Swarm, gossipsub};
use tokio::sync::watch;

use crate::messenger::{Diagnostics, KnownAddress, RedialPolicy, Traffic, worker_id_of};
use crate::swarm::Behaviour;

/// The swarm task's own record of its peers (see the module doc).
pub(crate) struct Peers {
    /// The peers the swarm holds a connection to, as of the last
    /// [`Self::refresh`].
    pub(crate) connected: BTreeSet<PeerId>,
    /// The best address known for each peer, ranked by where it came from
    /// (see `crate::messenger::AddressSource`). A peer that disconnects keeps its entry,
    /// which is where a redial finds the address to dial.
    pub(crate) addresses: BTreeMap<PeerId, KnownAddress>,
    /// The address this node gives other nodes for itself.
    pub(crate) local_addr: watch::Sender<Option<Multiaddr>>,
    pub(crate) redial: RedialTracker,
    /// The connected peers whose subscription to this node's shard topic
    /// has reached it.
    subscribers: BTreeSet<PeerId>,
    /// The peers in this node's gossip mesh for its shard.
    mesh: BTreeSet<PeerId>,
    /// The peer each address a JOIN asked turned out to be (see
    /// `crate::join`), so a peer asked again is asked over the connection
    /// it already has.
    pub(crate) asked: HashMap<Multiaddr, PeerId>,
    pub(crate) traffic: Traffic,
}

impl Peers {
    pub(crate) fn new(local_addr: watch::Sender<Option<Multiaddr>>, policy: RedialPolicy) -> Self {
        Peers {
            connected: BTreeSet::new(),
            addresses: BTreeMap::new(),
            local_addr,
            redial: RedialTracker::new(policy),
            subscribers: BTreeSet::new(),
            mesh: BTreeSet::new(),
            asked: HashMap::new(),
            traffic: Traffic::default(),
        }
    }

    /// Everything a `Net` reports about its peers, as of now.
    pub(crate) fn snapshot(&self) -> Diagnostics {
        Diagnostics {
            connected: self.connected.iter().map(worker_id_of).collect(),
            peer_addresses: self
                .addresses
                .iter()
                .map(|(peer, known)| (worker_id_of(peer), known.addr.clone()))
                .collect(),
            local_addr: self.local_addr.borrow().clone(),
            redial_attempts: self
                .redial
                .pending
                .iter()
                .map(|(peer, attempt)| (worker_id_of(peer), attempt.attempts_made))
                .collect(),
            shard_subscribers: self.subscribers.iter().map(worker_id_of).collect(),
            shard_mesh: self.mesh.iter().map(worker_id_of).collect(),
            traffic: self.traffic,
        }
    }

    /// Whether the swarm held a connection to `peer` as of the last
    /// [`Self::refresh`].
    pub(crate) fn is_connected(&self, peer: &PeerId) -> bool {
        self.connected.contains(peer)
    }

    /// Re-reads the swarm's connections, gossip mesh and shard subscribers
    /// after the swarm task has handled an event or a command, and hands
    /// the change in connections to the redial policy (see
    /// [`RedialTracker::connections_changed`]).
    pub(crate) fn refresh(&mut self, swarm: &Swarm<Behaviour>) {
        let now_connected: BTreeSet<PeerId> = swarm.connected_peers().copied().collect();
        // The mesh as of the last refresh: by the time a drop shows in the
        // connected set, gossipsub has already taken the peer out of it.
        self.redial.connections_changed(
            &self.connected,
            &now_connected,
            &self.mesh,
            &self.addresses,
        );
        self.connected = now_connected;

        let gossipsub = &swarm.behaviour().gossipsub;
        self.mesh = gossipsub.all_mesh_peers().copied().collect();
        let my_topics: BTreeSet<&gossipsub::TopicHash> = gossipsub.topics().collect();
        self.subscribers = gossipsub
            .all_peers()
            .filter(|(_, topics)| topics.iter().any(|topic| my_topics.contains(topic)))
            .map(|(peer, _)| *peer)
            .collect();
    }
}

/// The bounded redial schedule of [`RedialPolicy`]: which dropped peers are
/// being redialed, and how far along each is (see `crate::messenger`'s
/// "Reconnect/backoff" for which drops qualify).
pub(crate) struct RedialTracker {
    policy: RedialPolicy,
    /// Peers this node itself asked to disconnect: a local decision, not a
    /// failure, so never redialed. A peer leaves the set once it is seen to
    /// reconnect, so a later drop of it is judged afresh.
    locally_disconnected: HashSet<PeerId>,
    /// Every peer being redialed, working toward the policy's cap.
    pending: HashMap<PeerId, RedialAttempt>,
}

/// One peer's redial schedule.
struct RedialAttempt {
    attempts_made: u32,
    next_backoff: std::time::Duration,
    next_attempt_at: tokio::time::Instant,
    addr: Multiaddr,
}

impl RedialTracker {
    fn new(policy: RedialPolicy) -> Self {
        RedialTracker {
            policy,
            locally_disconnected: HashSet::new(),
            pending: HashMap::new(),
        }
    }

    /// How often the swarm task asks for [`Self::due`] redials.
    pub(crate) fn check_interval(&self) -> std::time::Duration {
        self.policy.check_interval
    }

    /// Notes that this node asked to disconnect `peer`: it is not redialed,
    /// and a redial already scheduled for it is dropped.
    pub(crate) fn disconnected_locally(&mut self, peer: PeerId) {
        self.locally_disconnected.insert(peer);
        self.pending.remove(&peer);
    }

    /// The redials due at `now`, each a peer and the address to dial it at.
    /// Each counts as an attempt; a peer that has used up the policy's
    /// attempts is given up on instead. Whether a redial worked shows in a
    /// later [`Peers::refresh`], which stops redialing a peer once it is
    /// connected again.
    pub(crate) fn due(&mut self, now: tokio::time::Instant) -> Vec<(PeerId, Multiaddr)> {
        let max_attempts = self.policy.max_attempts;
        let max_backoff = self.policy.max_backoff;
        self.pending.retain(|_, attempt| {
            attempt.next_attempt_at > now || attempt.attempts_made < max_attempts
        });
        let mut dials = Vec::new();
        for (peer, attempt) in &mut self.pending {
            if attempt.next_attempt_at > now {
                continue;
            }
            dials.push((*peer, attempt.addr.clone()));
            attempt.attempts_made += 1;
            attempt.next_backoff = std::cmp::min(attempt.next_backoff * 2, max_backoff);
            attempt.next_attempt_at = now + attempt.next_backoff;
        }
        dials
    }

    /// Updates the schedule for a change in connections from `before` to
    /// `now`: a peer connected again is done being redialed; a peer seen to
    /// reconnect is no longer excluded for having been disconnected locally;
    /// and a peer that dropped, was in the gossip mesh (`meshed`), was not
    /// disconnected locally and has an address on file is scheduled for its
    /// first redial.
    fn connections_changed(
        &mut self,
        before: &BTreeSet<PeerId>,
        now: &BTreeSet<PeerId>,
        meshed: &BTreeSet<PeerId>,
        addresses: &BTreeMap<PeerId, KnownAddress>,
    ) {
        self.pending.retain(|peer, _| !now.contains(peer));
        // Only a peer absent before and present now has reconnected. A
        // disconnect this node just asked for does not show in the swarm's
        // connections until a later refresh, so "connected now" alone would
        // lift the exclusion before the connection had even closed.
        for peer in now.difference(before) {
            self.locally_disconnected.remove(peer);
        }
        for peer in before.difference(now) {
            if self.locally_disconnected.contains(peer) || !meshed.contains(peer) {
                continue;
            }
            let Some(addr) = addresses.get(peer).map(|known| known.addr.clone()) else {
                continue;
            };
            let first_backoff = self.policy.initial_backoff;
            self.pending.entry(*peer).or_insert_with(|| RedialAttempt {
                attempts_made: 0,
                next_backoff: first_backoff,
                next_attempt_at: tokio::time::Instant::now() + first_backoff,
                addr,
            });
        }
    }
}
