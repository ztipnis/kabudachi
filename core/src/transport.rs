//! The peer-messaging abstraction the election state machine depends on: send
//! a message, drain your inbox, ask who you can reach.
//!
//! `reachable_peers` is a simulator-style oracle. A real transport can only
//! infer reachability from observed traffic, so election logic proven against
//! it assumes a perfect failure detector. The trait is generic (through
//! `broadcast`'s `impl IntoIterator`), so it is not `dyn`-compatible.

use std::collections::BTreeSet;

use crate::protocol::ids::WorkerId;
use crate::protocol::messages::ElectionMessage;

pub trait PeerMessenger {
    /// Delivery need not be immediate or reliable: an implementor may delay,
    /// drop, duplicate or reorder. The recipient sees delivered messages
    /// through `poll_inbox`.
    fn send(&self, from: WorkerId, to: WorkerId, message: ElectionMessage);

    fn broadcast(
        &self,
        from: WorkerId,
        to: impl IntoIterator<Item = WorkerId>,
        message: ElectionMessage,
    ) {
        for peer in to {
            self.send(from.clone(), peer, message.clone());
        }
    }

    /// Drains every message queued for `me`, each with its sender.
    fn poll_inbox(&self, me: WorkerId) -> Vec<(WorkerId, ElectionMessage)>;

    /// The other workers `me` can currently reach (never `me` itself). This is
    /// network-level only and knows nothing of ring structure.
    fn reachable_peers(&self, me: WorkerId) -> BTreeSet<WorkerId>;
}
