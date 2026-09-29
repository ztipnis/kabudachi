//! Elections, membership and failover over real sockets.
//! Shared helpers come from `../support` (see `support/mod.rs`).

#[path = "../support/mod.rs"]
mod support;

mod authority_split_brain;
mod indirect_initiator_election;
mod leader_loss;
mod membership_over_real_sockets;
mod orphaning;
mod three_node_join;
