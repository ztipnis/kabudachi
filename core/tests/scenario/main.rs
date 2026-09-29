//! Multi-node scenarios driven through the simulated network.
//! Shared helpers come from `../support` (see `support/mod.rs`).

#[path = "../support/mod.rs"]
mod support;

mod harness;
mod network;
mod scenario_catastrophic_authority;
mod scenario_election;
mod scenario_membership;
mod scenario_no_quorum;
mod scenario_partition;
