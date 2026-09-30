//! Property tests over configuration, flow and leadership invariants.
//! Shared helpers come from `../support` (see `support/mod.rs`).

#[path = "../support/mod.rs"]
mod support;

mod proptest_configuration_invariants;
mod proptest_decode;
mod proptest_flow_invariants;
mod proptest_leadership_invariants;
