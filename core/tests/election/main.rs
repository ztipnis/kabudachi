//! The election state machine's behaviour, one module per ADR-0001 area.
//! Shared helpers come from `../support` (see `support/mod.rs`).

#[path = "../support/mod.rs"]
mod support;

mod bootstrap_join;
mod carry_out;
mod certificate;
mod forced_recovery;
mod heartbeat;
mod joint_founding;
mod leader_heartbeat;
mod membership;
mod no_quorum;
mod roll_call;
mod step_down;
mod step;
mod timing;
mod voting;
