//! The election state machine's behaviour, one module per area.

mod bootstrap_join;
mod carry_out;
mod certificate;
mod forced_recovery;
mod heartbeat;
mod join_floor;
mod joint_founding;
mod leader_heartbeat;
mod membership;
mod no_quorum;
mod reconciling;
mod roll_call;
mod step_down;
mod step;
mod timing;
mod voting;
