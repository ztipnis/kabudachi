//! Claim arbitration, replacement and gossip publishing over real sockets.
//! Shared helpers come from `../support` (see `support/mod.rs`).

#[path = "../support/mod.rs"]
mod support;

mod claim_arbitration;
mod gossip_publish;
mod reconnect_timeout_replacement;
