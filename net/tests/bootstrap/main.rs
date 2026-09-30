//! Bootstrap, self-election, and peer routing over real sockets.
//! Shared helpers come from `../support` (see `support/mod.rs`).

#[path = "../support/mod.rs"]
mod support;

mod bootstrap_join;
mod bootstrap_self_elect;
