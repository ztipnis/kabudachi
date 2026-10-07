//! Integration tests for `kabudachi-net` over real sockets, built as one
//! binary with a module per area. Shared helpers live in `support`.

mod bootstrap;
mod claim;
mod discovery;
mod driver;
mod election;
mod join;
mod lifecycle;
mod reconcile;
mod records;
mod support;
mod transport;
