//! libp2p-based network transport for kabudachi. This crate is the only
//! place in the workspace allowed to depend on `libp2p`; `core` remains
//! transport-agnostic (its `WorkerNode` does no I/O, and `crate::driver`
//! carries its messages over the swarm built here). `bindings` also depends
//! on `tokio`, to bridge the native runtime to Python, but not on `libp2p`.

pub mod bootstrap;
pub mod claim;
pub mod codec;
pub mod driver;
mod exchange;
mod framing;
pub mod join;
pub mod join_codec;
pub mod messenger;
mod peers;
pub mod swarm;
pub mod worker;
