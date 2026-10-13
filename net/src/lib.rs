//! libp2p-based network transport for kabudachi. This crate is the only
//! place in the workspace allowed to depend on `libp2p`; `core` remains
//! transport-agnostic (its `WorkerNode` does no I/O, and `crate::driver`
//! carries its messages over the swarm built here). `bindings` also depends
//! on `tokio`, to bridge the native runtime to Python, but not on `libp2p`.

pub mod authority;
pub mod blob_exchange;
pub mod blobs;
pub mod bootstrap;
pub mod claim;
pub mod claimed_runs;
pub mod codec;
pub mod discovery;
pub mod driver;
mod exchange;
pub mod executor;
mod framing;
pub mod handoff;
pub mod join;
pub mod join_codec;
mod leader_search;
pub mod messenger;
pub mod payload_memory;
mod peers;
pub mod reconcile;
pub mod result_transport;
mod routing_refresh;
pub mod steal;
pub mod swarm;
pub mod task_exchange;
pub mod task_store;
mod wait_log;
pub mod worker;
