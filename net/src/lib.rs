//! libp2p-based network transport for kabudachi. This crate is the only
//! place in the workspace allowed to depend on `libp2p`/`tokio`; `core`
//! remains transport-agnostic (see `core::transport::PeerMessenger`, which a
//! later chunk implements on top of the swarm built here).

pub mod bootstrap;
pub mod claim_codec;
pub mod codec;
pub mod driver;
mod framing;
pub mod join_codec;
pub mod messenger;
pub mod swarm;
