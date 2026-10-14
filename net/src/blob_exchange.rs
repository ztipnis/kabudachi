//! How a worker asks a holder of a blob's owner's record for the blob
//! directly. The swarm task answers from its own store, so a worker answers
//! whatever its driver is doing, and with no driver at all.

pub mod codec;
