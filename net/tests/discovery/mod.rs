//! How a worker with room finds work: its own records, shard peers over the
//! steal exchange, then the leader.

mod backoff;
mod find;
mod steal;
