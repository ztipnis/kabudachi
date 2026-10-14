pub mod blob;
pub(crate) mod coalescing;
pub mod configuration;
pub mod coordination_authority;
pub mod election;
pub mod hashing;
pub mod in_memory_authority;
pub mod manifest;
pub mod protocol;
pub mod queues;
pub mod reconcile;
pub mod result_backend;
pub mod scheduler;
pub mod task_record;
pub mod time;
pub mod versions;

pub fn version() -> &'static str {
    "0.1.0"
}
