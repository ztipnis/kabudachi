//! Scheduler behaviour: backpressure, cancellation, retry, timing and task records.
//! Shared helpers come from `../support` (see `support/mod.rs`).

#[path = "../support/mod.rs"]
mod support;

mod scheduler_backpressure;
mod scheduler_cancel;
mod scheduler_coalescing;
mod scheduler_continuation;
mod scheduler_failure;
mod scheduler_lifecycle;
mod scheduler_loss;
mod scheduler_observer;
mod scheduler_retry;
mod scheduler_timing;
mod task_records;
