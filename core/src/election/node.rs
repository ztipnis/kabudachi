//! What a [`WorkerNode`](super::WorkerNode) does, one concern per module:
//! what it reads of itself and of the office it holds, how a step runs and
//! moves its state, how it follows a leader and leads one, its timers, its
//! part in elections and refusals, how it joins and rejoins, its standing
//! with the coordination authority, and how it departs.

mod follower;
mod leader;
mod office_reads;
mod queries;
mod step;
