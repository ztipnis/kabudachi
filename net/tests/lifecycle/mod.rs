//! A worker's calls to the leader about tasks and runs, and results that
//! reach their client, over real sockets.

mod compaction;
mod composites;
mod lifecycle;
mod own_requests;
mod results;
