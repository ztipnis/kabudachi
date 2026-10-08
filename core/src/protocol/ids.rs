//! Newtypes over the generated `{ value: String }` ID messages, so a
//! `WorkerId` can never be used where a `ShardId` is expected. `Ord` and `Hash`
//! are load-bearing: `BTreeMap`/`BTreeSet` keyed by these give deterministic
//! iteration order in simulations.

use crate::protocol::generated;

macro_rules! id_newtype {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                $name(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<generated::$name> for $name {
            fn from(raw: generated::$name) -> Self {
                $name(raw.value)
            }
        }

        impl From<$name> for generated::$name {
            fn from(id: $name) -> Self {
                generated::$name { value: id.0 }
            }
        }
    };
}

id_newtype!(
    /// Identifies a worker: one process incarnation participating in the
    /// cluster. A restarted process comes back under a new `WorkerId`, as a
    /// pending joiner, and the old one only ever leaves: the
    /// election's safety relies on no `WorkerId` voting again in a term it
    /// has already voted in, and a process keeps no record of its votes
    /// across a restart.
    WorkerId
);
id_newtype!(
    /// Identifies a single worker incarnation, carried on the wire beside
    /// its `WorkerId`. Since a `WorkerId` already names one incarnation,
    /// the two change together.
    IncarnationId
);
id_newtype!(
    /// Identifies a shard (an independently-elected partition of the cluster).
    ShardId
);

/// The name an operator gives a shard. It never changes: it names the shard's
/// gossip topic and its key space at the coordination authority. Each
/// incarnation of the shard under that name has its own [`ShardId`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardName(String);

impl ShardName {
    pub fn new(value: impl Into<String>) -> Self {
        ShardName(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ShardName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

id_newtype!(
    /// Identifies a task (the durable, user-defined unit of work).
    TaskId
);
id_newtype!(
    /// Identifies a single run (attempt) of a task.
    TaskRunId
);
id_newtype!(
    /// Identifies a registered task definition (the code a Task invokes), by
    /// a name that stays stable across processes and restarts.
    TaskDefinitionId
);

/// Mints the IDs of new Tasks and TaskRuns. Clients and workers mint IDs
/// independently, so an implementation must never hand out the same ID twice,
/// even across processes.
///
/// An ID must be at most [`MAX_ID_BYTES`] long. The scheduler's fixed claim
/// overhead reserve counts on it, so that a task admitted within
/// `MAX_SUBMISSION_BYTES` always fits one claim message. The scheduler checks
/// every ID it mints and panics on a longer one.
pub trait IdGenerator {
    fn next_task_id(&self) -> TaskId;
    fn next_task_run_id(&self) -> TaskRunId;
}

/// The longest a generated task or run ID may be, in bytes. A UUID is 36.
pub const MAX_ID_BYTES: usize = 256;

/// A new task ID from `ids`, checked against [`MAX_ID_BYTES`].
pub(crate) fn mint_task_id(ids: &impl IdGenerator) -> TaskId {
    let id = ids.next_task_id();
    assert!(
        id.as_str().len() <= MAX_ID_BYTES,
        "IdGenerator returned a task ID of {} bytes; the limit is {MAX_ID_BYTES}",
        id.as_str().len()
    );
    id
}

/// A new run ID from `ids`, checked against [`MAX_ID_BYTES`].
pub(crate) fn mint_task_run_id(ids: &impl IdGenerator) -> TaskRunId {
    let id = ids.next_task_run_id();
    assert!(
        id.as_str().len() <= MAX_ID_BYTES,
        "IdGenerator returned a task run ID of {} bytes; the limit is {MAX_ID_BYTES}",
        id.as_str().len()
    );
    id
}

/// Random, time-ordered UUIDv7 IDs, for real deployments. It reads the system
/// clock and a random source, so a simulation injects its own generator
/// instead.
#[derive(Debug, Clone, Copy, Default)]
pub struct Uuid7Ids;

impl IdGenerator for Uuid7Ids {
    fn next_task_id(&self) -> TaskId {
        TaskId::new(uuid::Uuid::now_v7().to_string())
    }

    fn next_task_run_id(&self) -> TaskRunId {
        TaskRunId::new(uuid::Uuid::now_v7().to_string())
    }
}
