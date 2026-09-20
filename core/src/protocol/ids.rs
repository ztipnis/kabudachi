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
    /// Identifies a worker (a process participating in the cluster).
    WorkerId
);
id_newtype!(
    /// Identifies a single worker incarnation (distinguishes restarts of the
    /// same `WorkerId` from one another).
    IncarnationId
);
id_newtype!(
    /// Identifies a shard (an independently-elected partition of the cluster).
    ShardId
);
id_newtype!(
    /// Identifies a task (the durable, user-defined unit of work).
    TaskId
);
id_newtype!(
    /// Identifies a single run (attempt) of a task.
    TaskRunId
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn worker_id_round_trips_through_generated_type() {
        let id = WorkerId::new("worker-1");
        let raw: generated::WorkerId = id.clone().into();
        assert_eq!(raw.value, "worker-1");
        let back: WorkerId = raw.into();
        assert_eq!(id, back);
    }

    #[test]
    fn shard_id_round_trips_through_generated_type() {
        let id = ShardId::new("shard-a".to_string());
        let raw: generated::ShardId = id.clone().into();
        let back: ShardId = raw.into();
        assert_eq!(id, back);
    }

    #[test]
    fn task_run_id_round_trips_through_generated_type() {
        let id = TaskRunId::new("run-42");
        let raw: generated::TaskRunId = id.clone().into();
        let back: TaskRunId = raw.into();
        assert_eq!(id, back);
    }

    #[test]
    fn as_str_returns_inner_value() {
        let id = TaskId::new("task-7");
        assert_eq!(id.as_str(), "task-7");
    }

    #[test]
    fn equal_values_compare_equal_and_hash_equal() {
        let a = WorkerId::new("same");
        let b = WorkerId::new("same");
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn different_values_compare_unequal() {
        let a = WorkerId::new("one");
        let b = WorkerId::new("two");
        assert_ne!(a, b);
    }

    #[test]
    fn ord_is_consistent_with_string_ord() {
        let a = WorkerId::new("a");
        let b = WorkerId::new("b");
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn ids_work_as_btree_map_keys_in_sorted_order() {
        let mut map = std::collections::BTreeMap::new();
        map.insert(WorkerId::new("charlie"), 3);
        map.insert(WorkerId::new("alice"), 1);
        map.insert(WorkerId::new("bob"), 2);

        let keys: Vec<&str> = map.keys().map(WorkerId::as_str).collect();
        assert_eq!(keys, vec!["alice", "bob", "charlie"]);
    }

    #[test]
    fn ids_work_as_hash_set_members() {
        let mut set = std::collections::HashSet::new();
        set.insert(IncarnationId::new("x"));
        set.insert(IncarnationId::new("x"));
        set.insert(IncarnationId::new("y"));
        assert_eq!(set.len(), 2);
    }
}
