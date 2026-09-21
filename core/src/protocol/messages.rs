//! Election message types re-exported from [`generated`], with typed ID
//! accessors as extension traits (import [`prelude`]).
//!
//! prost makes every message-typed field an `Option`. Fields that are always
//! present by protocol invariant get an accessor returning the `ids::*`
//! newtype, which panics if the field is absent (a malformed message is a peer
//! bug, not optionality to handle). Proto `optional` fields return `Option`.

use crate::protocol::generated;
use crate::protocol::ids;

pub use generated::{
    ElectionCertificate, ElectionMessage, LeaderHeartbeatAck, RollCall, RollCallObservation,
    SelfRemove, Task, TaskRun, TaskRunIdentity, VoteGrant, VoteReject, VoteRejectReason,
    VoteRequest, WorkerHeartbeat, election_message,
};

/// Defines the extension trait `$ext` with typed accessors for `$msg`'s ID
/// fields: `required` (panics if absent), `optional` and `repeated`. A trait
/// rather than an inherent `impl` because the generated types live in another
/// crate.
macro_rules! id_accessors {
    (
        $ext:ident for $msg:ident {
            required: [$($required:ident: $required_id:ident),*],
            optional: [$($optional:ident: $optional_id:ident),*],
            repeated: [$($repeated:ident: $repeated_id:ident),*] $(,)?
        }
    ) => {
        pub trait $ext {
            $(fn $required(&self) -> ids::$required_id;)*
            $(fn $optional(&self) -> Option<ids::$optional_id>;)*
            $(fn $repeated(&self) -> Vec<ids::$repeated_id>;)*
        }

        impl $ext for $msg {
            $(
                fn $required(&self) -> ids::$required_id {
                    self.$required
                        .clone()
                        .unwrap_or_else(|| {
                            panic!(
                                "{}.{} is required by protocol invariant but was absent",
                                stringify!($msg),
                                stringify!($required)
                            )
                        })
                        .into()
                }
            )*
            $(
                fn $optional(&self) -> Option<ids::$optional_id> {
                    self.$optional.clone().map(Into::into)
                }
            )*
            $(
                fn $repeated(&self) -> Vec<ids::$repeated_id> {
                    self.$repeated.iter().cloned().map(Into::into).collect()
                }
            )*
        }
    };
}

id_accessors!(WorkerHeartbeatIds for WorkerHeartbeat {
    required: [worker_id: WorkerId, incarnation_id: IncarnationId],
    optional: [],
    repeated: [],
});
id_accessors!(LeaderHeartbeatAckIds for LeaderHeartbeatAck {
    required: [shard_id: ShardId, leader_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(RollCallObservationIds for RollCallObservation {
    required: [worker_id: WorkerId],
    optional: [current_leader_seen: WorkerId],
    repeated: [],
});
id_accessors!(RollCallIds for RollCall {
    required: [shard_id: ShardId, initiator_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(VoteRequestIds for VoteRequest {
    required: [shard_id: ShardId, candidate_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(VoteGrantIds for VoteGrant {
    required: [shard_id: ShardId, candidate_id: WorkerId, voter_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(VoteRejectIds for VoteReject {
    required: [shard_id: ShardId, candidate_id: WorkerId, voter_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(ElectionCertificateIds for ElectionCertificate {
    required: [shard_id: ShardId, leader_id: WorkerId],
    optional: [],
    repeated: [granting_voters: WorkerId],
});
id_accessors!(SelfRemoveIds for SelfRemove {
    required: [worker_id: WorkerId, incarnation_id: IncarnationId, shard_id: ShardId],
    optional: [],
    repeated: [],
});
id_accessors!(TaskIds for Task {
    required: [task_id: TaskId, task_definition_id: TaskDefinitionId],
    optional: [],
    repeated: [],
});
id_accessors!(TaskRunIdentityIds for TaskRunIdentity {
    required: [task_run_id: TaskRunId, task_id: TaskId],
    optional: [parent_task_run_id: TaskRunId],
    repeated: [],
});

/// Every accessor trait, for a single glob import.
pub mod prelude {
    pub use super::{
        ElectionCertificateIds, LeaderHeartbeatAckIds, RollCallIds, RollCallObservationIds,
        SelfRemoveIds, TaskIds, TaskRunIdentityIds, VoteGrantIds, VoteRejectIds, VoteRequestIds,
        WorkerHeartbeatIds,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_request_candidate_id_returns_typed_id() {
        let raw = VoteRequest {
            shard_id: Some(generated::ShardId {
                value: "shard-1".into(),
            }),
            recovery_epoch: 1,
            term: 2,
            candidate_id: Some(generated::WorkerId {
                value: "worker-9".into(),
            }),
            membership_generation: 3,
            membership_digest: vec![],
            roll_call_digest: vec![],
        };

        assert_eq!(raw.candidate_id(), ids::WorkerId::new("worker-9"));
        assert_eq!(raw.shard_id(), ids::ShardId::new("shard-1"));
        assert_eq!(raw.term, 2);
    }

    #[test]
    fn self_remove_accessors_return_typed_ids() {
        let raw = SelfRemove {
            worker_id: Some(generated::WorkerId {
                value: "worker-1".into(),
            }),
            incarnation_id: Some(generated::IncarnationId {
                value: "incarnation-1".into(),
            }),
            shard_id: Some(generated::ShardId {
                value: "shard-2".into(),
            }),
            membership_generation: 7,
        };

        assert_eq!(raw.worker_id(), ids::WorkerId::new("worker-1"));
        assert_eq!(
            raw.incarnation_id(),
            ids::IncarnationId::new("incarnation-1")
        );
        assert_eq!(raw.shard_id(), ids::ShardId::new("shard-2"));
    }

    #[test]
    fn roll_call_observation_current_leader_seen_is_optional() {
        let without_leader = RollCallObservation {
            worker_id: Some(generated::WorkerId {
                value: "worker-1".into(),
            }),
            state: 0,
            highest_term_seen: 0,
            current_leader_seen: None,
            leader_contact_age_ticks: 0,
        };
        assert_eq!(without_leader.current_leader_seen(), None);

        let with_leader = RollCallObservation {
            current_leader_seen: Some(generated::WorkerId {
                value: "leader-1".into(),
            }),
            ..without_leader
        };
        assert_eq!(
            with_leader.current_leader_seen(),
            Some(ids::WorkerId::new("leader-1"))
        );
    }

    #[test]
    fn election_certificate_granting_voters_returns_typed_vec() {
        let raw = ElectionCertificate {
            shard_id: Some(generated::ShardId {
                value: "shard-1".into(),
            }),
            recovery_epoch: 0,
            term: 0,
            leader_id: Some(generated::WorkerId {
                value: "leader-1".into(),
            }),
            granting_voters: vec![
                generated::WorkerId {
                    value: "voter-1".into(),
                },
                generated::WorkerId {
                    value: "voter-2".into(),
                },
            ],
            membership_generation: 0,
        };

        assert_eq!(
            raw.granting_voters(),
            vec![ids::WorkerId::new("voter-1"), ids::WorkerId::new("voter-2")]
        );
    }

    #[test]
    #[should_panic(expected = "required by protocol invariant")]
    fn required_id_accessor_panics_when_absent() {
        let raw = WorkerHeartbeat {
            worker_id: None,
            incarnation_id: Some(generated::IncarnationId {
                value: "incarnation-1".into(),
            }),
            recovery_epoch_seen: 0,
            term_seen: 0,
            available_capacity: 0,
            active_task_runs_digest: vec![],
        };

        raw.worker_id();
    }
}
