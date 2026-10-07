//! Election message types re-exported from [`generated`], with typed ID
//! accessors as extension traits (import [`prelude`]).
//!
//! prost makes every message-typed field an `Option`. Fields that are always
//! present by protocol invariant get an accessor returning the `ids::*`
//! newtype, which panics if the field is absent (a malformed message is a peer
//! bug, not optionality to handle). Proto `optional` fields return `Option`.
//!
//! Configurations and generations get accessors the same way, returning the
//! domain [`Configuration`] and [`Generation`]. They panic if a required one
//! is absent or does not decode.
//!
//! The panicking accessors of the election payloads are implemented only for
//! [`Checked`] parts, which [`checked::decode`] builds after proving they
//! cannot panic. Task, claim and join messages keep their accessors on the raw
//! types; net checks those with [`WellFormed`].

use crate::configuration::{Configuration, Generation};
use crate::protocol::checked::{self, Checked};
use crate::protocol::generated;
use crate::protocol::ids;

pub use generated::{
    AckEcho, Claim, ClaimBatch, ClaimOldest, ClaimReject, ClaimRejectReason, ClaimRequest,
    ClaimResponse, ElectionCertificate, ElectionMessage, ElectionReject, ElectionRejectReason,
    JoinRequest, JoinResponse, KnownLeader, LeaderHeartbeatAck, RollCall, RollCallReply,
    AbsorbedGeneration, ChainEntry, CoalescingLink, SelfRemove, Task, TaskRecord, TaskRun,
    TaskRunIdentity, VoteGrant, VoteRequest, WorkerHeartbeat, chain_entry,
    claim_request, claim_response, election_message,
};
pub use generated::{
    CancelAnswer, CancelOutcome, CancelTask, ReportCompleted, ReportFailed, ReportStarted,
    RunCertified, RunFailed, StartAccepted, SubmitAccepted, SubmitTask, TaskReject,
    TaskRejectReason, TaskRequest, TaskResponse, task_request, task_response,
};

/// Whether every required ID field of a raw message is present. Nested
/// messages are not checked; [`checked::decode`] covers them.
pub(crate) trait RequiredIds {
    fn has_required_ids(&self) -> bool;
}

/// Defines the extension trait `$ext` with typed accessors for `$msg`'s ID
/// fields: `required` (panics if absent), `optional` and `repeated`, and
/// implements it for `$target`, either `$msg` itself or `Checked<$msg>`. Also
/// implements [`RequiredIds`] for `$msg`. A trait rather than an inherent
/// `impl` because the generated types live in another crate.
macro_rules! id_accessors {
    (
        $ext:ident for $target:ty, of $msg:ident {
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

        impl RequiredIds for $msg {
            fn has_required_ids(&self) -> bool {
                true $(&& self.$required.is_some())*
            }
        }

        impl $ext for $target {
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

id_accessors!(WorkerHeartbeatIds for Checked<WorkerHeartbeat>, of WorkerHeartbeat {
    required: [worker_id: WorkerId, incarnation_id: IncarnationId, shard_id: ShardId],
    optional: [],
    repeated: [],
});
id_accessors!(LeaderHeartbeatAckIds for Checked<LeaderHeartbeatAck>, of LeaderHeartbeatAck {
    required: [shard_id: ShardId, leader_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(RollCallIds for Checked<RollCall>, of RollCall {
    required: [shard_id: ShardId, initiator_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(RollCallReplyIds for Checked<RollCallReply>, of RollCallReply {
    required: [shard_id: ShardId, initiator_id: WorkerId, responder_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(VoteRequestIds for Checked<VoteRequest>, of VoteRequest {
    required: [shard_id: ShardId, candidate_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(VoteGrantIds for Checked<VoteGrant>, of VoteGrant {
    required: [shard_id: ShardId, candidate_id: WorkerId, voter_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(ElectionRejectIds for Checked<ElectionReject>, of ElectionReject {
    required: [shard_id: ShardId, initiator_id: WorkerId, rejecter_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(ElectionCertificateIds for Checked<ElectionCertificate>, of ElectionCertificate {
    required: [shard_id: ShardId, leader_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(SelfRemoveIds for Checked<SelfRemove>, of SelfRemove {
    required: [worker_id: WorkerId, incarnation_id: IncarnationId, shard_id: ShardId],
    optional: [],
    repeated: [],
});
id_accessors!(TaskIds for Task, of Task {
    required: [task_id: TaskId, task_definition_id: TaskDefinitionId],
    optional: [],
    repeated: [],
});
id_accessors!(TaskRunIdentityIds for TaskRunIdentity, of TaskRunIdentity {
    required: [task_run_id: TaskRunId, task_id: TaskId],
    optional: [parent_task_run_id: TaskRunId],
    repeated: [],
});
id_accessors!(JoinResponseIds for JoinResponse, of JoinResponse {
    required: [],
    optional: [leader_id: WorkerId],
    repeated: [],
});
id_accessors!(ClaimIds for Claim, of Claim {
    required: [task_run_id: TaskRunId],
    optional: [],
    repeated: [],
});

/// Whether every required configuration and generation of a raw message is
/// present and every one present decodes.
pub(crate) trait ValidConfigurations {
    fn has_valid_configurations(&self) -> bool;
}

/// Defines the extension trait `$ext` with accessors for `$msg`'s
/// configuration and generation fields, decoded into the domain types:
/// `configuration` and `generation` fields are required (they panic if
/// absent or invalid), `optional_generation` fields return `None` when
/// absent (and panic if present but invalid). Implements it for `$target`
/// (`Checked<$msg>`) and [`ValidConfigurations`] for `$msg`.
macro_rules! configuration_accessors {
    (
        $ext:ident for $target:ty, of $msg:ident {
            configuration: [$($configuration:ident),*],
            $(optional_configuration: [$($optional_configuration:ident),*],)?
            generation: [$($generation:ident),*],
            optional_generation: [$($optional:ident),*] $(,)?
        }
    ) => {
        pub trait $ext {
            $(fn $configuration(&self) -> Configuration;)*
            $($(fn $optional_configuration(&self) -> Option<Configuration>;)*)?
            $(fn $generation(&self) -> Generation;)*
            $(fn $optional(&self) -> Option<Generation>;)*
        }

        impl ValidConfigurations for $msg {
            fn has_valid_configurations(&self) -> bool {
                true
                    $(&& self.$configuration.as_ref().is_some_and(|raw| Configuration::try_from(raw).is_ok()))*
                    $($(&& self.$optional_configuration.as_ref().is_none_or(|raw| Configuration::try_from(raw).is_ok()))*)?
                    $(&& self.$generation.as_ref().is_some_and(|raw| Generation::try_from(raw).is_ok()))*
                    $(&& self.$optional.as_ref().is_none_or(|raw| Generation::try_from(raw).is_ok()))*
            }
        }

        impl $ext for $target {
            $(
                fn $configuration(&self) -> Configuration {
                    let raw = self.$configuration.as_ref().unwrap_or_else(|| {
                        panic!(
                            "{}.{} is required by protocol invariant but was absent",
                            stringify!($msg),
                            stringify!($configuration)
                        )
                    });
                    Configuration::try_from(raw).unwrap_or_else(|error| {
                        panic!("{}.{} is invalid: {error}", stringify!($msg), stringify!($configuration))
                    })
                }
            )*
            $($(
                fn $optional_configuration(&self) -> Option<Configuration> {
                    self.$optional_configuration.as_ref().map(|raw| {
                        Configuration::try_from(raw).unwrap_or_else(|error| {
                            panic!("{}.{} is invalid: {error}", stringify!($msg), stringify!($optional_configuration))
                        })
                    })
                }
            )*)?
            $(
                fn $generation(&self) -> Generation {
                    let raw = self.$generation.as_ref().unwrap_or_else(|| {
                        panic!(
                            "{}.{} is required by protocol invariant but was absent",
                            stringify!($msg),
                            stringify!($generation)
                        )
                    });
                    Generation::try_from(raw).unwrap_or_else(|error| {
                        panic!("{}.{} is invalid: {error}", stringify!($msg), stringify!($generation))
                    })
                }
            )*
            $(
                fn $optional(&self) -> Option<Generation> {
                    self.$optional.as_ref().map(|raw| {
                        Generation::try_from(raw).unwrap_or_else(|error| {
                            panic!("{}.{} is invalid: {error}", stringify!($msg), stringify!($optional))
                        })
                    })
                }
            )*
        }
    };
}

configuration_accessors!(WorkerHeartbeatConfigurations for Checked<WorkerHeartbeat>, of WorkerHeartbeat {
    configuration: [],
    generation: [],
    optional_generation: [configuration_generation, crawl_admission],
});
configuration_accessors!(LeaderHeartbeatAckConfigurations for Checked<LeaderHeartbeatAck>, of LeaderHeartbeatAck {
    configuration: [configuration],
    generation: [],
    optional_generation: [recipient_admission, recipient_prior_admission],
});
configuration_accessors!(RollCallConfigurations for Checked<RollCall>, of RollCall {
    configuration: [configuration],
    generation: [],
    optional_generation: [],
});
configuration_accessors!(RollCallReplyConfigurations for Checked<RollCallReply>, of RollCallReply {
    configuration: [],
    generation: [],
    optional_generation: [admission, prior_admission],
});
configuration_accessors!(VoteRequestConfigurations for Checked<VoteRequest>, of VoteRequest {
    configuration: [],
    generation: [roll_call_generation],
    optional_generation: [],
});
configuration_accessors!(ElectionRejectConfigurations for Checked<ElectionReject>, of ElectionReject {
    configuration: [],
    optional_configuration: [configuration],
    generation: [],
    optional_generation: [],
});
configuration_accessors!(SelfRemoveConfigurations for Checked<SelfRemove>, of SelfRemove {
    configuration: [],
    generation: [],
    optional_generation: [configuration_generation],
});

configuration_accessors!(ElectionCertificateConfigurations for Checked<ElectionCertificate>, of ElectionCertificate {
    configuration: [configuration],
    generation: [],
    optional_generation: [recipient_admission, recipient_prior_admission],
});

/// Whether a claim or join message received from a peer is one this node can
/// act on: it carries every required ID, including those of nested messages,
/// and fields that only make sense together are present together (for example
/// a `JoinResponse`'s leader and that leader's address). The required
/// accessors panic on an absent field, so a network boundary checks this
/// first and rejects a malformed message instead of handing it to code that
/// would panic or be left holding half an answer. Election messages are
/// checked by [`checked::decode`] instead.
pub trait WellFormed {
    fn is_well_formed(&self) -> bool;
}

impl WellFormed for ClaimRequest {
    fn is_well_formed(&self) -> bool {
        self.request.is_some()
    }
}

impl WellFormed for ClaimResponse {
    fn is_well_formed(&self) -> bool {
        match &self.result {
            Some(claim_response::Result::Accept(claim)) => claim_is_well_formed(claim),
            Some(claim_response::Result::Batch(batch)) => {
                batch.claims.iter().all(claim_is_well_formed)
            }
            Some(claim_response::Result::Reject(_)) | None => true,
        }
    }
}

fn claim_is_well_formed(claim: &Claim) -> bool {
    claim.has_required_ids() && claim.task.as_ref().is_none_or(|t| t.has_required_ids())
}

impl WellFormed for TaskRequest {
    /// An id or digest missing inside a request is for the leader to refuse
    /// as `TASK_REJECT_MALFORMED`; only a request that asks for nothing stops
    /// at the codec.
    fn is_well_formed(&self) -> bool {
        self.request.is_some()
    }
}

impl WellFormed for TaskResponse {
    fn is_well_formed(&self) -> bool {
        self.result.is_some()
    }
}

impl WellFormed for JoinRequest {
    fn is_well_formed(&self) -> bool {
        true
    }
}

impl WellFormed for JoinResponse {
    /// A leader pointer names a leader together with its address, or, for "no
    /// leader known", neither: one without the other is nothing a joiner can
    /// act on. Its term is one this node can act on (see [`checked::is_a_term`]).
    fn is_well_formed(&self) -> bool {
        let names_a_leader = self.leader_id.is_some();
        let gives_an_address = !self.leader_multiaddr.is_empty();
        names_a_leader == gives_an_address && checked::is_a_term(self.term)
    }
}

impl Checked<ElectionReject> {
    /// The leader the rejecter names and the term it led at, if it names one.
    pub fn named_leader(&self) -> Option<(ids::WorkerId, u64)> {
        self.leader
            .as_ref()
            .map(|leader| (leader.leader_id.clone().expect("decode checked the leader's id").into(), leader.term))
    }
}

/// Every accessor trait, for a single glob import.
pub mod prelude {
    pub use super::{
        ClaimIds, ElectionCertificateConfigurations, ElectionCertificateIds,
        ElectionRejectConfigurations, ElectionRejectIds, JoinResponseIds,
        LeaderHeartbeatAckConfigurations, LeaderHeartbeatAckIds, RollCallConfigurations,
        RollCallIds, RollCallReplyConfigurations, RollCallReplyIds, SelfRemoveConfigurations,
        SelfRemoveIds, TaskIds, TaskRunIdentityIds, VoteGrantIds, VoteRequestConfigurations,
        VoteRequestIds, WorkerHeartbeatConfigurations, WorkerHeartbeatIds,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leader_pointer() -> JoinResponse {
        JoinResponse {
            leader_id: Some(generated::WorkerId {
                value: "leader-1".into(),
            }),
            leader_multiaddr: "/ip4/127.0.0.1/tcp/4001".into(),
            term: 3,
            recovery_epoch: 1,
            recovery_epoch_lineage: 0,
        }
    }

    #[test]
    fn join_response_leader_id_is_optional() {
        assert_eq!(
            leader_pointer().leader_id(),
            Some(ids::WorkerId::new("leader-1"))
        );
        assert_eq!(JoinResponse::default().leader_id(), None);
    }

    #[test]
    fn well_formed_refuses_a_join_response_naming_a_term_of_u64_max() {
        let at = |term| JoinResponse {
            term,
            ..leader_pointer()
        };

        assert!(at(u64::MAX - 1).is_well_formed());
        assert!(!at(u64::MAX).is_well_formed());
    }

    #[test]
    fn well_formed_checks_claim_messages() {
        assert!(!ClaimRequest { request: None }.is_well_formed());
        let accept_without_run_id = ClaimResponse {
            result: Some(claim_response::Result::Accept(Claim::default())),
        };
        assert!(!accept_without_run_id.is_well_formed());
        let batch_with_a_claim_without_run_id = ClaimResponse {
            result: Some(claim_response::Result::Batch(ClaimBatch {
                claims: vec![Claim::default()],
            })),
        };
        assert!(!batch_with_a_claim_without_run_id.is_well_formed());
        let reject = ClaimResponse {
            result: Some(claim_response::Result::Reject(ClaimReject::default())),
        };
        assert!(reject.is_well_formed());
    }

    #[test]
    fn well_formed_join_response_names_a_leader_together_with_its_address_or_neither() {
        assert!(leader_pointer().is_well_formed());
        assert!(JoinResponse::default().is_well_formed());

        let leader_without_address = JoinResponse {
            leader_multiaddr: String::new(),
            ..leader_pointer()
        };
        assert!(!leader_without_address.is_well_formed());

        let address_without_leader = JoinResponse {
            leader_id: None,
            ..leader_pointer()
        };
        assert!(!address_without_leader.is_well_formed());
    }
}
