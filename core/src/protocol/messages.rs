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
//! is absent or does not decode, which [`WellFormed`] rules out for wire
//! messages.

use crate::configuration::{Configuration, Generation};
use crate::protocol::generated;
use crate::protocol::ids;

pub use generated::{
    AckEcho, Claim, ClaimBatch, ClaimOldest, ClaimReject, ClaimRejectReason, ClaimRequest,
    ClaimResponse, ElectionCertificate, ElectionMessage, ElectionReject, ElectionRejectReason,
    JoinRequest, JoinResponse, KnownLeader, LeaderHeartbeatAck, RollCall, RollCallReply,
    SelfRemove, Task, TaskRun, TaskRunIdentity, VoteGrant, VoteRequest, WorkerHeartbeat,
    claim_request, claim_response, election_message,
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
            /// Whether every `required` field is present, so none of the
            /// required accessors below can panic. Nested messages are not
            /// checked; [`WellFormed`] covers them for wire messages.
            fn has_required_ids(&self) -> bool;
            $(fn $required(&self) -> ids::$required_id;)*
            $(fn $optional(&self) -> Option<ids::$optional_id>;)*
            $(fn $repeated(&self) -> Vec<ids::$repeated_id>;)*
        }

        impl $ext for $msg {
            fn has_required_ids(&self) -> bool {
                true $(&& self.$required.is_some())*
            }
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
    required: [worker_id: WorkerId, incarnation_id: IncarnationId, shard_id: ShardId],
    optional: [],
    repeated: [],
});
id_accessors!(LeaderHeartbeatAckIds for LeaderHeartbeatAck {
    required: [shard_id: ShardId, leader_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(RollCallIds for RollCall {
    required: [shard_id: ShardId, initiator_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(RollCallReplyIds for RollCallReply {
    required: [shard_id: ShardId, initiator_id: WorkerId, responder_id: WorkerId],
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
id_accessors!(ElectionRejectIds for ElectionReject {
    required: [shard_id: ShardId, initiator_id: WorkerId, rejecter_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(KnownLeaderIds for KnownLeader {
    required: [leader_id: WorkerId],
    optional: [],
    repeated: [],
});
id_accessors!(ElectionCertificateIds for ElectionCertificate {
    required: [shard_id: ShardId, leader_id: WorkerId],
    optional: [],
    repeated: [],
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
id_accessors!(JoinResponseIds for JoinResponse {
    required: [],
    optional: [leader_id: WorkerId],
    repeated: [],
});
id_accessors!(ClaimIds for Claim {
    required: [task_run_id: TaskRunId],
    optional: [],
    repeated: [],
});

/// Defines the extension trait `$ext` with accessors for `$msg`'s
/// configuration and generation fields, decoded into the domain types:
/// `configuration` and `generation` fields are required (they panic if
/// absent or invalid), `optional_generation` fields return `None` when
/// absent (and panic if present but invalid).
macro_rules! configuration_accessors {
    (
        $ext:ident for $msg:ident {
            configuration: [$($configuration:ident),*],
            $(optional_configuration: [$($optional_configuration:ident),*],)?
            generation: [$($generation:ident),*],
            optional_generation: [$($optional:ident),*] $(,)?
        }
    ) => {
        pub trait $ext {
            /// Whether every required configuration and generation is
            /// present and every one present decodes, so none of the
            /// accessors below can panic.
            fn has_valid_configurations(&self) -> bool;
            $(fn $configuration(&self) -> Configuration;)*
            $($(fn $optional_configuration(&self) -> Option<Configuration>;)*)?
            $(fn $generation(&self) -> Generation;)*
            $(fn $optional(&self) -> Option<Generation>;)*
        }

        impl $ext for $msg {
            fn has_valid_configurations(&self) -> bool {
                true
                    $(&& self.$configuration.as_ref().is_some_and(|raw| Configuration::try_from(raw).is_ok()))*
                    $($(&& self.$optional_configuration.as_ref().is_none_or(|raw| Configuration::try_from(raw).is_ok()))*)?
                    $(&& self.$generation.as_ref().is_some_and(|raw| Generation::try_from(raw).is_ok()))*
                    $(&& self.$optional.as_ref().is_none_or(|raw| Generation::try_from(raw).is_ok()))*
            }
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

configuration_accessors!(WorkerHeartbeatConfigurations for WorkerHeartbeat {
    configuration: [],
    generation: [],
    optional_generation: [configuration_generation],
});
configuration_accessors!(LeaderHeartbeatAckConfigurations for LeaderHeartbeatAck {
    configuration: [configuration],
    generation: [],
    optional_generation: [recipient_admission, recipient_prior_admission],
});
configuration_accessors!(RollCallConfigurations for RollCall {
    configuration: [configuration],
    generation: [],
    optional_generation: [],
});
configuration_accessors!(RollCallReplyConfigurations for RollCallReply {
    configuration: [],
    generation: [],
    optional_generation: [admission, prior_admission],
});
configuration_accessors!(VoteRequestConfigurations for VoteRequest {
    configuration: [],
    generation: [roll_call_generation],
    optional_generation: [],
});
configuration_accessors!(ElectionRejectConfigurations for ElectionReject {
    configuration: [],
    optional_configuration: [configuration],
    generation: [],
    optional_generation: [],
});
configuration_accessors!(SelfRemoveConfigurations for SelfRemove {
    configuration: [],
    generation: [],
    optional_generation: [configuration_generation],
});

configuration_accessors!(ElectionCertificateConfigurations for ElectionCertificate {
    configuration: [configuration],
    generation: [],
    optional_generation: [recipient_admission, recipient_prior_admission],
});

/// Whether a message received from a peer is one this node can act on: it
/// carries every required ID, including those of nested messages; every
/// configuration and generation it carries is present where required and
/// valid; any fields that only make sense together are present together
/// (for example a `JoinResponse`'s leader and that leader's address); a
/// field that must agree with another does (the configuration a leader ack
/// or an election certificate carries is one its leader could have
/// announced); and every term is below
/// `u64::MAX`. The required accessors panic on an absent or invalid field,
/// so a network boundary checks this first and rejects a malformed message
/// instead of handing it to code that would panic or be left holding half
/// an answer.
pub trait WellFormed {
    fn is_well_formed(&self) -> bool;
}

impl WellFormed for ElectionMessage {
    /// A message with no payload is well formed: it carries no IDs, and a
    /// payload variant this build does not know decodes as `None`.
    fn is_well_formed(&self) -> bool {
        use election_message::Payload;
        match &self.payload {
            None => true,
            Some(Payload::Heartbeat(m)) => {
                m.has_required_ids()
                    && m.has_valid_configurations()
                    && is_a_term(m.term_seen)
                    && m.newest_accepted_ack
                        .as_ref()
                        .is_none_or(|echo| is_a_term(echo.term))
            }
            Some(Payload::HeartbeatAck(m)) => {
                m.has_required_ids()
                    && m.has_valid_configurations()
                    && is_a_term(m.term)
                    && carries_a_configuration_its_leader_could_announce(
                        m.configuration.as_ref(),
                        m.recovery_epoch,
                        m.term,
                    )
                    && prior_implies_admission(
                        m.recipient_admission.as_ref(),
                        m.recipient_prior_admission.as_ref(),
                    )
            }
            Some(Payload::RollCall(m)) => {
                m.has_required_ids() && m.has_valid_configurations() && is_a_term(m.term)
            }
            Some(Payload::RollCallReply(m)) => {
                m.has_required_ids()
                    && m.has_valid_configurations()
                    && is_a_term(m.term)
                    && prior_implies_admission(m.admission.as_ref(), m.prior_admission.as_ref())
            }
            Some(Payload::VoteRequest(m)) => {
                m.has_required_ids() && m.has_valid_configurations() && is_a_term(m.term)
            }
            Some(Payload::VoteGrant(m)) => m.has_required_ids() && is_a_term(m.term),
            Some(Payload::ElectionReject(m)) => {
                m.has_required_ids()
                    && m.has_valid_configurations()
                    && is_a_term(m.term)
                    && is_a_term(m.highest_term_seen)
                    && m.leader
                        .as_ref()
                        .is_none_or(|leader| leader.has_required_ids() && is_a_term(leader.term))
            }
            Some(Payload::ElectionCertificate(m)) => {
                m.has_required_ids()
                    && m.has_valid_configurations()
                    && is_a_term(m.term)
                    && carries_a_configuration_its_leader_could_announce(
                        m.configuration.as_ref(),
                        m.recovery_epoch,
                        m.term,
                    )
                    && prior_implies_admission(
                        m.recipient_admission.as_ref(),
                        m.recipient_prior_admission.as_ref(),
                    )
            }
            Some(Payload::SelfRemove(m)) => m.has_required_ids() && m.has_valid_configurations(),
        }
    }
}

/// Whether a term a peer names is one this node can act on: any but
/// `u64::MAX`. A node contests the term after the highest it has seen, so
/// one it learned of at `u64::MAX` would leave it no next term.
fn is_a_term(term: u64) -> bool {
    term < u64::MAX
}

/// Whether the configuration a leader ack or an election certificate
/// carries is one the leader that sent it, elected in `term` at
/// `recovery_epoch`, could have announced: from that recovery epoch, at a
/// term no later than `term`. A follower adopts the configuration either
/// carries. One from a later term than any the follower has seen would
/// outrank its own term once it leads, and the first change it announces
/// would panic ([`Generation::next_change`]); one from another recovery
/// epoch would make it refuse every roll call of its own epoch.
fn carries_a_configuration_its_leader_could_announce(
    configuration: Option<&generated::Configuration>,
    recovery_epoch: u64,
    term: u64,
) -> bool {
    configuration
        .and_then(|configuration| configuration.generation.as_ref())
        .is_some_and(|generation| {
            generation.recovery_epoch == recovery_epoch && generation.term <= term
        })
}

/// Whether a prior admission present without an admission would decode: a
/// prior admission records what a joint founding's respondent held *before*
/// the admission it answers with, so it never stands alone. Honest nodes
/// never send one without the other (`adopt_configuration` sets prior only
/// together with an admission); this rejects the shape at the edge rather
/// than let the old side's tally count a prior admission a peer never
/// actually held together with an admission.
fn prior_implies_admission(
    admission: Option<&generated::Generation>,
    prior_admission: Option<&generated::Generation>,
) -> bool {
    admission.is_some() || prior_admission.is_none()
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

impl WellFormed for JoinRequest {
    fn is_well_formed(&self) -> bool {
        true
    }
}

impl WellFormed for JoinResponse {
    /// A leader pointer names a leader together with its address, or, for "no
    /// leader known", neither: one without the other is nothing a joiner can
    /// act on. Its term is one this node can act on (see [`is_a_term`]).
    fn is_well_formed(&self) -> bool {
        let names_a_leader = self.leader_id.is_some();
        let gives_an_address = !self.leader_multiaddr.is_empty();
        names_a_leader == gives_an_address && is_a_term(self.term)
    }
}

/// Every accessor trait, for a single glob import.
pub mod prelude {
    pub use super::{
        ClaimIds, ElectionCertificateConfigurations, ElectionCertificateIds,
        ElectionRejectConfigurations, ElectionRejectIds, JoinResponseIds, KnownLeaderIds,
        LeaderHeartbeatAckConfigurations, LeaderHeartbeatAckIds, RollCallConfigurations,
        RollCallIds, RollCallReplyConfigurations, RollCallReplyIds, SelfRemoveConfigurations,
        SelfRemoveIds, TaskIds, TaskRunIdentityIds, VoteGrantIds, VoteRequestConfigurations,
        VoteRequestIds, WorkerHeartbeatConfigurations, WorkerHeartbeatIds,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::Single;

    /// A certificate from `leader-1` for `shard-1`, at recovery epoch 1 and
    /// term 3, carrying a single configuration of one voter at `generation`.
    fn certificate_carrying(generation: Generation) -> ElectionMessage {
        let configuration = Configuration::single(Single {
            generation,
            base: generation,
            voter_count: 1,
        });
        ElectionMessage {
            payload: Some(election_message::Payload::ElectionCertificate(
                ElectionCertificate {
                    shard_id: Some(generated::ShardId {
                        value: "shard-1".into(),
                    }),
                    recovery_epoch: 1,
                    term: 3,
                    leader_id: wid("leader-1"),
                    configuration: Some((&configuration).into()),
                    recipient_admission: Some(generation.into()),
                    recipient_prior_admission: None,
                },
            )),
        }
    }

    #[test]
    fn well_formed_refuses_a_certificate_whose_configuration_its_leader_could_not_have_announced() {
        let cases = [
            (Generation::new(1, 3, 1), true, "the certificate's own term"),
            (Generation::new(1, 2, 5), true, "an earlier term, led as is"),
            (Generation::new(1, 4, 1), false, "a later term"),
            (Generation::new(0, 3, 1), false, "an older recovery epoch"),
            (Generation::new(2, 3, 1), false, "a newer recovery epoch"),
        ];
        for (generation, well_formed, case) in cases {
            assert_eq!(
                certificate_carrying(generation).is_well_formed(),
                well_formed,
                "{case}"
            );
        }
    }

    #[test]
    fn well_formed_refuses_a_certificate_with_no_or_an_invalid_configuration() {
        let mut no_configuration = certificate_carrying(Generation::new(1, 3, 1));
        let mut invalid_prior = certificate_carrying(Generation::new(1, 3, 1));
        if let Some(election_message::Payload::ElectionCertificate(m)) =
            &mut no_configuration.payload
        {
            m.configuration = None;
        }
        if let Some(election_message::Payload::ElectionCertificate(m)) = &mut invalid_prior.payload
        {
            m.recipient_prior_admission = Some(generation_at_max());
        }

        assert!(!no_configuration.is_well_formed());
        assert!(!invalid_prior.is_well_formed());
    }

    #[test]
    fn well_formed_refuses_an_invalid_prior_admission_or_held_generation() {
        let reply = |admission, prior_admission| ElectionMessage {
            payload: Some(election_message::Payload::RollCallReply(RollCallReply {
                shard_id: Some(generated::ShardId {
                    value: "shard-1".into(),
                }),
                term: 1,
                initiator_id: wid("worker-a"),
                responder_id: wid("worker-b"),
                admission,
                prior_admission,
                ..Default::default()
            })),
        };
        let heartbeat = |configuration_generation| ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
                worker_id: wid("worker-a"),
                incarnation_id: Some(generated::IncarnationId {
                    value: "incarnation-1".into(),
                }),
                shard_id: Some(generated::ShardId {
                    value: "shard-1".into(),
                }),
                configuration_generation,
                ..Default::default()
            })),
        };

        let genesis = || Some(Generation::genesis(0).into());
        assert!(reply(genesis(), genesis()).is_well_formed());
        assert!(!reply(genesis(), Some(generation_at_max())).is_well_formed());
        assert!(reply(None, None).is_well_formed());
        assert!(!reply(Some(generation_at_max()), None).is_well_formed());
        assert!(heartbeat(None).is_well_formed());
        assert!(heartbeat(Some(Generation::genesis(0).into())).is_well_formed());
        assert!(!heartbeat(Some(generation_at_max())).is_well_formed());
    }

    /// A well-formed election message carrying the given admission and
    /// prior admission in one message's pair of generation fields.
    type WithAdmissions =
        Box<dyn Fn(Option<generated::Generation>, Option<generated::Generation>) -> ElectionMessage>;

    /// A prior admission set together with an admission decodes; one set
    /// alone does not, for each message a joint founding's respondent
    /// answers with (`RollCallReply`) or a leader answers back with
    /// (`LeaderHeartbeatAck`, `ElectionCertificate`). Honest nodes never
    /// send a prior admission without an admission
    /// (`adopt_configuration` sets them together), but the wire edge must
    /// still refuse the shape rather than let a peer bug feed a prior
    /// admission the old side's tally would otherwise count.
    #[test]
    fn well_formed_refuses_a_prior_admission_sent_without_an_admission() {
        let admission = Some(Generation::genesis(0).into());
        let prior_admission = Some(Generation::new(0, 0, 1).into());

        let reply = |admission, prior_admission| ElectionMessage {
            payload: Some(election_message::Payload::RollCallReply(RollCallReply {
                shard_id: Some(generated::ShardId {
                    value: "shard-1".into(),
                }),
                term: 1,
                initiator_id: wid("worker-a"),
                responder_id: wid("worker-b"),
                admission,
                prior_admission,
                ..Default::default()
            })),
        };
        let ack = |recipient_admission, recipient_prior_admission| {
            let mut ack = ack_carrying(Generation::new(1, 3, 1));
            if let Some(election_message::Payload::HeartbeatAck(m)) = &mut ack.payload {
                m.recipient_admission = recipient_admission;
                m.recipient_prior_admission = recipient_prior_admission;
            }
            ack
        };
        let certificate = |recipient_admission, recipient_prior_admission| {
            let mut certificate = certificate_carrying(Generation::new(1, 3, 1));
            if let Some(election_message::Payload::ElectionCertificate(m)) =
                &mut certificate.payload
            {
                m.recipient_admission = recipient_admission;
                m.recipient_prior_admission = recipient_prior_admission;
            }
            certificate
        };

        let cases: Vec<(&str, WithAdmissions)> = vec![
            ("RollCallReply", Box::new(reply)),
            ("LeaderHeartbeatAck", Box::new(ack)),
            ("ElectionCertificate", Box::new(certificate)),
        ];
        for (field, with_admissions) in cases {
            assert!(
                with_admissions(None, None).is_well_formed(),
                "{field}: neither present"
            );
            assert!(
                with_admissions(admission, None).is_well_formed(),
                "{field}: admission alone"
            );
            assert!(
                with_admissions(admission, prior_admission).is_well_formed(),
                "{field}: both present"
            );
            assert!(
                !with_admissions(None, prior_admission).is_well_formed(),
                "{field}: prior admission alone"
            );
        }
    }

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
            shard_id: Some(generated::ShardId {
                value: "shard-1".into(),
            }),
            newest_accepted_ack: None,
            configuration_generation: None,
            send_token: 0,
        };

        raw.worker_id();
    }

    fn wid(value: &str) -> Option<generated::WorkerId> {
        Some(generated::WorkerId {
            value: value.into(),
        })
    }

    fn roll_call(configuration: Option<generated::Configuration>) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::RollCall(RollCall {
                shard_id: Some(generated::ShardId {
                    value: "shard-1".into(),
                }),
                term: 1,
                configuration,
                initiator_id: wid("worker-a"),
                ..Default::default()
            })),
        }
    }

    fn valid_configuration() -> generated::Configuration {
        (&Configuration::genesis(0)).into()
    }

    #[test]
    fn well_formed_rejects_a_missing_or_invalid_configuration() {
        assert!(!roll_call(None).is_well_formed());
        let no_voters = generated::Configuration {
            electorate: Some(generated::configuration::Electorate::Single(
                generated::SingleElectorate { voter_count: 0 },
            )),
            ..valid_configuration()
        };
        assert!(!roll_call(Some(no_voters)).is_well_formed());
    }

    fn generation_at_max() -> generated::Generation {
        generated::Generation {
            recovery_epoch: 0,
            term: 0,
            counter: u64::MAX,
        }
    }

    #[test]
    fn well_formed_rejects_a_missing_required_generation() {
        let request = |roll_call_generation| ElectionMessage {
            payload: Some(election_message::Payload::VoteRequest(VoteRequest {
                shard_id: Some(generated::ShardId {
                    value: "shard-1".into(),
                }),
                candidate_id: wid("worker-a"),
                roll_call_generation,
                ..Default::default()
            })),
        };

        assert!(request(Some(Generation::genesis(0).into())).is_well_formed());
        assert!(!request(None).is_well_formed());
        assert!(!request(Some(generation_at_max())).is_well_formed());
    }

    #[test]
    fn well_formed_rejects_a_rejection_carrying_an_invalid_configuration_but_not_none() {
        let reject = |configuration| ElectionMessage {
            payload: Some(election_message::Payload::ElectionReject(ElectionReject {
                shard_id: Some(generated::ShardId {
                    value: "shard-1".into(),
                }),
                initiator_id: wid("worker-a"),
                rejecter_id: wid("worker-b"),
                configuration,
                ..Default::default()
            })),
        };
        let no_voters = generated::Configuration {
            electorate: Some(generated::configuration::Electorate::Single(
                generated::SingleElectorate { voter_count: 0 },
            )),
            ..valid_configuration()
        };

        assert!(reject(None).is_well_formed());
        assert!(reject(Some(valid_configuration())).is_well_formed());
        assert!(!reject(Some(no_voters)).is_well_formed());
    }

    #[test]
    fn well_formed_rejects_a_rejection_naming_a_leader_without_its_id() {
        let reject = |leader| ElectionMessage {
            payload: Some(election_message::Payload::ElectionReject(ElectionReject {
                shard_id: Some(generated::ShardId {
                    value: "shard-1".into(),
                }),
                initiator_id: wid("worker-a"),
                rejecter_id: wid("worker-b"),
                leader,
                ..Default::default()
            })),
        };

        assert!(reject(None).is_well_formed());
        assert!(
            reject(Some(KnownLeader {
                leader_id: wid("worker-c"),
                term: 2,
            }))
            .is_well_formed()
        );
        assert!(
            !reject(Some(KnownLeader {
                leader_id: None,
                term: 2,
            }))
            .is_well_formed()
        );
    }

    /// An ack at recovery epoch 1 and term 3 carrying a configuration at
    /// `generation`.
    fn ack_carrying(generation: Generation) -> ElectionMessage {
        let configuration = Configuration::single(Single {
            generation,
            base: generation,
            voter_count: 1,
        });
        ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(
                LeaderHeartbeatAck {
                    shard_id: Some(generated::ShardId {
                        value: "shard-1".into(),
                    }),
                    leader_id: wid("leader-1"),
                    recovery_epoch: 1,
                    term: 3,
                    configuration: Some((&configuration).into()),
                    ..Default::default()
                },
            )),
        }
    }

    #[test]
    fn well_formed_refuses_an_ack_whose_configuration_its_leader_could_not_have_announced() {
        let cases = [
            (Generation::new(1, 3, 1), true, "the ack's own term"),
            (Generation::new(1, 2, 5), true, "an earlier term"),
            (Generation::new(1, 4, 0), false, "a term above the ack's"),
            (Generation::new(1, 9, 1), false, "a term far above the ack's"),
            (Generation::new(0, 3, 1), false, "an older recovery epoch"),
            (Generation::new(2, 0, 0), false, "a newer recovery epoch"),
        ];
        for (generation, well_formed, case) in cases {
            assert_eq!(
                ack_carrying(generation).is_well_formed(),
                well_formed,
                "{case}"
            );
        }
    }

    /// A well-formed election message carrying the given term in one field.
    type WithTerm = Box<dyn Fn(u64) -> ElectionMessage>;

    /// Each term-valued field of an election message, named, with the
    /// message that carries a given term there.
    fn with_term_in_each_field() -> Vec<(&'static str, WithTerm)> {
        use election_message::Payload;
        let shard = || {
            Some(generated::ShardId {
                value: "shard-1".into(),
            })
        };
        let reject = move |term, highest_term_seen, leader_term| ElectionMessage {
            payload: Some(Payload::ElectionReject(ElectionReject {
                shard_id: shard(),
                term,
                initiator_id: wid("worker-a"),
                rejecter_id: wid("worker-b"),
                highest_term_seen,
                leader: Some(KnownLeader {
                    leader_id: wid("worker-c"),
                    term: leader_term,
                }),
                ..Default::default()
            })),
        };
        vec![
            (
                "WorkerHeartbeat.term_seen",
                Box::new(move |term| ElectionMessage {
                    payload: Some(Payload::Heartbeat(WorkerHeartbeat {
                        worker_id: wid("worker-a"),
                        incarnation_id: Some(generated::IncarnationId {
                            value: "incarnation-1".into(),
                        }),
                        shard_id: shard(),
                        term_seen: term,
                        ..Default::default()
                    })),
                }),
            ),
            (
                "AckEcho.term",
                Box::new(move |term| {
                    heartbeat_with(
                        shard(),
                        Some(AckEcho {
                            term,
                            send_token: 0,
                        }),
                    )
                }),
            ),
            (
                "LeaderHeartbeatAck.term",
                Box::new(|term| {
                    let mut ack = ack_carrying(Generation::new(1, 3, 1));
                    if let Some(Payload::HeartbeatAck(m)) = &mut ack.payload {
                        m.term = term;
                    }
                    ack
                }),
            ),
            (
                "RollCall.term",
                Box::new(|term| {
                    let mut call = roll_call(Some(valid_configuration()));
                    if let Some(Payload::RollCall(m)) = &mut call.payload {
                        m.term = term;
                    }
                    call
                }),
            ),
            (
                "RollCallReply.term",
                Box::new(move |term| ElectionMessage {
                    payload: Some(Payload::RollCallReply(RollCallReply {
                        shard_id: shard(),
                        term,
                        initiator_id: wid("worker-a"),
                        responder_id: wid("worker-b"),
                        ..Default::default()
                    })),
                }),
            ),
            (
                "VoteRequest.term",
                Box::new(move |term| ElectionMessage {
                    payload: Some(Payload::VoteRequest(VoteRequest {
                        shard_id: shard(),
                        term,
                        candidate_id: wid("worker-a"),
                        roll_call_generation: Some(Generation::genesis(0).into()),
                        ..Default::default()
                    })),
                }),
            ),
            (
                "VoteGrant.term",
                Box::new(move |term| ElectionMessage {
                    payload: Some(Payload::VoteGrant(VoteGrant {
                        shard_id: shard(),
                        term,
                        candidate_id: wid("worker-a"),
                        voter_id: wid("worker-b"),
                        ..Default::default()
                    })),
                }),
            ),
            (
                "ElectionReject.term",
                Box::new(move |term| reject(term, 1, 1)),
            ),
            (
                "ElectionReject.highest_term_seen",
                Box::new(move |term| reject(1, term, 1)),
            ),
            (
                "KnownLeader.term",
                Box::new(move |term| reject(1, 1, term)),
            ),
            (
                "ElectionCertificate.term",
                Box::new(|term| {
                    let mut certificate = certificate_carrying(Generation::new(1, 3, 1));
                    if let Some(Payload::ElectionCertificate(m)) = &mut certificate.payload {
                        m.term = term;
                    }
                    certificate
                }),
            ),
        ]
    }

    #[test]
    fn well_formed_refuses_a_term_of_u64_max_in_every_term_field() {
        for (field, with_term) in with_term_in_each_field() {
            assert!(with_term(u64::MAX - 1).is_well_formed(), "{field}");
            assert!(!with_term(u64::MAX).is_well_formed(), "{field}");
        }
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
    #[should_panic(expected = "required by protocol invariant")]
    fn a_required_configuration_accessor_panics_when_absent() {
        let _ = LeaderHeartbeatAck::default().configuration();
    }

    #[test]
    fn well_formed_rejects_a_missing_top_level_id() {
        let message = ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(
                LeaderHeartbeatAck {
                    shard_id: Some(generated::ShardId {
                        value: "shard-1".into(),
                    }),
                    leader_id: None,
                    configuration: Some(valid_configuration()),
                    ..Default::default()
                },
            )),
        };

        assert!(!message.is_well_formed());
    }

    fn heartbeat_with(
        shard_id: Option<generated::ShardId>,
        echo: Option<AckEcho>,
    ) -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
                worker_id: wid("worker-a"),
                incarnation_id: Some(generated::IncarnationId {
                    value: "incarnation-1".into(),
                }),
                shard_id,
                newest_accepted_ack: echo,
                ..Default::default()
            })),
        }
    }

    #[test]
    fn well_formed_rejects_a_heartbeat_that_names_no_shard() {
        let shard = Some(generated::ShardId {
            value: "shard-1".into(),
        });

        assert!(heartbeat_with(shard, None).is_well_formed());
        assert!(!heartbeat_with(None, None).is_well_formed());
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
