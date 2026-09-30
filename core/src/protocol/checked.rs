//! Checked decode of the election messages a peer sends.
//!
//! The election accessors (`worker_id()`, `configuration()` and so on) panic
//! on an absent or invalid field, because a well-formed message never lacks
//! one. [`decode`] proves a message well formed, and wraps it in a
//! [`CheckedMessage`], the only kind [`WorkerNode::step`] accepts. Its parts
//! are [`Checked`], and only they implement the panicking accessors, so no
//! accessor can fail on a value a caller can hold.
//!
//! A checked message is a proof wrapper over the prost message, not a parallel
//! domain type: its raw fields stay readable through `Deref`.
//!
//! [`WorkerNode::step`]: crate::election::WorkerNode::step

use std::ops::Deref;

use crate::protocol::generated;
use crate::protocol::messages::{
    ElectionCertificate, ElectionMessage, ElectionReject, LeaderHeartbeatAck, RequiredIds,
    RollCall, RollCallReply, SelfRemove, ValidConfigurations, VoteGrant, VoteRequest,
    WorkerHeartbeat, election_message,
};

/// An election message that decoding has proven well formed; the only kind
/// `WorkerNode::step` accepts. A proof wrapper over the prost message, not a
/// parallel domain type: its raw fields stay readable, and the accessors on
/// its parts cannot fail.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckedMessage(ElectionMessage);

/// Why [`decode`] refused a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MalformedMessage {
    #[error("{message} lacks a required id")]
    MissingId { message: &'static str },
    #[error("{message} lacks a required configuration or generation, or carries an invalid one")]
    InvalidConfiguration { message: &'static str },
    #[error("{message} names a term of u64::MAX")]
    TermAtMax { message: &'static str },
    #[error("{message} carries a configuration its leader could not have announced")]
    Unannounceable { message: &'static str },
    #[error("{message} carries a prior admission without an admission")]
    PriorWithoutAdmission { message: &'static str },
}

/// Checks `message` against every rule an election message must meet, and
/// returns it as a [`CheckedMessage`]:
///
/// - it carries every required ID, including those of nested messages;
/// - every configuration and generation it carries is present where required
///   and valid;
/// - every term is below `u64::MAX`: a node contests the term after the
///   highest it has seen, so one it learned of at `u64::MAX` would leave it
///   no next term;
/// - the configuration a leader ack or an election certificate carries is one
///   its leader, elected in the message's term at its recovery epoch, could
///   have announced: from that recovery epoch, at a term no later than the
///   message's. A follower adopts it. One from a later term than any the
///   follower has seen would outrank its own term once it leads, and the
///   first change it announces would panic
///   ([`Generation::next_change`](crate::configuration::Generation::next_change));
///   one from another recovery epoch would make it refuse every roll call of
///   its own epoch;
/// - a prior admission never stands without an admission: it records what a
///   joint founding's respondent held *before* the admission it answers with,
///   and honest nodes set them together. The edge refuses the shape rather
///   than let the old side's tally count a prior admission a peer never
///   actually held.
///
/// A message with no payload is well formed: it carries no IDs, and a
/// payload variant this build does not know decodes as none.
pub fn decode(message: ElectionMessage) -> Result<CheckedMessage, MalformedMessage> {
    use election_message::Payload;
    match &message.payload {
        None => {}
        Some(Payload::Heartbeat(m)) => {
            let name = "WorkerHeartbeat";
            has_ids_and_configurations(m, name)?;
            check_term(m.term_seen, name)?;
            if let Some(echo) = &m.newest_accepted_ack {
                check_term(echo.term, name)?;
            }
        }
        Some(Payload::HeartbeatAck(m)) => {
            let name = "LeaderHeartbeatAck";
            has_ids_and_configurations(m, name)?;
            check_term(m.term, name)?;
            check_announceable(m.configuration.as_ref(), m.recovery_epoch, m.term, name)?;
            check_prior(
                m.recipient_admission.as_ref(),
                m.recipient_prior_admission.as_ref(),
                name,
            )?;
        }
        Some(Payload::RollCall(m)) => {
            let name = "RollCall";
            has_ids_and_configurations(m, name)?;
            check_term(m.term, name)?;
        }
        Some(Payload::RollCallReply(m)) => {
            let name = "RollCallReply";
            has_ids_and_configurations(m, name)?;
            check_term(m.term, name)?;
            check_prior(m.admission.as_ref(), m.prior_admission.as_ref(), name)?;
        }
        Some(Payload::VoteRequest(m)) => {
            let name = "VoteRequest";
            has_ids_and_configurations(m, name)?;
            check_term(m.term, name)?;
        }
        Some(Payload::VoteGrant(m)) => {
            let name = "VoteGrant";
            check_ids(m, name)?;
            check_term(m.term, name)?;
        }
        Some(Payload::ElectionReject(m)) => {
            let name = "ElectionReject";
            has_ids_and_configurations(m, name)?;
            check_term(m.term, name)?;
            check_term(m.highest_term_seen, name)?;
            if let Some(leader) = &m.leader {
                if leader.leader_id.is_none() {
                    return Err(MalformedMessage::MissingId { message: name });
                }
                check_term(leader.term, name)?;
            }
        }
        Some(Payload::ElectionCertificate(m)) => {
            let name = "ElectionCertificate";
            has_ids_and_configurations(m, name)?;
            check_term(m.term, name)?;
            check_announceable(m.configuration.as_ref(), m.recovery_epoch, m.term, name)?;
            check_prior(
                m.recipient_admission.as_ref(),
                m.recipient_prior_admission.as_ref(),
                name,
            )?;
        }
        Some(Payload::SelfRemove(m)) => {
            has_ids_and_configurations(m, "SelfRemove")?;
        }
    }
    Ok(CheckedMessage(message))
}

fn check_ids(message: &impl RequiredIds, name: &'static str) -> Result<(), MalformedMessage> {
    message
        .has_required_ids()
        .then_some(())
        .ok_or(MalformedMessage::MissingId { message: name })
}

fn has_ids_and_configurations<M: RequiredIds + ValidConfigurations>(
    message: &M,
    name: &'static str,
) -> Result<(), MalformedMessage> {
    check_ids(message, name)?;
    message
        .has_valid_configurations()
        .then_some(())
        .ok_or(MalformedMessage::InvalidConfiguration { message: name })
}

fn check_term(term: u64, name: &'static str) -> Result<(), MalformedMessage> {
    is_a_term(term)
        .then_some(())
        .ok_or(MalformedMessage::TermAtMax { message: name })
}

/// Whether a term a peer names is one this node can act on: any but
/// `u64::MAX`.
pub(crate) fn is_a_term(term: u64) -> bool {
    term < u64::MAX
}

/// Whether the configuration a leader ack or an election certificate
/// carries is one the leader that sent it, elected in `term` at
/// `recovery_epoch`, could have announced (see [`decode`]).
fn check_announceable(
    configuration: Option<&generated::Configuration>,
    recovery_epoch: u64,
    term: u64,
    name: &'static str,
) -> Result<(), MalformedMessage> {
    configuration
        .and_then(|configuration| configuration.generation.as_ref())
        .is_some_and(|generation| {
            generation.recovery_epoch == recovery_epoch && generation.term <= term
        })
        .then_some(())
        .ok_or(MalformedMessage::Unannounceable { message: name })
}

/// A prior admission present without an admission would not decode.
fn check_prior(
    admission: Option<&generated::Generation>,
    prior_admission: Option<&generated::Generation>,
    name: &'static str,
) -> Result<(), MalformedMessage> {
    (admission.is_some() || prior_admission.is_none())
        .then_some(())
        .ok_or(MalformedMessage::PriorWithoutAdmission { message: name })
}

impl CheckedMessage {
    /// The message as the prost type, for encoding it.
    pub fn message(&self) -> &ElectionMessage {
        &self.0
    }

    pub fn into_message(self) -> ElectionMessage {
        self.0
    }

    /// The payload, cloned; `None` if the message carries none, or one this
    /// build does not know.
    pub fn payload(&self) -> Option<CheckedPayload> {
        self.0.payload.clone().map(CheckedPayload::of)
    }

    pub fn into_payload(self) -> Option<CheckedPayload> {
        self.0.payload.map(CheckedPayload::of)
    }
}

/// One checked payload.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckedPayload {
    Heartbeat(Checked<WorkerHeartbeat>),
    HeartbeatAck(Checked<LeaderHeartbeatAck>),
    RollCall(Checked<RollCall>),
    RollCallReply(Checked<RollCallReply>),
    VoteRequest(Checked<VoteRequest>),
    VoteGrant(Checked<VoteGrant>),
    ElectionReject(Checked<ElectionReject>),
    SelfRemove(Checked<SelfRemove>),
    ElectionCertificate(Checked<ElectionCertificate>),
}

impl CheckedPayload {
    /// Wraps a payload of a message `decode` accepted.
    fn of(payload: election_message::Payload) -> Self {
        use election_message::Payload;
        match payload {
            Payload::Heartbeat(m) => Self::Heartbeat(Checked(m)),
            Payload::HeartbeatAck(m) => Self::HeartbeatAck(Checked(m)),
            Payload::RollCall(m) => Self::RollCall(Checked(m)),
            Payload::RollCallReply(m) => Self::RollCallReply(Checked(m)),
            Payload::VoteRequest(m) => Self::VoteRequest(Checked(m)),
            Payload::VoteGrant(m) => Self::VoteGrant(Checked(m)),
            Payload::ElectionReject(m) => Self::ElectionReject(Checked(m)),
            Payload::SelfRemove(m) => Self::SelfRemove(Checked(m)),
            Payload::ElectionCertificate(m) => Self::ElectionCertificate(Checked(m)),
        }
    }
}

/// A part of a checked message: its raw fields through `Deref`, and the
/// prelude's accessors, which cannot fail on it. Built only by this module.
#[derive(Debug, Clone, PartialEq)]
pub struct Checked<T>(T);

impl<T> Deref for Checked<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{Configuration, Generation, Single};
    use crate::protocol::messages::{AckEcho, KnownLeader};

    /// Whether `decode` accepts a message.
    trait IsDecoded {
        fn is_decoded(&self) -> bool;
    }

    impl IsDecoded for ElectionMessage {
        fn is_decoded(&self) -> bool {
            decode(self.clone()).is_ok()
        }
    }

    #[test]
    fn decode_accepts_a_message_with_no_payload() {
        assert_eq!(
            decode(ElectionMessage { payload: None }),
            Ok(CheckedMessage(ElectionMessage { payload: None }))
        );
        assert!(decode(ElectionMessage { payload: None })
            .unwrap()
            .payload()
            .is_none());
    }

    #[test]
    fn decode_names_the_rule_a_message_breaks() {
        let certificate = |edit: fn(&mut ElectionCertificate)| {
            let mut message = certificate_carrying(Generation::new(1, 3, 1));
            let Some(election_message::Payload::ElectionCertificate(m)) = &mut message.payload
            else {
                unreachable!()
            };
            edit(m);
            decode(message).unwrap_err()
        };
        let name = "ElectionCertificate";

        assert_eq!(
            certificate(|m| m.leader_id = None),
            MalformedMessage::MissingId { message: name }
        );
        assert_eq!(
            certificate(|m| m.recipient_prior_admission = Some(generation_at_max())),
            MalformedMessage::InvalidConfiguration { message: name }
        );
        assert_eq!(
            certificate(|m| m.term = u64::MAX),
            MalformedMessage::TermAtMax { message: name }
        );
        assert_eq!(
            certificate(|m| m.term = 2),
            MalformedMessage::Unannounceable { message: name }
        );
        assert_eq!(
            certificate(|m| {
                m.recipient_admission = None;
                m.recipient_prior_admission = Some(Generation::genesis(0).into());
            }),
            MalformedMessage::PriorWithoutAdmission { message: name }
        );
    }

    /// A certificate from `leader-1` for `shard-1`, at recovery epoch 1 and
    /// term 3, carrying a single configuration of one voter at `generation`.
    fn certificate_carrying(generation: Generation) -> ElectionMessage {
        let configuration = Configuration::single(Single {
            generation,
            base: generation,
            voter_count: 1,
        }).expect("valid");
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
    fn decode_refuses_a_certificate_whose_configuration_its_leader_could_not_have_announced() {
        let cases = [
            (Generation::new(1, 3, 1), true, "the certificate's own term"),
            (Generation::new(1, 2, 5), true, "an earlier term, led as is"),
            (Generation::new(1, 4, 1), false, "a later term"),
            (Generation::new(0, 3, 1), false, "an older recovery epoch"),
            (Generation::new(2, 3, 1), false, "a newer recovery epoch"),
        ];
        for (generation, well_formed, case) in cases {
            assert_eq!(
                certificate_carrying(generation).is_decoded(),
                well_formed,
                "{case}"
            );
        }
    }

    #[test]
    fn decode_refuses_a_certificate_with_no_or_an_invalid_configuration() {
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

        assert!(!no_configuration.is_decoded());
        assert!(!invalid_prior.is_decoded());
    }

    #[test]
    fn decode_refuses_an_invalid_prior_admission_or_held_generation() {
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
        assert!(reply(genesis(), genesis()).is_decoded());
        assert!(!reply(genesis(), Some(generation_at_max())).is_decoded());
        assert!(reply(None, None).is_decoded());
        assert!(!reply(Some(generation_at_max()), None).is_decoded());
        assert!(heartbeat(None).is_decoded());
        assert!(heartbeat(Some(Generation::genesis(0).into())).is_decoded());
        assert!(!heartbeat(Some(generation_at_max())).is_decoded());
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
    /// (`ShardStanding` sets them together), but the wire edge must
    /// still refuse the shape rather than let a peer bug feed a prior
    /// admission the old side's tally would otherwise count.
    #[test]
    fn decode_refuses_a_prior_admission_sent_without_an_admission() {
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
                with_admissions(None, None).is_decoded(),
                "{field}: neither present"
            );
            assert!(
                with_admissions(admission, None).is_decoded(),
                "{field}: admission alone"
            );
            assert!(
                with_admissions(admission, prior_admission).is_decoded(),
                "{field}: both present"
            );
            assert!(
                !with_admissions(None, prior_admission).is_decoded(),
                "{field}: prior admission alone"
            );
        }
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
    fn decode_refuses_a_missing_or_invalid_configuration() {
        assert!(!roll_call(None).is_decoded());
        let no_voters = generated::Configuration {
            electorate: Some(generated::configuration::Electorate::Single(
                generated::SingleElectorate { voter_count: 0 },
            )),
            ..valid_configuration()
        };
        assert!(!roll_call(Some(no_voters)).is_decoded());
    }

    fn generation_at_max() -> generated::Generation {
        generated::Generation {
            recovery_epoch: 0,
            term: 0,
            counter: u64::MAX,
        }
    }

    #[test]
    fn decode_refuses_a_missing_required_generation() {
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

        assert!(request(Some(Generation::genesis(0).into())).is_decoded());
        assert!(!request(None).is_decoded());
        assert!(!request(Some(generation_at_max())).is_decoded());
    }

    #[test]
    fn decode_refuses_a_rejection_carrying_an_invalid_configuration_but_not_none() {
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

        assert!(reject(None).is_decoded());
        assert!(reject(Some(valid_configuration())).is_decoded());
        assert!(!reject(Some(no_voters)).is_decoded());
    }

    #[test]
    fn decode_refuses_a_rejection_naming_a_leader_without_its_id() {
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

        assert!(reject(None).is_decoded());
        assert!(
            reject(Some(KnownLeader {
                leader_id: wid("worker-c"),
                term: 2,
            }))
            .is_decoded()
        );
        assert!(
            !reject(Some(KnownLeader {
                leader_id: None,
                term: 2,
            }))
            .is_decoded()
        );
    }

    /// An ack at recovery epoch 1 and term 3 carrying a configuration at
    /// `generation`.
    fn ack_carrying(generation: Generation) -> ElectionMessage {
        let configuration = Configuration::single(Single {
            generation,
            base: generation,
            voter_count: 1,
        }).expect("valid");
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
    fn decode_refuses_an_ack_whose_configuration_its_leader_could_not_have_announced() {
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
                ack_carrying(generation).is_decoded(),
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
    fn decode_refuses_a_term_of_u64_max_in_every_term_field() {
        for (field, with_term) in with_term_in_each_field() {
            assert!(with_term(u64::MAX - 1).is_decoded(), "{field}");
            assert!(!with_term(u64::MAX).is_decoded(), "{field}");
        }
    }



    #[test]
    fn decode_refuses_a_missing_top_level_id() {
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

        assert!(!message.is_decoded());
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
    fn decode_refuses_a_heartbeat_that_names_no_shard() {
        let shard = Some(generated::ShardId {
            value: "shard-1".into(),
        });

        assert!(heartbeat_with(shard, None).is_decoded());
        assert!(!heartbeat_with(None, None).is_decoded());
    }
}
