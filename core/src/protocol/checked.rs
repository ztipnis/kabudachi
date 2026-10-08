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
            check_announceable(
                m.configuration.as_ref(),
                (m.recovery_epoch, m.recovery_epoch_lineage),
                m.term,
                name,
            )?;
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
            check_announceable(
                m.configuration.as_ref(),
                (m.recovery_epoch, m.recovery_epoch_lineage),
                m.term,
                name,
            )?;
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
    recovery_epoch: (u64, u64),
    term: u64,
    name: &'static str,
) -> Result<(), MalformedMessage> {
    configuration
        .and_then(|configuration| configuration.generation.as_ref())
        .is_some_and(|generation| {
            (generation.recovery_epoch, generation.recovery_epoch_lineage) == recovery_epoch
                && generation.term <= term
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
