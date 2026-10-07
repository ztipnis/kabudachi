//! Message, ID and configuration builders shared by the election tests.

use kabudachi_core::configuration::{Admission, Configuration, Generation, Joint, Single};
use kabudachi_core::election::{ElectionTimings, Input, KnownConfiguration};
use kabudachi_core::protocol::checked::{self, CheckedMessage};
use kabudachi_core::protocol::ids::IncarnationId;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionCertificate, ElectionMessage, ElectionReject, ElectionRejectReason,
    JoinResponse, KnownLeader, LeaderHeartbeatAck, RollCall, RollCallReply, SelfRemove, VoteGrant,
    VoteRequest, WorkerHeartbeat, election_message,
};
use kabudachi_core::time::Duration;

const SHARD: &str = "shard-1";

pub fn worker(id: &str) -> WorkerId {
    WorkerId::new(id)
}

pub fn shard(id: &str) -> ShardId {
    ShardId::new(id)
}

/// The timings the election tests build nodes with: a follower heartbeats
/// four times per `suspect_timeout`. Each heartbeat confirms the ack that
/// answered the one before it, so a leader's newest confirmation is at most
/// two intervals old, half the suspicion timeout, well inside its lease of
/// nine tenths; and a follower whose heartbeats are answered never suspects
/// its leader. A roll call runs for a quarter of `suspect_timeout` too.
pub fn timings(suspect_timeout: Duration) -> ElectionTimings {
    let quarter = Duration::from_ticks((suspect_timeout.as_ticks() / 4).max(1));
    ElectionTimings::new(suspect_timeout, quarter).with_roll_call_deadline(quarter)
}

/// How long after its last leader contact any node built with
/// `timings(suspect_timeout ticks)` has suspected its leader, whatever the
/// jitter on its suspicion timeout: that jitter lengthens it by less than
/// a half.
pub fn past_any_suspicion(suspect_timeout: u64) -> Duration {
    Duration::from_ticks(suspect_timeout + suspect_timeout.div_ceil(2))
}

/// A JOIN answer that names no leader. A node started on it stays
/// `Bootstrapping` (see `Input::JoinAnswer`), for a test to join it
/// later or watch it wait.
pub fn no_leader_yet() -> JoinResponse {
    JoinResponse::default()
}

/// The genesis generation at recovery epoch 0, which the tests' statically
/// configured nodes are all admitted at.
pub fn g0() -> Generation {
    Generation::genesis(0)
}

/// A configuration of `voter_count` voters admitted at [`g0`].
pub fn configuration_of(voter_count: usize) -> Configuration {
    Configuration::single(Single {
        generation: g0(),
        base: g0(),
        voter_count,
    }).expect("valid")
}

/// A voter of `configuration_of(voter_count)`, admitted at [`g0`].
pub fn voter_of(voter_count: usize) -> KnownConfiguration {
    KnownConfiguration {
        configuration: configuration_of(voter_count),
        admission: Some(g0()),
    }
}

pub fn message(payload: election_message::Payload) -> ElectionMessage {
    ElectionMessage {
        payload: Some(payload),
    }
}

/// `message`, decoded: what a node accepts. Builders return raw messages, for
/// tests to compare and to malform on purpose; a test that hands one to a
/// node decodes it here, which also proves every message a node sends decodes.
pub fn checked(message: ElectionMessage) -> CheckedMessage {
    checked::decode(message).expect("a test builder builds well-formed messages")
}

/// `Input::Message` delivering `message`, decoded, from `from`.
pub fn message_input(from: &WorkerId, message: ElectionMessage) -> Input {
    Input::Message {
        from: from.clone(),
        message: checked(message),
    }
}

/// A roll call for `shard-1` by `initiator`, contesting `term` under
/// `configuration`, stamped `timestamp_millis`.
pub fn roll_call(
    initiator: &WorkerId,
    term: u64,
    configuration: &Configuration,
    timestamp_millis: u64,
) -> RollCall {
    RollCall {
        shard_id: Some(shard(SHARD).into()),
        term,
        configuration: Some(configuration.into()),
        timestamp_millis,
        initiator_id: Some(initiator.clone().into()),
        initiator_address: String::new(),
    }
}

pub fn roll_call_message(call: RollCall) -> ElectionMessage {
    message(election_message::Payload::RollCall(call))
}

/// `responder`'s reply, admitted at `admission`, to `initiator`'s roll call
/// for `term` in `shard-1`.
pub fn roll_call_reply(
    initiator: &WorkerId,
    term: u64,
    responder: &WorkerId,
    admission: Option<Generation>,
) -> ElectionMessage {
    message(election_message::Payload::RollCallReply(RollCallReply {
        shard_id: Some(shard(SHARD).into()),
        term,
        initiator_id: Some(initiator.clone().into()),
        responder_id: Some(responder.clone().into()),
        responder_address: String::new(),
        admission: admission.map(Into::into),
        prior_admission: None,
    }))
}

/// A `WorkerHeartbeat` from `sender` for `shard-1` at recovery epoch 0,
/// having seen term 1, echoing `newest_accepted_ack` and holding no
/// configuration; its incarnation is `incarnation-1` and it reports no
/// capacity or running work.
pub fn heartbeat(sender: &WorkerId, newest_accepted_ack: Option<AckEcho>) -> WorkerHeartbeat {
    WorkerHeartbeat {
        worker_id: Some(sender.clone().into()),
        incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
        recovery_epoch_seen: 0,
        term_seen: 1,
        available_capacity: 0,
        active_task_runs_digest: vec![],
        shard_id: Some(shard(SHARD).into()),
        newest_accepted_ack,
        configuration_generation: None,
        send_token: 0,
        routing_crawled: false,
        crawl_admission: None,
    }
}

pub fn heartbeat_message(heartbeat: WorkerHeartbeat) -> ElectionMessage {
    message(election_message::Payload::Heartbeat(heartbeat))
}

/// An ack from `leader`, elected in `term`, for `shard-1` at recovery epoch
/// 0, carrying `configuration` and the recipient's `recipient_admission`,
/// sent at tick 0.
pub fn leader_ack(
    leader: &WorkerId,
    term: u64,
    configuration: &Configuration,
    recipient_admission: Option<Generation>,
) -> LeaderHeartbeatAck {
    LeaderHeartbeatAck {
        shard_id: Some(shard(SHARD).into()),
        leader_id: Some(leader.clone().into()),
        recovery_epoch: 0,
        term,
        configuration: Some(configuration.into()),
        recipient_admission: recipient_admission.map(Into::into),
        send_token: 0,
        recipient_prior_admission: None,
        heartbeat_token: None,
        recovery_epoch_lineage: None,
    }
}

pub fn ack_message(ack: LeaderHeartbeatAck) -> ElectionMessage {
    message(election_message::Payload::HeartbeatAck(ack))
}

/// `rejecter`'s refusal, for `reason`, of `initiator`'s roll call or vote
/// request for `term` in `shard-1`, naming the highest term it has seen,
/// carrying its configuration `configuration_of(3)` at recovery epoch 0 and, if any, naming its
/// leader with that leader's term.
pub fn election_reject(
    initiator: &WorkerId,
    term: u64,
    rejecter: &WorkerId,
    reason: ElectionRejectReason,
    highest_term_seen: u64,
    leader: Option<(&WorkerId, u64)>,
) -> ElectionMessage {
    message(election_message::Payload::ElectionReject(ElectionReject {
        shard_id: Some(shard(SHARD).into()),
        term,
        initiator_id: Some(initiator.clone().into()),
        rejecter_id: Some(rejecter.clone().into()),
        reason: reason as i32,
        highest_term_seen,
        configuration: Some((&configuration_of(3)).into()),
        leader: leader.map(|(leader, term)| KnownLeader {
            leader_id: Some(leader.clone().into()),
            term,
        }),
        recovery_epoch: Some(0),
        recovery_epoch_lineage: None,
    }))
}

/// A `VoteRequest` for `shard-1`, for a roll call run under [`g0`].
pub fn vote_request(candidate: WorkerId, recovery_epoch: u64, term: u64) -> VoteRequest {
    VoteRequest {
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch,
        term,
        candidate_id: Some(candidate.into()),
        roll_call_generation: Some(g0().into()),
    }
}

pub fn vote_request_message(request: VoteRequest) -> ElectionMessage {
    message(election_message::Payload::VoteRequest(request))
}

/// A `VoteGrant` for `shard-1` at recovery epoch 0.
pub fn vote_grant(candidate: WorkerId, voter: WorkerId, term: u64) -> VoteGrant {
    VoteGrant {
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch: 0,
        term,
        candidate_id: Some(candidate.into()),
        voter_id: Some(voter.into()),
    }
}

pub fn vote_grant_message(grant: VoteGrant) -> ElectionMessage {
    message(election_message::Payload::VoteGrant(grant))
}

/// `leader`'s certificate for `shard-1` at recovery epoch 0 that, winning
/// the election for `term`, it leads `configuration`, where the recipient
/// holds `admission`.
pub fn election_certificate(
    leader: &WorkerId,
    term: u64,
    configuration: &Configuration,
    admission: Admission,
) -> ElectionCertificate {
    ElectionCertificate {
        shard_id: Some(shard(SHARD).into()),
        recovery_epoch: 0,
        term,
        leader_id: Some(leader.clone().into()),
        configuration: Some(configuration.into()),
        recipient_admission: admission.current.map(Into::into),
        recipient_prior_admission: admission.prior.map(Into::into),
    }
}

/// The joint configuration an election for `term` under
/// `configuration_of(old_voter_count)` founds with `respondents`
/// respondents: its new side at generation and base (0, `term`, 1), its old
/// side the configuration at [`g0`].
pub fn founded_from_g0(term: u64, old_voter_count: usize, respondents: usize) -> Configuration {
    let founded = Generation::new(0, term, 1);
    Configuration::joint(Joint {
        generation: founded,
        base: founded,
        batch_generation: founded,
        old_base: g0(),
        old_generation: g0(),
        old_voter_count,
        new_voter_count: respondents,
    }).expect("valid")
}

/// What `founded_from_g0(term, …)` commits to: its `respondents` alone, at
/// the next generation a leader of `leader_term` announces, which is also its
/// base (the commit re-bases it there).
pub fn committed_from_g0(term: u64, leader_term: u64, respondents: usize) -> Configuration {
    let committed = Generation::new(0, term, 1).next_change(leader_term);
    Configuration::single(Single {
        generation: committed,
        base: committed,
        voter_count: respondents,
    }).expect("valid")
}

pub fn election_certificate_message(certificate: ElectionCertificate) -> ElectionMessage {
    message(election_message::Payload::ElectionCertificate(certificate))
}

/// `worker`'s `SelfRemove` for `shard_id`, sent under a configuration at
/// [`g0`], having seen term 1 at most.
pub fn self_remove(worker: &WorkerId, shard_id: &str) -> SelfRemove {
    SelfRemove {
        worker_id: Some(worker.clone().into()),
        incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
        shard_id: Some(shard(shard_id).into()),
        configuration_generation: Some(g0().into()),
        term_seen: 1,
        leader_term: 1,
    }
}

pub fn self_remove_message(msg: SelfRemove) -> ElectionMessage {
    message(election_message::Payload::SelfRemove(msg))
}
