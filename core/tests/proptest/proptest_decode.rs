//! Property test for the election message edge: whatever a peer sends,
//! `checked::decode` never panics, and every message it accepts can be
//! stepped by a voter, a leader and a bootstrapping node without a panic. The
//! rules `decode` enforces are the ones that keep `WorkerNode::step` from
//! panicking (`Generation::next_change`, the next term of a roll call), so
//! this is the guarantee that a peer cannot crash a worker.
//!
//! Messages are generated as values, not bytes: decoding bytes into a message
//! is prost's job. Terms and counters are drawn from small values and the
//! edges around `u64::MAX`, and each optional field is independently absent.

use kabudachi_core::election::{Entry, Identity, Input, WorkerNode};
use kabudachi_core::protocol::checked;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionCertificate, ElectionMessage, ElectionReject, KnownLeader,
    LeaderHeartbeatAck, RollCall, RollCallReply, SelfRemove, VoteGrant, VoteRequest,
    WorkerHeartbeat, election_message::Payload,
};
use kabudachi_core::protocol::ids::IncarnationId;
use kabudachi_core::time::Duration;
use proptest::prelude::*;

use crate::support::builders::{configuration_of, no_leader_yet, shard, timings, worker};
use crate::support::clock::FakeClock;
use crate::support::node::{TestNode, elect, voter_node};

const SUSPECT_TIMEOUT: u64 = 100;

fn term() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0),
        Just(1),
        Just(2),
        Just(u64::MAX - 1),
        Just(u64::MAX)
    ]
}

fn counter() -> impl Strategy<Value = u64> {
    prop_oneof![0..3u64, Just(u64::MAX)]
}

fn worker_id() -> impl Strategy<Value = Option<generated::WorkerId>> {
    prop_oneof![
        1 => Just(None),
        6 => prop_oneof![Just("me"), Just("peer"), Just("other")]
            .prop_map(|id| Some(worker(id).into())),
    ]
}

fn shard_id() -> impl Strategy<Value = Option<generated::ShardId>> {
    prop_oneof![
        1 => Just(None),
        6 => prop_oneof![Just("shard-1"), Just("shard-2")].prop_map(|id| Some(shard(id).into())),
    ]
}

fn incarnation_id() -> impl Strategy<Value = Option<generated::IncarnationId>> {
    prop_oneof![
        1 => Just(None),
        6 => Just(Some(IncarnationId::new("incarnation-1").into())),
    ]
}

fn generation() -> impl Strategy<Value = generated::Generation> {
    (0..2u64, 0..4u64, counter()).prop_map(|(recovery_epoch, term, counter)| {
        generated::Generation {
            recovery_epoch,
            term,
            counter,
        }
    })
}

fn optional_generation() -> impl Strategy<Value = Option<generated::Generation>> {
    prop_oneof![1 => Just(None), 3 => generation().prop_map(Some)]
}

fn configuration() -> impl Strategy<Value = Option<generated::Configuration>> {
    prop_oneof![
        1 => Just(None),
        4 => (1..4usize).prop_map(|voters| Some((&configuration_of(voters)).into())),
        1 => Just(Some(generated::Configuration {
            electorate: Some(generated::configuration::Electorate::Single(
                generated::SingleElectorate { voter_count: 0 },
            )),
            ..(&configuration_of(1)).into()
        })),
    ]
}

fn heartbeat() -> impl Strategy<Value = Payload> {
    (
        worker_id(),
        incarnation_id(),
        shard_id(),
        term(),
        prop::option::of(term()),
        optional_generation(),
    )
        .prop_map(
            |(worker_id, incarnation_id, shard_id, term_seen, echo, configuration_generation)| {
                Payload::Heartbeat(WorkerHeartbeat {
                    worker_id,
                    incarnation_id,
                    shard_id,
                    term_seen,
                    newest_accepted_ack: echo.map(|term| AckEcho {
                        term,
                        send_token: 0,
                    }),
                    configuration_generation,
                    ..Default::default()
                })
            },
        )
}

fn heartbeat_ack() -> impl Strategy<Value = Payload> {
    (
        (shard_id(), worker_id(), 0..2u64, term()),
        configuration(),
        optional_generation(),
        optional_generation(),
    )
        .prop_map(
            |((shard_id, leader_id, recovery_epoch, term), configuration, admission, prior)| {
                Payload::HeartbeatAck(LeaderHeartbeatAck {
                    shard_id,
                    leader_id,
                    recovery_epoch,
                    term,
                    configuration,
                    recipient_admission: admission,
                    recipient_prior_admission: prior,
                    ..Default::default()
                })
            },
        )
}

fn roll_call() -> impl Strategy<Value = Payload> {
    (shard_id(), worker_id(), term(), configuration()).prop_map(
        |(shard_id, initiator_id, term, configuration)| {
            Payload::RollCall(RollCall {
                shard_id,
                initiator_id,
                term,
                configuration,
                ..Default::default()
            })
        },
    )
}

fn roll_call_reply() -> impl Strategy<Value = Payload> {
    (
        (shard_id(), worker_id(), worker_id(), term()),
        optional_generation(),
        optional_generation(),
    )
        .prop_map(
            |((shard_id, initiator_id, responder_id, term), admission, prior_admission)| {
                Payload::RollCallReply(RollCallReply {
                    shard_id,
                    initiator_id,
                    responder_id,
                    term,
                    admission,
                    prior_admission,
                    ..Default::default()
                })
            },
        )
}

fn vote_request() -> impl Strategy<Value = Payload> {
    (shard_id(), worker_id(), term(), optional_generation()).prop_map(
        |(shard_id, candidate_id, term, roll_call_generation)| {
            Payload::VoteRequest(VoteRequest {
                shard_id,
                candidate_id,
                term,
                roll_call_generation,
                ..Default::default()
            })
        },
    )
}

fn vote_grant() -> impl Strategy<Value = Payload> {
    (shard_id(), worker_id(), worker_id(), term()).prop_map(
        |(shard_id, candidate_id, voter_id, term)| {
            Payload::VoteGrant(VoteGrant {
                shard_id,
                candidate_id,
                voter_id,
                term,
                ..Default::default()
            })
        },
    )
}

fn election_reject() -> impl Strategy<Value = Payload> {
    (
        (shard_id(), worker_id(), worker_id()),
        (term(), term()),
        configuration(),
        prop::option::of((worker_id(), term())),
    )
        .prop_map(
            |((shard_id, initiator_id, rejecter_id), (term, highest_term_seen), configuration, leader)| {
                Payload::ElectionReject(ElectionReject {
                    shard_id,
                    initiator_id,
                    rejecter_id,
                    term,
                    highest_term_seen,
                    configuration,
                    leader: leader.map(|(leader_id, term)| KnownLeader { leader_id, term }),
                    ..Default::default()
                })
            },
        )
}

fn self_remove() -> impl Strategy<Value = Payload> {
    (
        worker_id(),
        incarnation_id(),
        shard_id(),
        optional_generation(),
        term(),
        term(),
    )
        .prop_map(
            |(worker_id, incarnation_id, shard_id, configuration_generation, term_seen, leader_term)| {
                Payload::SelfRemove(SelfRemove {
                    worker_id,
                    incarnation_id,
                    shard_id,
                    configuration_generation,
                    term_seen,
                    leader_term,
                })
            },
        )
}

fn election_certificate() -> impl Strategy<Value = Payload> {
    (
        (shard_id(), worker_id(), 0..2u64, term()),
        configuration(),
        optional_generation(),
        optional_generation(),
    )
        .prop_map(
            |((shard_id, leader_id, recovery_epoch, term), configuration, admission, prior)| {
                Payload::ElectionCertificate(ElectionCertificate {
                    shard_id,
                    leader_id,
                    recovery_epoch,
                    term,
                    configuration,
                    recipient_admission: admission,
                    recipient_prior_admission: prior,
                })
            },
        )
}

fn arbitrary_election_message() -> impl Strategy<Value = ElectionMessage> {
    prop_oneof![
        1 => Just(None),
        2 => heartbeat().prop_map(Some),
        2 => heartbeat_ack().prop_map(Some),
        2 => roll_call().prop_map(Some),
        2 => roll_call_reply().prop_map(Some),
        2 => vote_request().prop_map(Some),
        2 => vote_grant().prop_map(Some),
        2 => election_reject().prop_map(Some),
        2 => self_remove().prop_map(Some),
        2 => election_certificate().prop_map(Some),
    ]
    .prop_map(|payload| ElectionMessage { payload })
}

/// An `Active` voter of three, a leader of one, and a node still
/// `Bootstrapping`, each named `me` in `shard-1`.
fn a_voter_a_leader_and_a_bootstrapping_node() -> [TestNode; 3] {
    let clock = FakeClock::new();
    let me = worker("me");
    let voter = voter_node(&clock, &me, 3, SUSPECT_TIMEOUT);
    let mut leader = voter_node(&clock, &me, 1, SUSPECT_TIMEOUT);
    elect(&mut leader, &clock, SUSPECT_TIMEOUT, &[]);
    let bootstrapping = WorkerNode::start(
        Identity {
            id: me,
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT)),
        },
        Entry::Joining(no_leader_yet()),
        clock,
        None,
    )
    .0;
    [voter, leader, bootstrapping]
}

proptest! {
    // Whatever election message a peer sends, decoding it never panics, and
    // a voter, a leader and a bootstrapping node each step what decode
    // accepts without panicking.
    #[test]
    fn decode_never_panics_and_a_node_steps_what_it_accepts(message in arbitrary_election_message()) {
        if let Ok(checked) = checked::decode(message) {
            for mut node in a_voter_a_leader_and_a_bootstrapping_node() {
                let _ = node.step(Input::Message { from: worker("peer"), message: checked.clone() });
            }
        }
    }
}
