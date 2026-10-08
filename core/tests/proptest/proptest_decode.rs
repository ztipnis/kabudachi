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

use kabudachi_core::configuration::{Admission, Configuration, Generation, Single};
use kabudachi_core::election::{Entry, Identity, Input, WorkerNode};
use kabudachi_core::protocol::checked;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::messages::{
    AckEcho, Claim, ClaimBatch, ClaimRequest, ClaimResponse, ElectionCertificate, ElectionMessage,
    ElectionReject, JoinResponse, KnownLeader, LeaderHeartbeatAck, RollCall, RollCallReply,
    SelfRemove, TaskRequest, VoteGrant, VoteRequest, WellFormed, WorkerHeartbeat, claim_response,
    election_message::Payload,
};
use kabudachi_core::protocol::ids::IncarnationId;
use kabudachi_core::time::Duration;
use proptest::prelude::*;

use crate::support::builders::{epoch, self, configuration_of, g0, no_leader_yet, shard, timings, worker};
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
            recovery_epoch_lineage: 0,
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
                    recovery_epoch_lineage: 0,
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
                    recovery_epoch_lineage: 0,
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
    #![proptest_config(crate::proptest::config(256))]

    // Whatever election message a peer sends, decoding it never panics, and
    // a voter, a leader and a bootstrapping node each step what decode
    // accepts without panicking.
    #[test]
    fn decode_never_panics_and_a_node_steps_what_it_accepts(message in arbitrary_election_message()) {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        if let Ok(checked) = checked::decode(message) {
            for mut node in a_voter_a_leader_and_a_bootstrapping_node() {
                let _ = node.step(Input::Message { from: worker("peer"), message: checked.clone() });
            }
        }
    }
}

// The claim and join messages net's codecs read, each missing something a
// node cannot act on without: net rejects them at the codec as invalid data.
#[test]
fn a_claim_or_join_message_missing_what_a_node_needs_is_not_well_formed() {
    let a_leader = || Some(worker("leader").into());
    let cases: [(&str, bool); 7] = [
        (
            "a claim request that asks for nothing",
            ClaimRequest { request: None }.is_well_formed(),
        ),
        (
            "an accepted claim missing its task run id",
            ClaimResponse {
                result: Some(claim_response::Result::Accept(Claim::default())),
            }
            .is_well_formed(),
        ),
        (
            "a claim batch holding a claim missing its task run id",
            ClaimResponse {
                result: Some(claim_response::Result::Batch(ClaimBatch {
                    claims: vec![Claim::default()],
                })),
            }
            .is_well_formed(),
        ),
        (
            "a task request that asks for nothing",
            TaskRequest { request: None }.is_well_formed(),
        ),
        (
            "a leader pointer missing its address",
            JoinResponse {
                leader_id: a_leader(),
                leader_multiaddr: String::new(),
                term: 3,
                ..Default::default()
            }
            .is_well_formed(),
        ),
        (
            "a leader pointer with an address and no leader",
            JoinResponse {
                leader_id: None,
                leader_multiaddr: "/ip4/127.0.0.1/tcp/1".into(),
                term: 3,
                ..Default::default()
            }
            .is_well_formed(),
        ),
        (
            "a leader pointer at a term no leader holds",
            JoinResponse {
                leader_id: a_leader(),
                leader_multiaddr: "/ip4/127.0.0.1/tcp/1".into(),
                term: u64::MAX,
                ..Default::default()
            }
            .is_well_formed(),
        ),
    ];
    for (what, well_formed) in cases {
        assert!(!well_formed, "{what} was accepted");
    }
    assert!(
        JoinResponse::default().is_well_formed(),
        "\"no leader known\" is a well formed answer"
    );
}

/// `decode` refuses each way a peer can malform an election message, and
/// accepts the message one step inside the boundary: a missing or invalid
/// configuration, generation or id, a term or counter at `u64::MAX`, a
/// configuration its leader could not have announced, a prior admission
/// without an admission.
#[test]
fn decode_refuses_each_malformed_message_and_accepts_its_boundary_twin() {
    use election_message::Payload;
    use kabudachi_core::protocol::messages::election_message;

    fn at_max() -> generated::Generation {
        generated::Generation {
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
            term: 0,
            counter: u64::MAX,
        }
    }
    fn genesis() -> generated::Generation {
        g0().into()
    }
    fn generation(recovery_epoch: u64, term: u64, counter: u64) -> generated::Generation {
        generated::Generation {
            recovery_epoch,
            recovery_epoch_lineage: 0,
            term,
            counter,
        }
    }
    fn single_of(generation: Generation) -> generated::Configuration {
        (&Configuration::single(Single {
            generation,
            base: g0(),
            voter_count: 1,
        })
        .unwrap())
            .into()
    }
    fn zero_voters() -> generated::Configuration {
        generated::Configuration {
            electorate: Some(generated::configuration::Electorate::Single(
                generated::SingleElectorate { voter_count: 0 },
            )),
            ..(&configuration_of(1)).into()
        }
    }
    fn certificate(edit: impl FnOnce(&mut ElectionCertificate)) -> ElectionMessage {
        let mut certificate = builders::election_certificate(
            &worker("leader"),
            3,
            &configuration_of(1),
            Admission {
                current: Some(g0()),
                prior: None,
            },
        );
        edit(&mut certificate);
        builders::election_certificate_message(certificate)
    }
    fn ack(edit: impl FnOnce(&mut LeaderHeartbeatAck)) -> ElectionMessage {
        let mut ack = builders::leader_ack(&worker("leader"), 3, &configuration_of(1), Some(g0()));
        edit(&mut ack);
        builders::ack_message(ack)
    }
    fn reply(edit: impl FnOnce(&mut RollCallReply)) -> ElectionMessage {
        let mut reply = RollCallReply {
            shard_id: Some(shard("shard-1").into()),
            term: 1,
            initiator_id: Some(worker("a").into()),
            responder_id: Some(worker("b").into()),
            admission: Some(genesis()),
            ..Default::default()
        };
        edit(&mut reply);
        builders::message(Payload::RollCallReply(reply))
    }
    fn heartbeat(edit: impl FnOnce(&mut WorkerHeartbeat)) -> ElectionMessage {
        let mut heartbeat = builders::heartbeat(&worker("a"), None);
        edit(&mut heartbeat);
        builders::heartbeat_message(heartbeat)
    }
    fn vote_request(edit: impl FnOnce(&mut VoteRequest)) -> ElectionMessage {
        let mut request = builders::vote_request(worker("a"), 0, 1);
        edit(&mut request);
        builders::vote_request_message(request)
    }
    fn vote_grant(edit: impl FnOnce(&mut VoteGrant)) -> ElectionMessage {
        let mut grant = builders::vote_grant(worker("a"), worker("b"), 1);
        edit(&mut grant);
        builders::vote_grant_message(grant)
    }
    fn reject(edit: impl FnOnce(&mut ElectionReject)) -> ElectionMessage {
        let mut reject = ElectionReject {
            shard_id: Some(shard("shard-1").into()),
            initiator_id: Some(worker("a").into()),
            rejecter_id: Some(worker("b").into()),
            leader: Some(KnownLeader {
                leader_id: Some(worker("c").into()),
                term: 1,
            }),
            ..Default::default()
        };
        edit(&mut reject);
        builders::message(Payload::ElectionReject(reject))
    }
    fn roll_call_carrying(configuration: Option<generated::Configuration>) -> ElectionMessage {
        let mut call = builders::roll_call(&worker("a"), 1, &configuration_of(1), 0);
        call.configuration = configuration;
        builders::roll_call_message(call)
    }
    fn joint(edit: impl FnOnce(&mut generated::JointElectorate)) -> generated::Configuration {
        let mut electorate = generated::JointElectorate {
            batch_generation: Some(generation(1, 2, 3)),
            old_voter_count: 3,
            new_voter_count: 5,
            old_base: Some(generation(1, 1, 0)),
            old_generation: Some(generation(1, 1, 4)),
        };
        edit(&mut electorate);
        generated::Configuration {
            generation: Some(generation(1, 2, 5)),
            base: Some(generation(1, 2, 0)),
            electorate: Some(generated::configuration::Electorate::Joint(electorate)),
        }
    }
    fn configuration(
        edit: impl FnOnce(&mut generated::Configuration),
    ) -> Option<generated::Configuration> {
        let mut configuration = generated::Configuration::from(&configuration_of(1));
        edit(&mut configuration);
        Some(configuration)
    }

    let accepted = [
        ("a certificate", certificate(|_| {})),
        ("an ack", ack(|_| {})),
        ("a reply with both admissions", reply(|m| m.prior_admission = Some(genesis()))),
        ("a reply with no admission", reply(|m| m.admission = None)),
        ("a heartbeat holding a generation", heartbeat(|m| m.configuration_generation = Some(genesis()))),
        ("a vote request", vote_request(|_| {})),
        ("a rejection naming no leader and no configuration", reject(|m| m.leader = None)),
        ("a rejection with a valid configuration", reject(|m| m.configuration = Some((&configuration_of(1)).into()))),
        ("a message with no payload", ElectionMessage { payload: None }),
        ("a configuration from an earlier term", certificate(|m| m.configuration = Some(single_of(Generation::new(epoch(0), 2, 1))))),
        ("a counter one below u64::MAX", roll_call_carrying(configuration(|c| c.generation = Some(generation(0, 0, u64::MAX - 1))))),
        ("a valid joint configuration", roll_call_carrying(Some(joint(|_| {})))),
    ];
    let refused = [
        ("a certificate whose configuration is from a later term", certificate(|m| m.configuration = Some(single_of(Generation::new(epoch(0), 4, 1))))),
        ("a certificate whose configuration is from another recovery epoch", certificate(|m| m.configuration = Some(single_of(Generation::new(epoch(1), 0, 1))))),
        ("a certificate whose configuration is of another lineage", certificate(|m| m.recovery_epoch_lineage = 1)),
        ("an ack whose configuration is from a later term", ack(|m| m.configuration = Some(single_of(Generation::new(epoch(0), 4, 1))))),
        ("a certificate with no configuration", certificate(|m| m.configuration = None)),
        ("a certificate with an invalid prior admission", certificate(|m| m.recipient_prior_admission = Some(at_max()))),
        ("a certificate with a prior admission and no admission", certificate(|m| {
            m.recipient_admission = None;
            m.recipient_prior_admission = Some(genesis());
        })),
        ("an ack with a prior admission and no admission", ack(|m| {
            m.recipient_admission = None;
            m.recipient_prior_admission = Some(genesis());
        })),
        ("a reply with a prior admission and no admission", reply(|m| {
            m.admission = None;
            m.prior_admission = Some(genesis());
        })),
        ("a reply with an invalid admission", reply(|m| m.admission = Some(at_max()))),
        ("a reply with an invalid prior admission", reply(|m| m.prior_admission = Some(at_max()))),
        ("a heartbeat holding an invalid generation", heartbeat(|m| m.configuration_generation = Some(at_max()))),
        ("a vote request with no roll call generation", vote_request(|m| m.roll_call_generation = None)),
        ("a vote request with an invalid roll call generation", vote_request(|m| m.roll_call_generation = Some(at_max()))),
        ("a rejection with an invalid configuration", reject(|m| m.configuration = Some(zero_voters()))),
        ("a rejection naming a leader with no id", reject(|m| m.leader = Some(KnownLeader { leader_id: None, term: 2 }))),
        ("an ack with no leader id", ack(|m| m.leader_id = None)),
        ("a heartbeat naming no shard", heartbeat(|m| m.shard_id = None)),
        ("a roll call with no configuration", roll_call_carrying(None)),
        ("a roll call with no voters", roll_call_carrying(Some(zero_voters()))),
        ("a generation counter at u64::MAX", roll_call_carrying(configuration(|c| c.generation = Some(at_max())))),
        ("a base counter at u64::MAX", roll_call_carrying(configuration(|c| c.base = Some(at_max())))),
        ("a configuration with no generation", roll_call_carrying(configuration(|c| c.generation = None))),
        ("a configuration with no base", roll_call_carrying(configuration(|c| c.base = None))),
        ("a configuration with no electorate", roll_call_carrying(configuration(|c| c.electorate = None))),
        ("a joint configuration with no batch generation", roll_call_carrying(Some(joint(|j| j.batch_generation = None)))),
        ("a joint configuration with no old base", roll_call_carrying(Some(joint(|j| j.old_base = None)))),
        ("a joint configuration with no old generation", roll_call_carrying(Some(joint(|j| j.old_generation = None)))),
        ("a joint configuration whose old base is after its base", roll_call_carrying(Some(joint(|j| j.old_base = Some(generation(1, 2, 9)))))),
        ("a joint configuration whose old generation counter is u64::MAX", roll_call_carrying(Some(joint(|j| j.old_generation = Some(generation(1, 1, u64::MAX)))))),
    ];

    type WithTerm = fn(u64) -> ElectionMessage;
    let term_fields: [(&str, WithTerm); 11] = [
        ("WorkerHeartbeat.term_seen", |t| heartbeat(|m| m.term_seen = t)),
        ("AckEcho.term", |t| heartbeat(|m| m.newest_accepted_ack = Some(AckEcho { term: t, send_token: 0 }))),
        ("LeaderHeartbeatAck.term", |t| ack(|m| m.term = t)),
        ("RollCall.term", |t| {
            let mut call = builders::roll_call(&worker("a"), t, &configuration_of(1), 0);
            call.term = t;
            builders::roll_call_message(call)
        }),
        ("RollCallReply.term", |t| reply(|m| m.term = t)),
        ("VoteRequest.term", |t| vote_request(|m| m.term = t)),
        ("VoteGrant.term", |t| vote_grant(|m| m.term = t)),
        ("ElectionReject.term", |t| reject(|m| m.term = t)),
        ("ElectionReject.highest_term_seen", |t| reject(|m| m.highest_term_seen = t)),
        ("KnownLeader.term", |t| reject(|m| m.leader = Some(KnownLeader { leader_id: Some(worker("c").into()), term: t }))),
        ("ElectionCertificate.term", |t| certificate(|m| m.term = t)),
    ];

    for (what, message) in accepted {
        assert!(checked::decode(message).is_ok(), "{what} was refused");
    }
    for (what, message) in refused {
        assert!(checked::decode(message).is_err(), "{what} was accepted");
    }
    for (field, with_term) in term_fields {
        assert!(checked::decode(with_term(u64::MAX - 1)).is_ok(), "{field} one below u64::MAX was refused");
        assert!(checked::decode(with_term(u64::MAX)).is_err(), "{field} at u64::MAX was accepted");
    }
}
