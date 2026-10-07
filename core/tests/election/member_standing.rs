//! When a member of a shard with a coordination authority stands for
//! election: only while its latest read of the authority's recovery epoch
//! confirms its own. Each node here is stepped by hand, so the test chooses
//! when, and whether, the authority answers the read the node asks for.

use crate::support::authority::{asked, authority_ttl};
use crate::support::builders::{ack_message, leader_ack, message_input, shard, timings, worker};
use crate::support::clock::FakeClock;
use crate::support::node::{TestNode, published_roll_calls};

use std::collections::BTreeMap;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::{AuthorityError, LiveRegistrations, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, Entry, Identity, Input,
    KnownConfiguration, Output, WorkerNode,
};
use kabudachi_core::protocol::ids::IncarnationId;
use kabudachi_core::protocol::messages::LeaderHeartbeatAck;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

const SUSPECT_TIMEOUT_TICKS: u64 = 10;

/// A node and its clock.
struct Member {
    node: TestNode,
    clock: FakeClock,
}

impl Member {
    /// A voter of 3, with an authority, at recovery epoch `number` of
    /// lineage 0 (the lineage of every node started on a known
    /// configuration).
    fn at_epoch(number: u64) -> Self {
        let generation = Generation::genesis(number);
        let known = KnownConfiguration {
            configuration: Configuration::single(Single {
                generation,
                base: generation,
                voter_count: 3,
            })
            .expect("valid"),
            admission: Some(generation),
        };
        let clock = FakeClock::new();
        let (node, _) = WorkerNode::start(
            Identity {
                id: worker("w1"),
                incarnation: IncarnationId::new("incarnation-1"),
                shard: shard("shard-1"),
                timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
            },
            Entry::Known(known),
            clock.clone(),
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
        );
        Member { node, clock }
    }

    /// Lets time pass for `ticks` and ticks the node once.
    fn tick_after(&mut self, ticks: u64) -> Vec<Output> {
        self.clock.advance(Duration::from_ticks(ticks));
        self.node.step(Input::Tick).outputs
    }

    /// Lets the node suspect its leader; returns the read of the authority's
    /// epoch it asks for as it does.
    fn suspects_its_leader(&mut self) -> AuthorityCall {
        let outputs = self.tick_after(SUSPECT_TIMEOUT_TICKS * 2);
        assert_eq!(self.node.state(), WorkerState::LeaderSuspect);
        asked(&outputs, AuthorityRequest::ReadRecoveryEpoch)
    }

    /// The authority answers `call`, a read of its epoch, with `result`.
    fn authority_answers(
        &mut self,
        call: &AuthorityCall,
        result: Result<Option<RecoveryEpoch>, AuthorityError>,
    ) -> Vec<Output> {
        self.node
            .step(Input::Authority(AuthorityReply::RecoveryEpoch {
                token: call.token,
                sent_at: call.sent_at,
                result,
            }))
            .outputs
    }

    /// Lets `ticks` pass in renewal-sized steps, answering every registration
    /// the node asks for so it stays registered, and returns each read of the
    /// authority's epoch it asked meanwhile.
    fn waits_registered(&mut self, ticks: u64) -> Vec<AuthorityCall> {
        let renewal = authority_ttl().as_ticks() / 3;
        let mut reads = Vec::new();
        let mut waited = 0;
        while waited < ticks {
            let step = renewal.min(ticks - waited);
            waited += step;
            for output in self.tick_after(step) {
                let Output::Authority(call) = output else {
                    continue;
                };
                match call.request {
                    AuthorityRequest::Register => {
                        let _ = self.node.step(Input::Authority(AuthorityReply::Registered {
                            token: call.token,
                            sent_at: call.sent_at,
                            result: Ok(authority_ttl()),
                        }));
                    }
                    AuthorityRequest::ReadRecoveryEpoch => reads.push(call),
                    _ => {}
                }
            }
        }
        reads
    }

    /// Ticks the node and whether it published a roll call.
    fn stands(&mut self) -> bool {
        !published_roll_calls(&self.tick_after(0)).is_empty()
    }
}

fn own_epoch() -> RecoveryEpoch {
    RecoveryEpoch::new(0, 0)
}

// The authority names an epoch other than the member's: the member's own is
// dead, so it must not elect there. It rejoins with the authority's epoch as
// its floor, whether that is another lineage's at its number or above it, or
// a lower one than its own in its lineage. It does not stand beside the
// authority's epoch. (A later epoch of its own lineage it stands beside only
// to roll a census, and rejoins after repeated refusals.)
#[test]
fn a_member_whose_read_names_another_epoch_rejoins_instead_of_standing() {
    let cases = [
        (0, RecoveryEpoch::new(0, 9)),
        (3, RecoveryEpoch::new(7, 9)),
        (5, RecoveryEpoch::new(2, 0)),
    ];
    for (own, named) in cases {
        let mut member = Member::at_epoch(own);
        let read = member.suspects_its_leader();

        let answered = member.authority_answers(&read, Ok(Some(named)));

        assert_eq!(member.node.state(), WorkerState::Bootstrapping, "{own} vs {named:?}");
        assert_eq!(member.node.join_floor().epoch(), Some(named), "{own} vs {named:?}");
        assert!(published_roll_calls(&answered).is_empty());
        assert!(!member.stands(), "{own} vs {named:?}");
    }
}

#[test]
fn a_member_stands_at_its_own_epoch_when_the_authority_holds_none() {
    let mut member = Member::at_epoch(0);
    let read = member.suspects_its_leader();

    member.authority_answers(&read, Ok(None));

    assert!(member.stands());
}

#[test]
fn a_member_whose_read_failed_does_not_stand_until_a_later_read_succeeds() {
    let mut member = Member::at_epoch(0);
    let failed = member.suspects_its_leader();
    assert!(!member.stands(), "no answer yet");
    member.authority_answers(&failed, Err(AuthorityError::Unavailable));
    assert!(!member.stands());

    // A node does not ask again at once: the next read is a renewal
    // interval after the last.
    let renewal = authority_ttl().as_ticks() / 3;
    let before = member.tick_after(renewal - 1);
    assert!(
        !before
            .iter()
            .any(|output| matches!(output, Output::Authority(call) if call.request == AuthorityRequest::ReadRecoveryEpoch))
    );
    let again = asked(&member.tick_after(1), AuthorityRequest::ReadRecoveryEpoch);
    assert!(!member.stands(), "asked, not yet answered");

    member.authority_answers(&again, Ok(Some(own_epoch())));

    assert!(member.stands());
}

// The call to the authority may simply be slow: a member keeps waiting for
// the read it asked, however many renewal intervals it takes, and stands on
// its answer.
#[test]
fn a_member_waits_for_a_slow_read_rather_than_asking_again() {
    let mut member = Member::at_epoch(0);
    let read = member.suspects_its_leader();

    let asked_meanwhile = member.waits_registered(authority_ttl().as_ticks() / 3 * 2);
    assert!(asked_meanwhile.is_empty(), "a read is still awaited");
    member.authority_answers(&read, Ok(Some(own_epoch())));

    assert!(member.stands());
}

// A read the transport lost is never answered, and a node cannot tell it from
// a slow one, so after a TTL it asks again. Whichever of the two answers
// arrives first is the answer.
#[test]
fn a_member_asks_again_after_a_ttl_and_either_answer_stands() {
    for answered_first in [0, 1] {
        let mut member = Member::at_epoch(0);
        let first = member.suspects_its_leader();

        let again = member.waits_registered(authority_ttl().as_ticks());
        assert_eq!(again.len(), 1, "one more read, a TTL after the first");
        assert_ne!(first.token, again[0].token);
        member.authority_answers(&[first, again[0]][answered_first], Ok(Some(own_epoch())));

        assert!(member.stands(), "answered {answered_first}");
    }
}

// An epoch adopted from a leader's ack ends the confirmation of the one left
// behind: after the next suspicion the member reads again, and stands at the
// epoch it adopted.
#[test]
fn a_member_reads_again_after_adopting_another_epoch_from_an_ack() {
    let mut member = Member::at_epoch(0);
    let read = member.suspects_its_leader();
    member.authority_answers(&read, Ok(Some(own_epoch())));

    let leader = worker("w2");
    let recovered = Configuration::single(Single {
        generation: Generation::new(1, 2, 1),
        base: Generation::new(1, 2, 1),
        voter_count: 2,
    })
    .expect("valid");
    let _ = member.node.step(message_input(
        &leader,
        ack_message(LeaderHeartbeatAck {
            recovery_epoch: 1,
            ..leader_ack(&leader, 2, &recovered, None)
        }),
    ));
    assert_eq!(member.node.state(), WorkerState::Active);
    assert_eq!(member.node.recovery_epoch(), 1);

    let reread = member.suspects_its_leader();
    assert!(!member.stands(), "the old confirmation does not carry over");

    member.authority_answers(&reread, Ok(Some(RecoveryEpoch::new(1, 0))));

    assert!(member.stands());
}

// An attempt to stand spends the confirmation: a member whose roll call fell
// short reads the authority again before the next, since the authority may
// have moved while it failed.
#[test]
fn a_member_whose_roll_call_failed_reads_again_before_the_next() {
    let mut member = Member::at_epoch(0);
    let read = member.suspects_its_leader();
    member.authority_answers(&read, Ok(Some(own_epoch())));
    assert!(member.stands());

    let roll_call_deadline = timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)).roll_call_deadline;
    let closed = member.tick_after(roll_call_deadline.as_ticks());
    assert_eq!(member.node.state(), WorkerState::NoQuorum);
    // Its authority path comes first; once that gives up, it reads.
    let live = asked(&closed, AuthorityRequest::ReadLiveRegistrations);
    let gave_up = member
        .node
        .step(Input::Authority(AuthorityReply::LiveRegistrations {
            token: live.token,
            sent_at: live.sent_at,
            result: Err(AuthorityError::Unavailable),
        }))
        .outputs;
    let reread = asked(&gave_up, AuthorityRequest::ReadRecoveryEpoch);
    // Well past the backoff after a failed roll call, it still waits for the
    // authority's answer.
    assert!(published_roll_calls(&member.tick_after(SUSPECT_TIMEOUT_TICKS * 4)).is_empty());

    member.authority_answers(&reread, Ok(Some(own_epoch())));

    assert!(member.stands());
}

// An answer to a read that reaches a node already back with a leader names an
// epoch it has no business acting on.
#[test]
fn a_read_answered_after_the_member_followed_a_leader_again_changes_nothing() {
    let mut member = Member::at_epoch(0);
    let read = member.suspects_its_leader();
    let leader = worker("w2");
    let current = Configuration::single(Single {
        generation: Generation::genesis(0),
        base: Generation::genesis(0),
        voter_count: 3,
    })
    .expect("valid");
    let _ = member
        .node
        .step(message_input(&leader, ack_message(leader_ack(&leader, 1, &current, None))));
    assert_eq!(member.node.state(), WorkerState::Active);

    member.authority_answers(&read, Ok(Some(RecoveryEpoch::new(7, 7))));

    assert_eq!(member.node.state(), WorkerState::Active);
}

// A node whose roll call fell short runs its authority path first. One call
// of it that never comes back must not park the node: past the time it would
// retry its roll call it gives the path up and reads the epoch again.
#[test]
fn a_member_whose_recovery_call_hangs_gives_the_recovery_up_and_reads_again() {
    let mut member = Member::at_epoch(0);
    let read = member.suspects_its_leader();
    member.authority_answers(&read, Ok(Some(own_epoch())));
    assert!(member.stands());
    let roll_call_deadline = timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)).roll_call_deadline;
    let closed = member.tick_after(roll_call_deadline.as_ticks());
    assert_eq!(member.node.state(), WorkerState::NoQuorum);
    let _hangs = asked(&closed, AuthorityRequest::ReadLiveRegistrations);

    let reads = member.waits_registered(SUSPECT_TIMEOUT_TICKS * 8);

    assert!(!reads.is_empty(), "the node read the authority's epoch again");
}

// A swap whose reply never comes may have landed, so the node cannot take the
// attempt back at once; but it cannot wait for ever either. Past the time any
// call to the authority is given, it counts the swap lost, forgets the reply,
// and reads the authority's epoch, which shows it the epoch the swap made if
// it landed.
#[test]
fn a_member_whose_swap_never_gets_a_reply_gives_the_recovery_up_after_a_ttl_and_reads_again() {
    let mut member = Member::at_epoch(0);
    let read = member.suspects_its_leader();
    member.authority_answers(&read, Ok(Some(own_epoch())));
    assert!(member.stands());
    let closed = member.tick_after(
        timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)).roll_call_deadline.as_ticks(),
    );
    let live = asked(&closed, AuthorityRequest::ReadLiveRegistrations);
    let after_live = member
        .node
        .step(Input::Authority(AuthorityReply::LiveRegistrations {
            token: live.token,
            sent_at: live.sent_at,
            result: Ok(LiveRegistrations::new(
                BTreeMap::from([(worker("w1"), "w1:1".to_string())]),
                true,
            )),
        }))
        .outputs;
    let epoch_read = asked(&after_live, AuthorityRequest::ReadRecoveryEpoch);
    let after_epoch = member.authority_answers(&epoch_read, Ok(Some(own_epoch())));
    let lost_swap = asked(
        &after_epoch,
        AuthorityRequest::SwapRecoveryEpoch {
            expected: Some(own_epoch()),
            new: RecoveryEpoch::new(1, 0),
        },
    );

    let reads = member.waits_registered(authority_ttl().as_ticks() * 2);

    assert!(!reads.is_empty(), "the node read the authority's epoch again");

    // The swap's reply, held all this while, comes after the give-up. It does
    // not stand the node at the epoch it swapped to; the node reads the
    // epoch, finds the swap's epoch, and goes on to a census beside it.
    let _ = member.node.step(Input::Authority(AuthorityReply::RecoveryEpochSwapped {
        token: lost_swap.token,
        expected: Some(own_epoch()),
        new: RecoveryEpoch::new(1, 0),
        sent_at: lost_swap.sent_at,
        result: Ok(()),
    }));
    assert_ne!(member.node.state(), WorkerState::Candidate);
    assert_eq!(member.node.recovery_epoch(), 0);
    member.authority_answers(&reads[0], Ok(Some(RecoveryEpoch::new(1, 0))));
    assert!(member.stands(), "the node rolls a census beside the epoch the swap made");
}

// A member whose roll call fell short and whose authority path was refused
// for other live registrations reads again. If the authority is still ahead of
// it, it is one of the nodes holding the live count above its own respondents:
// it gives its census a few more calls, for workers whose calls fell apart to
// find one another, and then rejoins, which takes it out of that count.
#[test]
fn a_member_whose_authority_path_was_refused_again_and_again_rejoins_the_later_epoch() {
    let later = RecoveryEpoch::new(1, 0);
    let mut member = Member::at_epoch(0);
    let mut read = member.suspects_its_leader();
    let mut refusals = 0;
    while member.node.state() != WorkerState::Bootstrapping {
        assert!(refusals < 10, "never rejoined: {:?}", member.node.state());
        member.authority_answers(&read, Ok(Some(later)));
        if member.node.state() == WorkerState::Bootstrapping {
            break;
        }
        // A call after a refused one waits out a backoff first.
        let mut waited = 0;
        while published_roll_calls(&member.tick_after(1)).is_empty() {
            waited += 1;
            assert!(
                waited < SUSPECT_TIMEOUT_TICKS * 40,
                "the member called no census beside the later epoch"
            );
        }
        // The call's deadline may be widened by the backoff.
        let live = loop {
            let closed = member.tick_after(1);
            if let Some(call) = closed.into_iter().find_map(|output| match output {
                Output::Authority(call)
                    if call.request == AuthorityRequest::ReadLiveRegistrations =>
                {
                    Some(call)
                }
                _ => None,
            }) {
                break call;
            }
            waited += 1;
            assert!(waited < SUSPECT_TIMEOUT_TICKS * 40, "the census never closed");
        };
        let gave_up = member
            .node
            .step(Input::Authority(AuthorityReply::LiveRegistrations {
                token: live.token,
                sent_at: live.sent_at,
                result: Ok(LiveRegistrations::new(
                    BTreeMap::from([
                        (worker("w1"), "w1:1".to_string()),
                        (worker("w2"), "w2:1".to_string()),
                    ]),
                    true,
                )),
            }))
            .outputs;
        assert!(
            !gave_up.iter().any(|output| matches!(
                output,
                Output::Authority(AuthorityCall {
                    request: AuthorityRequest::SwapRecoveryEpoch { .. },
                    ..
                })
            )),
            "a lone member beside other live workers swaps nothing"
        );
        refusals += 1;
        read = asked(&gave_up, AuthorityRequest::ReadRecoveryEpoch);
    }
    assert_eq!(refusals, 3);
}
