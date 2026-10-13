//! A leader told to hand over stops granting, tells every peer it is
//! handing over, and asks the voter that confirmed its acks last to stand at
//! once; if no one takes office within a suspicion timeout it leads again.
//! The voter it asks stands without waiting out a suspicion, and a voter
//! that heard the leader is handing over answers that call.

use kabudachi_core::election::{Input, Output};
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::{AckEcho, TransferLeadership};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::{Clock, Duration};

use crate::support::builders::{
    ack_message, checked, configuration_of, g0, heartbeat, heartbeat_message, leader_ack,
    roll_call, roll_call_message, transfer_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    TestNode, acks_to, commit_founding, connect, deliver, elect, grants, published_roll_calls,
    rejects_sent_to, sent_to, tick, voter_node,
};

const SUSPECT: u64 = 40;

/// `follower`'s heartbeat confirming an ack of `leader`'s term sent now,
/// holding the configuration `leader` leads.
fn confirm(leader: &mut TestNode, clock: &FakeClock, follower: &WorkerId) -> Vec<Output> {
    let mut beat = heartbeat(
        follower,
        Some(AckEcho {
            term: leader.term(),
            send_token: clock.now().as_ticks(),
        }),
    );
    beat.configuration_generation = leader
        .configuration()
        .map(|configuration| configuration.generation().into());
    deliver(leader, follower, heartbeat_message(beat))
}

fn transfers_to(outputs: &[Output], to: &WorkerId) -> Vec<Checked<TransferLeadership>> {
    sent_to(outputs, to)
        .into_iter()
        .filter_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::TransferLeadership(asked)) => Some(asked),
            _ => None,
        })
        .collect()
}

#[test]
fn a_leader_told_to_hand_over_stops_granting_asks_its_freshest_voter_and_leads_again_when_no_one_takes_over()
 {
    let clock = FakeClock::new();
    let (a, b, c) = (worker("a"), worker("b"), worker("c"));
    let mut leader = voter_node(&clock, &a, 3, SUSPECT);
    connect(&mut leader, &[b.clone(), c.clone()]);
    elect(&mut leader, &clock, SUSPECT, &[b.clone(), c.clone()]);
    commit_founding(&mut leader, &clock, &[b.clone(), c.clone()]);
    clock.advance(Duration::from_ticks(1));
    confirm(&mut leader, &clock, &c);

    let handed = leader.step(Input::TransferLeadership).outputs;

    assert_eq!(
        grants(&handed),
        vec![None],
        "it stops granting in the step that asks anyone to stand"
    );
    assert!(
        handed.contains(&Output::TransferLeadership { to: c.clone() }),
        "c confirmed last"
    );
    let asked = transfers_to(&handed, &c);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].term, leader.term());
    let told_b = acks_to(&handed, &b);
    assert!(
        !told_b.is_empty() && told_b.iter().all(|ack| ack.leader_draining),
        "every peer hears it"
    );
    assert_eq!(
        leader.state(),
        WorkerState::Leader,
        "it keeps its office for its successor"
    );

    // c never stands. The followers keep confirming; while the hand-over
    // lasts the leader vouches for no heartbeat.
    for _ in 0..3 {
        clock.advance(Duration::from_ticks(SUSPECT / 4));
        let answered = confirm(&mut leader, &clock, &b);
        confirm(&mut leader, &clock, &c);
        assert!(
            acks_to(&answered, &b)
                .iter()
                .all(|ack| ack.leader_draining && ack.heartbeat_token.is_none())
        );
    }
    clock.advance(Duration::from_ticks(SUSPECT / 4));
    let resumed = tick(&mut leader);
    assert!(
        matches!(grants(&resumed).last(), Some(Some(_))),
        "it leads again a suspicion timeout on"
    );
    let answered = confirm(&mut leader, &clock, &b);
    assert!(
        acks_to(&answered, &b)
            .iter()
            .all(|ack| !ack.leader_draining)
    );
}

#[test]
fn the_voter_asked_to_stand_does_so_at_once_and_a_voter_that_heard_the_leader_hand_over_answers_it()
 {
    let clock = FakeClock::new();
    let (a, b, c) = (worker("a"), worker("b"), worker("c"));
    let configuration = configuration_of(3);
    let mut target = voter_node(&clock, &b, 3, SUSPECT);
    let mut voter = voter_node(&clock, &c, 3, SUSPECT);
    for node in [&mut target, &mut voter] {
        deliver(node, &a, ack_message(leader_ack(&a, 1, &configuration, Some(g0()))));
    }
    let mut draining = leader_ack(&a, 1, &configuration, Some(g0()));
    draining.leader_draining = true;
    deliver(&mut voter, &a, ack_message(draining.clone()));

    let stood = deliver(&mut target, &a, transfer_message(&a, &b, 1));
    assert_eq!(
        target.state(),
        WorkerState::RollCall,
        "it stands without waiting out a suspicion"
    );
    let call = published_roll_calls(&stood).remove(0);
    assert_eq!(call.term, 2);
    // The leader's draining ack does not end the call it asked for.
    deliver(&mut target, &a, ack_message(draining));
    assert_eq!(target.state(), WorkerState::RollCall);

    let answered = deliver(
        &mut voter,
        &b,
        roll_call_message(roll_call(&b, call.term, &configuration, 0)),
    );
    assert!(
        rejects_sent_to(&answered, &b).is_empty(),
        "no LEADER_STILL_VALID"
    );
    assert_eq!(sent_to(&answered, &b).len(), 1, "it answers the call");
}
