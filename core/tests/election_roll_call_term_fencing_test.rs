//! Chunk C7-fix: reproduces, then guards against, the split-brain race C7's
//! real-network test (`net/tests/ring_roll_call_leader_loss_test.rs`)
//! discovered empirically: `choose_candidate` derived a `RollCall`'s
//! contested term from whatever `highest_term_seen` values happened to be
//! present in its accumulated `responses` *at the moment a given node
//! evaluated them* — including this node's own, which can be bumped mid-
//! flight by granting a vote for a completely unrelated candidacy — instead
//! of from the call's own `highest_term_seen` field, fixed once at
//! `begin_roll_call` and never mutated as the call is forwarded. That let the
//! very same stale, still-circulating call mint a different, incoherent term
//! depending purely on which nodes it happened to pass through and when,
//! which could produce a second, live `Leader` nobody actually contested.
//!
//! `roll_call_term_is_derived_from_the_calls_own_origin_term...` reproduces
//! the race directly: the external call's own `highest_term_seen` field
//! (fixed at its origin) reads 0, but one of its already-accumulated
//! `responses` carries a `highest_term_seen` of 5 — modelling an earlier hop
//! whose own local state had been bumped by granting a vote for a
//! completely unrelated candidacy before it appended its observation and
//! forwarded the call onward. This node under test is never itself
//! elevated (so the new `on_roll_call` staleness guard does not apply here
//! — that is covered separately below), isolating this test to the
//! `choose_candidate` term-derivation fix alone. Before the fix, the node's
//! resulting candidacy term reflects the accumulated observations' momentary
//! maximum (6); after the fix, it reflects only the call's own fixed origin
//! term (1), exactly as README §12.5 intends.
//!
//! `stale_roll_call_is_dropped_when_...` covers the new `on_roll_call` guard:
//! a call whose own `highest_term_seen` is behind what this node already
//! knows is dropped outright, rather than processed and forwarded onward to
//! keep contributing to the same incoherent-escalation bug.
//!
//! `on_roll_call_normal_case_still_elects_at_the_calls_own_term` is the
//! regression control: an ordinary, non-stale roll call (this node's own
//! `highest_term_seen` never elevated) must still elect at `call
//! .highest_term_seen + 1`, exactly as before the fix.

mod support;

use support::builders::{make_network, observation, roll_call, roll_call_message, shard, vote_request, worker};

use support::candidate::predict_winner;

use kabudachi_core::election::WorkerNode;
use kabudachi_core::membership::RingMembership;
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_core::transport::PeerMessenger;
use support::clock::FakeClock;
use support::coordination_authority::FakeCoordinationAuthority;
use support::network::FakeNetwork;

const SHARD: &str = "shard-1";

fn make_node_with_ring(
    clock: &FakeClock,
    network: &FakeNetwork,
    my_id: WorkerId,
    electorate: &[WorkerId],
    suspect_timeout: Duration,
) -> WorkerNode<FakeClock, FakeNetwork, RingMembership, FakeCoordinationAuthority> {
    WorkerNode::new(
        my_id,
        IncarnationId::new("incarnation-1"),
        shard(SHARD),
        clock.clone(),
        network.clone(),
        RingMembership::new(electorate.iter().cloned().collect()),
        FakeCoordinationAuthority::new(),
        suspect_timeout,
    )
}

/// Finds a `(self_id, other_id)` pair from `labels` for which `self_id` wins
/// a 2-member election under the default hash function at *both*
/// `term_a` and `term_b` — needed so the reproduction test's outcome turns
/// purely on which *term* is used, not on which candidate happens to win at
/// whichever term a given code path computes.
fn find_pair_winning_at_both_terms(
    labels: &[WorkerId],
    term_a: u64,
    term_b: u64,
) -> (WorkerId, WorkerId) {
    labels
        .iter()
        .flat_map(|x| labels.iter().map(move |y| (x, y)))
        .find(|(x, y)| {
            x != y
                && predict_winner(&shard(SHARD), 0, term_a, &[(*x).clone(), (*y).clone()]) == **x
                && predict_winner(&shard(SHARD), 0, term_b, &[(*x).clone(), (*y).clone()]) == **x
        })
        .map(|(x, y)| (x.clone(), y.clone()))
        .expect("some pair among the label pool must win at both term values")
}

#[test]
fn roll_call_term_is_derived_from_the_calls_own_origin_term_not_a_momentarily_elevated_local_term()
 {
    // The call's own `highest_term_seen` (fixed at its origin) yields the
    // correct term. The buggy derivation instead maxes in this node's own
    // (momentarily elevated) `highest_term_seen` of 5, yielding 6.
    let correct_term = 1;
    let buggy_term = 6;

    let labels: Vec<WorkerId> = (1..=40).map(|i| worker(&format!("w{i}"))).collect();
    let (self_id, other_id) = find_pair_winning_at_both_terms(&labels, correct_term, buggy_term);

    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let network = make_network(&clock, &[self_id.clone(), other_id.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other_id.clone()],
        suspect_timeout,
    );

    // Get the node under test into RollCall (its own roll call is quorum-1
    // short in a 2-member electorate, so it just forwards and waits). This
    // node's own `highest_term_seen` stays untouched (0) throughout — the
    // new staleness guard added to `on_roll_call` must not fire here; only
    // the `choose_candidate` term-derivation fix is under test.
    clock.advance(Duration::from_ticks(11));
    node.tick(); // Active -> LeaderSuspect
    node.tick(); // LeaderSuspect -> RollCall
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    network.poll_inbox(other_id.clone()); // drain its own forwarded call

    // A second, independently-originated roll call arrives. Its own
    // `highest_term_seen` field (fixed at *its* origin) is 0, but the one
    // response it already carries reports `highest_term_seen: 5` — that
    // earlier hop's own local state had been bumped (by granting a vote for
    // some unrelated candidacy) before it appended its observation and
    // forwarded the call on, exactly the momentary, per-hop state the old
    // code incorrectly folded into the term.
    let call = roll_call(
        "external-call-1",
        other_id.clone(),
        vec![observation(other_id.clone(), 5)],
    );
    node.on_message(other_id.clone(), roll_call_message(call));

    // Quorum (2 of 2) is reached (other_id's carried observation + this
    // node's own, freshly appended) and this node was engineered to win at
    // both candidate term values, so it becomes Candidate either way — only
    // the resulting term distinguishes correct behavior from the bug.
    assert_eq!(
        node.state(),
        WorkerState::Candidate,
        "engineered to win at both the correct and buggy term values"
    );
    assert_eq!(
        node.term(),
        correct_term,
        "the contested term must come from the call's own origin-time highest_term_seen (0 + 1 \
         = {correct_term}), not from this node's own momentarily-elevated local highest_term_seen \
         (which would incorrectly yield {buggy_term})"
    );
}

#[test]
fn stale_roll_call_is_dropped_when_this_node_already_knows_of_a_higher_term() {
    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let self_id = worker("w1");
    let succ = worker("w2");
    let network = make_network(&clock, &[self_id.clone(), succ.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), succ.clone()],
        suspect_timeout,
    );

    // Elevate this node's own highest_term_seen to 5, exactly as in the
    // reproduction test above, but without putting the node into RollCall —
    // the guard must apply regardless of this node's own state.
    clock.advance(Duration::from_ticks(11));
    node.on_vote_request(&vote_request(succ.clone(), 0, 5));
    network.pump();
    network.poll_inbox(succ.clone()); // drain the VoteGrant sent by granting the vote above

    // A roll call whose own highest_term_seen (0, the builder's default) is
    // now stale relative to this node's own must be dropped outright: not
    // processed, not forwarded.
    let call = roll_call("stale-call-1", worker("initiator"), vec![]);
    node.on_message(worker("someone"), roll_call_message(call));

    network.pump();
    assert!(
        network.poll_inbox(succ).is_empty(),
        "a roll call whose own highest_term_seen is behind what this node already knows must \
         be dropped, not forwarded onward to keep contributing to the incoherent-escalation bug"
    );
    assert_eq!(
        node.state(),
        WorkerState::Active,
        "dropping a stale roll call must not itself change this node's state"
    );
}

#[test]
fn on_roll_call_normal_case_still_elects_at_the_calls_own_term() {
    // Regression control: with no local elevation, the fix must produce the
    // exact same outcome as before — call.highest_term_seen (0) + 1.
    let labels: Vec<WorkerId> = (1..=40).map(|i| worker(&format!("w{i}"))).collect();
    let self_id = predict_winner(&shard(SHARD), 0, 1, &labels);
    let other_id = labels.iter().find(|w| **w != self_id).unwrap().clone();

    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let network = make_network(&clock, &[self_id.clone(), other_id.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other_id.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    network.poll_inbox(other_id.clone());

    let call = roll_call(
        "external-call-1",
        other_id.clone(),
        vec![observation(other_id.clone(), 0)],
    );
    node.on_message(other_id.clone(), roll_call_message(call));

    assert_eq!(node.state(), WorkerState::Candidate);
    assert_eq!(node.term(), 1, "no local elevation occurred, so the term must still be call.highest_term_seen (0) + 1");
}

#[test]
fn a_new_candidate_records_its_contested_term_so_a_call_re_contesting_it_is_dropped() {
    // A candidate never receives its own `LeaderHeartbeatAck`, so its
    // candidacy term is the only thing that can raise its own
    // `highest_term_seen`. Without that, a later roll call still carrying
    // the pre-election term would re-contest the term this node already
    // holds instead of being dropped as stale.
    let labels: Vec<WorkerId> = (1..=40).map(|i| worker(&format!("w{i}"))).collect();
    let self_id = predict_winner(&shard(SHARD), 0, 1, &labels);
    let other_id = labels.iter().find(|w| **w != self_id).unwrap().clone();

    let clock = FakeClock::new();
    let suspect_timeout = Duration::from_ticks(10);
    let network = make_network(&clock, &[self_id.clone(), other_id.clone()]);
    let mut node = make_node_with_ring(
        &clock,
        &network,
        self_id.clone(),
        &[self_id.clone(), other_id.clone()],
        suspect_timeout,
    );

    clock.advance(Duration::from_ticks(11));
    node.tick();
    node.tick();
    assert_eq!(node.state(), WorkerState::RollCall);
    network.pump();
    network.poll_inbox(other_id.clone());

    let call = roll_call(
        "external-call-1",
        other_id.clone(),
        vec![observation(other_id.clone(), 0)],
    );
    node.on_message(other_id.clone(), roll_call_message(call));
    assert_eq!(node.state(), WorkerState::Candidate);
    assert_eq!(node.term(), 1);
    network.pump();
    network.poll_inbox(other_id.clone()); // drain the VoteRequest

    // Another call still carrying highest_term_seen 0 would contest term 1
    // again; this node now knows term 1, so the call is stale.
    let stale = roll_call("external-call-2", other_id.clone(), vec![]);
    node.on_message(other_id.clone(), roll_call_message(stale));

    network.pump();
    assert!(
        network.poll_inbox(other_id).is_empty(),
        "a candidate at term 1 must drop a roll call whose own highest_term_seen (0) would \
         re-contest term 1, not forward it"
    );
}
