//! An election founds a joint configuration: the respondents of its roll call on the new side,
//! the configuration the roll call ran under on the old side. Every roll
//! call, win and lease under it needs a majority of both sides until its
//! leader commits it, once a majority of each side holds it. These tests
//! pin the cases that founding the new side alone got wrong, and the
//! commit.


use crate::support::builders::checked;
use kabudachi_core::protocol::checked::{Checked, CheckedPayload};
use std::collections::{BTreeMap, VecDeque};

use kabudachi_core::configuration::{Configuration, Generation, Joint};
use kabudachi_core::election::{Entry, Identity, Input, KnownConfiguration, Output, WorkerNode};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::messages::{
    AckEcho, ElectionMessage, LeaderHeartbeatAck, RollCall, election_message,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd};
use kabudachi_core::time::{Clock, Duration, Instant};
use crate::support::builders::{
    ack_message, committed_from_g0, configuration_of, founded_from_g0, g0, heartbeat,
    heartbeat_message, leader_ack, past_any_suspicion, roll_call, roll_call_message,
    roll_call_reply, shard, timings, vote_grant, vote_grant_message, vote_request,
    vote_request_message, worker,
};
use crate::support::clock::FakeClock;
use crate::support::node::{
    TestNode, close_roll_call, connect, deliver, finish_reconciling, grants, published_roll_calls, sent, sent_to,
    start_roll_call, state_changes, tick, voter_node,
};

/// Every node here suspects its leader after this many ticks.
const SUSPECT: u64 = 10;

/// A node of `configuration_of(voter_count)` admitted at `admission`
/// (`None` for a pending member).
fn node_of(
    clock: &FakeClock,
    me: &WorkerId,
    voter_count: usize,
    admission: Option<Generation>,
) -> TestNode {
    node_holding(clock, me, configuration_of(voter_count), admission)
}

fn node_holding(
    clock: &FakeClock,
    me: &WorkerId,
    configuration: Configuration,
    admission: Option<Generation>,
) -> TestNode {
    WorkerNode::start(
        Identity {
            id: me.clone(),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard("shard-1"),
            timings: timings(Duration::from_ticks(SUSPECT)),
        },
        Entry::Known(KnownConfiguration {
            configuration,
            admission,
        }),
        clock.clone(),
        None,
    )
    .0
}

/// Whether `grant`, the last one a node reported, still lets it act at
/// `now`.
fn is_valid(grant: &Option<LeadershipGrant>, now: Instant) -> bool {
    match grant {
        None => false,
        Some(LeadershipGrant {
            valid_until: LeaseEnd::Unbounded,
            ..
        }) => true,
        Some(LeadershipGrant {
            valid_until: LeaseEnd::At(end),
            ..
        }) => now < *end,
    }
}

/// Nodes on one fake clock, all connected to one another, whose messages the
/// test delivers by hand.
struct Shard {
    clock: FakeClock,
    nodes: BTreeMap<WorkerId, TestNode>,
    last_grant: BTreeMap<WorkerId, Option<LeadershipGrant>>,
    /// Messages sent but not yet delivered, each with its sender and
    /// addressee, kept from one [`Shard::run`] to the next.
    in_flight: VecDeque<(WorkerId, WorkerId, ElectionMessage)>,
}

impl Shard {
    fn of(clock: &FakeClock, nodes: Vec<(WorkerId, TestNode)>) -> Self {
        let mut nodes: BTreeMap<WorkerId, TestNode> = nodes.into_iter().collect();
        let everyone: Vec<WorkerId> = nodes.keys().cloned().collect();
        for id in &everyone {
            let peers: Vec<WorkerId> = everyone.iter().filter(|p| *p != id).cloned().collect();
            connect(nodes.get_mut(id).unwrap(), &peers);
        }
        Shard {
            clock: clock.clone(),
            last_grant: everyone.iter().map(|id| (id.clone(), None)).collect(),
            nodes,
            in_flight: VecDeque::new(),
        }
    }

    fn node(&mut self, id: &WorkerId) -> &mut TestNode {
        self.nodes.get_mut(id).unwrap()
    }

    /// Runs `initiator`'s roll call by hand: it reaches `respondents`, each
    /// answers, and each of `granters` grants its vote. Returns what the step
    /// that won produced, and asserts it won.
    fn elect(
        &mut self,
        initiator: &WorkerId,
        respondents: &[&WorkerId],
        granters: &[&WorkerId],
    ) -> Vec<Output> {
        self.elect_overheard(initiator, respondents, granters, &[])
    }

    /// [`Shard::elect`], where the roll call also reaches `overhearing`,
    /// whose replies are lost: each learns of the call's term, and is no
    /// respondent.
    fn elect_overheard(
        &mut self,
        initiator: &WorkerId,
        respondents: &[&WorkerId],
        granters: &[&WorkerId],
        overhearing: &[&WorkerId],
    ) -> Vec<Output> {
        let clock = self.clock.clone();
        let started = start_roll_call(self.node(initiator), &clock, SUSPECT);
        let call = published_roll_calls(&started).remove(0);
        self.elect_call(initiator, call, respondents, granters, overhearing)
    }

    /// [`Shard::elect`] for an `initiator` that forgot the call it answered
    /// when a leader's ack reached it: it calls the term of that call again,
    /// `refuser`, which saw it won, refuses it as stale, and the initiator's
    /// next call contests the term after.
    fn elect_after_refusal(
        &mut self,
        initiator: &WorkerId,
        refuser: &WorkerId,
        respondents: &[&WorkerId],
        granters: &[&WorkerId],
    ) -> Vec<Output> {
        let clock = self.clock.clone();
        let refused = published_roll_calls(&start_roll_call(self.node(initiator), &clock, SUSPECT))
            .remove(0);
        let refusal = deliver(
            self.node(refuser),
            initiator,
            roll_call_message((*refused).clone()),
        );
        for message in sent_to(&refusal, initiator) {
            deliver(self.node(initiator), refuser, message);
        }
        // Only the initiator ticks: its retry waits on its own clock reading,
        // which a few suspicion windows always outlast.
        let mut next = None;
        for _ in 0..4 * SUSPECT {
            clock.advance(Duration::from_ticks(1));
            next = published_roll_calls(&tick(self.node(initiator)))
                .into_iter()
                .next();
            if next.is_some() {
                break;
            }
        }
        let next = next.expect("the initiator calls again");
        assert_eq!(next.term, refused.term + 1, "setup invariant");
        self.elect_call(initiator, next, respondents, granters, &[])
    }

    /// [`Shard::elect_overheard`] for `initiator`'s roll call `call`, already
    /// published.
    fn elect_call(
        &mut self,
        initiator: &WorkerId,
        call: Checked<RollCall>,
        respondents: &[&WorkerId],
        granters: &[&WorkerId],
        overhearing: &[&WorkerId],
    ) -> Vec<Output> {
        let clock = self.clock.clone();
        for listener in overhearing {
            deliver(
                self.node(listener),
                initiator,
                roll_call_message((*call).clone()),
            );
        }
        for respondent in respondents {
            let answered = deliver(
                self.node(respondent),
                initiator,
                roll_call_message((*call).clone()),
            );
            for message in sent_to(&answered, initiator) {
                deliver(self.node(initiator), respondent, message);
            }
        }
        let stood = close_roll_call(self.node(initiator), &clock, SUSPECT);
        assert_eq!(
            self.nodes[initiator].state(),
            WorkerState::Candidate,
            "setup invariant: {initiator:?} stands"
        );
        let mut won = Vec::new();
        for voter in granters {
            let request = sent_to(&stood, voter).remove(0);
            let granted = deliver(self.node(voter), initiator, request);
            for grant in sent_to(&granted, initiator) {
                won = deliver(self.node(initiator), voter, grant);
            }
        }
        assert_eq!(
            self.nodes[initiator].state(),
            WorkerState::LeaderReconciling,
            "setup invariant: {initiator:?} wins"
        );
        let won = self.after_step(initiator, won);
        assert_eq!(self.nodes[initiator].state(), WorkerState::Leader);
        won
    }

    /// Hands out `winner`'s `won` outputs, then runs every node for `ticks`
    /// ticks, as [`Shard::run`] does.
    fn run_partitioned(
        &mut self,
        winner: &WorkerId,
        won: Vec<Output>,
        side: impl Fn(&WorkerId) -> bool,
        ticks: u64,
    ) {
        let everyone: Vec<WorkerId> = self.nodes.keys().cloned().collect();
        queue(&mut self.in_flight, winner, &won, &everyone);
        self.run(side, ticks, |_| false);
    }

    /// Runs every node for up to `ticks` ticks, delivering each message at
    /// once unless `side` puts its sender and addressee on different sides
    /// of a partition, where it is lost. Stops, keeping what is still in
    /// flight, as soon as `stop` holds after a delivery or a tick, and
    /// returns whether it did. After every tick, asserts that no two nodes
    /// hold a valid grant.
    fn run(
        &mut self,
        side: impl Fn(&WorkerId) -> bool,
        ticks: u64,
        stop: impl Fn(&Shard) -> bool,
    ) -> bool {
        let everyone: Vec<WorkerId> = self.nodes.keys().cloned().collect();
        for _ in 0..ticks {
            while let Some((from, to, message)) = self.in_flight.pop_front() {
                if side(&from) != side(&to) {
                    continue;
                }
                let outputs = deliver(self.node(&to), &from, message);
                let outputs = self.after_step(&to, outputs);
                queue(&mut self.in_flight, &to, &outputs, &everyone);
                if stop(self) {
                    return true;
                }
            }
            let now = self.clock.now();
            let holding: Vec<&WorkerId> = self
                .last_grant
                .iter()
                .filter(|(_, grant)| is_valid(grant, now))
                .map(|(id, _)| id)
                .collect();
            assert!(
                holding.len() <= 1,
                "{holding:?} all hold a valid grant at {now:?}: {:?}",
                self.last_grant
            );
            self.clock.advance(Duration::from_ticks(1));
            for id in &everyone {
                let outputs = self.node(id).step(Input::Tick).outputs;
                let outputs = self.after_step(id, outputs);
                queue(&mut self.in_flight, id, &outputs, &everyone);
            }
            if stop(self) {
                return true;
            }
        }
        false
    }

    /// Hands each message among `outputs`, which `from` produced, to its
    /// addressee if that is one of `to`; the rest are lost, and so is
    /// whatever the addressees answer.
    fn hand_out(&mut self, outputs: &[Output], from: &WorkerId, to: &[&WorkerId]) {
        for (recipient, message) in sent(outputs) {
            if to.contains(&&recipient) {
                let answered = deliver(self.node(&recipient), from, message);
                self.after_step(&recipient, answered);
            }
        }
    }

    /// Ticks `id` once, keeping what it sends in flight.
    fn tick(&mut self, id: &WorkerId) {
        let everyone: Vec<WorkerId> = self.nodes.keys().cloned().collect();
        let outputs = self.node(id).step(Input::Tick).outputs;
        let outputs = self.after_step(id, outputs);
        queue(&mut self.in_flight, id, &outputs, &everyone);
    }

    /// What a step of `id` produced, with what finishing its reconciliation
    /// produced too if it took office, and its grants noted. A node tested
    /// for its election alone has no tasks to reconcile.
    fn after_step(&mut self, id: &WorkerId, mut outputs: Vec<Output>) -> Vec<Output> {
        if self.nodes[id].state() == WorkerState::LeaderReconciling {
            outputs.extend(finish_reconciling(self.node(id)));
        }
        self.record_grants(id, &outputs);
        outputs
    }

    fn holds_joint(&self, id: &WorkerId) -> bool {
        self.nodes[id]
            .configuration()
            .is_some_and(Configuration::is_joint)
    }

    fn record_grants(&mut self, node: &WorkerId, outputs: &[Output]) {
        if let Some(grant) = grants(outputs).last() {
            self.last_grant.insert(node.clone(), *grant);
        }
    }

    fn holds_a_valid_grant(&self, id: &WorkerId) -> bool {
        is_valid(&self.last_grant[id], self.clock.now())
    }
}

/// Queues what `from` sent or published among `outputs`, a publication to
/// every other node.
fn queue(
    in_flight: &mut VecDeque<(WorkerId, WorkerId, ElectionMessage)>,
    from: &WorkerId,
    outputs: &[Output],
    everyone: &[WorkerId],
) {
    for output in outputs {
        match output {
            Output::Send { to, message } => {
                in_flight.push_back((from.clone(), to.clone(), message.clone()));
            }
            Output::Publish { message } => {
                for to in everyone.iter().filter(|to| *to != from) {
                    in_flight.push_back((from.clone(), to.clone(), message.clone()));
                }
            }
            _ => {}
        }
    }
}

// ---- Partitions after a founding ----

/// C0 = {a, b, c}; d and e are pending. a's roll call reaches b, d and e
/// but not c; a wins on grants from b and d and founds the configuration of
/// {a, b, d, e}. Its certificate and ack to b are lost, and a partition cuts
/// {b, c} off from {a, d, e}. b, still holding C0, is elected with c's
/// vote. Founding the new side alone let d and e keep a's lease going, and
/// later re-elect a under it in b's own term.
#[test]
fn a_founding_no_old_side_majority_holds_never_leads_beside_a_leader_elected_under_the_old_configuration()
 {
    let clock = FakeClock::new();
    let (a, b, c, d, e) = (
        worker("a"),
        worker("b"),
        worker("c"),
        worker("d"),
        worker("e"),
    );
    let mut shard = Shard::of(
        &clock,
        vec![
            (a.clone(), voter_node(&clock, &a, 3, SUSPECT)),
            (b.clone(), voter_node(&clock, &b, 3, SUSPECT)),
            (c.clone(), voter_node(&clock, &c, 3, SUSPECT)),
            (d.clone(), node_of(&clock, &d, 3, None)),
            (e.clone(), node_of(&clock, &e, 3, None)),
        ],
    );

    let won = shard.elect(&a, &[&b, &d, &e], &[&b, &d]);
    shard.run_partitioned(&a, won, |id| [&a, &d, &e].contains(&id), SUSPECT * 6);

    assert_eq!(shard.nodes[&b].state(), WorkerState::Leader);
    assert!(shard.holds_a_valid_grant(&b));
    assert_ne!(shard.nodes[&a].state(), WorkerState::Leader);
}

/// C0 has five voters. a's roll call reaches b and c only; a wins and
/// founds the configuration of {a, b, c}. A partition then cuts {a, b} off
/// from {c, d, e}, which are three of C0's five and elect one of themselves
/// under C0. a and b are a majority of the founded three, but not of C0's
/// five, so a's lease runs out and it cannot be elected again.
#[test]
fn a_founding_that_shrank_the_voter_count_never_leads_beside_the_voters_it_left_out() {
    let clock = FakeClock::new();
    let ids: Vec<WorkerId> = ["a", "b", "c", "d", "e"].map(worker).to_vec();
    let [a, b, c, d, e] = [0, 1, 2, 3, 4].map(|index| ids[index].clone());
    let mut shard = Shard::of(
        &clock,
        ids.iter()
            .map(|id| (id.clone(), voter_node(&clock, id, 5, SUSPECT)))
            .collect(),
    );

    let won = shard.elect(&a, &[&b, &c], &[&b, &c]);
    shard.run_partitioned(&a, won, |id| [&a, &b].contains(&id), SUSPECT * 6);

    let leaders: Vec<&WorkerId> = [&c, &d, &e]
        .into_iter()
        .filter(|id| shard.nodes[*id].state() == WorkerState::Leader)
        .collect();
    assert_eq!(leaders.len(), 1, "{c:?}, {d:?} and {e:?} elect a leader");
    assert!(shard.holds_a_valid_grant(leaders[0]));
    assert_ne!(shard.nodes[&a].state(), WorkerState::Leader);
}

// ---- Two foundings from one configuration ----

/// The joint configuration founded from `configuration_of(3)` in `term`
/// by `respondents` respondents, with the admission generation it admits
/// them at.
fn founded_in(term: u64, respondents: usize) -> (Configuration, Generation) {
    let founded = founded_from_g0(term, 3, respondents);
    let generation = founded.generation();
    (founded, generation)
}

/// Has `initiator`, whose leader contact has gone stale, start a roll call
/// and hands it each reply in `replies` (responder, admission, prior
/// admission). Returns its call's term, with the node still `RollCall`.
fn call_with_replies(
    clock: &FakeClock,
    initiator: &mut TestNode,
    replies: &[(&WorkerId, Option<Generation>, Option<Generation>)],
) -> u64 {
    clock.advance(past_any_suspicion(SUSPECT));
    // Active to LeaderSuspect; the next tick starts the call.
    let _ = initiator.step(Input::Tick);
    let call = published_roll_calls(&initiator.step(Input::Tick).outputs).remove(0);
    let me = call.initiator_id();
    for (responder, admission, prior) in replies {
        let mut reply = roll_call_reply(&me, call.term, responder, *admission);
        if let Some(election_message::Payload::RollCallReply(inner)) = &mut reply.payload {
            inner.prior_admission = prior.map(Into::into);
        }
        deliver(initiator, responder, reply);
    }
    call.term
}

/// A scripted counterexample trace's fourth and third terms. C0 = {A, B, C};
/// P1..P5 and Q1..Q3 are pending. In term 1, A founds J1 = (0, 1, 1) with
/// the P's, who hold it with no prior admission; in term 2, B founds
/// J2 = (0, 2, 1) under C0 with C and the Q's. Counting old sides by
/// [old base, batch generation), a term-3 roll call by Q1 under J2 would
/// count P1 and P2, admitted by J1, as C0 voters and stand with no C0 voter
/// at all, while A could still win term 4 under J1 with the real C0 voters
/// A and C.
#[test]
fn a_joiner_a_rival_founding_admitted_never_counts_as_a_voter_of_the_configuration_both_came_from()
{
    let clock = FakeClock::new();
    let (j1, admitted_by_j1) = founded_in(1, 7);
    let (j2, admitted_by_j2) = founded_in(2, 5);
    let [p1, p2, p3, p4, p5, q2, q3, c] =
        ["p1", "p2", "p3", "p4", "p5", "q2", "q3", "c"].map(worker);

    // Term 3: Q1, holding J2, is answered by P1, P2, Q2 and Q3.
    let mut q1 = node_holding(&clock, &worker("q1"), j2, Some(admitted_by_j2));
    call_with_replies(
        &clock,
        &mut q1,
        &[
            (&p1, Some(admitted_by_j1), None),
            (&p2, Some(admitted_by_j1), None),
            (&q2, Some(admitted_by_j2), None),
            (&q3, Some(admitted_by_j2), None),
        ],
    );
    let closed = close_roll_call(&mut q1, &clock, SUSPECT);
    assert_eq!(
        state_changes(&closed),
        vec![WorkerState::NoQuorum],
        "no voter of C0 answered: no old-side majority"
    );

    // Term 4: A, holding J1 as a re-admitted voter of C0, is answered by C
    // (still at C0) and P3..P5.
    let a = worker("a");
    let mut node_a = voter_node(&clock, &a, 3, SUSPECT);
    let mut admitting = leader_ack(&worker("b"), 1, &j1, Some(admitted_by_j1));
    admitting.recipient_prior_admission = Some(g0().into());
    deliver(&mut node_a, &worker("b"), ack_message(admitting));
    call_with_replies(
        &clock,
        &mut node_a,
        &[
            (&c, Some(g0()), None),
            (&p3, Some(admitted_by_j1), None),
            (&p4, Some(admitted_by_j1), None),
            (&p5, Some(admitted_by_j1), None),
        ],
    );
    close_roll_call(&mut node_a, &clock, SUSPECT);
    assert_eq!(
        node_a.state(),
        WorkerState::Candidate,
        "A and C are two of C0's three; A and P3..P5 four of J1's seven"
    );
}

/// `a`, elected leader of term 1 by `b` and the pending `p`, having founded
/// the joint configuration of {a, b, p} from `configuration_of(3)`.
fn leader_of_a_founding(clock: &FakeClock) -> (TestNode, WorkerId, WorkerId) {
    let (a, b, p) = (worker("a"), worker("b"), worker("p"));
    let mut node = voter_node(clock, &a, 3, SUSPECT);
    connect(&mut node, &[b.clone(), p.clone()]);
    let call = published_roll_calls(&start_roll_call(&mut node, clock, SUSPECT)).remove(0);
    deliver(
        &mut node,
        &b,
        roll_call_reply(&a, call.term, &b, Some(g0())),
    );
    deliver(&mut node, &p, roll_call_reply(&a, call.term, &p, None));
    close_roll_call(&mut node, clock, SUSPECT);
    deliver(
        &mut node,
        &b,
        vote_grant_message(vote_grant(a.clone(), b.clone(), call.term)),
    );
    assert_eq!(node.state(), WorkerState::LeaderReconciling, "setup invariant");
    finish_reconciling(&mut node);
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    assert_eq!(
        node.configuration(),
        Some(&founded_from_g0(1, 3, 3)),
        "setup invariant"
    );
    (node, b, p)
}

/// A heartbeat from `from` confirming the ack of term `acked_term` sent
/// now, on `clock`, and saying it holds a configuration at `held`.
fn heartbeat_holding(
    clock: &FakeClock,
    from: &WorkerId,
    acked_term: u64,
    held: Generation,
) -> ElectionMessage {
    let mut message = heartbeat_message(heartbeat(
        from,
        Some(AckEcho {
            term: acked_term,
            send_token: clock.now().as_ticks(),
        }),
    ));
    if let Some(election_message::Payload::Heartbeat(inner)) = &mut message.payload {
        inner.configuration_generation = Some(held.into());
    }
    message
}

/// The configuration of the one ack among `outputs`, to `to`.
fn acked(outputs: &[Output], to: &WorkerId) -> Checked<LeaderHeartbeatAck> {
    sent_to(outputs, to)
        .into_iter()
        .find_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::HeartbeatAck(ack)) => Some(ack),
            _ => None,
        })
        .expect("an ack")
}

#[test]
fn a_leader_commits_its_founding_once_a_majority_of_each_side_holds_it() {
    let clock = FakeClock::new();
    let (mut leader, b, p) = leader_of_a_founding(&clock);
    let founded = founded_from_g0(1, 3, 3).generation();

    let from_p = deliver(&mut leader, &p, heartbeat_holding(&clock, &p, 1, founded));
    assert!(
        acked(&from_p, &p).configuration().is_joint(),
        "a and p: one of C0's three"
    );
    let from_b = deliver(&mut leader, &b, heartbeat_holding(&clock, &b, 1, founded));

    let committed = committed_from_g0(1, 1, 3);
    assert_eq!(leader.configuration(), Some(&committed));
    assert_eq!(
        leader.admission(),
        Some(committed.generation()),
        "re-based at the commit"
    );
    assert_eq!(leader.prior_admission(), None);
    let ack = acked(&from_b, &b);
    assert_eq!(ack.configuration(), committed);
    assert_eq!(ack.recipient_admission(), Some(committed.generation()));
    assert_eq!(ack.recipient_prior_admission(), None);
}

/// A leader counts toward its commit only a heartbeat confirming an
/// ack of its own term. c, a voter of C0 that missed a's founding in term
/// 1, founds J2 from C0 in term 2 with b's vote; b then holds J2, a later
/// configuration than a's J1, but it heartbeats c, confirming c's acks, so
/// a never counts it.
#[test]
fn a_voter_that_went_on_to_elect_a_rival_founding_never_helps_commit_the_earlier_one() {
    let clock = FakeClock::new();
    let (mut leader, b, p) = leader_of_a_founding(&clock);
    let (j1, j2) = (
        founded_from_g0(1, 3, 3).generation(),
        founded_from_g0(2, 3, 2).generation(),
    );
    deliver(&mut leader, &p, heartbeat_holding(&clock, &p, 1, j1));

    let from_b = deliver(&mut leader, &b, heartbeat_holding(&clock, &b, 2, j2));

    assert!(leader.configuration().is_some_and(Configuration::is_joint));
    assert!(acked(&from_b, &b).configuration().is_joint());
}

/// The term fence, from the voter's side: a voter of C0 that granted a's
/// term-1 vote, then a rival's term-2 vote under C0, takes no notice of a's
/// term-1 ack, so it never echoes one, and never holds a's configuration
/// through it.
#[test]
fn a_voter_that_granted_a_later_term_ignores_the_earlier_winners_ack() {
    let clock = FakeClock::new();
    let (a, c) = (worker("a"), worker("c"));
    let mut b = voter_node(&clock, &worker("b"), 3, SUSPECT);
    clock.advance(past_any_suspicion(SUSPECT));
    for (candidate, term) in [(&a, 1), (&c, 2)] {
        deliver(
            &mut b,
            candidate,
            roll_call_message(roll_call(candidate, term, &configuration_of(3), 0)),
        );
        let granted = deliver(
            &mut b,
            candidate,
            vote_request_message(vote_request(candidate.clone(), 0, term)),
        );
        assert_eq!(sent_to(&granted, candidate).len(), 1, "setup invariant");
    }
    let (j1, admitted) = founded_in(1, 3);
    let mut from_a = leader_ack(&a, 1, &j1, Some(admitted));
    from_a.recipient_prior_admission = Some(g0().into());

    deliver(&mut b, &a, ack_message(from_a));

    assert_eq!(b.configuration(), Some(&configuration_of(3)));
    assert_eq!(b.state(), WorkerState::Active);
    clock.advance(Duration::from_ticks(SUSPECT));
    let heartbeats = b.step(Input::Tick).outputs;
    assert!(
        sent_to(&heartbeats, &a).is_empty(),
        "b does not follow a: {heartbeats:?}"
    );
}

// ---- An election under an uncommitted founding ----

/// `b`, a voter of C0 whose ack from a admitted it to J1 (founded in term
/// 1), wins term 2 under J1 with the grants of `d` (admitted to J1) and `c`
/// (still at C0); the pending `x` answers too.
fn leader_re_leading_a_founding(clock: &FakeClock) -> (TestNode, Vec<Output>, [WorkerId; 3]) {
    let (j1, admitted) = founded_in(1, 3);
    let me = worker("b");
    let mut node = voter_node(clock, &me, 3, SUSPECT);
    let old_leader = worker("a");
    let mut admitting = leader_ack(&old_leader, 1, &j1, Some(admitted));
    admitting.recipient_prior_admission = Some(g0().into());
    deliver(&mut node, &old_leader, ack_message(admitting));
    let (fellow, left_out, joiner) = (worker("d"), worker("c"), worker("x"));
    connect(
        &mut node,
        &[fellow.clone(), left_out.clone(), joiner.clone()],
    );

    let term = call_with_replies(
        clock,
        &mut node,
        &[
            (&fellow, Some(admitted), None),
            (&left_out, Some(g0()), None),
            (&joiner, None, None),
        ],
    );
    close_roll_call(&mut node, clock, SUSPECT);
    assert_eq!(node.state(), WorkerState::Candidate, "setup invariant");
    let mut won = Vec::new();
    for voter in [&fellow, &left_out] {
        won = deliver(
            &mut node,
            voter,
            vote_grant_message(vote_grant(me.clone(), voter.clone(), term)),
        );
    }
    assert_eq!(node.state(), WorkerState::LeaderReconciling, "setup invariant");
    won.extend(finish_reconciling(&mut node));
    assert_eq!(node.state(), WorkerState::Leader, "setup invariant");
    (node, won, [fellow, left_out, joiner])
}

/// J1 re-stamped at the term-2 winner's generation (0, 2, 2), re-based
/// there: its new side counts the two respondents it re-admitted, b and d,
/// not J1's three.
fn j1_re_stamped_in_term_2() -> Configuration {
    let restamped = Generation::new(0, 2, 2);
    Configuration::joint(Joint {
        generation: restamped,
        base: restamped,
        batch_generation: restamped,
        old_base: g0(),
        old_generation: g0(),
        old_voter_count: 3,
        new_voter_count: 2,
    }).expect("valid")
}

#[test]
fn an_election_under_an_uncommitted_founding_re_stamps_it_at_its_own_term() {
    let clock = FakeClock::new();
    let (node, won, [fellow, left_out, joiner]) = leader_re_leading_a_founding(&clock);

    let restamped = j1_re_stamped_in_term_2();
    assert_eq!(node.configuration(), Some(&restamped));
    assert_eq!(node.admission(), Some(restamped.generation()));
    assert_eq!(node.prior_admission(), Some(g0()));
    let to_fellow = acked(&won, &fellow);
    assert_eq!(to_fellow.configuration(), restamped);
    assert_eq!(
        to_fellow.recipient_admission(),
        Some(restamped.generation()),
        "re-admitted where the new side is re-based"
    );
    let to_left_out = acked(&won, &left_out);
    assert_eq!(to_left_out.recipient_admission(), Some(g0()));
    assert_eq!(
        acked(&won, &joiner).recipient_admission(),
        None,
        "a joiner stays pending until a founding admits it"
    );
    let certificate = sent_to(&won, &fellow)
        .into_iter()
        .find_map(|message| match checked(message).into_payload() {
            Some(CheckedPayload::ElectionCertificate(certificate)) => Some(certificate),
            _ => None,
        })
        .expect("a certificate to a respondent");
    assert_eq!(certificate.configuration(), restamped);
    assert_eq!(
        certificate.recipient_admission(),
        Some(restamped.generation())
    );
}

/// A re-stamped founding commits only on echoes of its re-stamped
/// generation; one saying the founding's own, which this leader no longer
/// leads, does not count.
#[test]
fn a_re_stamped_founding_commits_only_on_echoes_of_its_re_stamped_generation() {
    let clock = FakeClock::new();
    let (mut leader, _, [fellow, left_out, _]) = leader_re_leading_a_founding(&clock);
    let (founded, restamped) = (
        founded_from_g0(1, 3, 3).generation(),
        j1_re_stamped_in_term_2().generation(),
    );

    for member in [&fellow, &left_out] {
        deliver(
            &mut leader,
            member,
            heartbeat_holding(&clock, member, 2, founded),
        );
    }
    assert!(
        leader.configuration().is_some_and(Configuration::is_joint),
        "echoes of the founding's own generation"
    );

    for member in [&fellow, &left_out] {
        deliver(
            &mut leader,
            member,
            heartbeat_holding(&clock, member, 2, restamped),
        );
    }
    // The commit (b and d, at (0, 2, 3)) is followed in the same step by
    // the batch that admits c, a respondent the re-stamp left at its old
    // admission, which confirmed this leader's ack.
    let committed = restamped.next_change(2);
    let batch = committed.next_change(2);
    assert_eq!(
        leader.configuration(),
        Some(&Configuration::joint(Joint {
            generation: batch,
            base: batch,
            batch_generation: batch,
            old_base: committed,
            old_generation: committed,
            old_voter_count: 2,
            new_voter_count: 3,
        }).expect("valid"))
    );
}

/// A known liveness cost: a member that missed the ack of a change counts
/// as no voter of it, until a later ack repairs its admission.
#[test]
fn a_member_that_missed_a_commits_ack_counts_again_once_an_ack_repairs_it() {
    let clock = FakeClock::new();
    let (mut leader, b, p) = leader_of_a_founding(&clock);
    let founded = founded_from_g0(1, 3, 3).generation();
    deliver(&mut leader, &p, heartbeat_holding(&clock, &p, 1, founded));
    deliver(&mut leader, &b, heartbeat_holding(&clock, &b, 1, founded));
    let committed = committed_from_g0(1, 1, 3);
    assert_eq!(leader.configuration(), Some(&committed), "setup invariant");

    assert!(
        !committed.is_voter(Some(founded)),
        "p, still at the founding's admission, is no voter of the commit"
    );
    let mut follower = node_holding(&clock, &p, founded_from_g0(1, 3, 3), Some(founded));
    let repaired = acked(
        &deliver(&mut leader, &p, heartbeat_holding(&clock, &p, 1, founded)),
        &p,
    );
    deliver(&mut follower, &worker("a"), ack_message((*repaired).clone()));
    assert_eq!(follower.configuration(), Some(&committed));
    assert_eq!(follower.admission(), Some(committed.generation()));
    assert!(committed.is_voter(follower.admission()));
}

// ---- Rival foundings from one configuration, after a commit ----

/// A scripted counterexample shard, up to its third term. C0 = {a, b, c};
/// p1..p3, q1 and q2 are pending.
///
/// - Term 1: a's roll call reaches b and p1..p3, and a wins with b and p1,
///   founding J1 from C0. Only p1 and p2 hear of it.
/// - Term 2: b, which never heard from a, wins under C0 with c and q1 among
///   c, q1, q2 and p3, founding J2. Only q1 and q2 hear of it. Then a's
///   certificate reaches p3 late, and a, with no ack confirmed, goes
///   `NoQuorum`.
/// - Term 3: p3, which the certificate made forget its answer to b's call,
///   calls term 2 first and is refused as stale, then wins term 3 under J1
///   with a, c and p1, among a, c, p1 and p2 (c still holds C0, older than
///   J1), and its certificates reach them all.
fn rival_foundings_up_to_term_3() -> Shard {
    let clock = FakeClock::new();
    let ids = ["a", "b", "c", "p1", "p2", "p3", "q1", "q2"].map(worker);
    let [a, b, c, p1, p2, p3, q1, q2] = ids.clone();
    let mut shard = Shard::of(
        &clock,
        ids.iter()
            .map(|id| {
                let node = if [&a, &b, &c].contains(&id) {
                    voter_node(&clock, id, 3, SUSPECT)
                } else {
                    node_of(&clock, id, 3, None)
                };
                (id.clone(), node)
            })
            .collect(),
    );

    let won_by_a = shard.elect(&a, &[&b, &p1, &p2, &p3], &[&b, &p1]);
    shard.hand_out(&won_by_a, &a, &[&p1, &p2]);
    let won_by_b = shard.elect(&b, &[&c, &q1, &q2, &p3], &[&c, &q1]);
    shard.hand_out(&won_by_b, &b, &[&q1, &q2]);
    shard.hand_out(&won_by_a, &a, &[&p3]);
    shard.tick(&a);
    assert_eq!(
        shard.nodes[&a].state(),
        WorkerState::NoQuorum,
        "setup invariant"
    );
    shard.in_flight.clear();
    let won_by_p3 = shard.elect_after_refusal(
        &p3,
        &c,
        &[&a, &c, &p1, &p2],
        &[&a, &c, &p1],
    );
    shard.hand_out(&won_by_p3, &p3, &[&a, &c, &p1, &p2]);
    shard
}

/// Re-review Critical 1. p3 commits J1 on echoes from a, c and p1; then a
/// partition cuts {b, c, q1, q2} off before any committed ack reaches c.
/// Leading J1 unchanged left it at an older generation than J2, so c
/// answered and granted b's term-4 call under J2 (c and b are two of C0's
/// three, b, q1 and q2 three of J2's five) while p3 still led S1 with a,
/// p1 and p2.
#[test]
fn a_commit_leaves_no_rival_founding_from_the_same_configuration_electable() {
    let mut shard = rival_foundings_up_to_term_3();
    let [a, b, c, p1, p2, p3, q1, q2] = ["a", "b", "c", "p1", "p2", "p3", "q1", "q2"].map(worker);

    let committed = shard.run(
        |id| ![&b, &q1, &q2].contains(&id),
        SUSPECT * 10,
        |shard| !shard.holds_joint(&p3),
    );
    assert!(committed, "setup invariant: p3 commits J1");
    shard.run(
        |id| [&a, &p1, &p2, &p3].contains(&id),
        SUSPECT * 30,
        |_| false,
    );

    assert!(shard.holds_a_valid_grant(&p3), "p3 keeps leading its side");
    for cut_off in [&b, &c, &q1, &q2] {
        assert_ne!(shard.nodes[cut_off].state(), WorkerState::Leader);
    }
}

/// Re-review Critical 2. Once p3 has committed J1 and p2 holds the
/// committed configuration, a partition cuts {p2, b, q1, q2} off. S1's
/// voter range ran from J1's base to a later term's generation, so it took
/// in J2's admissions: b, q1 and q2, never admitted to J1, counted as S1
/// voters, and p2 won term 4 beside p3.
#[test]
fn a_rival_foundings_admissions_never_count_as_voters_of_a_later_configuration() {
    let mut shard = rival_foundings_up_to_term_3();
    let [a, b, c, p1, p2, p3, q1, q2] = ["a", "b", "c", "p1", "p2", "p3", "q1", "q2"].map(worker);

    let committed = shard.run(
        |id| ![&b, &q1, &q2].contains(&id),
        SUSPECT * 10,
        |shard| !shard.holds_joint(&p3) && !shard.holds_joint(&p2),
    );
    assert!(committed, "setup invariant: p2 holds p3's commit");
    shard.run(
        |id| [&a, &c, &p1, &p3].contains(&id),
        SUSPECT * 30,
        |_| false,
    );

    assert!(shard.holds_a_valid_grant(&p3), "p3 keeps leading its side");
    for cut_off in [&p2, &b, &q1, &q2] {
        assert_ne!(shard.nodes[cut_off].state(), WorkerState::Leader);
    }
}

/// A counterexample scenario with a second pending joiner: the commit
/// counted only echoes of J1's own generation, and still b won beside p.
/// C0 = {a, b, c}; p and q are pending.
///
/// a founds J1 with b's grant, then b founds J2 under C0 with c's, and only
/// p hears of J1 (and of b's call, though its reply is lost; a's certificate
/// makes it forget the call, so it contests term 2 first, is refused, and
/// contests the term after). In term 3 p wins under J1 with a and c, and
/// commits it on their echoes. Only then does b's term-2 certificate reach c, which
/// granted b that term and so accepts it, taking on J2, newer than J1. A
/// partition then cuts {b, c, q} off from {a, p}, and b or c won term 4
/// under J2 beside p.
#[test]
fn a_voter_that_helped_commit_never_elects_a_rival_founding_it_learns_of_later() {
    let clock = FakeClock::new();
    let [a, b, c, p, q] = ["a", "b", "c", "p", "q"].map(worker);
    let mut shard = Shard::of(
        &clock,
        vec![
            (a.clone(), voter_node(&clock, &a, 3, SUSPECT)),
            (b.clone(), voter_node(&clock, &b, 3, SUSPECT)),
            (c.clone(), voter_node(&clock, &c, 3, SUSPECT)),
            (p.clone(), node_of(&clock, &p, 3, None)),
            (q.clone(), node_of(&clock, &q, 3, None)),
        ],
    );

    let won_by_a = shard.elect(&a, &[&b, &p], &[&b]);
    let won_by_b = shard.elect_overheard(&b, &[&c, &q], &[&c], &[&p]);
    shard.hand_out(&won_by_a, &a, &[&p]);
    shard.tick(&a);
    shard.tick(&b);
    assert_eq!(
        shard.nodes[&a].state(),
        WorkerState::NoQuorum,
        "setup invariant"
    );
    shard.in_flight.clear();
    let won_by_p = shard.elect_after_refusal(&p, &c, &[&a, &c], &[&a, &c]);
    shard.hand_out(&won_by_p, &p, &[&a, &c]);
    let committed = shard.run(
        |id| [&a, &c, &p].contains(&id),
        SUSPECT,
        |shard| !shard.holds_joint(&p),
    );
    assert!(committed, "setup invariant: p commits J1");
    shard.hand_out(&won_by_b, &b, &[&c]);
    shard.run(|id| [&a, &p].contains(&id), SUSPECT * 30, |_| false);

    assert!(shard.holds_a_valid_grant(&p), "p keeps leading its side");
    for cut_off in [&b, &c, &q] {
        assert_ne!(shard.nodes[cut_off].state(), WorkerState::Leader);
    }
}

// ---- Survivors of a commit ----

/// The leader of the configuration founded in term 1 commits it and stops.
/// The commit's ack reaches p1 but not p2, so p1 holds the committed
/// configuration and p2 the joint one, each unable to count the other: p1
/// refuses p2's calls as stale, and p2 answers p1's as a new voter. p1's
/// refusal carries the commit, which p2 takes up: the shard then elects a
/// leader on a configuration later than the commit.
#[test]
fn survivors_split_by_a_commit_one_missed_elect_a_leader() {
    let clock = FakeClock::new();
    let (joint, founded) = founded_in(1, 3);
    let committed = committed_from_g0(1, 1, 3);
    let (p1, p2) = (worker("p1"), worker("p2"));
    let mut shard = Shard::of(
        &clock,
        vec![
            (
                p1.clone(),
                node_holding(&clock, &p1, committed.clone(), Some(committed.generation())),
            ),
            (p2.clone(), node_holding(&clock, &p2, joint, Some(founded))),
        ],
    );

    let elected = shard.run(
        |_| true,
        SUSPECT * 20,
        |shard| {
            shard
                .nodes
                .values()
                .any(|node| node.state() == WorkerState::Leader)
        },
    );

    assert!(elected, "{:?}", shard.nodes.values().map(TestNode::state).collect::<Vec<_>>());
    let leader = shard
        .nodes
        .values()
        .find(|node| node.state() == WorkerState::Leader)
        .expect("elected");
    assert!(
        leader
            .configuration()
            .is_some_and(|held| held.generation() > committed.generation()),
        "the leader leads on a configuration built on the commit, not on the joint one"
    );
}
