//! A node's life with a coordination authority (ADR-0001 decisions 11 and
//! 12), driven by hand at the node's interface: the authority calls it asks
//! its driver to make, its registration and orphaning, the recovery fence a
//! leader must hold, and the authority path a roll call short of its
//! returning quorum takes. Every authority call a step asks for is made on
//! a `FaultingAuthority` at once and its reply handed straight back, as a
//! driver does.


use crate::support::builders::{
    ack_message, configuration_of, g0, leader_ack, roll_call_reply, shard, timings, voter_of,
    worker,
};

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority, RecoveryEpoch};
use kabudachi_core::election::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, CallKind, DropMessages,
    ElectionTimings, Entry, Identity, Input, Issuer, KnownConfiguration, Output, ReplyToken, Step, WorkerNode,
    carry_out,
};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::election_message;
use kabudachi_core::protocol::messages::{JoinResponse, LeaderHeartbeatAck};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeaseEnd, Scheduler};
use kabudachi_core::time::{Clock, Duration};
use kabudachi_testkit::FaultingAuthority;
use crate::support::authority::{
    AtOnce, asked, authority_ttl, epoch, register_all, seed_shard, warmed_up_authority,
};
use crate::support::clock::FakeClock;
use crate::support::ids::SequentialIds;
use crate::support::node::{TestNode, grants, published_roll_calls, sent_to, state_changes};

const SHARD: &str = "shard-1";
const SUSPECT_TIMEOUT_TICKS: u64 = 10;

/// The timings every node here runs, unless a test sets its own.
fn default_timings() -> ElectionTimings {
    timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS))
}

/// A node and the authority its driver reaches for it.
struct Driven {
    me: WorkerId,
    node: TestNode,
    scheduler: Scheduler<FakeClock, SequentialIds>,
    authority: FaultingAuthority<FakeClock>,
    clock: FakeClock,
}

impl Driven {
    /// `me`'s node, a voter admitted at `g0` of a configuration of
    /// `voter_count`, with an authority whose TTL is the tests' default,
    /// reached through `authority`, and its first step taken.
    fn voter(
        clock: &FakeClock,
        authority: &FaultingAuthority<FakeClock>,
        me: &str,
        voter_count: usize,
    ) -> (Self, Vec<Output>) {
        Driven::with(
            clock,
            authority,
            me,
            voter_of(voter_count),
            default_timings(),
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
        )
    }

    fn with(
        clock: &FakeClock,
        authority: &FaultingAuthority<FakeClock>,
        me: &str,
        known: KnownConfiguration,
        timings: ElectionTimings,
        authority_timings: Option<AuthorityTimings>,
    ) -> (Self, Vec<Output>) {
        let (node, started) = WorkerNode::start(
            Identity {
                id: worker(me),
                incarnation: IncarnationId::new("incarnation-1"),
                shard: shard(SHARD),
                timings,
            },
            Entry::Known(known),
            clock.clone(),
            authority_timings,
        );
        let mut driven = Driven {
            me: worker(me),
            node,
            scheduler: Scheduler::new(clock.clone(), SequentialIds::new()),
            authority: authority.for_another_worker(),
            clock: clock.clone(),
        };
        let _ = driven.carry(started);
        let first = driven.step(Input::Tick);
        (driven, first)
    }

    /// Steps the node with `input` and carries the step out (see
    /// [`Self::carry`]); returns everything it produced, in order.
    fn step(&mut self, input: Input) -> Vec<Output> {
        let step = self.node.step(input);
        self.carry(step)
    }

    /// Carries `step` out as a driver does, with no peers: every authority
    /// call is made at once and its reply handed back, until the node asks
    /// for none. Returns everything it produced, in order.
    fn carry(&mut self, step: Step) -> Vec<Output> {
        let mut all = Vec::new();
        let _ = carry_out(
            &mut self.node,
            step,
            &mut self.scheduler,
            &mut DropMessages,
            &mut AtOnce::new(&self.authority, shard(SHARD), self.me.clone()),
            |_, _, _, step| all.extend(step.outputs.iter().cloned()),
        );
        all
    }

    fn tick(&mut self) -> Vec<Output> {
        self.step(Input::Tick)
    }

    fn advance(&mut self, ticks: u64) -> Vec<Output> {
        self.clock.advance(Duration::from_ticks(ticks));
        self.tick()
    }

    /// Lets the node suspect its leader and start a roll call, then closes
    /// that call at its deadline, with a reply from each of `respondents`
    /// admitted at `g0` in between. Returns what the closing tick produced.
    fn run_roll_call(&mut self, respondents: &[WorkerId]) -> Vec<Output> {
        self.answer_roll_call(respondents);
        let deadline = default_timings().roll_call_deadline;
        self.advance(deadline.as_ticks())
    }

    /// Lets the node suspect its leader and start a roll call, answered by
    /// each of `respondents` admitted at `g0`, and stops short of its
    /// deadline.
    fn answer_roll_call(&mut self, respondents: &[WorkerId]) {
        self.advance(SUSPECT_TIMEOUT_TICKS * 2);
        assert_eq!(self.node.state(), WorkerState::LeaderSuspect);
        let started = self.tick();
        let call = published_roll_calls(&started).remove(0);
        for respondent in respondents {
            let reply = roll_call_reply(&self.me, call.term, respondent, Some(g0()));
            self.step(Input::Message {
                from: respondent.clone(),
                message: reply,
            });
        }
    }
}

fn authority_calls(outputs: &[Output]) -> Vec<AuthorityRequest> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Authority(call) => Some(call.request),
            _ => None,
        })
        .collect()
}

fn ttl_ticks() -> u64 {
    authority_ttl().as_ticks()
}

/// A TTL less its tenth for drift: how long a registration or fence lasts
/// on the node's clock.
fn lasting_ticks() -> u64 {
    ttl_ticks() - ttl_ticks().div_ceil(10)
}

fn live_workers(authority: &FaultingAuthority<FakeClock>) -> Vec<WorkerId> {
    authority
        .live_registrations(&shard(SHARD))
        .expect("the handle is reachable")
        .addresses()
        .keys()
        .cloned()
        .collect()
}

#[test]
fn a_node_registers_at_once_and_renews_every_third_of_its_ttl() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    let (mut driven, first) = Driven::voter(&clock, &authority, "w1", 3);

    assert_eq!(authority_calls(&first), vec![AuthorityRequest::Register]);
    assert_eq!(live_workers(&authority), vec![worker("w1")]);

    // Nothing more until a third of the TTL has passed.
    let third = ttl_ticks() / 3;
    driven.clock.advance(Duration::from_ticks(third - 1));
    assert!(authority_calls(&driven.tick()).is_empty());
    driven.clock.advance(Duration::from_ticks(1));
    assert_eq!(
        authority_calls(&driven.tick()),
        vec![AuthorityRequest::Register]
    );
}

#[test]
fn a_node_that_cannot_renew_fences_itself_before_its_registration_lapses() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, []);
    let reconnect_ticks = 50;
    let (mut driven, _) = Driven::with(
        &clock,
        &authority,
        "w1",
        voter_of(1),
        default_timings().with_reconnect_timeout(Duration::from_ticks(reconnect_ticks)),
        Some(AuthorityTimings {
            ttl: authority_ttl(),
        }),
    );
    // A lone voter leads once its own roll call closes, and takes the fence.
    driven.advance(SUSPECT_TIMEOUT_TICKS * 2);
    driven.tick();
    driven.advance(SUSPECT_TIMEOUT_TICKS);
    assert_eq!(driven.node.state(), WorkerState::Leader);
    let registered_at = clock.now();
    let configuration = driven.node.configuration().cloned();
    driven.authority.set_reachable(false);

    let mut outputs = Vec::new();
    while driven.node.state() == WorkerState::Leader {
        outputs.extend(driven.advance(100));
    }

    assert_eq!(driven.node.state(), WorkerState::Fenced);
    let fenced_at = clock.now();
    assert!(
        fenced_at - registered_at <= Duration::from_ticks(ttl_ticks()),
        "it fenced itself {:?} after its last registration, past the TTL",
        fenced_at - registered_at
    );
    assert!(
        live_workers(&authority).contains(&worker("w1")),
        "it fenced itself while the authority still counted it"
    );
    assert_eq!(grants(&outputs).last(), Some(&None), "its grant is withdrawn");
    let abort_by = outputs
        .iter()
        .filter_map(|output| match output {
            Output::AbortDeadline(deadline) => Some(*deadline),
            _ => None,
        })
        .next_back()
        .flatten();
    assert_eq!(
        abort_by,
        Some(fenced_at + Duration::from_ticks(reconnect_ticks - reconnect_ticks.div_ceil(10)))
    );
    assert_eq!(
        driven.node.configuration().cloned(),
        configuration,
        "it keeps its configuration"
    );
    assert!(driven.node.admission().is_some(), "and its admission");
}

/// A voter of 3 fenced for having lost its authority, with the authority at
/// epoch 0.
fn fenced_voter() -> Driven {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, []);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 3);
    driven.authority.set_reachable(false);
    driven.advance(lasting_ticks());
    assert_eq!(driven.node.state(), WorkerState::Fenced);
    driven
}

#[test]
fn a_fenced_node_ignores_elections() {
    let mut driven = fenced_voter();
    let peer = worker("w2");

    let outputs = driven.step(Input::Message {
        from: peer.clone(),
        message: roll_call_message_for(&peer, 7),
    });

    assert!(sent_to(&outputs, &peer).is_empty(), "it answers no roll call");
}

fn roll_call_message_for(
    initiator: &WorkerId,
    term: u64,
) -> kabudachi_core::protocol::messages::ElectionMessage {
    crate::support::builders::roll_call_message(crate::support::builders::roll_call(
        initiator,
        term,
        &configuration_of(3),
        0,
    ))
}

#[test]
fn a_fenced_node_rejoins_as_pending_once_the_epoch_has_moved_on() {
    let mut driven = fenced_voter();
    driven.authority.set_reachable(true);
    driven
        .authority
        .compare_and_swap_recovery_epoch(&shard(SHARD), Some(epoch(0)), epoch(1))
        .expect("a recovery elsewhere moved the epoch on");

    while driven.node.state() == WorkerState::Fenced {
        driven.advance(1_000);
    }

    assert_eq!(driven.node.state(), WorkerState::Bootstrapping);
    assert_eq!(driven.node.configuration(), None);
    assert!(driven.node.is_pending_member());
    assert_eq!(driven.node.recovery_epoch(), 1);
    // A pointer to a leader left on the old epoch does not take it back.
    let _ = driven.node.step(Input::JoinAnswer(JoinResponse {
        leader_id: Some(worker("w1").into()),
        leader_multiaddr: "w1".to_string(),
        term: 5,
        recovery_epoch: 0,
        recovery_epoch_lineage: 0,
    }));
    assert_eq!(driven.node.state(), WorkerState::Bootstrapping);
    // Its driver joins it again, to the leader of the new epoch.
    let _ = driven.node.step(Input::JoinAnswer(JoinResponse {
        leader_id: Some(worker("w2").into()),
        leader_multiaddr: "w2".to_string(),
        term: 1,
        recovery_epoch: 1,
        recovery_epoch_lineage: 0,
    }));
    assert_eq!(driven.node.state(), WorkerState::Active);
    assert_eq!(driven.node.recovery_epoch(), 1);
}

// A driver hands authority replies back whenever they arrive, possibly out
// of order. A fenced node decides to resume on the epoch read it asked for
// once it could register again; an older read, taken before the shard moved
// on, must not make it resume at an epoch the survivors have left.
#[test]
fn a_fenced_node_ignores_an_epoch_read_it_asked_for_before_reconnecting() {
    let mut driven = fenced_voter();
    driven.clock.advance(Duration::from_ticks(1_000));
    let now = driven.clock.now();
    // Straight into the node, not carried: two registrations answered, each
    // making the fenced node ask for the epoch. Register replies are never
    // matched, so any token serves.
    let registered = || {
        Input::Authority(AuthorityReply::Registered {
            token: ReplyToken {
                issuer: Issuer::Node,
                kind: CallKind::Register,
                number: 0,
            },
            sent_at: now,
            result: Ok(authority_ttl()),
        })
    };
    let registered_1 = driven.node.step(registered());
    let first = asked(&registered_1.outputs, AuthorityRequest::ReadRecoveryEpoch);
    let registered_2 = driven.node.step(registered());
    let second = asked(&registered_2.outputs, AuthorityRequest::ReadRecoveryEpoch);

    let answer = |call: AuthorityCall, epoch| {
        Input::Authority(AuthorityReply::RecoveryEpoch {
            token: call.token,
            sent_at: call.sent_at,
            result: Ok(Some(epoch)),
        })
    };
    let _ = driven.node.step(answer(first, epoch(0)));
    assert_eq!(driven.node.state(), WorkerState::Fenced, "a stale read is ignored");

    let _ = driven.node.step(answer(second, epoch(1)));
    assert_eq!(driven.node.state(), WorkerState::Bootstrapping);
}

// A shard founded afresh after a flush may reuse the old shard's epoch
// number. A node that rejoins it must not be taken back to the old shard by
// a pointer to one of its leaders at that same number.
#[test]
fn a_node_rejoining_a_shard_founded_afresh_ignores_a_pointer_into_its_old_lineage() {
    let mut driven = fenced_voter();
    driven.authority.flush();
    driven.authority.set_reachable(true);
    let refounded = RecoveryEpoch::new(0, 9);
    driven
        .authority
        .compare_and_swap_recovery_epoch(&shard(SHARD), None, refounded)
        .expect("a bootstrapper founds the shard afresh");

    while driven.node.state() == WorkerState::Fenced {
        driven.advance(1_000);
    }
    assert_eq!(driven.node.state(), WorkerState::Bootstrapping);

    let pointer = |number, lineage| JoinResponse {
        leader_id: Some(worker("w2").into()),
        leader_multiaddr: "w2".to_string(),
        term: 1,
        recovery_epoch: number,
        recovery_epoch_lineage: lineage,
    };
    for number in [0, 5] {
        let _ = driven.node.step(Input::JoinAnswer(pointer(number, 0)));
        assert_eq!(
            driven.node.state(),
            WorkerState::Bootstrapping,
            "a leader of the lost shard leads nothing, at epoch {number} or any other"
        );
    }
    let _ = driven.node.step(Input::JoinAnswer(pointer(0, refounded.lineage)));
    assert_eq!(driven.node.state(), WorkerState::Active);
    assert_eq!(driven.node.recovery_lineage(), Some(refounded.lineage));
}

#[test]
fn a_fenced_node_stays_fenced_while_the_epoch_is_missing() {
    let mut driven = fenced_voter();
    driven.authority.flush();
    driven.authority.set_reachable(true);

    for _ in 0..10 {
        driven.advance(ttl_ticks() / 3);
    }

    assert_eq!(driven.node.state(), WorkerState::Fenced);
}

/// `w1`, the only voter of its configuration, with the authority at epoch 0,
/// once its own roll call has closed; returns what that step produced.
fn lone_winner(authority_timings: Option<AuthorityTimings>) -> (Driven, Vec<Output>) {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, []);
    let (mut driven, _) = Driven::with(
        &clock,
        &authority,
        "w1",
        voter_of(1),
        default_timings(),
        authority_timings,
    );
    driven.advance(SUSPECT_TIMEOUT_TICKS * 2);
    driven.tick();
    let won = driven.advance(SUSPECT_TIMEOUT_TICKS);
    assert_eq!(driven.node.state(), WorkerState::Leader);
    (driven, won)
}

#[test]
fn a_leader_with_an_authority_acts_only_while_it_holds_the_fence() {
    let (driven, won) = lone_winner(Some(AuthorityTimings {
        ttl: authority_ttl(),
    }));

    assert!(
        authority_calls(&won).contains(&AuthorityRequest::AcquireFence { recovery_epoch: epoch(0) })
    );
    assert_eq!(
        grants(&won).last(),
        Some(&Some(kabudachi_core::scheduler::LeadershipGrant {
            term: 1,
            recovery_epoch: 0,
            valid_until: LeaseEnd::At(driven.clock.now() + Duration::from_ticks(lasting_ticks())),
        })),
        "even a lone leader's grant ends with its fence"
    );
}

#[test]
fn a_new_leader_waits_out_the_fence_another_worker_holds() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, []);
    authority
        .acquire_fence(&shard(SHARD), &worker("old-leader"), epoch(0))
        .expect("the old leader holds the fence");
    let fence_ends = clock.now() + authority_ttl();
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 1);
    driven.advance(SUSPECT_TIMEOUT_TICKS * 2);
    driven.tick();
    let won = driven.advance(SUSPECT_TIMEOUT_TICKS);
    assert_eq!(driven.node.state(), WorkerState::Leader);
    assert_eq!(grants(&won), Vec::new(), "no grant while another holds the fence");

    let mut outputs = Vec::new();
    while !grants(&outputs).iter().any(Option::is_some) {
        outputs.extend(driven.advance(100));
    }

    assert!(clock.now() >= fence_ends, "it acted before the old fence ran out");
}

#[test]
fn a_leader_whose_republish_fails_asks_for_its_fence_again_only_when_due() {
    let (mut driven, _) = lone_winner(Some(AuthorityTimings {
        ttl: authority_ttl(),
    }));
    let now = driven.clock.now();

    let refused = driven.node.step(Input::Authority(AuthorityReply::Fence {
        token: ReplyToken {
            issuer: Issuer::Node,
            kind: CallKind::AcquireFence,
            number: 0,
        },
        recovery_epoch: epoch(0),
        sent_at: now,
        result: Err(AuthorityError::EpochConflict { current: None }),
    }));
    assert!(
        authority_calls(&refused.outputs).contains(&AuthorityRequest::SwapRecoveryEpoch {
            expected: None,
            new: epoch(0)
        })
    );
    let swap = asked(
        &refused.outputs,
        AuthorityRequest::SwapRecoveryEpoch {
            expected: None,
            new: epoch(0),
        },
    );
    let unreachable = driven
        .node
        .step(Input::Authority(AuthorityReply::RecoveryEpochSwapped {
            token: swap.token,
            expected: None,
            new: epoch(0),
            sent_at: now,
            result: Err(AuthorityError::Unavailable),
        }));

    assert!(
        !authority_calls(&unreachable.outputs)
            .contains(&AuthorityRequest::AcquireFence { recovery_epoch: epoch(0) }),
        "{:?}",
        unreachable.outputs
    );
    assert!(unreachable.next_deadline > Some(now));
}

#[test]
fn a_leader_whose_fence_names_a_later_epoch_steps_down() {
    let (mut driven, _) = lone_winner(Some(AuthorityTimings {
        ttl: authority_ttl(),
    }));
    driven
        .authority
        .compare_and_swap_recovery_epoch(&shard(SHARD), Some(epoch(0)), epoch(1))
        .expect("a recovery elsewhere moved the epoch on");

    let mut outputs = Vec::new();
    while driven.node.state() == WorkerState::Leader {
        outputs.extend(driven.advance(1_000));
    }

    assert_eq!(state_changes(&outputs)[0], WorkerState::LeaderSuspect);
    assert_eq!(grants(&outputs).first(), Some(&None));
}

#[test]
fn a_leader_that_steps_down_stops_renewing_its_fence() {
    let (mut driven, won) = lone_winner(Some(AuthorityTimings {
        ttl: authority_ttl(),
    }));
    assert!(
        authority_calls(&won).contains(&AuthorityRequest::AcquireFence { recovery_epoch: epoch(0) }),
        "setup invariant: it took the fence"
    );
    let won_at = driven.clock.now();
    let later_leader = || Input::Message {
        from: worker("leader-2"),
        message: ack_message(leader_ack(
            &worker("leader-2"),
            2,
            &configuration_of(3),
            Some(g0()),
        )),
    };
    let deposed = driven.step(later_leader());
    assert_eq!(driven.node.state(), WorkerState::Active, "{deposed:?}");

    // Keep it following past its next fence renewal.
    let mut outputs = deposed;
    while driven.clock.now() <= won_at + Duration::from_ticks(ttl_ticks() / 3 + 1) {
        driven.clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS - 1));
        outputs.extend(driven.step(later_leader()));
    }

    assert_eq!(driven.node.state(), WorkerState::Active);
    assert!(
        authority_calls(&outputs)
            .iter()
            .all(|request| *request == AuthorityRequest::Register),
        "a node that no longer leads asks for no fence: {:?}",
        authority_calls(&outputs)
    );
}

// `await_authority` read the clock once to remember the call and again to
// stamp it: a tick between the two reads left the node waiting on an
// instant no reply carries.
#[test]
fn a_fenced_node_resumes_on_its_epoch_read_even_when_its_clock_moves_while_it_asks() {
    let mut driven = fenced_voter();
    driven.authority.set_reachable(true);
    driven.clock.advance_on_every_read(Duration::from_ticks(1));
    for _ in 0..10 {
        if driven.node.state() != WorkerState::Fenced {
            break;
        }
        driven.advance(ttl_ticks() / 3);
    }
    assert_eq!(driven.node.state(), WorkerState::Active, "its epoch is still its own");
}

/// `w1` in `short_roll_call(&["w1", "w2"], Some(0), true)`'s recovery,
/// stopped where it awaits its live-set read: that call, not yet performed.
fn awaiting_its_live_set_read() -> (Driven, AuthorityCall) {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    authority
        .compare_and_swap_recovery_epoch(&shard(SHARD), None, epoch(0))
        .expect("the shard has no epoch yet");
    register_all(&authority, &shard(SHARD), &[worker("w2")]);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 5);
    driven.answer_roll_call(&[worker("w2")]);
    let deadline = default_timings().roll_call_deadline;
    driven.clock.advance(deadline);
    let closed = driven.node.step(Input::Tick);
    let read = asked(&closed.outputs, AuthorityRequest::ReadLiveRegistrations);
    assert_eq!(driven.node.state(), WorkerState::NoQuorum, "setup invariant");
    (driven, read)
}

// A reply is matched by its call's issuer, kind and number, not by when the
// call was sent: a reply of another kind carrying the awaited number (a
// stray or a duplicate) must not answer the recovery's live-set read.
#[test]
fn a_reply_of_another_kind_does_not_answer_the_awaited_call() {
    let (mut driven, read) = awaiting_its_live_set_read();

    let stray = driven.node.step(Input::Authority(AuthorityReply::RecoveryEpoch {
        token: ReplyToken {
            kind: CallKind::ReadRecoveryEpoch,
            ..read.token
        },
        sent_at: read.sent_at,
        result: Ok(Some(epoch(0))),
    }));
    assert!(authority_calls(&stray.outputs).is_empty(), "{:?}", stray.outputs);
    assert_eq!(driven.node.state(), WorkerState::NoQuorum);

    let answered = driven.node.step(Input::Authority(read.perform(
        &driven.authority,
        &shard(SHARD),
        &worker("w1"),
        "w1",
    )));
    assert!(
        authority_calls(&answered.outputs).contains(&AuthorityRequest::ReadRecoveryEpoch),
        "the recovery is still going: {:?}",
        answered.outputs
    );
}

// The bootstrap cascade numbers its calls from 0, as the node does, so a
// reply to one of its calls can carry the node's awaited kind and number.
// Its issuer tells them apart: the node never takes it for its own, whatever
// net does with it at handover.
#[test]
fn a_reply_the_cascade_issued_does_not_answer_the_nodes_awaited_call() {
    let (mut driven, read) = awaiting_its_live_set_read();
    assert_eq!(read.token.issuer, Issuer::Node, "setup invariant");
    let cascades = AuthorityCall {
        token: ReplyToken {
            issuer: Issuer::Cascade,
            ..read.token
        },
        ..read
    };

    // A real live set, of the awaited kind and number, which would move the
    // recovery on if it were taken.
    let stray = driven.node.step(Input::Authority(cascades.perform(
        &driven.authority,
        &shard(SHARD),
        &worker("w1"),
        "w1",
    )));
    assert!(authority_calls(&stray.outputs).is_empty(), "{:?}", stray.outputs);
    assert_eq!(driven.node.state(), WorkerState::NoQuorum);

    // Had the cascade's reply been taken, it would have emptied the slot and
    // this one would be ignored.
    let answered = driven.node.step(Input::Authority(read.perform(
        &driven.authority,
        &shard(SHARD),
        &worker("w1"),
        "w1",
    )));
    assert!(
        authority_calls(&answered.outputs).contains(&AuthorityRequest::ReadRecoveryEpoch),
        "{:?}",
        answered.outputs
    );
}

/// `w1`, a voter of 5, its roll call answered by `w2` alone, with `live`
/// registered (and warm) and the epoch at `epoch`: the returning quorum is
/// short (2 of 5), so the call closes into the authority path.
fn short_roll_call(
    live: &[&str],
    epoch: Option<u64>,
    warm: bool,
) -> (Driven, Vec<Output>) {
    let clock = FakeClock::new();
    let authority = if warm {
        warmed_up_authority(&clock)
    } else {
        FaultingAuthority::new(clock.clone(), authority_ttl())
    };
    if let Some(number) = epoch {
        authority
            .compare_and_swap_recovery_epoch(&shard(SHARD), None, self::epoch(number))
            .expect("the shard has no epoch yet");
    }
    // `w1` registers itself on its first step.
    let others: Vec<WorkerId> = live
        .iter()
        .map(|id| worker(id))
        .filter(|id| *id != worker("w1"))
        .collect();
    register_all(&authority, &shard(SHARD), &others);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 5);
    let closed = driven.run_roll_call(&[worker("w2")]);
    (driven, closed)
}

#[test]
fn a_roll_call_short_of_its_quorum_recovers_through_a_majority_of_the_live_registrations() {
    // w3, w4 and w5 are gone: their registrations lapsed, so the authority
    // counts 2 live workers, and w1 and w2 are a majority of them.
    let (mut driven, closed) = short_roll_call(&["w1", "w2"], Some(0), true);

    assert_eq!(
        authority_calls(&closed),
        vec![
            AuthorityRequest::ReadLiveRegistrations,
            AuthorityRequest::ReadRecoveryEpoch,
            AuthorityRequest::SwapRecoveryEpoch {
                expected: Some(epoch(0)),
                new: epoch(1)
            },
            AuthorityRequest::AcquireFence { recovery_epoch: epoch(1) },
        ]
    );
    assert_eq!(
        state_changes(&closed),
        vec![
            WorkerState::NoQuorum,
            WorkerState::Candidate,
            WorkerState::LeaderReconciling,
            WorkerState::Leader,
        ]
    );
    let term = driven.node.term();
    let founded = Generation::new(1, term, 1);
    assert_eq!(driven.node.recovery_epoch(), 1);
    assert_eq!(
        driven.node.configuration(),
        Some(&Configuration::single(Single {
            generation: founded,
            base: founded,
            voter_count: 2,
        }).expect("valid"))
    );
    assert_eq!(driven.node.admission(), Some(founded));
    assert_eq!(
        driven.authority.read_recovery_epoch(&shard(SHARD)),
        Ok(Some(epoch(1)))
    );
    // Its ack to w2 carries the new epoch and w2's admission there, which
    // w2 adopts (see the stale-JOIN test below).
    let acks: Vec<LeaderHeartbeatAck> = sent_to(
        &driven.step(Input::PeerConnected(worker("w2"))),
        &worker("w2"),
    )
    .into_iter()
    .filter_map(|message| match message.payload {
        Some(election_message::Payload::HeartbeatAck(ack)) => Some(ack),
        _ => None,
    })
    .collect();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].recovery_epoch, 1);
    assert_eq!(acks[0].recipient_admission, Some(founded.into()));
}

#[test]
fn the_authority_path_trusts_no_count_during_warm_up() {
    let (driven, closed) = short_roll_call(&["w1", "w2"], Some(0), false);

    assert_eq!(
        authority_calls(&closed),
        vec![AuthorityRequest::ReadLiveRegistrations]
    );
    assert_eq!(driven.node.state(), WorkerState::NoQuorum);
    assert_eq!(driven.node.recovery_epoch(), 0);
}

#[test]
fn the_authority_path_needs_a_majority_of_the_live_registrations() {
    // w3 and w4 are still registered: 2 of 4 live is no majority.
    let (driven, closed) = short_roll_call(&["w1", "w2", "w3", "w4"], Some(0), true);

    assert_eq!(
        authority_calls(&closed),
        vec![AuthorityRequest::ReadLiveRegistrations]
    );
    assert_eq!(driven.node.state(), WorkerState::NoQuorum);
    assert_eq!(
        driven.authority.read_recovery_epoch(&shard(SHARD)),
        Ok(Some(epoch(0)))
    );
}

#[test]
fn a_node_whose_authority_holds_an_epoch_it_cannot_recover_from_rejoins_it() {
    // A voter of 5 at epoch 5 of lineage 0. The authority holds another
    // epoch: lower in the same lineage (restored from an old backup, or put
    // back by a leader that republished after a flush), or of another
    // lineage (founded afresh). A recovery from either would swap an epoch
    // number this node's shard may already have used, so it swaps nothing;
    // and, as a fenced node reconnecting does, it follows the authority:
    // it rejoins at that epoch rather than stay NoQuorum beside it.
    for (held, lineage) in [(2, 0), (9, 7)] {
        let clock = FakeClock::new();
        let authority = warmed_up_authority(&clock);
        authority
            .compare_and_swap_recovery_epoch(
                &shard(SHARD),
                None,
                RecoveryEpoch::new(held, lineage),
            )
            .expect("the shard has no epoch yet");
        register_all(&authority, &shard(SHARD), &[worker("w2")]);
        let (mut driven, _) = Driven::with(
            &clock,
            &authority,
            "w1",
            KnownConfiguration {
                configuration: Configuration::single(Single {
                    generation: Generation::genesis(5),
                    base: Generation::genesis(5),
                    voter_count: 5,
                }).expect("valid"),
                admission: Some(Generation::genesis(5)),
            },
            default_timings(),
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
        );

        let closed = driven.run_roll_call(&[worker("w2")]);

        assert!(
            !authority_calls(&closed)
                .iter()
                .any(|call| matches!(call, AuthorityRequest::SwapRecoveryEpoch { .. })),
            "{closed:?}"
        );
        assert_eq!(driven.node.state(), WorkerState::Bootstrapping);
        assert_eq!(
            (driven.node.recovery_epoch(), driven.node.recovery_lineage()),
            (held, Some(lineage)),
            "it rejoins at the authority's epoch, as its floor"
        );
        assert_eq!(driven.node.configuration(), None);
        assert_eq!(
            driven.authority.read_recovery_epoch(&shard(SHARD)),
            Ok(Some(RecoveryEpoch::new(held, lineage)))
        );

        // A survivor still leading its old epoch cannot take it back there:
        // it rejoins through JOIN alone.
        let old_leader = worker("w3");
        driven.step(Input::Message {
            from: old_leader.clone(),
            message: ack_message(LeaderHeartbeatAck {
                recovery_epoch: 5,
                recovery_epoch_lineage: Some(0),
                ..leader_ack(&old_leader, 9, &configuration_of(5), Some(g0()))
            }),
        });
        assert_eq!(
            (
                driven.node.state(),
                driven.node.recovery_epoch(),
                driven.node.recovery_lineage()
            ),
            (WorkerState::Bootstrapping, held, Some(lineage)),
            "an ack from its old epoch's leader moved its floor"
        );
    }
}

#[test]
fn a_leader_whose_fence_names_an_epoch_it_cannot_recover_from_rejoins_it() {
    let (mut driven, _) = lone_winner(Some(AuthorityTimings {
        ttl: authority_ttl(),
    }));
    // The shard was founded afresh under it.
    let refounded = RecoveryEpoch::new(0, 7);
    driven
        .authority
        .compare_and_swap_recovery_epoch(&shard(SHARD), Some(epoch(0)), refounded)
        .expect("a founder replaced the epoch");

    let mut outputs = Vec::new();
    while driven.node.state() == WorkerState::Leader {
        outputs.extend(driven.advance(1_000));
    }

    assert_eq!(grants(&outputs).first(), Some(&None));
    assert_eq!(
        state_changes(&outputs),
        vec![WorkerState::NoQuorum, WorkerState::Bootstrapping],
        "rather than win again and meet the same conflict for ever"
    );
    assert_eq!(
        (driven.node.recovery_epoch(), driven.node.recovery_lineage()),
        (refounded.number, Some(refounded.lineage))
    );
}

#[test]
fn a_node_ignores_an_ack_at_its_epoch_number_from_another_lineage() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, []);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 3);
    let stranger = worker("stranger");

    driven.step(Input::Message {
        from: stranger.clone(),
        message: ack_message(LeaderHeartbeatAck {
            recovery_epoch: 0,
            recovery_epoch_lineage: Some(7),
            ..leader_ack(&stranger, 4, &configuration_of(3), Some(g0()))
        }),
    });

    assert_eq!(
        driven.node.known_leader(),
        None,
        "a leader of another shard's epoch 0 is no leader of this one"
    );
}

#[test]
fn a_node_ignores_an_ack_from_a_lower_epoch_of_another_lineage() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    authority
        .compare_and_swap_recovery_epoch(&shard(SHARD), None, RecoveryEpoch::new(1, 0))
        .expect("the shard has no epoch yet");
    let epoch_1 = Generation::new(1, 0, 0);
    let known = KnownConfiguration {
        configuration: Configuration::single(Single {
            generation: epoch_1,
            base: epoch_1,
            voter_count: 3,
        }).expect("valid"),
        admission: Some(epoch_1),
    };
    let (mut driven, _) = Driven::with(
        &clock,
        &authority,
        "w1",
        known,
        default_timings(),
        Some(AuthorityTimings {
            ttl: authority_ttl(),
        }),
    );
    let stranger = worker("stranger");

    driven.step(Input::Message {
        from: stranger.clone(),
        message: ack_message(LeaderHeartbeatAck {
            recovery_epoch: 0,
            recovery_epoch_lineage: Some(7),
            ..leader_ack(&stranger, 4, &configuration_of(3), Some(g0()))
        }),
    });

    assert_eq!(
        driven.node.known_leader(),
        None,
        "a lower epoch of another lineage is no leader of this one"
    );
    assert_eq!(driven.node.recovery_epoch(), 1);
}

#[test]
fn a_node_that_adopts_a_later_epoch_from_an_ack_resumes_it_after_fencing() {
    // w1 is at epoch 0 of lineage 0; the leader it hears leads epoch 1 of
    // lineage 7, which the authority holds.
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    let recovered = RecoveryEpoch::new(1, 7);
    authority
        .compare_and_swap_recovery_epoch(&shard(SHARD), None, recovered)
        .expect("the shard has no epoch yet");
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 3);
    let leader = worker("leader");
    let configuration = Configuration::single(Single {
        generation: Generation::new(1, 2, 1),
        base: Generation::new(1, 2, 1),
        voter_count: 3,
    }).expect("valid");
    driven.step(Input::Message {
        from: leader.clone(),
        message: ack_message(LeaderHeartbeatAck {
            recovery_epoch: recovered.number,
            recovery_epoch_lineage: Some(recovered.lineage),
            ..leader_ack(&leader, 2, &configuration, None)
        }),
    });
    assert_eq!(
        (driven.node.recovery_epoch(), driven.node.recovery_lineage()),
        (recovered.number, Some(recovered.lineage)),
        "the ack's epoch comes with its lineage"
    );

    driven.authority.set_reachable(false);
    driven.advance(lasting_ticks());
    assert_eq!(driven.node.state(), WorkerState::Fenced);
    driven.authority.set_reachable(true);
    while driven.node.state() == WorkerState::Fenced {
        driven.advance(1_000);
    }

    assert_eq!(
        driven.node.state(),
        WorkerState::Active,
        "the authority's epoch is its own: it resumes rather than rejoins"
    );
}

#[test]
fn a_lost_swap_race_leaves_the_node_no_quorum_at_its_epoch() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, [&worker("w2")]);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 5);
    driven.authority.lose_next_race();

    let _ = driven.run_roll_call(&[worker("w2")]);

    assert_eq!(driven.node.state(), WorkerState::NoQuorum);
    assert_eq!(driven.node.recovery_epoch(), 0);
    assert_eq!(driven.node.configuration(), Some(&configuration_of(5)));
}

#[test]
fn the_authority_path_recovers_a_shard_left_at_a_swapped_epoch_with_no_leader() {
    // A swap to epoch 1 was applied but its caller never heard (its reply
    // was lost), so no leader leads epoch 1.
    let (driven, _) = {
        let clock = FakeClock::new();
        let authority = warmed_up_authority(&clock);
        seed_shard(&authority, &shard(SHARD), 0, [&worker("w2")]);
        authority
            .compare_and_swap_recovery_epoch(&shard(SHARD), Some(epoch(0)), epoch(1))
            .expect("the ambiguous swap");
        let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 5);
        let closed = driven.run_roll_call(&[worker("w2")]);
        (driven, closed)
    };

    assert_eq!(driven.node.state(), WorkerState::Leader);
    assert_eq!(driven.node.recovery_epoch(), 2);
}

#[test]
fn a_pending_joiner_pointed_at_a_stale_epoch_adopts_its_leaders_later_one() {
    // A responder cut off from the leader, which has since recovered the
    // shard at epoch 1, still answers JOIN with epoch 0.
    let clock = FakeClock::new();
    let leader = worker("leader");
    let mut joiner = WorkerNode::start(
        Identity {
            id: worker("w1"),
            incarnation: IncarnationId::new("incarnation-1"),
            shard: shard(SHARD),
            timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)),
        },
        Entry::Joining(JoinResponse {
            leader_id: Some(leader.clone().into()),
            leader_multiaddr: "leader".to_string(),
            term: 5,
            recovery_epoch: 0,
            recovery_epoch_lineage: 0,
        }),
        clock.clone(),
        None,
    )
    .0;
    let recovered = Configuration::single(Single {
        generation: Generation::new(1, 2, 1),
        base: Generation::new(1, 2, 1),
        voter_count: 2,
    }).expect("valid");

    let _ = joiner.step(Input::Message {
        from: leader.clone(),
        message: ack_message(LeaderHeartbeatAck {
            recovery_epoch: 1,
            ..leader_ack(&leader, 2, &recovered, None)
        }),
    });
    clock.advance(timings(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS)).heartbeat_interval);
    let outputs = joiner.step(Input::Tick).outputs;

    assert_eq!(joiner.recovery_epoch(), 1, "a later epoch's leader wins over its term");
    assert_eq!(joiner.configuration(), Some(&recovered));
    assert_eq!(joiner.known_leader(), Some((leader.clone(), 2)));
    let heartbeat = sent_to(&outputs, &leader)
        .into_iter()
        .find_map(|message| match message.payload {
            Some(election_message::Payload::Heartbeat(heartbeat)) => Some(heartbeat),
            _ => None,
        })
        .expect("it heartbeats its leader when its next heartbeat is due");
    assert_eq!(heartbeat.recovery_epoch_seen, 1);
    assert_eq!(
        heartbeat.newest_accepted_ack.map(|echo| echo.term),
        Some(2),
        "it echoes only the new epoch's ack"
    );
}
