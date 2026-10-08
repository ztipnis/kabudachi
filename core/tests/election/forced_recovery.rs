//! A node's life with a coordination authority, driven by hand at the node's
//! interface: the epoch reads and replies its driver may hand back late or out
//! of order, how a rejoining node validates a pointer, the recovery fence a
//! leader must hold, the ack lineages it ignores, and the authority path a
//! roll call short of its returning quorum takes. Every authority call a step
//! asks for is made on a `FaultingAuthority` at once and its reply handed
//! straight back, as a driver does.

use crate::support::authority::{name_of, read_epoch as held_epoch, swap_epoch};
use crate::support::builders::{
    message_input,
    ack_message, configuration_of, g0, leader_ack, roll_call_reply, shard, timings, voter_of,
    worker,
};

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch, ShardRecord};
use kabudachi_core::election::{
    AuthorityCall, AuthorityReply, AuthorityRequest, AuthorityTimings, CallKind, DropMessages,
    ElectionTimings, Entry, Identity, Input, Issuer, KnownConfiguration, Output, ReplyToken, Step, WorkerNode,
    carry_out,
};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::{
    JoinResponse, LeaderHeartbeatAck,
};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::reconcile::Rebuild;
use kabudachi_core::scheduler::{LeaseEnd, Scheduler};
use kabudachi_core::time::{Clock, Duration};
use kabudachi_testkit::FaultingAuthority;
use crate::support::authority::{
    AtOnce, asked, authority_ttl, epoch, register_all, seed_shard, warmed_up_authority,
};
use crate::support::clock::FakeClock;
use crate::support::ids::SequentialIds;
use crate::support::node::{TestNode, grants, published_roll_calls, state_changes};

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

    /// Hands the node, which has just won, the end of its reconciliation, as
    /// its driver does once its scheduler has rebuilt and republished (a
    /// shard with no tasks has nothing to rebuild); returns what that
    /// produced.
    fn finish_reconciling(&mut self) -> Vec<Output> {
        let office = self
            .node
            .office_term()
            .expect("finish_reconciling: the node holds office");
        self.scheduler
            .reconcile(Rebuild::default())
            .expect("the scheduler reconciles for the office it was told of");
        self.step(Input::Reconciled(office))
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
        self.start_roll_call(respondents);
    }

    /// Starts the roll call a node already suspecting its leader, and so
    /// confirmed by its read of the authority's epoch, may start, answered by
    /// each of `respondents` admitted at `g0`.
    fn start_roll_call(&mut self, respondents: &[WorkerId]) {
        let started = self.tick();
        let call = published_roll_calls(&started).remove(0);
        for respondent in respondents {
            let reply = roll_call_reply(&self.me, call.term, respondent, Some(g0()));
            self.step(message_input(&respondent, reply));
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

    let _ = driven.node.step(answer(second, RecoveryEpoch::new(1, 1)));
    assert_eq!(driven.node.state(), WorkerState::Bootstrapping);
}

// A swap whose reply was lost leaves the authority at a later epoch of the
// fenced node's own lineage, possibly with no leader there. The node cannot
// tell, so it resumes and suspects its leader as any follower does, and its
// own roll calls decide whether to recover that epoch or to rejoin it.
#[test]
fn a_fenced_node_reconnecting_to_a_later_epoch_of_its_lineage_resumes_instead_of_rejoining() {
    let mut driven = fenced_voter();
    driven.authority.set_reachable(true);
    swap_epoch(&driven
        .authority, &shard(SHARD), Some(epoch(0)), epoch(1))
        .expect("the swap lands");

    driven.advance(lasting_ticks());

    assert_ne!(driven.node.state(), WorkerState::Fenced);
    assert_ne!(driven.node.state(), WorkerState::Bootstrapping);
}

/// A node back in `Bootstrapping` at a floor of epoch 2 of lineage 1.
fn rejoining_at_floor_two_of_lineage_one() -> Driven {
    let mut driven = fenced_voter();
    driven.authority.set_reachable(true);
    swap_epoch(&driven
        .authority, &shard(SHARD), Some(epoch(0)), RecoveryEpoch::new(2, 1))
        .expect("a recovery elsewhere moved the epoch on");
    while driven.node.state() == WorkerState::Fenced {
        driven.advance(1_000);
    }
    assert_eq!(driven.node.state(), WorkerState::Bootstrapping);
    driven
}

/// A JOIN answer naming a leader of epoch 2 of `lineage`.
fn pointer_at_epoch_two_of(lineage: u64) -> Input {
    Input::JoinAnswer(JoinResponse {
        leader_id: Some(worker("w2").into()),
        leader_multiaddr: "w2".to_string(),
        term: 1,
        recovery_epoch: 2,
        recovery_epoch_lineage: lineage,
    })
}

/// A read of the authority's epoch asked for with a token of its own number,
/// as a driver does, and answered at once with `held`.
fn read_epoch(node: &mut TestNode, number: u64, held: RecoveryEpoch) {
    let _ = node.step(Input::AuthorityEpochAsked(read_token(number)));
    let _ = node.step(Input::AuthorityEpochRead {
        token: read_token(number),
        held,
    });
}

fn read_token(number: u64) -> ReplyToken {
    ReplyToken {
        issuer: Issuer::Cascade,
        kind: CallKind::ReadRecoveryEpoch,
        number,
    }
}

// A pointer taken from a delayed or lagging search is not the shard's say:
// the node holds it, still rejoining and settled nowhere, until a read of the
// authority names the pointer's epoch, lineage included. Only the answer to
// the latest read counts, and each is applied once.
#[test]
fn a_rejoining_node_takes_a_pointer_only_once_a_read_of_the_authority_names_its_epoch() {
    type Script = fn(&mut TestNode);
    let rows: [Script; 6] = [
        // A read naming the pointer's epoch makes it a member.
        |node| {
            let _ = node.step(pointer_at_epoch_two_of(1));
            assert_eq!(node.state(), WorkerState::Joining);
            assert_eq!(node.known_leader(), None);

            read_epoch(node, 1, RecoveryEpoch::new(2, 1));
            assert_eq!(node.state(), WorkerState::Active);
            assert_eq!(node.known_leader(), Some((worker("w2"), 1)));
            assert_eq!((node.recovery_epoch(), node.recovery_lineage()), (2, Some(1)));
        },
        // An ack from the held pointer's leader confirms nothing.
        |node| {
            let _ = node.step(pointer_at_epoch_two_of(1));

            let _ = node.step(message_input(
                &worker("w2"),
                ack_message(leader_ack(&worker("w2"), 1, &configuration_of(3), None)),
            ));

            assert_eq!(node.state(), WorkerState::Joining);
            assert_eq!(node.configuration(), None);
        },
        // A read naming another epoch drops the pointer.
        |node| {
            let _ = node.step(pointer_at_epoch_two_of(1));

            read_epoch(node, 1, RecoveryEpoch::new(1, 2));

            assert_eq!(node.state(), WorkerState::Bootstrapping);
            assert_eq!(node.known_leader(), None);
            assert_eq!((node.recovery_epoch(), node.recovery_lineage()), (1, Some(2)));
        },
        // An answer to an older read than the latest is dropped.
        |node| {
            let _ = node.step(pointer_at_epoch_two_of(1));
            let _ = node.step(Input::AuthorityEpochAsked(read_token(1)));
            let _ = node.step(Input::AuthorityEpochAsked(read_token(2)));

            let _ = node.step(Input::AuthorityEpochRead {
                token: read_token(1),
                held: RecoveryEpoch::new(7, 9),
            });
            assert_eq!(node.state(), WorkerState::Joining);

            let _ = node.step(Input::AuthorityEpochRead {
                token: read_token(2),
                held: RecoveryEpoch::new(2, 1),
            });
            assert_eq!(node.state(), WorkerState::Active);

            let _ = node.step(Input::AuthorityEpochRead {
                token: read_token(2),
                held: RecoveryEpoch::new(7, 9),
            });
            assert_eq!(node.state(), WorkerState::Active, "an answer is applied once");
        },
        // A read asked before the pointer was taken says nothing of it.
        |node| {
            let _ = node.step(Input::AuthorityEpochAsked(read_token(1)));
            let _ = node.step(pointer_at_epoch_two_of(1));

            let _ = node.step(Input::AuthorityEpochRead {
                token: read_token(1),
                held: RecoveryEpoch::new(2, 1),
            });

            assert_eq!(node.state(), WorkerState::Joining);
        },
        // A delayed read of a refounded lineage leaves the floor on the new one.
        |node| {
            let _ = node.step(Input::AuthorityEpochAsked(read_token(1)));
            let _ = node.step(Input::AuthorityEpochAsked(read_token(2)));
            let _ = node.step(Input::AuthorityEpochRead {
                token: read_token(2),
                held: RecoveryEpoch::new(1, 2),
            });
            let _ = node.step(Input::AuthorityEpochRead {
                token: read_token(1),
                held: RecoveryEpoch::new(2, 1),
            });
            assert_eq!((node.recovery_epoch(), node.recovery_lineage()), (1, Some(2)));

            // Lineage 1's old leader is still reachable and numbered above
            // the floor.
            let _ = node.step(pointer_at_epoch_two_of(1));
            assert_ne!(node.state(), WorkerState::Active);
        },
    ];

    for script in rows {
        let mut driven = rejoining_at_floor_two_of_lineage_one();
        script(&mut driven.node);
    }
}

/// `w1`, the only voter of its configuration, with the authority at epoch 0,
/// once its own roll call has closed; returns what that step produced.
fn lone_winner(authority_timings: Option<AuthorityTimings>) -> (Driven, Vec<Output>) {
    lone_winner_reaching(authority_timings, true)
}

/// As [`lone_winner`], with the authority unreachable from the start when
/// `reachable` is false, so the winner takes office with no fence granted.
fn lone_winner_reaching(
    authority_timings: Option<AuthorityTimings>,
    reachable: bool,
) -> (Driven, Vec<Output>) {
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
    // It suspects its leader first, so that its read of the authority's epoch
    // confirms its own, and only then loses the authority.
    driven.advance(SUSPECT_TIMEOUT_TICKS * 2);
    driven.authority.set_reachable(reachable);
    driven.tick();
    let mut won = driven.advance(SUSPECT_TIMEOUT_TICKS);
    assert_eq!(driven.node.state(), WorkerState::LeaderReconciling);
    won.extend(driven.finish_reconciling());
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
            recovery_epoch: epoch(0),
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
        .acquire_fence(
            &name_of(&shard(SHARD)),
            &worker("old-leader"),
            &ShardRecord {
                shard_id: shard(SHARD),
                recovery_epoch: epoch(0),
            },
        )
        .expect("the old leader holds the fence");
    let fence_ends = clock.now() + authority_ttl();
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 1);
    driven.advance(SUSPECT_TIMEOUT_TICKS * 2);
    driven.tick();
    let mut won = driven.advance(SUSPECT_TIMEOUT_TICKS);
    assert_eq!(driven.node.state(), WorkerState::LeaderReconciling);
    won.extend(driven.finish_reconciling());
    assert_eq!(driven.node.state(), WorkerState::Leader);
    assert_eq!(grants(&won), Vec::new(), "no grant while another holds the fence");

    let mut outputs = Vec::new();
    while !grants(&outputs).iter().any(Option::is_some) {
        outputs.extend(driven.advance(100));
    }

    assert!(clock.now() >= fence_ends, "it acted before the old fence ran out");
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
    swap_epoch(&authority, &shard(SHARD), None, epoch(0))
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
// call was sent: a reply of another kind carrying the awaited number (a stray
// or a duplicate), or one the bootstrap cascade issued (it numbers its calls
// from 0, as the node does), must not answer the recovery's live-set read, or
// empty its slot so the real answer is ignored.
#[test]
fn a_reply_that_is_not_the_awaited_calls_does_not_answer_it() {
    type Stray = fn(&Driven, &AuthorityCall) -> AuthorityReply;
    let strays: [Stray; 2] = [
        |_, read| AuthorityReply::RecoveryEpoch {
            token: ReplyToken {
                kind: CallKind::ReadRecoveryEpoch,
                ..read.token
            },
            sent_at: read.sent_at,
            result: Ok(Some(epoch(0))),
        },
        // A real live set, of the awaited kind and number, which would move
        // the recovery on if it were taken.
        |driven, read| {
            AuthorityCall {
                token: ReplyToken {
                    issuer: Issuer::Cascade,
                    ..read.token
                },
                ..*read
            }
            .perform(&driven.authority, &name_of(&shard(SHARD)), &shard(SHARD), &worker("w1"), "w1")
        },
    ];

    for stray in strays {
        let (mut driven, read) = awaiting_its_live_set_read();
        assert_eq!(read.token.issuer, Issuer::Node, "setup invariant");

        let reply = stray(&driven, &read);
        let ignored = driven.node.step(Input::Authority(reply));
        assert!(authority_calls(&ignored.outputs).is_empty(), "{:?}", ignored.outputs);
        assert_eq!(driven.node.state(), WorkerState::NoQuorum);

        let answered = driven.node.step(Input::Authority(read.perform(
            &driven.authority,
            &name_of(&shard(SHARD)),
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
        swap_epoch(&authority, &shard(SHARD), None, self::epoch(number))
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
fn the_authority_path_needs_a_majority_of_the_live_registrations() {
    // w3 and w4 are still registered: 2 of 4 live is no majority.
    let (driven, closed) = short_roll_call(&["w1", "w2", "w3", "w4"], Some(0), true);

    // The attempt gave up after its one read; the node then reads the
    // authority's epoch before it may stand again.
    assert_eq!(
        authority_calls(&closed),
        vec![
            AuthorityRequest::ReadLiveRegistrations,
            AuthorityRequest::ReadRecoveryEpoch
        ]
    );
    assert_eq!(driven.node.state(), WorkerState::NoQuorum);
    assert_eq!(
        held_epoch(&driven.authority, &shard(SHARD)),
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
    //
    // The node's read of the authority at its suspicion confirmed its own
    // epoch; the authority moves before its roll call closes short.
    for (held, lineage) in [(2, 0), (9, 7)] {
        let clock = FakeClock::new();
        let authority = warmed_up_authority(&clock);
        swap_epoch(&authority, &shard(SHARD), None, RecoveryEpoch::new(5, 0))
            .expect("the shard has no epoch yet");
        register_all(&authority, &shard(SHARD), &[worker("w2")]);
        let (mut driven, _) = Driven::with(
            &clock,
            &authority,
            "w1",
            KnownConfiguration {
                configuration: Configuration::single(Single {
                    generation: Generation::genesis(epoch(5)),
                    base: Generation::genesis(epoch(5)),
                    voter_count: 5,
                }).expect("valid"),
                admission: Some(Generation::genesis(epoch(5))),
            },
            default_timings(),
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
        );

        driven.advance(SUSPECT_TIMEOUT_TICKS * 2);
        swap_epoch(
            &authority,
            &shard(SHARD),
            Some(RecoveryEpoch::new(5, 0)),
            RecoveryEpoch::new(held, lineage),
        )
            .expect("the epoch moves on after the node's read");
        driven.start_roll_call(&[worker("w2")]);
        let closed = driven.advance(default_timings().roll_call_deadline.as_ticks());

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
            held_epoch(&driven.authority, &shard(SHARD)),
            Ok(Some(RecoveryEpoch::new(held, lineage)))
        );

        // A survivor still leading its old epoch cannot take it back there:
        // it rejoins through JOIN alone.
        let old_leader = worker("w3");
        let epoch_5 = Configuration::single(Single {
            generation: Generation::genesis(epoch(5)),
            base: Generation::genesis(epoch(5)),
            voter_count: 5,
        })
        .expect("valid");
        driven.step(message_input(
            &old_leader,
            ack_message(LeaderHeartbeatAck {
                recovery_epoch: 5,
                recovery_epoch_lineage: 0,
                ..leader_ack(
                    &old_leader,
                    9,
                    &epoch_5,
                    Some(Generation::genesis(epoch(5))),
                )
            }),
        ));
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
    swap_epoch(&driven
        .authority, &shard(SHARD), Some(epoch(0)), refounded)
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

// Two nodes at one epoch number of two lineages agree which epoch is newer:
// the node of the lower lineage follows the other's leader, the node of the
// higher lineage ignores the lower one's, and an epoch numbered below is
// older whatever its lineage.
#[test]
fn nodes_at_one_epoch_number_of_two_lineages_agree_which_epoch_is_newer() {
    // (the node's epoch, the ack's epoch, whether the node follows it)
    let rows = [
        (RecoveryEpoch::new(1, 5), RecoveryEpoch::new(1, 7), true),
        (RecoveryEpoch::new(1, 7), RecoveryEpoch::new(1, 5), false),
        (RecoveryEpoch::new(1, 5), RecoveryEpoch::new(0, 9), false),
    ];
    for (own, heard, follows) in rows {
        let clock = FakeClock::new();
        let authority = warmed_up_authority(&clock);
        swap_epoch(&authority, &shard(SHARD), None, own)
            .expect("the shard has no epoch yet");
        let configuration = configuration_at_epoch(own);
        let (mut driven, _) = Driven::with(
            &clock,
            &authority,
            "w1",
            KnownConfiguration {
                admission: Some(configuration.generation()),
                configuration,
            },
            default_timings(),
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
        );
        let stranger = worker("stranger");

        driven.step(ack_of_epoch(&stranger, heard));

        let now_at = if follows { heard } else { own };
        assert_eq!(
            (driven.node.recovery_epoch(), driven.node.recovery_lineage()),
            (now_at.number, Some(now_at.lineage)),
            "a node at {own:?} hearing {heard:?}"
        );
        assert_eq!(
            driven.node.known_leader().is_some(),
            follows,
            "a node at {own:?} hearing {heard:?}"
        );
    }
}

/// A configuration of three voters founded at `recovery_epoch`.
fn configuration_at_epoch(recovery_epoch: RecoveryEpoch) -> Configuration {
    let generation = Generation::new(recovery_epoch, 0, 0);
    Configuration::single(Single {
        generation,
        base: generation,
        voter_count: 3,
    })
    .expect("valid")
}

/// An ack from `leader` of `recovery_epoch`, term 9.
fn ack_of_epoch(leader: &WorkerId, recovery_epoch: RecoveryEpoch) -> Input {
    message_input(
        leader,
        ack_message(LeaderHeartbeatAck {
            recovery_epoch: recovery_epoch.number,
            recovery_epoch_lineage: recovery_epoch.lineage,
            ..leader_ack(
                leader,
                9,
                &configuration_at_epoch(recovery_epoch),
                Some(Generation::new(recovery_epoch, 0, 0)),
            )
        }),
    )
}

/// Moves a leader's clock to the instant its fence lapses while its
/// registration, renewed on the way, still stands: its fence renewals
/// failed, its registrations did not.
fn lapse_the_fence(driven: &mut Driven) {
    let half = lasting_ticks() / 2;
    driven.clock.advance(Duration::from_ticks(half));
    let _ = driven.node.step(Input::Authority(AuthorityReply::Registered {
        token: ReplyToken {
            issuer: Issuer::Node,
            kind: CallKind::Register,
            number: 0,
        },
        sent_at: driven.clock.now(),
        result: Ok(authority_ttl()),
    }));
    driven.clock.advance(Duration::from_ticks(lasting_ticks() - half));
}

/// Flushes the authority's records, then runs the leader's clock to the end
/// of its fence. The flushed authority is warming up and refuses the leader's
/// renewals, so the fence lapses, though the node never learned of the flush
/// by any message.
fn flush_the_fence(driven: &mut Driven) {
    driven.authority.flush();
    for _ in 0..3 {
        driven.advance(lasting_ticks() / 3);
    }
}

// The leader leads the epoch it took office at from the moment it takes
// office, whether its fence is not yet granted, live, flushed away, or lapsed
// by time. Another lineage's leader numbered above it is no newer for that: it
// must not take the shard from the leader of its office epoch, who would then
// leave every worker with no leader and no way to found one while the epoch
// stands.
#[test]
fn a_leader_that_took_office_at_the_authoritys_epoch_ignores_an_ack_of_another_lineage_however_the_fence_stands() {
    type Disturb = fn(&mut Driven);
    let cases: [(&str, bool, Disturb); 4] = [
        ("no fence granted yet", false, |_| {}),
        ("a live fence", true, |_| {}),
        ("a flushed fence", true, flush_the_fence),
        ("a fence lapsed by time", true, lapse_the_fence),
    ];
    for (stands, granted, disturb) in cases {
        let (mut driven, won) = lone_winner_reaching(
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
            granted,
        );
        assert_eq!(
            grants(&won).iter().any(Option::is_some),
            granted,
            "{stands}: it acts only once it holds the fence"
        );
        disturb(&mut driven);

        driven.step(ack_of_epoch(&worker("stranger"), RecoveryEpoch::new(5, 7)));

        assert_eq!(driven.node.state(), WorkerState::Leader, "{stands}");
        assert_eq!(
            (driven.node.recovery_epoch(), driven.node.recovery_lineage()),
            (0, Some(0)),
            "{stands}"
        );
    }
}

// The standing ends with the leadership: a later epoch of its own lineage
// takes the leader's office, and the node then follows the plain order, in
// which another lineage's higher number is newer.
#[test]
fn a_leader_adopts_a_later_epoch_of_its_lineage_and_then_follows_the_plain_order() {
    for (stands, granted) in [("no fence granted yet", false), ("a lapsed fence", true)] {
        let (mut driven, _) = lone_winner_reaching(
            Some(AuthorityTimings {
                ttl: authority_ttl(),
            }),
            granted,
        );
        let own_leader = worker("own-leader");
        let stranger = worker("stranger");
        if granted {
            lapse_the_fence(&mut driven);
        }

        driven.step(ack_of_epoch(&own_leader, epoch(1)));
        assert_eq!(driven.node.state(), WorkerState::Active, "{stands}");
        assert_eq!(driven.node.recovery_epoch(), 1, "{stands}");

        driven.step(ack_of_epoch(&stranger, RecoveryEpoch::new(5, 7)));
        assert_eq!(
            (driven.node.recovery_epoch(), driven.node.recovery_lineage()),
            (5, Some(7)),
            "{stands}"
        );
    }
}

#[test]
fn a_lost_swap_race_sends_the_node_to_rejoin_the_epoch_that_won() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, [&worker("w2")]);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 5);
    driven.authority.lose_next_race();

    let _ = driven.run_roll_call(&[worker("w2")]);

    // The rival's swap made epoch 1 the authority's: the node's next read of
    // it, before it may stand again, finds its own epoch dead.
    assert_eq!(driven.node.state(), WorkerState::Bootstrapping);
    assert_eq!(driven.node.join_floor().epoch(), Some(epoch(1)));
    assert_eq!(driven.node.configuration(), None);
}

// A swap that went out may have landed whatever the node saw: the authority
// holds the new epoch, and the old leader's fence is already being waited out.
// A reply that comes after the roll call's retry is due, but within the time
// any call to the authority is given, is still the answer to a swap that
// happened, so the node stands at the epoch it swapped to, rather than leave
// it empty-handed to rejoin an epoch with no leader. (A reply later than that
// call timeout finds the attempt given up, and changes nothing.)
#[test]
fn a_swap_reply_after_the_rolls_retry_within_the_call_timeout_still_makes_the_node_stand_at_the_new_epoch() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, [&worker("w2")]);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 5);
    driven.answer_roll_call(&[worker("w2")]);
    driven.clock.advance(default_timings().roll_call_deadline);
    let closed = driven.node.step(Input::Tick);

    let perform = |driven: &Driven, call: AuthorityCall| {
        Input::Authority(call.perform(
            &driven.authority,
            &name_of(&shard(SHARD)),
            &shard(SHARD),
            &worker("w1"),
            "w1",
        ))
    };
    let live = asked(&closed.outputs, AuthorityRequest::ReadLiveRegistrations);
    let read = driven.node.step(perform(&driven, live));
    let epoch_read = asked(&read.outputs, AuthorityRequest::ReadRecoveryEpoch);
    let read = driven.node.step(perform(&driven, epoch_read));
    let swap = asked(
        &read.outputs,
        AuthorityRequest::SwapRecoveryEpoch {
            expected: Some(epoch(0)),
            new: epoch(1),
        },
    );
    // The swap lands, and its reply is held past the next roll call's due time.
    let reply = perform(&driven, swap);
    driven.clock.advance(Duration::from_ticks(SUSPECT_TIMEOUT_TICKS * 10));
    let _ = driven.node.step(Input::Tick);

    let step = driven.node.step(reply);
    let _ = driven.carry(step);

    assert_eq!(driven.node.recovery_epoch(), 1);
    assert_ne!(
        driven.node.state(),
        WorkerState::Bootstrapping,
        "the node stands at the epoch it swapped to"
    );
}

// A roll call that returns a quorum of the node's own, dead epoch does not
// stand the node there: beside a later epoch of its lineage, no leader of its
// own epoch can hold the fence, so the node takes the respondents to the
// authority path, and leads the epoch after the one the lost swap made.
#[test]
fn a_roll_call_that_returns_a_quorum_beside_a_later_epoch_is_a_census_not_an_election() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    seed_shard(&authority, &shard(SHARD), 0, [&worker("w2"), &worker("w3")]);
    let (mut driven, _) = Driven::voter(&clock, &authority, "w1", 3);
    swap_epoch(&authority, &shard(SHARD), Some(epoch(0)), epoch(1))
        .expect("the ambiguous swap");

    let _ = driven.run_roll_call(&[worker("w2")]);

    assert_eq!(driven.node.state(), WorkerState::LeaderReconciling);
    assert_eq!(driven.node.recovery_epoch(), 2);
    driven.finish_reconciling();
    assert_eq!(driven.node.state(), WorkerState::Leader);
}
