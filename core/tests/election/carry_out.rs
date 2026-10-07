//! `election::carry_out`, the one loop every driver carries a step out
//! through: a step's grant reaches the scheduler before any of its messages
//! leave, and a reply the performer answers at once is handed back to the
//! node, and its own step carried out, before the loop returns.

use std::cell::RefCell;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, AuthorityRequest, AuthorityTimings,
    DropMessages, Entry, Identity, Input, MessageSink, NoAuthority, Output, Step, WorkerNode,
    carry_out,
};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{Observer, Scheduler};
use kabudachi_core::time::Duration;

use crate::support::authority::{AtOnce, authority_ttl, warmed_up_authority};
use crate::support::builders::{heartbeat, heartbeat_message, shard, timings, voter_of, worker};
use crate::support::clock::FakeClock;
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::node::{TestNode, commit_founding, connect, deliver, elect};
use crate::support::spy::Spy;

const SHARD: &str = "shard-1";
const SUSPECT_TIMEOUT: u64 = 10;

fn identity(me: &WorkerId) -> Identity {
    Identity {
        id: me.clone(),
        incarnation: IncarnationId::new("incarnation-1"),
        shard: shard(SHARD),
        timings: timings(Duration::from_ticks(SUSPECT_TIMEOUT)),
    }
}

/// `me`'s node, founding its shard alone at recovery epoch 0, and the step
/// it starts with.
fn founder(
    clock: &FakeClock,
    me: &WorkerId,
    authority: Option<AuthorityTimings>,
) -> (TestNode, Step) {
    WorkerNode::start(
        identity(me),
        Entry::Founding {
            recovery_epoch: RecoveryEpoch::new(0, 0),
            registered_at: None,
        },
        clock.clone(),
        authority,
    )
}

/// Records, in `log`, each message it is handed.
struct LoggingSink<'a> {
    log: &'a RefCell<Vec<String>>,
}

impl MessageSink for LoggingSink<'_> {
    fn send(&mut self, to: WorkerId, _: ElectionMessage) {
        self.log
            .borrow_mut()
            .push(format!("send to {}", to.as_str()));
    }

    fn publish(&mut self, _: ElectionMessage) {
        self.log.borrow_mut().push("publish".to_string());
    }
}

/// Answers nothing now: every reply would come later.
struct Deferred {
    asked: Vec<AuthorityCall>,
}

impl AuthorityPerformer for Deferred {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        self.asked.push(call);
        None
    }
}

/// Carries `first` out on `node` with no peers and no authority.
fn carry<O: Observer>(
    node: &mut TestNode,
    first: Step,
    scheduler: &mut Scheduler<FakeClock, SequentialIds, O>,
) {
    let _ = carry_out(
        node,
        first,
        scheduler,
        &mut DropMessages,
        &mut NoAuthority,
        |_, _, _, _| {},
    );
}

#[test]
fn grant_is_applied_before_any_message_leaves() {
    let clock = FakeClock::new();
    let (me, peers) = (worker("w1"), [worker("peer-a"), worker("peer-b")]);
    let (mut node, first) = WorkerNode::start(
        identity(&me),
        Entry::Known(voter_of(3)),
        clock.clone(),
        None,
    );
    let spy = Spy::default();
    let mut scheduler =
        Scheduler::with_observer(clock.clone(), SequentialIds::new(), spy.clone());
    carry(&mut node, first, &mut scheduler);
    connect(&mut node, &peers);
    let _ = elect(&mut node, &clock, SUSPECT_TIMEOUT, &peers);
    commit_founding(&mut node, &clock, &peers);
    for peer in &peers {
        let mut beat = heartbeat(peer, None);
        beat.routing_crawled = true;
        beat.crawl_admission = node
            .configuration()
            .map(|configuration| configuration.generation().into());
        let _ = deliver(&mut node, peer, heartbeat_message(beat));
    }
    // The grant a lease of this leader's gave its scheduler.
    scheduler.set_leadership_grant(Some(unbounded_grant()));

    // A leader drains by withdrawing its grant and then acking every
    // connected peer a last time, in one step.
    let drained = node.step(Input::Drain);
    assert!(
        drained.outputs.contains(&Output::Grant(None)),
        "{drained:?}"
    );
    let log = RefCell::new(Vec::new());
    let _ = carry_out(
        &mut node,
        drained,
        &mut scheduler,
        &mut LoggingSink { log: &log },
        &mut NoAuthority,
        |_, _, _, _| {
            log.borrow_mut()
                .push(format!("scheduler leading: {}", spy.leading()));
        },
    );

    assert_eq!(
        log.into_inner(),
        vec![
            "scheduler leading: false".to_string(),
            "send to peer-a".to_string(),
            "send to peer-b".to_string(),
        ]
    );
}

#[test]
fn an_immediate_reply_is_fed_back_before_returning() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    let me = worker("w1");
    let ttl = AuthorityTimings {
        ttl: authority_ttl(),
    };
    let (mut node, started) = founder(&clock, &me, Some(ttl));
    let mut scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    carry(&mut node, started, &mut scheduler);
    // A node with an authority registers at its first tick.
    let first = node.step(Input::Tick);
    let mut performer = AtOnce::new(&authority, shard(SHARD), me.clone());
    let mut observed = Vec::new();

    let _ = carry_out(
        &mut node,
        first,
        &mut scheduler,
        &mut DropMessages,
        &mut performer,
        |_, _, input, _| observed.push(input.cloned()),
    );

    assert_eq!(performer.performed, vec![AuthorityRequest::Register]);
    assert!(
        matches!(
            observed.as_slice(),
            [
                None,
                Some(Input::Authority(AuthorityReply::Registered {
                    result: Ok(_),
                    ..
                }))
            ]
        ),
        "{observed:?}"
    );
    let again = node.step(Input::Tick);
    assert!(
        !again
            .outputs
            .iter()
            .any(|output| matches!(output, Output::Authority(_))),
        "the registration was answered, so nothing is asked again at once: {again:?}"
    );
}

#[test]
fn a_deferred_reply_leaves_nothing_pending() {
    let clock = FakeClock::new();
    let authority = warmed_up_authority(&clock);
    let me = worker("w1");
    let ttl = AuthorityTimings {
        ttl: authority_ttl(),
    };
    let (mut node, started) = founder(&clock, &me, Some(ttl));
    let mut scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    carry(&mut node, started, &mut scheduler);
    let first = node.step(Input::Tick);
    let mut performer = Deferred { asked: Vec::new() };
    let mut observed = 0;

    let deadline = carry_out(
        &mut node,
        first.clone(),
        &mut scheduler,
        &mut DropMessages,
        &mut performer,
        |_, _, _, _| observed += 1,
    );

    assert_eq!(observed, 1);
    assert_eq!(deadline, first.next_deadline);
    let [call] = performer.asked.as_slice() else {
        panic!("one call asked for: {:?}", performer.asked);
    };
    let reply = call.perform(&authority, &shard(SHARD), &me, me.as_str());
    let state = node.state();
    let late = node.step(Input::Authority(reply));
    assert_eq!(node.state(), state, "{late:?}");
}

#[test]
fn a_step_without_a_grant_leaves_the_scheduler_as_it_was() {
    let clock = FakeClock::new();
    let (mut node, _) = founder(&clock, &worker("w1"), None);
    let ack_like = Output::Send {
        to: worker("w2"),
        message: ElectionMessage::default(),
    };
    let leading_spy = Spy::default();
    let mut leading =
        Scheduler::with_observer(clock.clone(), SequentialIds::new(), leading_spy.clone());
    leading.set_leadership_grant(Some(unbounded_grant()));
    let mark = leading_spy.mark();
    let not_leading_spy = Spy::default();
    let mut not_leading =
        Scheduler::with_observer(clock.clone(), SequentialIds::new(), not_leading_spy.clone());

    carry(
        &mut node,
        Step {
            outputs: vec![
                Output::StateChanged(WorkerState::NoQuorum),
                ack_like.clone(),
            ],
            next_deadline: None,
        },
        &mut leading,
    );
    carry(
        &mut node,
        Step {
            outputs: vec![Output::StateChanged(WorkerState::Leader), ack_like],
            next_deadline: None,
        },
        &mut not_leading,
    );

    assert!(
        leading_spy.since(mark).is_empty(),
        "a step without a grant told the leading scheduler nothing"
    );
    assert!(leading_spy.leading());
    assert!(
        not_leading_spy.since(0).is_empty(),
        "a step without a grant told the other scheduler nothing"
    );
}
