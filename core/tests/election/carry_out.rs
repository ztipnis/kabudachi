//! `election::carry_out`, the one loop every driver carries a step out
//! through: a step's grant reaches the scheduler before any of its messages
//! leave.

use std::cell::RefCell;

use kabudachi_core::election::{
    DropMessages, Entry, Identity, Input, MessageSink, NoAuthority, Output, Step, WorkerNode,
    carry_out,
};
use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
use kabudachi_core::protocol::messages::ElectionMessage;
use kabudachi_core::scheduler::{Observer, Scheduler};
use kabudachi_core::time::Duration;

use crate::support::builders::{heartbeat, heartbeat_message, shard, timings, voter_of, worker};
use crate::support::clock::FakeClock;
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::node::{TestNode, commit_founding, connect, deliver, elect};

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
    let mut scheduler = Scheduler::new(clock.clone(), SequentialIds::new());
    carry(&mut node, first, &mut scheduler);
    connect(&mut node, &peers);
    let _ = elect(&mut node, &clock, SUSPECT_TIMEOUT, &peers);
    commit_founding(&mut node, &clock, &peers);
    for peer in &peers {
        let mut beat = heartbeat(peer, None);
        beat.routing_crawled = true;
        beat.admission_generation = node
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
        |_, scheduler, _, _| {
            log.borrow_mut()
                .push(format!("scheduler leading: {}", scheduler.is_leader()));
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
