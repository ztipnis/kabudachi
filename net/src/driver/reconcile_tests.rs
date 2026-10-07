//! A reconciling leader's republish and late answers as the driver runs them,
//! on a real node, scheduler and `Net`, with a clock the test moves.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

use libp2p::Multiaddr;

use kabudachi_core::configuration::{Configuration, Generation, Single};
use kabudachi_core::election::{ElectionTimings, Entry, Identity, KnownConfiguration};
use kabudachi_core::protocol::checked;
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::ids::{IncarnationId, ShardId, TaskDefinitionId, Uuid7Ids};
use kabudachi_core::protocol::messages::{
    AckEcho, RollCallReply, VoteGrant, WorkerHeartbeat, election_message,
};
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd, Submission};
use kabudachi_core::time::{Duration as TickDuration, Instant};

use tokio::time::timeout;

use super::*;
use crate::task_store::placement::ReplicationFactor;
use crate::test_support::{TEST_TIMEOUT, wait_for_input};

const SUSPECT_MS: u64 = 100;
const HEARTBEAT_MS: u64 = 25;

#[derive(Clone)]
struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        Instant::at(self.0.load(Ordering::SeqCst))
    }
    fn wall_clock_millis(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn shard() -> ShardId {
    ShardId::new("shard-1")
}

fn configuration(voters: usize) -> Configuration {
    Configuration::single(Single {
        generation: Generation::genesis(0),
        base: Generation::genesis(0),
        voter_count: voters,
    })
    .expect("valid")
}

/// A node, its scheduler and the driver's reconciliation state, stepped by hand.
struct Office<'n> {
    node: WorkerNode<ManualClock>,
    scheduler: Scheduler<ManualClock, Uuid7Ids, RecordOutbox>,
    net: &'n Net,
    clock: ManualClock,
    unsettled: RecordWrites,
    reconciliation: Option<LeaderReconciliation<'n>>,
    seen: Vec<Output>,
}

impl<'n> Office<'n> {
    fn new(net: &'n Net, voters: usize) -> Self {
        let clock = ManualClock(Arc::new(AtomicU64::new(1_000)));
        let identity = Identity {
            id: net.local_worker_id(),
            incarnation: IncarnationId::new("incarnation-0"),
            shard: shard(),
            timings: ElectionTimings::new(
                TickDuration::from_millis(SUSPECT_MS),
                TickDuration::from_millis(HEARTBEAT_MS),
            )
            .with_roll_call_deadline(TickDuration::from_millis(HEARTBEAT_MS)),
        };
        let known = KnownConfiguration {
            configuration: configuration(voters),
            admission: Some(Generation::genesis(0)),
        };
        let (node, first) = WorkerNode::start(identity, Entry::Known(known), clock.clone(), None);
        let scheduler = Scheduler::with_observer(clock.clone(), Uuid7Ids, RecordOutbox::default());
        let mut office = Office {
            node,
            scheduler,
            net,
            clock,
            unsettled: RecordWrites::default(),
            reconciliation: None,
            seen: Vec::new(),
        };
        let Office { node, scheduler, net, unsettled, seen, .. } = &mut office;
        let mut observe = |_: &WorkerNode<ManualClock>, _: Option<&Input>, step: &Step| {
            seen.extend(step.outputs.iter().cloned());
        };
        Stepper {
            node,
            scheduler,
            net,
            replication_factor: ReplicationFactor::DEFAULT,
            unsettled,
            calls: None,
            observe: &mut observe,
            runs_heard: &mut Vec::new(),
        }
        .carry(first, None);
        office
    }

    fn step(&mut self, input: Input) {
        let Office { node, scheduler, net, unsettled, seen, .. } = self;
        let mut observe = |_: &WorkerNode<ManualClock>, _: Option<&Input>, step: &Step| {
            seen.extend(step.outputs.iter().cloned());
        };
        Stepper {
            node,
            scheduler,
            net,
            replication_factor: ReplicationFactor::DEFAULT,
            unsettled,
            calls: None,
            observe: &mut observe,
            runs_heard: &mut Vec::new(),
        }
        .step(input);
    }

    fn message(&mut self, from: &WorkerId, payload: election_message::Payload) {
        let message = ElectionMessage { payload: Some(payload) };
        self.step(Input::Message {
            from: from.clone(),
            message: checked::decode(message).expect("well formed"),
        });
    }

    /// Wins office over `peers`, each of which answers the roll call and
    /// grants its vote; the node reconciles.
    fn win_office_with(&mut self, peers: &[WorkerId]) {
        self.clock.advance(2 * SUSPECT_MS);
        self.step(Input::Tick);
        self.step(Input::Tick);
        let call = self
            .seen
            .iter()
            .find_map(|output| match output {
                Output::Publish { message } => match &message.payload {
                    Some(election_message::Payload::RollCall(call)) => Some(call.clone()),
                    _ => None,
                },
                _ => None,
            })
            .expect("the node published a roll call");
        let me = self.net.local_worker_id();
        for peer in peers {
            self.message(
                peer,
                election_message::Payload::RollCallReply(RollCallReply {
                    shard_id: Some(shard().into()),
                    term: call.term,
                    initiator_id: Some(me.clone().into()),
                    responder_id: Some(peer.clone().into()),
                    responder_address: String::new(),
                    admission: Some(Generation::genesis(0).into()),
                    prior_admission: None,
                }),
            );
        }
        self.clock.advance(HEARTBEAT_MS);
        self.step(Input::Tick);
        for peer in peers {
            self.message(
                peer,
                election_message::Payload::VoteGrant(VoteGrant {
                    shard_id: Some(shard().into()),
                    recovery_epoch: 0,
                    term: call.term,
                    candidate_id: Some(me.clone().into()),
                    voter_id: Some(peer.clone().into()),
                }),
            );
        }
        assert_eq!(self.node.state(), WorkerState::LeaderReconciling);
    }

    /// Hands the leader a heartbeat from each of `members` that confirms its
    /// latest ack and says they hold its configuration: its lease runs on
    /// from now, and the configuration it founded commits.
    fn hear_from(&mut self, members: &[WorkerId]) {
        let held = self.node.configuration().expect("a configuration").generation();
        for member in members {
            let heartbeat = heartbeat(member, self.node.term(), self.clock.now(), Some(held));
            self.message(member, heartbeat);
        }
    }

    /// One pass of the driver's loop for the reconciliation: settles the
    /// republished writes whose outcomes arrived, then runs the office.
    fn turn(&mut self) -> usize {
        let outcomes = self.net.take_write_outcomes();
        let arrived = outcomes.len();
        settle_republish(&mut self.reconciliation, outcomes, self.clock.now());
        self.reconcile();
        arrived
    }

    /// Answers the peers' questions until the round has taken in `completions`
    /// answers or lookups.
    async fn pump(&mut self, peers: &[&Net], completions: usize) {
        let mut taken = 0;
        timeout(TEST_TIMEOUT, async {
            while taken < completions {
                for peer in peers {
                    respond_to_reconcile_requests(peer);
                }
                tokio::select! {
                    () = next_reconciliation(&mut self.reconciliation) => taken += 1,
                    () = tokio::time::sleep(StdDuration::from_millis(10)) => {}
                }
            }
        })
        .await
        .expect("the round took in the answers within the timeout");
    }

    fn reconcile(&mut self) -> Option<Instant> {
        reconcile_office(
            &mut self.reconciliation,
            &mut self.node,
            &mut self.scheduler,
            self.net,
            &self.clock,
            ReplicationFactor::DEFAULT,
            &mut self.unsettled,
            None,
            &mut |_: &WorkerNode<ManualClock>, _: Option<&Input>, _: &Step| {},
        )
    }
}

fn heartbeat(who: &WorkerId, term: u64, now: Instant, held: Option<Generation>) -> election_message::Payload {
    election_message::Payload::Heartbeat(WorkerHeartbeat {
        worker_id: Some(who.clone().into()),
        incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
        recovery_epoch_seen: 0,
        term_seen: term,
        available_capacity: 0,
        active_task_runs_digest: vec![],
        shard_id: Some(shard().into()),
        newest_accepted_ack: Some(AckEcho { term, send_token: now.as_ticks() }),
        configuration_generation: held.map(Into::into),
        send_token: 0,
        crawl_admission: None,
        routing_crawled: false,
    })
}

/// A `Net` of the shard listening on loopback.
async fn shard_net() -> (Net, Multiaddr) {
    let net = Net::for_shard(shard(), None);
    let at = timeout(TEST_TIMEOUT, net.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()))
        .await
        .expect("the net listened within the timeout");
    (net, at)
}

/// A `Net` of the shard that `net` is connected to.
async fn peer_of(net: &Net) -> Net {
    let (peer, at) = shard_net().await;
    net.dial(at);
    wait_for_input(&peer, &Input::PeerConnected(net.local_worker_id())).await;
    peer
}

/// The record a leader of an earlier term wrote of a task nobody has run.
fn record_of_an_earlier_leader(clock: &ManualClock) -> TaskRecord {
    let mut earlier = Scheduler::with_observer(clock.clone(), Uuid7Ids, RecordOutbox::default());
    earlier.set_leadership_grant(Some(LeadershipGrant {
        term: 0,
        recovery_epoch: RecoveryEpoch::new(0, 0),
        valid_until: LeaseEnd::Unbounded,
    }));
    let submission = Submission::new(TaskDefinitionId::new("demo.task"), 1, b"input".to_vec(), "default");
    earlier.submit(submission).expect("an unlimited leader takes it");
    earlier.observer_mut().take().remove(0)
}

fn task_of(record: &TaskRecord) -> TaskId {
    Write::of(record).task_id
}

/// Has `net` store `record` itself, as the only holder, and waits until it has.
async fn store_on(net: &Net, mut record: TaskRecord) {
    record.placement = vec![net.local_worker_id().into()];
    let write = Write::of(&record);
    net.write_records(vec![PlacedWrite { record, quorum: 1 }]);
    timeout(TEST_TIMEOUT, async {
        loop {
            let stored = net.take_write_outcomes();
            if stored.iter().any(|outcome| outcome.write == write && outcome.stored) {
                return;
            }
            net.wait_for_arrival().await;
        }
    })
    .await
    .expect("the record was stored within the timeout");
}

fn term_of(net: &Net, task: &TaskId) -> Option<u64> {
    net.held_records().get(task)?.version.map(|version| version.leader_term)
}

/// A leader of three voters, of which the two others answered its
/// reconciliation and then went away, so it holds the record of a task it
/// republished and cannot get a quorum to store it. Returns it with the
/// task.
async fn republishing_with_both_voters_gone<'n>(me: &'n Net) -> (Office<'n>, TaskId) {
    let (a, b) = (peer_of(me).await, peer_of(me).await);
    let mut office = Office::new(me, 3);
    let voters = [a.local_worker_id(), b.local_worker_id()];
    // A task this leader is one of the two voters nearest to, so that it holds
    // the record whichever third voter then joins.
    let two = ReplicationFactor::new(NonZeroUsize::new(2).expect("two"));
    let record = loop {
        let record = record_of_an_earlier_leader(&office.clock);
        let all = [me.local_worker_id(), voters[0].clone(), voters[1].clone()];
        let nearest = placement(&task_of(&record), &all, two).expect("placed").holders;
        if nearest.contains(&me.local_worker_id()) {
            break record;
        }
    };
    let task = task_of(&record);
    store_on(me, record).await;
    office.win_office_with(&voters);
    office.hear_from(&voters);
    office.reconcile();
    office.pump(&[&a, &b], 2).await;
    drop((a, b));
    // The round has all it asked for: it is rebuilt and written again.
    office.turn();
    (office, task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_waiting_for_a_quorum_are_placed_again_on_a_voter_that_joins_and_reach_it() {
    let (me, _) = shard_net().await;
    let (mut office, task) = republishing_with_both_voters_gone(&me).await;
    let voters = office.node.voters();
    // A newcomer that, with this leader, holds the record once it is a voter.
    let newcomer = loop {
        let candidate = peer_of(&me).await;
        let mut grown = voters.clone();
        grown.push(candidate.local_worker_id());
        let holders = placement(&task, &grown, ReplicationFactor::DEFAULT).expect("placed").holders;
        if holders.contains(&candidate.local_worker_id()) && holders.contains(&me.local_worker_id()) {
            break candidate;
        }
    };

    let (term, now) = (office.node.term(), office.clock.now());
    let id = newcomer.local_worker_id();
    office.message(&id, heartbeat(&id, term, now, None));
    assert_eq!(office.node.voters().len(), 4, "the newcomer is a voter");
    timeout(TEST_TIMEOUT, async {
        while term_of(&newcomer, &task) != Some(term) {
            office.clock.advance(HEARTBEAT_MS);
            office.turn();
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("the newcomer was written the record at the leader's term");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconciling_leader_that_loses_its_lease_leaves_office_and_stops_writing() {
    let (me, _) = shard_net().await;
    let (mut office, _) = republishing_with_both_voters_gone(&me).await;
    // The write of the republish went out once and was refused: no voter held it.
    timeout(TEST_TIMEOUT, async {
        while office.turn() == 0 {
            me.wait_for_arrival().await;
        }
    })
    .await
    .expect("the first write was refused within the timeout");

    // Nothing confirmed its leadership since: a suspicion timeout later its lease is over.
    office.clock.advance(2 * SUSPECT_MS);
    office.step(Input::Tick);
    assert_eq!(office.node.state(), WorkerState::NoQuorum);
    office.turn();
    assert!(office.reconciliation.is_none(), "the driver dropped the reconciliation");

    // The write would be issued again within a heartbeat interval if it were still run.
    office.clock.advance(10 * HEARTBEAT_MS);
    office.turn();
    tokio::time::sleep(StdDuration::from_millis(300)).await;
    assert_eq!(office.turn(), 0, "no republish write was issued after it left office");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_answers_met_while_the_scheduler_does_not_lead_are_kept_and_adopted_once_it_leads() {
    let (me, _) = shard_net().await;
    let (a, slow) = (peer_of(&me).await, peer_of(&me).await);
    let mut office = Office::new(&me, 3);
    // A record only the slow voter holds: this leader learns of it from its answer.
    let record = record_of_an_earlier_leader(&office.clock);
    let task = task_of(&record);
    store_on(&slow, record).await;
    let voters = [a.local_worker_id(), slow.local_worker_id()];
    office.win_office_with(&voters);
    office.hear_from(&voters);
    office.reconcile();

    // One voter answered, which is a quorum once the grace is over: it leads without the slow one.
    office.pump(&[&a], 1).await;
    // Its lease holds meanwhile: the voter that answered keeps confirming it.
    for _ in 0..3 {
        office.clock.advance(SUSPECT_MS / 2);
        office.hear_from(&[a.local_worker_id()]);
    }
    office.turn();
    assert!(office.scheduler.is_leader());

    // Its lease lapses before the slow voter answers.
    office.scheduler.set_leadership_grant(None);
    office.pump(&[&slow], 1).await;
    office.turn();
    office.pump(&[&slow], 1).await;
    office.turn();

    let (term, epoch) = (office.node.term(), office.node.office_term().expect("in office").recovery_epoch);
    office.scheduler.set_leadership_grant(Some(LeadershipGrant {
        term,
        recovery_epoch: epoch,
        valid_until: LeaseEnd::Unbounded,
    }));
    office.clock.advance(SUSPECT_MS);
    office.turn();

    let claimed = office.scheduler.claim_oldest(&a.local_worker_id(), 1).expect("it leads");
    let claimed: Vec<TaskId> = claimed.iter().map(|claim| claim.task.task_id.clone().expect("a claim names its task").into()).collect();
    assert_eq!(claimed, [task], "the record the slow voter reported was adopted");
}
