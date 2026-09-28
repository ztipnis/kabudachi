//! A five-worker shard over real sockets loses its leader, and the four
//! survivors elect one replacement (ADR-0001 decisions 3 to 6, 13 and 15).
//!
//! Every worker starts through its entry point (`kabudachi_net::worker`)
//! with no coordination authority: a founder leads alone, four more join it
//! as pending members and are admitted in admission batches. The leader is
//! then cut off from every other worker (`Net::block_peer`, on both sides of
//! each pair): its connections close and none reopens, while it keeps
//! running, as across a network partition.
//!
//! What it checks is how the survivors settle: however many of them suspect
//! the leader close together, the gossip roll call's suppression and
//! tie-break leave one candidate, whose vote and certificate make it the
//! leader every survivor follows, and which passes straight through
//! `LeaderReconciling` (reconciliation is Phase 3). The ignored
//! `census_latency_over_repeated_runs` measures, over the same scenario, how
//! long a census takes to come back, which is what the roll-call deadline's
//! default is set from (see `ElectionTimings::DEFAULT_ROLL_CALL_DEADLINE`).

mod support;

use std::time::{Duration as StdDuration, Instant as StdInstant};

use kabudachi_core::election::{ElectionTimings, Output};
use kabudachi_core::protocol::ids::ShardId;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::{ElectionMessage, election_message};
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use kabudachi_net::worker::WorkerConfig;
use libp2p::Multiaddr;
use tokio::time::timeout;

use support::worker::{Batch, TimelineWorker as RunningWorker, isolate, spawn_timeline_worker};

const SHARD: &str = "shard-1";

/// Generous backstop for each thing the test waits for.
const WAIT_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// Well above loopback connection set-up, so a worker's first suspicion
/// comes after it is connected.
const SUSPECT_TIMEOUT_MS: u64 = 300;

/// Twice this fits well inside the lease (nine tenths of the suspicion
/// timeout).
const HEARTBEAT_INTERVAL_MS: u64 = 50;

fn timings(roll_call_deadline: Duration) -> ElectionTimings {
    ElectionTimings::new(
        Duration::from_millis(SUSPECT_TIMEOUT_MS),
        Duration::from_millis(HEARTBEAT_INTERVAL_MS),
    )
    .with_roll_call_deadline(roll_call_deadline)
}

/// Starts a worker listening on loopback, bootstrapping through `seeds` on
/// `roll_call_deadline` (`support::worker::spawn_timeline_worker`, shared
/// with the other worker-entry-point test files; this file needs its full
/// batch timeline, not just the latest `Seen`, so it uses that rather than
/// `support::worker::spawn_worker`).
async fn spawn_worker(roll_call_deadline: Duration, seeds: Vec<Multiaddr>) -> RunningWorker {
    let config = WorkerConfig::new(
        ShardId::new(SHARD),
        "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
        timings(roll_call_deadline),
    )
    .with_seeds(seeds)
    .with_retry_interval(StdDuration::from_millis(50));
    spawn_timeline_worker(config).await
}

/// Polls until `until` holds.
async fn wait_until(what: &str, mut until: impl FnMut() -> bool) {
    timeout(WAIT_TIMEOUT, async {
        while !until() {
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within the timeout"));
}

/// A founder leading four admitted workers, each a voter of the committed
/// configuration of five and seeing every other subscribed to the shard's
/// gossip: the founder first.
///
/// Each joiner is given only the founder's address and reaches the others
/// through kad's crawl. The leader is cut off only once every worker sees
/// every other in the shard's gossip, so its roll calls can reach them.
/// Until kad crawls again as members are admitted (E10's fix), a joiner
/// whose one crawl ran before the others joined never sees them, and this
/// waits out its timeout.
async fn five_admitted_workers(roll_call_deadline: Duration) -> Vec<RunningWorker> {
    let founder = spawn_worker(roll_call_deadline, vec![]).await;
    wait_until("the founder leads", || {
        founder
            .latest()
            .is_some_and(|batch| batch.state == WorkerState::Leader)
    })
    .await;
    let mut workers = vec![founder];
    for _ in 0..4 {
        let seeds = vec![workers[0].address.clone()];
        workers.push(spawn_worker(roll_call_deadline, seeds).await);
    }
    let founder = workers[0].id.clone();
    wait_until(
        "every worker is a voter of one committed configuration under the founder",
        || {
            workers.iter().all(|worker| {
                worker.latest().is_some_and(|batch| {
                    batch.settled_member
                        && batch.leader.as_ref().map(|(id, _)| id) == Some(&founder)
                })
            })
        },
    )
    .await;
    let ids: Vec<WorkerId> = workers.iter().map(|worker| worker.id.clone()).collect();
    wait_until(
        "every worker sees every other subscribed to the shard",
        || {
            workers.iter().all(|worker| {
                let subscribers = worker.net.shard_subscribers();
                ids.iter()
                    .filter(|id| **id != worker.id)
                    .all(|id| subscribers.contains(id))
            })
        },
    )
    .await;
    workers
}

/// How a shard of five settled after its leader was cut off.
struct LeaderLoss {
    /// How long after the leader was cut off every survivor followed the
    /// new one.
    failover: StdDuration,
    old_leader: WorkerId,
    new_leader: WorkerId,
    term: u64,
    /// The four survivors' batches since the leader was cut off.
    survivors: Vec<(WorkerId, Vec<Batch>)>,
}

impl LeaderLoss {
    fn batches_of(&self, worker: &WorkerId) -> &[Batch] {
        &self
            .survivors
            .iter()
            .find(|(id, _)| id == worker)
            .expect("a survivor")
            .1
    }

    /// The other workers whose replies the new leader's winning roll call
    /// had counted by the last batch it ended still `RollCall`, sorted. A
    /// reply taken in the very batch the call closes in is counted by the
    /// node but missing here.
    fn census(&self) -> Vec<WorkerId> {
        let mut census: Vec<WorkerId> = self
            .batches_of(&self.new_leader)
            .iter()
            .rev()
            .find(|batch| !batch.roll_call_respondents.is_empty())
            .expect("the new leader ran a roll call")
            .roll_call_respondents
            .iter()
            .filter(|id| **id != self.new_leader)
            .cloned()
            .collect();
        census.sort();
        census
    }

    /// How long after the new leader published its winning roll call each
    /// reply it counted reached it: batch by batch, so each is at most one
    /// driver batch late.
    fn census_latencies(&self) -> Vec<StdDuration> {
        let batches = self.batches_of(&self.new_leader);
        let published_at = batches
            .iter()
            .find(|batch| {
                batch.outputs.iter().any(|output| {
                    matches!(
                        output,
                        Output::Publish { message: ElectionMessage {
                            payload: Some(election_message::Payload::RollCall(call)),
                        } } if call.term == self.term
                    )
                })
            })
            .unwrap_or_else(|| {
                panic!(
                    "the new leader published the roll call it won (term {}): {:#?}",
                    self.term,
                    batches
                        .iter()
                        .filter(|batch| !batch.outputs.is_empty())
                        .map(|batch| (batch.state, batch.leader.clone(), &batch.outputs))
                        .collect::<Vec<_>>()
                )
            })
            .at;
        self.census()
            .iter()
            .filter_map(|id| {
                batches
                    .iter()
                    .find(|batch| batch.roll_call_respondents.contains(id))
                    .map(|batch| batch.at - published_at)
            })
            .collect()
    }
}

/// Builds a shard of five, cuts its leader off, and waits until every
/// survivor follows one new leader.
async fn lose_the_leader(roll_call_deadline: Duration) -> LeaderLoss {
    let workers = five_admitted_workers(roll_call_deadline).await;
    let old_leader = workers[0].id.clone();
    let survivors = &workers[1..];
    let cut_off_at = StdInstant::now();
    isolate(&workers[0], survivors);

    let mut settled = None;
    wait_until("every survivor follows one new leader", || {
        let leaders: Vec<Option<(WorkerId, u64)>> = survivors
            .iter()
            .map(|worker| worker.latest().and_then(|batch| batch.leader))
            .collect();
        settled = match &leaders[..] {
            [Some(first), rest @ ..]
                if first.0 != old_leader
                    && rest.iter().all(|other| other.as_ref() == Some(first)) =>
            {
                Some(first.clone())
            }
            _ => None,
        };
        settled.is_some()
    })
    .await;
    let failover = cut_off_at.elapsed();
    let (new_leader, term) = settled.expect("every survivor follows one leader");
    LeaderLoss {
        failover,
        old_leader,
        new_leader,
        term,
        survivors: survivors
            .iter()
            .map(|worker| (worker.id.clone(), worker.batches_since(cut_off_at)))
            .collect(),
    }
}

fn state_changes(batches: &[Batch]) -> Vec<WorkerState> {
    batches
        .iter()
        .flat_map(|batch| &batch.outputs)
        .filter_map(|output| match output {
            Output::StateChanged(state) => Some(*state),
            _ => None,
        })
        .collect()
}

/// The workers `batches` sent a message to whose payload `matches` accepts,
/// sorted.
fn sent_to(
    batches: &[Batch],
    matches: impl Fn(&election_message::Payload) -> bool,
) -> Vec<WorkerId> {
    let mut recipients: Vec<WorkerId> = batches
        .iter()
        .flat_map(|batch| &batch.outputs)
        .filter_map(|output| match output {
            Output::Send {
                to,
                message:
                    ElectionMessage {
                        payload: Some(payload),
                    },
            } if matches(payload) => Some(to.clone()),
            _ => None,
        })
        .collect();
    recipients.sort();
    recipients
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_survivors_of_a_lost_leader_settle_on_one_initiator_that_leads() {
    let loss = lose_the_leader(ElectionTimings::DEFAULT_ROLL_CALL_DEADLINE).await;

    // Several survivors may start a roll call, but one stands: every other
    // initiator gave its call up for a better one, was refused, or answered
    // one before its own was due.
    let candidates: Vec<&WorkerId> = loss
        .survivors
        .iter()
        .filter(|(_, batches)| state_changes(batches).contains(&WorkerState::Candidate))
        .map(|(id, _)| id)
        .collect();
    assert_eq!(candidates, vec![&loss.new_leader]);

    // Its census counted a quorum of the five (itself and two more), and
    // its vote and certificate went to the same workers, each of them one
    // it had counted.
    let census = loss.census();
    let leader_batches = loss.batches_of(&loss.new_leader);
    let asked = sent_to(leader_batches, |payload| {
        matches!(payload, election_message::Payload::VoteRequest(request)
            if request.term == loss.term)
    });
    let certified = sent_to(leader_batches, |payload| {
        matches!(payload, election_message::Payload::ElectionCertificate(certificate)
            if certificate.term == loss.term)
    });
    assert!(asked.len() >= 2, "{asked:?}");
    assert_eq!(certified, asked);
    assert!(
        census.iter().all(|worker| asked.contains(worker)),
        "{census:?} {asked:?}"
    );

    // It went from its win straight through `LeaderReconciling` to
    // `Leader`, with nothing to reconcile yet.
    let changes = state_changes(leader_batches);
    let stood = changes
        .iter()
        .position(|state| *state == WorkerState::Candidate)
        .expect("it stood");
    assert_eq!(
        &changes[stood..],
        &[
            WorkerState::Candidate,
            WorkerState::LeaderReconciling,
            WorkerState::Leader
        ],
        "{changes:?}"
    );

    // No survivor contested a later term against it, or named any other
    // leader after the loss.
    for (id, batches) in &loss.survivors {
        let later_calls: Vec<u64> = batches
            .iter()
            .flat_map(|batch| &batch.outputs)
            .filter_map(|output| match output {
                Output::Publish {
                    message:
                        ElectionMessage {
                            payload: Some(election_message::Payload::RollCall(call)),
                        },
                } if call.term > loss.term => Some(call.term),
                _ => None,
            })
            .collect();
        assert!(later_calls.is_empty(), "{id:?} called {later_calls:?}");
    }
    for (id, batches) in &loss.survivors {
        let named: Vec<&(WorkerId, u64)> = batches
            .iter()
            .filter_map(|batch| batch.leader.as_ref())
            .filter(|(leader, _)| *leader != loss.old_leader)
            .collect();
        assert!(
            named
                .iter()
                .all(|leader| **leader == (loss.new_leader.clone(), loss.term)),
            "{id:?} named {named:?}"
        );
    }
}

/// The `percent`th percentile of `samples` (nearest rank).
fn percentile(samples: &mut [StdDuration], percent: usize) -> StdDuration {
    samples.sort();
    let rank = (samples.len() * percent).div_ceil(100).max(1);
    samples[rank - 1]
}

// The measurement the roll-call deadline's default is set from: the time
// from the winning roll call's publication to each reply it counted
// reaching the initiator, and to the last of them (the whole census), over
// `KABUDACHI_CENSUS_RUNS` leader losses (default 30), each on a deadline long
// enough not to cut a census short. Slow, so ignored; run it with
// `KABUDACHI_CENSUS_RUNS=30 cargo test -p kabudachi-net --test
// leader_loss_test -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement over repeated runs, not a check"]
async fn census_latency_over_repeated_runs() {
    let runs: usize = std::env::var("KABUDACHI_CENSUS_RUNS")
        .ok()
        .and_then(|runs| runs.parse().ok())
        .unwrap_or(30);
    let generous_deadline = Duration::from_millis(2_000);
    let (mut replies, mut censuses, mut failovers) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..runs {
        let loss = lose_the_leader(generous_deadline).await;
        let latencies = loss.census_latencies();
        censuses.push(*latencies.iter().max().expect("the census had replies"));
        replies.extend(latencies);
        failovers.push(loss.failover);
    }
    for (what, samples) in [
        ("reply", &mut replies),
        ("whole census", &mut censuses),
        ("failover", &mut failovers),
    ] {
        println!(
            "{what}: n={} p50={:?} p99={:?} max={:?}",
            samples.len(),
            percentile(samples, 50),
            percentile(samples, 99),
            percentile(samples, 100),
        );
    }
}
