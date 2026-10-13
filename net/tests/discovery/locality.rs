//! Where each worker's runs are found. Five workers, each running what its
//! driver claims through an executor, over records skewed onto three of
//! them: a worker, the leader included, takes the records it holds first;
//! an idle worker steals from the nearest distance class first and widens
//! one class at a time; the shard's oldest tasks come only after those; and
//! the work ends up spread over all five.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::queues::Subscription;
use kabudachi_core::task_record::RecordKey;
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_core::versions::RunnableVersions;
use kabudachi_net::discovery::Stage;
use kabudachi_net::messenger::Net;
use kabudachi_net::task_store::placement::{ReplicationFactor, placement};
use libp2p::PeerId;
use libp2p::futures::future::join_all;
use libp2p::kad::KBucketKey;

use crate::support::deadline::within_deadline;
use crate::support::executor::FakeExecutor;
use crate::support::records::{
    Voters, claims_in, placing_among_every_voter, plain_with, submitted_with, wait_until_held,
};

/// How many task ids are tried before a shard's identities count as too
/// unlucky to lay out.
const IDS_TRIED: usize = 1 << 16;

/// Records the near follower holds: one it runs itself, three an idle
/// worker steals.
const NEAR_TASKS: usize = 4;
/// Records the heavy follower holds: three it runs first, one the leader
/// takes as the shard's oldest, one an idle worker steals once it widens,
/// and five for the spread phase.
const HEAVY_TASKS: usize = 10;
/// Records the leader holds and runs itself. It holds one more, at
/// [`NEWER_VERSION`], which it never claims.
const LEADER_TASKS: usize = 2;
/// The input version every worker runs the test's task definition at.
const RUNNABLE_VERSION: u32 = 3;
/// The input version of the one record no worker here can run.
const NEWER_VERSION: u32 = 5;

/// Each record is written to one voter only, so who holds what is exact.
fn one_holder() -> ReplicationFactor {
    ReplicationFactor::new(NonZeroUsize::MIN)
}

/// The first `count` task ids, of [`IDS_TRIED`], whose one holder among
/// `voters` is `holder`; `None` if fewer land there.
fn held_by(voters: &[WorkerId], holder: &WorkerId, count: usize) -> Option<Vec<TaskId>> {
    let mut held = Vec::new();
    for n in 0..IDS_TRIED {
        if held.len() == count {
            break;
        }
        let task = TaskId::new(format!("task-{n:06}"));
        if placement(&RecordKey::Task(task.clone()), voters, one_holder())?.holders[0] == *holder {
            held.push(task);
        }
    }
    (held.len() == count).then_some(held)
}

/// `tasks`, the ones whose record keys are nearest `worker`'s own key first.
fn nearest_first(worker: &WorkerId, tasks: &[TaskId]) -> Vec<TaskId> {
    let local = KBucketKey::from(PeerId::from_str(worker.as_str()).expect("a worker id is a peer id"));
    let mut tasks = tasks.to_vec();
    tasks.sort_by_key(|task| KBucketKey::new(RecordKey::Task(task.clone()).kad_bytes()).distance(&local));
    tasks
}

/// The next `count` runs `executor` is handed, each with its stage and task.
async fn runs(executor: &mut FakeExecutor, count: usize) -> Vec<(Stage, TaskId)> {
    let mut runs = Vec::new();
    for _ in 0..count {
        let (stage, _, claim) = executor.next_found().await;
        runs.push((stage, claim.task.as_ref().expect("a claim carries its task").task_id()));
    }
    runs
}


/// Which voter, by index, plays which part.
#[derive(Debug, Clone, Copy)]
struct Roles {
    leader: usize,
    idle: usize,
    near: usize,
    heavy: usize,
    other: usize,
}

/// The index of the distance class in `targets` that holds `peer`.
fn class_of(targets: &[Vec<WorkerId>], peer: &WorkerId) -> usize {
    targets
        .iter()
        .position(|class| class.contains(peer))
        .expect("every other voter is a steal target")
}

/// Drives the shard until `voter`'s steal targets are the other voters, all
/// of them and no one else, and returns them.
async fn steal_targets_once_routed(shard: &mut Voters, voter: usize) -> Vec<Vec<WorkerId>> {
    let net = shard.nets[voter].clone();
    let others: BTreeSet<WorkerId> = shard.others(voter).into_iter().map(|other| shard.id(other)).collect();
    shard
        .drive_until(async {
            loop {
                let targets = net.steal_targets().await;
                if targets.iter().flatten().cloned().collect::<BTreeSet<_>>() == others {
                    return targets;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
}

/// Roles for the shard: an idle follower whose steal targets put the near
/// follower in a nearer class than the heavy one. `None` if no follower's
/// classes allow it.
async fn roles_once_routed(shard: &mut Voters, leader: usize) -> Option<(Roles, Vec<Vec<WorkerId>>)> {
    let followers = shard.others(leader);
    for &idle in &followers {
        let targets = steal_targets_once_routed(shard, idle).await;
        let peers: Vec<usize> = followers.iter().copied().filter(|f| *f != idle).collect();
        for &near in &peers {
            for &heavy in &peers {
                let (near_id, heavy_id) = (shard.id(near), shard.id(heavy));
                if class_of(&targets, &near_id) < class_of(&targets, &heavy_id) {
                    let other = *peers.iter().find(|f| **f != near && **f != heavy).expect("three peers");
                    return Some((Roles { leader, idle, near, heavy, other }, targets));
                }
            }
        }
    }
    None
}

/// How many steal requests each voter has received.
async fn steals_received(nets: &[Arc<Net>]) -> Vec<u64> {
    let mut received = Vec::new();
    for net in nets {
        received.push(net.diagnostics().await.traffic.steal_requests_received);
    }
    received
}

fn tasks(runs: &[(Stage, TaskId)]) -> Vec<TaskId> {
    runs.iter().map(|(_, task)| task.clone()).collect()
}

/// The reason `net`'s record of `task` gives for the task being blocked,
/// once it shows one.
async fn blocked_reason(net: &Net, task: &TaskId) -> String {
    loop {
        match net.held_records().get(task) {
            Some(record) if record.blocked_since.is_some() => return record.blocked_reason,
            _ => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn work_is_found_own_records_first_then_nearer_peers_then_farther_and_spreads_over_every_worker() {
    within_deadline(async {
        // Peers in one distance class for every follower leave nothing to
        // tell nearer from farther, so start again (rarely) when no roles fit.
        let runnable = RunnableVersions::default().with(TaskDefinitionId::new("demo.task"), RUNNABLE_VERSION);
        let (mut shard, mut executors, roles, targets, (heavy_tasks, near_tasks, mut leader_tasks)) = loop {
            let mut shard = Voters::start_without_client(5, one_holder()).await;
            let executors: Vec<FakeExecutor> = (0..5)
                .map(|voter| {
                    let (executor, endpoint) = FakeExecutor::new();
                    shard.set_executor(voter, endpoint);
                    shard.set_runnable_versions(voter, runnable.clone());
                    executor
                })
                .collect();
            let leader = shard.drive_until_a_leader().await;
            let voters = placing_among_every_voter(&mut shard, leader).await;
            let Some((roles, targets)) = roles_once_routed(&mut shard, leader).await else {
                continue;
            };
            let layout = (
                held_by(&voters, &shard.id(roles.heavy), HEAVY_TASKS),
                held_by(&voters, &shard.id(roles.near), NEAR_TASKS),
                held_by(&voters, &shard.id(leader), LEADER_TASKS + 1),
            );
            if let (Some(heavy), Some(near), Some(own)) = layout {
                break (shard, executors, roles, targets, (heavy, near, own));
            }
        };
        let newer = leader_tasks.pop().expect("the leader holds a newer record");
        let Roles { leader, idle, near, heavy, other } = roles;
        let nets = shard.nets.clone();
        let ids: Vec<WorkerId> = (0..5).map(|voter| shard.id(voter)).collect();
        let start = WallTime::now(&RealClock::new()).as_unix_millis() - 60_000;

        // Every record is submitted a millisecond after the one before, by a
        // follower that holds none of them: the newer record first, so it is
        // the shard's oldest task; then the heavy follower's, so the oldest
        // after it are its own; then the near follower's, then the leader's.
        shard
            .drive_until(async {
                let mut at_newer = plain_with(b"input");
                at_newer.source_version = NEWER_VERSION;
                let first = WallTime::from_unix_millis(start);
                submitted_with(&nets[other], &ids[leader], (newer.as_str(), first), at_newer).await;
                let submitted = heavy_tasks.iter().chain(&near_tasks).chain(&leader_tasks);
                for (n, task) in submitted.enumerate() {
                    let at = WallTime::from_unix_millis(start + 1 + n as u64);
                    submitted_with(&nets[other], &ids[leader], (task.as_str(), at), plain_with(b"input")).await;
                }
                wait_until_held(nets[heavy].clone(), heavy_tasks.clone()).await;
                wait_until_held(nets[near].clone(), near_tasks.clone()).await;
                let held_by_leader = leader_tasks.iter().cloned().chain([newer.clone()]).collect();
                wait_until_held(nets[leader].clone(), held_by_leader).await;
            })
            .await;

        // Own first: a follower with places runs its own nearest records and
        // asks no one.
        let before = steals_received(&nets).await;
        executors[near].grant(1);
        executors[heavy].grant(3);
        let (near_own, heavy_own) = {
            let [near_executor, heavy_executor] = executors.get_disjoint_mut([near, heavy]).expect("two voters");
            shard
                .drive_until(async { tokio::join!(runs(near_executor, 1), runs(heavy_executor, 3)) })
                .await
        };
        assert!(
            near_own.iter().chain(&heavy_own).all(|(stage, _)| *stage == Stage::Own),
            "{near_own:?} {heavy_own:?}"
        );
        assert_eq!(tasks(&near_own), nearest_first(&ids[near], &near_tasks)[..1]);
        assert_eq!(tasks(&heavy_own), nearest_first(&ids[heavy], &heavy_tasks)[..3], "nearest keys first");
        assert_eq!(steals_received(&nets).await, before, "no worker with its own work asked anyone");

        // The leader too: the records it holds, nearest first, and only then
        // the shard's oldest task, the heavy follower's oldest left. The newer
        // record, its own and the shard's oldest, is neither: the leader cannot
        // run it, so its own stage skips it and blocks it for the leader, and
        // its oldest pass it over.
        let before = steals_received(&nets).await;
        executors[leader].grant(LEADER_TASKS as u32 + 1);
        let leader_runs = shard.drive_until(runs(&mut executors[leader], LEADER_TASKS + 1)).await;
        let oldest_left = heavy_tasks
            .iter()
            .find(|task| !tasks(&heavy_own).contains(task))
            .expect("the heavy follower has tasks left")
            .clone();
        let expected: Vec<(Stage, TaskId)> = nearest_first(&ids[leader], &leader_tasks)
            .into_iter()
            .map(|task| (Stage::Own, task))
            .chain([(Stage::Oldest, oldest_left)])
            .collect();
        assert_eq!(leader_runs, expected, "the leader runs its own records, nearest first, before the shard's oldest");
        assert_eq!(steals_received(&nets).await, before, "the leader asks no one");
        let reason = shard.drive_until(blocked_reason(&nets[leader], &newer)).await;
        assert!(reason.contains(&format!("newer than this worker's {RUNNABLE_VERSION}")), "{reason}");

        // The newer record leaves the shard: a worker not in its blocked set
        // claims it as the shard's oldest and reports it incompatible, so it
        // ends for good and no later phase can be handed it.
        let claimed = shard
            .drive_until(nets[other].claim_oldest(ids[leader].clone(), 1, &Subscription::Every, None))
            .await
            .expect("the leader answers");
        assert_eq!(claims_in(&claimed), vec![newer.clone()], "only the leader is in the newer record's blocked set");
        shard
            .drive_until(nets[other].report_blocked(ids[leader].clone(), newer.clone(), "not run here".to_owned(), true))
            .await
            .expect("the leader notes the block");

        // Steal outward: the idle worker takes the near class's work, then
        // widens one class at a time until its places are filled.
        let before = steals_received(&nets).await;
        executors[idle].grant(NEAR_TASKS as u32);
        let stolen = shard.drive_until(runs(&mut executors[idle], NEAR_TASKS)).await;
        let after = steals_received(&nets).await;
        let (class_near, class_heavy) = (class_of(&targets, &ids[near]), class_of(&targets, &ids[heavy]));
        let near_left: BTreeSet<TaskId> =
            near_tasks.iter().filter(|task| !tasks(&near_own).contains(task)).cloned().collect();
        assert!(stolen[..3].iter().all(|(stage, _)| *stage == Stage::Peer { class: class_near }), "{stolen:?}");
        assert_eq!(tasks(&stolen[..3]).into_iter().collect::<BTreeSet<_>>(), near_left);
        assert_eq!(stolen[3].0, Stage::Peer { class: class_heavy }, "{stolen:?}");
        assert!(heavy_tasks.contains(&stolen[3].1));
        for voter in (0..5).filter(|voter| *voter != idle) {
            let expected = u64::from(class_of(&targets, &ids[voter]) <= class_heavy);
            assert_eq!(
                after[voter] - before[voter],
                expected,
                "every class up to the heavy one is asked once and no farther one at all: voter {voter}"
            );
        }

        // Spread: one place each, five tasks left, all on the heavy worker.
        for executor in &executors {
            executor.grant(1);
        }
        let spread: Vec<(Stage, TaskId)> = shard
            .drive_until(join_all(executors.iter_mut().map(|executor| async { runs(executor, 1).await.remove(0) })))
            .await;
        assert_eq!(spread[heavy].0, Stage::Own);
        assert_eq!(spread[leader].0, Stage::Oldest, "the leader holds nothing left to run, so it takes the oldest");
        for voter in [idle, near, other] {
            assert_ne!(spread[voter].0, Stage::Own, "voter {voter} holds nothing left to run");
        }

        let mut ran: BTreeMap<usize, usize> = BTreeMap::new();
        let mut every_task = Vec::new();
        for (voter, list) in [(near, &near_own), (heavy, &heavy_own), (leader, &leader_runs), (idle, &stolen)] {
            *ran.entry(voter).or_default() += list.len();
            every_task.extend(tasks(list));
        }
        for (voter, run) in spread.iter().enumerate() {
            *ran.entry(voter).or_default() += 1;
            every_task.push(run.1.clone());
        }
        assert_eq!(
            ran,
            BTreeMap::from([(idle, 5), (near, 2), (heavy, 4), (other, 1), (leader, 4)]),
            "the worker that held ten of sixteen records ran four, and every worker ran some"
        );
        let submitted: BTreeSet<TaskId> = heavy_tasks.iter().chain(&near_tasks).chain(&leader_tasks).cloned().collect();
        assert_eq!(every_task.len(), submitted.len(), "no task ran twice");
        assert_eq!(every_task.into_iter().collect::<BTreeSet<_>>(), submitted);
    })
    .await
}
