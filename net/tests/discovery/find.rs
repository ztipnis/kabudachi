//! A worker with room claims what it holds itself before it asks anyone,
//! then steals from peers nearer before farther, then asks the leader; a
//! claim its stale view suggested is refused and discovery moves on.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::Claim;
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_net::discovery::{Found, Stage};
use kabudachi_net::messenger::{Net, PlacedWrite};
use kabudachi_net::task_store::placement::{ReplicationFactor, placement};
use kabudachi_net::task_store::record_key;
use libp2p::PeerId;
use libp2p::kad::{KBucketDistance, KBucketKey};

use crate::support::deadline::within_deadline;
use crate::support::net::wait_until_registered;
use crate::support::records::{
    ThreeVoters, Voters, claimed, plain_with, submitted_through, submitted_with, wait_until_held,
};

/// Each record is written to one voter only, so who holds what differs from
/// voter to voter.
fn one_holder() -> ReplicationFactor {
    ReplicationFactor::new(NonZeroUsize::MIN)
}

fn ids(shard: &Voters) -> Vec<WorkerId> {
    (0..shard.nets.len()).map(|voter| shard.id(voter)).collect()
}

fn tasks_of(found: &Found) -> Vec<TaskId> {
    found
        .claims
        .iter()
        .map(|(_, claim): &(Stage, Claim)| claim.task.as_ref().expect("a claim carries its task").task_id())
        .collect()
}

fn sorted(mut tasks: Vec<TaskId>) -> Vec<TaskId> {
    tasks.sort();
    tasks
}

/// How far `peer`'s key is from `worker`'s.
fn distance(worker: &WorkerId, peer: &WorkerId) -> KBucketDistance {
    let key = |id: &WorkerId| KBucketKey::from(PeerId::from_str(id.as_str()).expect("a worker id is a peer id"));
    key(worker).distance(&key(peer))
}

/// `tasks`, the ones whose record keys are nearest `worker`'s own key first.
fn nearest_first(worker: &WorkerId, tasks: &[TaskId]) -> Vec<TaskId> {
    let local = KBucketKey::from(PeerId::from_str(worker.as_str()).expect("a worker id is a peer id"));
    let mut tasks = tasks.to_vec();
    tasks.sort_by_key(|task| KBucketKey::new(record_key(task)).distance(&local));
    tasks
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

/// Drives the shard until `leader` places records among every voter, and
/// returns them: where a task's record goes is then [`placement`] over them.
async fn placing_among_every_voter(shard: &mut Voters, leader: usize) -> Vec<WorkerId> {
    loop {
        let voters = shard.with(leader, |node, _| node.voters()).await;
        if voters.len() == shard.nets.len() {
            return voters;
        }
        shard.drive_until(tokio::time::sleep(Duration::from_millis(10))).await;
    }
}

/// How many task ids [`laid_out`] tries before the voters' identities count
/// as too unlucky to use.
const IDS_TRIED: usize = 1 << 16;

/// Task ids chosen by where their records go, each held by one voter.
struct Layout {
    /// Held by the worker, in id order, which is not nearest first.
    mine: Vec<TaskId>,
    /// Held by a voter of the worker's nearest distance class, with that voter.
    near: (TaskId, WorkerId),
    /// Held by a voter of a farther class, with that voter.
    far: (TaskId, WorkerId),
}

/// The first of [`IDS_TRIED`] task ids that, placed among `voters`, give a
/// [`Layout`] for `worker`, whose steal targets are `targets`; `None` if they
/// give none.
fn laid_out(voters: &[WorkerId], worker: &WorkerId, targets: &[Vec<WorkerId>]) -> Option<Layout> {
    let (mut mine, mut near, mut far) = (Vec::new(), None, None);
    for n in 0..IDS_TRIED {
        let task = TaskId::new(format!("task-{n:06}"));
        let holder = placement(&task, voters, one_holder())?.holders.remove(0);
        if holder == *worker {
            if nearest_first(worker, &mine) == mine {
                mine.push(task);
            }
        } else if targets[0].contains(&holder) {
            near.get_or_insert((task, holder));
        } else {
            far.get_or_insert((task, holder));
        }
        if nearest_first(worker, &mine) != mine
            && let (Some(near), Some(far)) = (&near, &far)
        {
            return Some(Layout {
                mine,
                near: near.clone(),
                far: far.clone(),
            });
        }
    }
    None
}

fn now() -> WallTime {
    WallTime::now(&RealClock::new())
}

/// The voter, by index, that holds `task`; one does when each record is
/// written to one voter.
fn holder_of(nets: &[Arc<Net>], task: &TaskId) -> usize {
    (0..nets.len())
        .find(|voter| nets[*voter].held_records().get(task).is_some())
        .expect("one voter holds it")
}

/// How many steal requests each voter has received.
async fn steals_received(nets: &[Arc<Net>]) -> Vec<u64> {
    let mut received = Vec::new();
    for net in nets {
        received.push(net.diagnostics().await.traffic.steal_requests_received);
    }
    received
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_claims_its_own_records_nearest_first_and_then_steals_nearer_peers_before_farther() {
    within_deadline(async {
        // Peers in one distance class leave nothing to tell nearer from
        // farther, so start again (rarely) when its peers fall in one, or
        // when no task ids land where the test needs them.
        let (mut shard, leader, worker, targets, layout) = loop {
            let mut shard = Voters::start_without_client(5, one_holder()).await;
            let leader = shard.drive_until_a_leader().await;
            let voters = placing_among_every_voter(&mut shard, leader).await;
            let worker = shard.others(leader)[0];
            let targets = steal_targets_once_routed(&mut shard, worker).await;
            if targets.len() < 2 {
                continue;
            }
            if let Some(layout) = laid_out(&voters, &shard.id(worker), &targets) {
                break (shard, leader, worker, targets, layout);
            }
        };
        let (nets, ids) = (shard.nets.clone(), ids(&shard));
        let class_of = |voter: usize| {
            targets
                .iter()
                .position(|class| class.contains(&ids[voter]))
                .expect("every other voter is a steal target")
        };
        let Layout { mine, near, far } = layout;
        let theirs = vec![near.0.clone(), far.0.clone()];
        let held_by = |holder: &WorkerId| nets[ids.iter().position(|id| id == holder).expect("a voter")].clone();

        let (own, near, far, asked) = shard
            .drive_until(async {
                for task in mine.iter().chain(&theirs) {
                    submitted_with(&nets[worker], &ids[leader], (task.as_str(), now()), plain_with(b"input")).await;
                }
                wait_until_held(nets[worker].clone(), mine.clone()).await;
                for (task, holder) in [near, far] {
                    wait_until_held(held_by(&holder), vec![task]).await;
                }
                let own = nets[worker].discover(ids[leader].clone(), mine.len(), now(), false).await;
                let before = steals_received(&nets).await;
                let near = nets[worker].discover(ids[leader].clone(), 1, now(), false).await;
                let after = steals_received(&nets).await;
                let asked: Vec<u64> = before.iter().zip(&after).map(|(before, after)| after - before).collect();
                let far = nets[worker].discover(ids[leader].clone(), 100, now(), false).await;
                (own, near, far, asked)
            })
            .await;

        assert_eq!(own.stopped, None);
        assert!(own.claims.iter().all(|(stage, _)| *stage == Stage::Own), "{own:?}");
        assert_eq!(
            tasks_of(&own),
            nearest_first(&ids[worker], &mine),
            "its own records, the nearest keys first, and nothing it did not hold"
        );

        let away = |peer: &WorkerId| distance(&ids[worker], peer);
        assert!(
            targets.windows(2).all(|pair| {
                pair[0].iter().map(away).max() < pair[1].iter().map(away).min()
            }),
            "steal targets run from the nearest distance class outward: {targets:?}"
        );

        assert_eq!(near.stopped, None);
        assert_eq!(near.claims.len(), 1, "{near:?}");
        assert_eq!(near.claims[0].0, Stage::Peer { class: 0 });
        for voter in 0..nets.len() {
            if voter == worker {
                continue;
            }
            let expected = u64::from(class_of(voter) == 0);
            assert_eq!(
                asked[voter], expected,
                "a limit the nearest class meets asks it alone: voter {voter} of class {}",
                class_of(voter)
            );
        }

        assert_eq!(far.stopped, None);
        for (stage, claim) in &far.claims {
            let Stage::Peer { class } = stage else {
                panic!("what it did not hold came from a peer, not {stage:?}");
            };
            let task = claim.task.as_ref().expect("a claim carries its task").task_id();
            let holder = holder_of(&nets, &task);
            assert_eq!(*class, class_of(holder), "{task:?} was found in the class of its holder");
        }
        let mut found = tasks_of(&near);
        found.extend(tasks_of(&far));
        assert_eq!(sorted(found), sorted(theirs), "every task held elsewhere was found");
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_the_workers_stale_record_suggested_is_refused_and_discovery_moves_on() {
    within_deadline(async {
        let (mut shard, client) = Voters::start_with(3, one_holder()).await;
        let leader = shard.drive_until_a_leader().await;
        let (nets, ids) = (shard.nets.clone(), ids(&shard));
        let submitter = shard.others(leader)[0];

        let found = shard
            .drive_until(async {
                let task = submitted_through(&nets[submitter], &ids[leader], plain_with(b"input")).await;
                let holder = holder_of(&nets, &task);
                // One voter that is no leader and holds nothing is the
                // worker; the other voter that is no leader is the rival.
                let others: Vec<usize> = (0..nets.len()).filter(|voter| *voter != leader).collect();
                let worker = *others.iter().find(|voter| **voter != holder).expect("two voters are no leader");
                let rival = *others.iter().find(|voter| **voter != worker).expect("two voters are no leader");
                let waiting = nets[holder].held_records().get(&task).expect("held");

                // The rival takes the task, then the worker is handed the
                // record as it was before: a view that lags the leader's.
                claimed(&nets[rival], &ids[leader], &task).await;
                let mut lagging = waiting;
                lagging.placement = vec![ids[worker].clone().into()];
                client.write_records(vec![PlacedWrite::new(lagging, 1)]);
                wait_until_held(nets[worker].clone(), vec![task]).await;

                nets[worker].discover(ids[leader].clone(), 1, now(), false).await
            })
            .await;

        assert_eq!(found.stale, 1, "its record showed the task waiting; the leader said otherwise");
        assert!(found.claims.is_empty(), "{found:?}");
        assert_eq!(found.stopped, None);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_knows_no_peers_asks_the_leader_for_the_oldest_tasks() {
    within_deadline(async {
        let (mut shard, _client) = ThreeVoters::start().await;
        let leader = shard.drive_until_a_leader().await;
        let (nets, ids) = (shard.nets.clone(), ids(&shard));
        let submitter = shard.others(leader)[0];

        // A net that stores no records has no peers to steal from.
        let bare = Arc::new(Net::new());
        let at = bare.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
        nets[leader].dial(at);
        wait_until_registered(&bare, &ids[leader]).await;
        shard.join_as_pending(&bare, leader).await;

        let (tasks, found) = shard
            .drive_until(async {
                let first = submitted_through(&nets[submitter], &ids[leader], plain_with(b"first")).await;
                let second = submitted_through(&nets[submitter], &ids[leader], plain_with(b"second")).await;
                let found = bare.discover(ids[leader].clone(), 5, now(), false).await;
                (vec![first, second], found)
            })
            .await;

        assert!(found.claims.iter().all(|(stage, _)| *stage == Stage::Oldest), "{found:?}");
        assert_eq!(sorted(tasks_of(&found)), sorted(tasks));
    })
    .await
}
