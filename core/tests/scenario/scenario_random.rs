//! Seeded random scenarios on the `Cluster` harness. Each seed draws a
//! cluster of 3 to 7 voters and up to 2 pending members, every node using an
//! authority, lets it elect a leader, then runs a phase of random faults and
//! a quiet phase: every fault is lifted and the cluster runs on until it is
//! quiescent.
//!
//! The faults are partitions and stalls, either brief (1 to a third of the
//! suspicion timeout in ticks) or long (the suspicion timeout to twice it),
//! duplicated, reordered, dropped and delayed
//! messages, drain requests to voters, and authority outages, for every node
//! at once or one node alone. Faults stack. The quiet phase heals the
//! partition, ends the drop, delay, duplication and reordering, and makes the
//! authority reachable again; a stall ends by itself.
//!
//! Throughout, no two nodes hold a valid grant at once and no term has two
//! leaders. Once the quiet phase is over, the cluster has exactly one leader,
//! which alone holds a valid grant, and every node that was not asked to
//! drain follows it. A node asked to drain that finishes draining may end in
//! any state, since it leaves the cluster, and one that does not must follow
//! like the rest; the drains are capped below half the voters, so the
//! rest can still elect.
//!
//! A known gap remains: when every member of a shard is `Bootstrapping`
//! at a leaderless epoch, which nodes that rejoined the authority's epoch
//! after its leader drained or stopped can all be, no node can recover it.
//! The default seeds and seeds 0 to 3999 avoid it; a wider sweep through
//! `KABUDACHI_SIM_SEEDS` can still find it. So this test can fail on a
//! liveness gap in the election as well as on a broken invariant. A failure
//! names its seed.
//!
//! A run is reproduced by its seed alone, which a failure prints. By default
//! the fixed seeds in `SEEDS` run. `KABUDACHI_SIM_SEEDS` replaces them: `N`
//! runs seeds 0 to N-1, `a,b,c` runs those seeds and `a..b` runs seeds a to
//! b-1, to search wider before closing a change to the election. A trailing
//! comma is allowed, so `N,` runs the single seed N, even `u64::MAX`.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::support::builders::past_any_suspicion;
use crate::support::harness::{Answer, Cluster, Ticket};
use kabudachi_core::election::{Output, StopReason};
use kabudachi_core::task_record::{VersionOrder, identify};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::protocol::ids::TaskDefinitionId;
use kabudachi_core::scheduler::Submission;
use kabudachi_core::time::Duration;

/// The seeds a run checks unless `KABUDACHI_SIM_SEEDS` names others.
const SEEDS: [u64; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

const SEEDS_VARIABLE: &str = "KABUDACHI_SIM_SEEDS";

/// Every cluster's suspicion timeout, in ticks, and the step the quiet phase
/// runs in.
const SUSPECT_TICKS: u64 = 10;

/// How long past a suspicion timeout a silent worker is reported lost, in
/// ticks: short, so that a loss and its repair fit a run.
const RECONNECT_TICKS: u64 = 3 * SUSPECT_TICKS;

/// The authority's TTL for registrations and fences, in ticks. The tests'
/// default of 30 s is 30000 ticks, which would dwarf this simulation's
/// suspicion timeout and fault phases and leave a new leader waiting out the
/// old holder's fence for the whole run; six suspicion timeouts keeps fences
/// slow enough to matter yet short enough to expire inside the quiet phase.
const AUTHORITY_TTL_TICKS: u64 = 6 * SUSPECT_TICKS;
const TICK_SIZE: Duration = Duration::from_ticks(5);

/// The longest a brief cut or a stall lasts, in ticks.
const LONGEST_CUT: u64 = SUSPECT_TICKS / 3;

/// The bounds of a long cut or stall, in ticks: from the suspicion timeout to
/// twice it.
const LONG_CUT_LOW: u64 = SUSPECT_TICKS;
const LONG_CUT_HIGH: u64 = 2 * SUSPECT_TICKS;

/// How long the cluster runs after its faults are lifted before it is
/// checked for quiescence, and the most steps that then takes.
///
/// The quiet phase must outlast the authority's TTL plus one renewal retry,
/// since a new leader may have to wait out the previous holder's fence and,
/// after an unavailable reply, retries only after a third of the TTL.
const QUIET_TICKS: u64 = 10 * SUSPECT_TICKS + 2 * AUTHORITY_TTL_TICKS;
const QUIESCENCE_STEPS: usize = 400;

/// The seeds to run, from `KABUDACHI_SIM_SEEDS` if it is set.
fn seeds() -> Vec<u64> {
    let Ok(spec) = std::env::var(SEEDS_VARIABLE) else {
        return SEEDS.to_vec();
    };
    let number = |text: &str| -> u64 {
        text.trim()
            .parse()
            .unwrap_or_else(|_| panic!("{SEEDS_VARIABLE}={spec:?}: {text:?} is not a number"))
    };
    let seeds: Vec<u64> = if let Some((from, to)) = spec.split_once("..") {
        (number(from)..number(to)).collect()
    } else if spec.contains(',') {
        let list = spec.trim().strip_suffix(',').unwrap_or(&spec);
        list.split(',').map(number).collect()
    } else {
        (0..number(&spec)).collect()
    };
    assert!(
        !seeds.is_empty(),
        "{SEEDS_VARIABLE}={spec:?} names no seeds, so nothing would run"
    );
    seeds
}

/// One fault, or a step of time, in a random fault phase.
#[derive(Debug)]
enum Event {
    Advance(Duration),
    /// Cuts the nodes with `true` off from those with `false` for `hold`,
    /// then heals the cut.
    Partition(Vec<bool>, Duration),
    Stall(usize, Duration),
    DuplicateRate(f64),
    DropRate(f64),
    Delay(Duration),
    /// Asks the voter at this index among the voters to drain.
    Drain(usize),
    /// Makes the authority unreachable, for every node or for the one at the
    /// index, or reachable again for every node.
    AuthorityOutage,
    AuthorityCut(usize),
    AuthorityRestored,
    /// Submits a task to whichever node leads, if one does.
    Submit,
    /// Takes the node at the index down for good, as a host that died: it is
    /// stepped no more, and no other node, record store or authority hears
    /// from it again. At most one per run.
    Down(usize),
}

fn draw_event(rng: &mut ChaCha8Rng, nodes: usize, voters: usize) -> Event {
    let up_to = |rng: &mut ChaCha8Rng, most: u64| Duration::from_ticks(rng.random_range(1..=most));
    let long = |rng: &mut ChaCha8Rng| {
        Duration::from_ticks(rng.random_range(LONG_CUT_LOW..=LONG_CUT_HIGH))
    };
    let sides = |rng: &mut ChaCha8Rng| (0..nodes).map(|_| rng.random_bool(0.5)).collect();
    match rng.random_range(0..64) {
        0..=23 => Event::Advance(up_to(rng, 15)),
        24..=28 => Event::Partition(sides(rng), up_to(rng, LONGEST_CUT)),
        29..=31 => Event::Partition(sides(rng), long(rng)),
        32..=35 => Event::Stall(rng.random_range(0..nodes), up_to(rng, LONGEST_CUT)),
        36..=37 => Event::Stall(rng.random_range(0..nodes), long(rng)),
        38..=39 => Event::DuplicateRate([0.0, 0.1][rng.random_range(0..2)]),
        40..=42 => Event::DropRate([0.0, 0.05, 0.2][rng.random_range(0..3)]),
        43..=45 => Event::Delay(Duration::from_ticks(rng.random_range(0..=3))),
        46..=47 => Event::Drain(rng.random_range(0..voters)),
        48..=49 => Event::AuthorityOutage,
        50..=52 => Event::AuthorityCut(rng.random_range(0..nodes)),
        53..=55 => Event::AuthorityRestored,
        56..=61 => Event::Submit,
        _ => Event::Down(rng.random_range(0..nodes)),
    }
}

fn apply(
    cluster: &mut Cluster,
    ids: &[WorkerId],
    voters: &[WorkerId],
    submissions: &mut Vec<Ticket>,
    event: &Event,
) {
    let set_reachable = |cluster: &Cluster, id: &WorkerId, reachable| {
        cluster.node_authority(id).set_reachable(reachable);
    };
    match event {
        Event::Advance(dt) => cluster.advance(*dt),
        Event::Partition(sides, hold) => {
            let (first, second): (Vec<_>, Vec<_>) =
                ids.iter().zip(sides).partition(|(_, first)| **first);
            cluster.partition(
                first.into_iter().map(|(id, _)| id.clone()).collect(),
                second.into_iter().map(|(id, _)| id.clone()).collect(),
            );
            cluster.advance(*hold);
            cluster.heal();
        }
        Event::Stall(index, dt) => cluster.stall(&ids[*index], *dt),
        Event::DuplicateRate(rate) => cluster.network().set_duplicate_rate(*rate),
        Event::DropRate(rate) => cluster.network().set_drop_rate(*rate),
        Event::Delay(delay) => cluster.network().set_delay(*delay),
        Event::Drain(index) => cluster.drain(&voters[*index]),
        Event::AuthorityOutage => ids.iter().for_each(|id| set_reachable(cluster, id, false)),
        Event::AuthorityCut(index) => set_reachable(cluster, &ids[*index], false),
        Event::AuthorityRestored => ids
            .iter()
            .filter(|id| !cluster.is_down(id))
            .for_each(|id| set_reachable(cluster, id, true)),
        Event::Down(index) => cluster.set_down(&ids[*index]),
        Event::Submit => {
            if let Some(leader) = cluster.leader() {
                let task = Submission::new(TaskDefinitionId::new("random.task"), 0, b"in".to_vec(), "default");
                submissions.push(cluster.submit(&leader, task));
            }
        }
    }
}

/// Panics if the steps taken since the last call, or the grants so far, break
/// an invariant that holds at every moment: at most one valid grant, and at
/// most one leader of any (recovery epoch, term).
fn check_safety(cluster: &mut Cluster, leaders: &mut BTreeMap<(u64, u64), BTreeSet<WorkerId>>) {
    for record in cluster.take_steps() {
        if record
            .outputs
            .contains(&Output::StateChanged(WorkerState::Leader))
        {
            let holders = leaders
                .entry((record.recovery_epoch, record.term))
                .or_default();
            holders.insert(record.node.clone());
            assert!(
                holders.len() == 1,
                "(epoch, term) {:?} has more than one leader: {holders:?}",
                (record.recovery_epoch, record.term)
            );
        }
    }
    assert_eq!(
        cluster.first_grant_overlap(),
        None,
        "two nodes held a valid grant at once"
    );
}

/// Runs `seed`, and returns whether a different worker took over leadership
/// at some point: a worker that wins several terms in a row does not count.
fn run_seed(seed: u64) -> bool {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let voter_count = rng.random_range(3..=7);
    let pending = rng.random_range(0..=2);
    let mut cluster = Cluster::bootstrap_with_authority_ttl_and_reconnect_timeout(
        voter_count,
        pending,
        Duration::from_ticks(SUSPECT_TICKS),
        Duration::from_ticks(AUTHORITY_TTL_TICKS),
        Duration::from_ticks(RECONNECT_TICKS),
    );
    cluster.network().seed(seed);
    cluster.network().set_reorder(rng.random_bool(0.5));
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();
    let voters: Vec<WorkerId> = ids
        .iter()
        .filter(|id| !cluster.pending_members().contains(*id))
        .cloned()
        .collect();
    let mut drained = BTreeSet::new();
    let mut submissions = Vec::new();
    let mut down: Option<WorkerId> = None;

    // Recording starts before the first election so the first leader counts
    // toward the one-leader-per-(epoch, term) check too.
    cluster.record_steps();
    let mut leaders = BTreeMap::new();

    // A freshly built cluster elects its leader before any fault.
    cluster.advance(past_any_suspicion(SUSPECT_TICKS));
    cluster.run_until_quiescent(TICK_SIZE, QUIESCENCE_STEPS);
    assert!(
        cluster.leader().is_some(),
        "a fresh cluster must elect a leader"
    );
    check_safety(&mut cluster, &mut leaders);

    for _ in 0..rng.random_range(40..=120) {
        let event = draw_event(&mut rng, ids.len(), voters.len());
        if let Event::Drain(index) = event {
            // Below half the voters, so the rest can still elect.
            let gone = drained.len() + usize::from(down.is_some());
            if gone >= (voters.len() - 1) / 2 || !drained.insert(voters[index].clone()) {
                continue;
            }
        }
        if let Event::Down(index) = &event {
            // One loss, counted with the drains against the voters that must
            // stay to elect.
            if down.is_some() || drained.len() + 1 > (voters.len() - 1) / 2 {
                continue;
            }
            down = Some(ids[*index].clone());
        }
        apply(&mut cluster, &ids, &voters, &mut submissions, &event);
        check_safety(&mut cluster, &mut leaders);
    }

    cluster.network().set_duplicate_rate(0.0);
    cluster.network().set_reorder(false);
    cluster.network().set_drop_rate(0.0);
    cluster.network().set_delay(Duration::from_ticks(0));
    for id in ids.iter().filter(|id| !cluster.is_down(id)) {
        cluster.node_authority(id).set_reachable(true);
    }
    cluster.advance(Duration::from_ticks(QUIET_TICKS));
    let steps = cluster.run_until_quiescent(TICK_SIZE, QUIESCENCE_STEPS);
    assert!(steps < QUIESCENCE_STEPS, "the cluster never went quiet after the faults were lifted");
    check_safety(&mut cluster, &mut leaders);

    let states = cluster.states();
    let leader = cluster
        .leader()
        .unwrap_or_else(|| panic!("no leader after the faults were lifted: {states:?}"));
    cluster.assert_at_most_one_in_leader_state();
    assert_eq!(
        cluster.valid_grant_holders(),
        [leader.clone()].into_iter().collect(),
        "the leader alone must hold a valid grant: {states:?}"
    );
    for id in ids.iter().filter(|id| **id != leader) {
        if drained.contains(id) && cluster.node(id).stop_reason() == Some(StopReason::Drained) {
            continue;
        }
        if cluster.is_down(id) {
            continue;
        }
        assert_eq!(states[id], WorkerState::Active, "{id:?} must follow");
        assert_eq!(
            cluster.node(id).known_leader().map(|(id, _)| id),
            Some(leader.clone()),
            "{id:?} must follow {leader:?}"
        );
    }
    // Every write has been acknowledged and no refused one waits to be
    // published again before the records are looked at.
    for _ in 0..QUIESCENCE_STEPS {
        if cluster.records().next_due().is_none() && !cluster.has_repair_due() {
            break;
        }
        cluster.advance(TICK_SIZE);
    }
    assert!(
        cluster.records().next_due().is_none() && !cluster.has_repair_due(),
        "writes were still outstanding after the cluster went quiet"
    );
    check_records_are_held(&mut cluster, &leader, &submissions);
    leaders.values().flatten().collect::<BTreeSet<_>>().len() > 1
}

/// Panics unless every task submitted is held by the leader or uncertain to it,
/// and every one the leader holds has its newest record
/// stored by more than half of its placement that is up.
fn check_records_are_held(cluster: &mut Cluster, leader: &WorkerId, submissions: &[Ticket]) {
    let submitted: Vec<_> = submissions
        .iter()
        .filter_map(|ticket| match cluster.answer(*ticket) {
            Some(Answer::Submitted(task)) => Some(task.clone()),
            _ => None,
        })
        .collect();
    // Nothing in a run claims, finishes or cancels a task, so none may be lost:
    // the leader holds it, or it is still uncertain to it, because holders it
    // cannot hear may hide a newer revision.
    let mut held = Vec::new();
    for task in submitted {
        if cluster.scheduler_mut(leader).holds(&task) {
            held.push(task);
        } else {
            assert!(
                cluster.scheduler_mut(leader).is_uncertain(&task),
                "{task:?} was submitted and the leader neither holds it nor is waiting on its holders"
            );
        }
    }
    for task in held {
        let newest = cluster
            .node_ids()
            .iter()
            .filter_map(|holder| cluster.records().held_by(holder, &task))
            .reduce(|newest, record| {
                let (_, held) = identify(&newest).expect("a held record is identified");
                let (_, other) = identify(&record).expect("a held record is identified");
                if held.order(&other) == VersionOrder::Newer { record } else { newest }
            });
        let newest = newest.unwrap_or_else(|| panic!("{task:?} is held by no node"));
        let (_, version) = identify(&newest).expect("a held record is identified");
        let placement: Vec<WorkerId> = newest.placement.iter().cloned().map(WorkerId::from).collect();
        let holding = placement
            .iter()
            .filter(|holder| !cluster.is_down(holder))
            .filter(|holder| {
                cluster.records().held_by(holder, &task).is_some_and(|record| {
                    identify(&record).is_ok_and(|(_, held)| held == version)
                })
            })
            .count();
        assert!(
            holding > placement.len() / 2,
            "{task:?}: its newest record, placed on {placement:?}, is stored by {holding} of them"
        );
    }
}

#[test]
fn seeded_fault_phases_end_with_one_leader_that_everyone_follows() {
    let mut seeds_that_changed_leader = 0;
    let seeds = seeds();
    for &seed in &seeds {
        match catch_unwind(AssertUnwindSafe(|| run_seed(seed))) {
            Ok(changed_leader) => seeds_that_changed_leader += usize::from(changed_leader),
            Err(failure) => {
                let reason = failure
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| failure.downcast_ref::<&str>().copied())
                    .unwrap_or("a panic with no message");
                panic!(
                    "seed {seed} failed (rerun it with {SEEDS_VARIABLE}={seed},): {reason}"
                );
            }
        }
    }
    // Only the fixed seeds are known to exercise a re-election.
    if std::env::var(SEEDS_VARIABLE).is_err() {
        assert!(
            seeds_that_changed_leader > 0,
            "no fixed seed's faults ever made a different worker take over as leader"
        );
    }
}
