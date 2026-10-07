//! Property tests for the configuration tally:
//! two sets of workers that each reach a quorum in the same configuration
//! share a voter, so two elections, or two commits, counted against one
//! configuration can never both succeed with disjoint supporters.
//!
//! Generations are random triples from a small space, and most admission
//! and prior admission generations land exactly on a bound: the old side's
//! base or generation, the batch generation or the current one. Every
//! voter count is the number of workers the configuration admits plus a random
//! slack, never fewer: a worker that still holds a count from before a removal
//! holds a larger one.
//!
//! The oracle below classifies workers straight from the generation bounds,
//! not through the module, so a tally that counted a non-voter would break
//! the shared-voter property.
//!
//! A third property runs a leader's roster through random histories from
//! genesis (joiners, promises of admission and their confirmations, admission
//! batches, commits, removals, and elections won under the roster's
//! configuration) and checks that each change keeps
//! adjacent configurations sharing a majority: no quorum of the
//! configuration after a change is disjoint from a quorum of the one before
//! it, counting only the workers still
//! present. A joiner holds the admission promised it as soon as it confirms
//! it, before any batch exists, and answers elections with it: it counts
//! wherever that generation is a configuration's, so the roster must
//! announce nothing else there. A removed worker never votes again: it has stopped, and the
//! term guard on its SELF_REMOVE covers the votes it cast before (the TLA+
//! model checks that part). Several removed together could otherwise make
//! up a quorum of the configuration before, as a single one never can.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::configuration::{
    Admission, Configuration, Generation, Joint, Roster, Single, Tally,
};
use kabudachi_core::protocol::ids::WorkerId;
use proptest::prelude::*;

#[derive(Debug, Clone)]
struct Worker {
    /// `None` for a pending member.
    admission: Option<Generation>,
    /// The admission it held before an election re-admitted it, if any.
    prior: Option<Generation>,
    in_first_set: bool,
    in_second_set: bool,
}

impl Worker {
    fn admission(&self) -> Admission {
        Admission {
            current: self.admission,
            prior: self.prior,
        }
    }
}

fn generation_strategy() -> impl Strategy<Value = Generation> {
    (0u64..3, 0u64..3, 0u64..4)
        .prop_map(|(recovery_epoch, term, counter)| Generation::new(recovery_epoch, term, counter))
}

/// The generations a joint configuration is built from: the old side's base
/// and generation, then the batch and current generations, in ascending
/// order with the old generation strictly before the batch.
#[derive(Debug, Clone, Copy)]
struct Bounds {
    old_base: Generation,
    old_generation: Generation,
    batch: Generation,
    current: Generation,
}

fn bounds_strategy() -> impl Strategy<Value = Bounds> {
    [
        generation_strategy(),
        generation_strategy(),
        generation_strategy(),
        generation_strategy(),
    ]
    .prop_map(|mut generations| {
        generations.sort();
        generations
    })
    .prop_filter(
        "the old generation precedes the batch",
        |[_, old, batch, _]| old < batch,
    )
    .prop_map(|[old_base, old_generation, batch, current]| Bounds {
        old_base,
        old_generation,
        batch,
        current,
    })
}

/// Bounds, whether the joint configuration is an election's founding (its
/// base at the batch generation) or an admission batch (its base at the old
/// base), and workers whose admission and prior admission generations are
/// mostly exactly those bounds, where an off-by-one in a side boundary would
/// show.
fn scenario_strategy() -> impl Strategy<Value = (Bounds, bool, Vec<Worker>)> {
    (bounds_strategy(), any::<bool>()).prop_flat_map(|(bounds, founding)| {
        let admission = prop_oneof![
            Just(None),
            Just(Some(bounds.old_base)),
            Just(Some(bounds.old_generation)),
            Just(Some(bounds.batch)),
            Just(Some(bounds.current)),
            generation_strategy().prop_map(Some),
        ];
        let prior = prop_oneof![
            Just(None),
            Just(Some(bounds.old_base)),
            Just(Some(bounds.old_generation)),
            generation_strategy().prop_map(Some),
        ];
        let worker = (
            admission,
            prior,
            prop::bool::weighted(0.6),
            prop::bool::weighted(0.6),
        )
            .prop_map(|(admission, prior, in_first_set, in_second_set)| Worker {
                admission,
                prior,
                in_first_set,
                in_second_set,
            });
        (
            Just(bounds),
            Just(founding),
            prop::collection::vec(worker, 1..12),
        )
    })
}

fn within(generation: Option<Generation>, from: Generation, through: Generation) -> bool {
    generation.is_some_and(|generation| from <= generation && generation <= through)
}

/// Feeds `tally` the workers `in_set` selects; each is named by its index.
fn fed(mut tally: Tally, workers: &[Worker], in_set: impl Fn(&Worker) -> bool) -> Tally {
    for (index, worker) in workers.iter().enumerate() {
        if in_set(worker) {
            tally.record(WorkerId::new(format!("worker-{index}")), worker.admission());
        }
    }
    tally
}

proptest! {
    #![proptest_config(crate::proptest::config(256))]

    /// A prior admission generation plays no part in a single configuration.
    #[test]
    fn two_quorums_of_one_single_configuration_share_a_voter(
        (bounds, _, workers) in scenario_strategy(),
        slack in 0usize..3,
    ) {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        let (base, current) = (bounds.old_base, bounds.current);
        let is_voter = |worker: &Worker| within(worker.admission, base, current);
        // A real configuration always has at least one voter (even genesis
        // does); `Configuration::single` now debug_asserts that.
        let voter_count =
            (workers.iter().filter(|worker| is_voter(worker)).count() + slack).max(1);
        let configuration = Configuration::single(Single {
            generation: current,
            base,
            voter_count,
        }).expect("valid");

        let first = fed(Tally::against(&configuration), &workers, |worker| worker.in_first_set);
        let second = fed(Tally::against(&configuration), &workers, |worker| worker.in_second_set);

        if first.has_quorum() && second.has_quorum() {
            let share_a_voter = workers
                .iter()
                .any(|worker| worker.in_first_set && worker.in_second_set && is_voter(worker));
            prop_assert!(share_a_voter);
        }
    }

    /// Two quorums of one joint configuration share a voter of the old
    /// side, a worker admitted, or once admitted, within the configuration
    /// it moves from, and a voter of the new side.
    #[test]
    fn two_quorums_of_one_joint_configuration_share_a_voter_of_each_side(
        (bounds, founding, workers) in scenario_strategy(),
        old_slack in 0usize..3,
        new_slack in 0usize..3,
    ) {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        let base = if founding { bounds.batch } else { bounds.old_base };
        let is_old_side_voter = |worker: &Worker| {
            within(worker.admission, bounds.old_base, bounds.old_generation)
                || within(worker.prior, bounds.old_base, bounds.old_generation)
        };
        let is_new_side_voter = |worker: &Worker| within(worker.admission, base, bounds.current);
        // Both sides of a real joint configuration hold at least one voter;
        // `Configuration::joint` debug_asserts that.
        let old_voter_count =
            (workers.iter().filter(|worker| is_old_side_voter(worker)).count() + old_slack).max(1);
        let new_voter_count =
            (workers.iter().filter(|worker| is_new_side_voter(worker)).count() + new_slack).max(1);
        let configuration = Configuration::joint(Joint {
            generation: bounds.current,
            base,
            batch_generation: bounds.batch,
            old_base: bounds.old_base,
            old_generation: bounds.old_generation,
            old_voter_count,
            new_voter_count,
        }).expect("valid");

        let first = fed(Tally::against(&configuration), &workers, |worker| worker.in_first_set);
        let second = fed(Tally::against(&configuration), &workers, |worker| worker.in_second_set);

        if first.has_quorum() && second.has_quorum() {
            let shared = |on_side: &dyn Fn(&Worker) -> bool| {
                workers
                    .iter()
                    .any(|worker| worker.in_first_set && worker.in_second_set && on_side(worker))
            };
            prop_assert!(shared(&is_old_side_voter), "an old-side voter");
            prop_assert!(shared(&is_new_side_voter), "a new-side voter");
        }
    }
}

/// The workers a roster history draws from; worker 0 creates the shard and
/// leads it.
const POOL: usize = 5;

#[derive(Debug, Clone)]
enum RosterOp {
    /// A worker heartbeats the leader and is held pending.
    Join(usize),
    /// The leader promises the workers chosen by bit, among those waiting,
    /// admission in a coming batch.
    Promise(u8),
    /// The workers chosen by bit say they hold the admission promised them.
    Hold(u8),
    /// A batch of the workers promised, once all hold their promise, and the
    /// leader too if it asks.
    Batch(bool),
    /// The workers chosen by bit say they hold the leader's configuration,
    /// then the leader tries to commit it.
    Confirm(u8),
    /// The workers chosen by bit drain, and the leader takes them out
    /// together, in one generation. The leader among
    /// them (worker 0) ends the history: it announces the configuration
    /// without itself, then stops.
    Remove(u8),
    /// An election for the next term, answered by the workers chosen by bit
    /// and the leader, is won if they are a quorum of the configuration.
    Elect(u8),
}

fn roster_op_strategy() -> impl Strategy<Value = RosterOp> {
    prop_oneof![
        3 => (0..POOL).prop_map(RosterOp::Join),
        2 => any::<u8>().prop_map(RosterOp::Promise),
        3 => any::<u8>().prop_map(RosterOp::Hold),
        3 => any::<bool>().prop_map(RosterOp::Batch),
        4 => any::<u8>().prop_map(RosterOp::Confirm),
        2 => any::<u8>().prop_map(RosterOp::Remove),
        1 => any::<u8>().prop_map(RosterOp::Elect),
    ]
}

fn pool_worker(index: usize) -> WorkerId {
    WorkerId::new(format!("w{index}"))
}

fn chosen(bits: u8) -> BTreeSet<WorkerId> {
    (0..POOL)
        .filter(|index| bits & (1 << index) != 0)
        .map(pool_worker)
        .collect()
}

/// Every worker's counted admission in `roster`: what the roster admitted it
/// at, or the admission it holds from a promise (`held`) when the roster has
/// admitted it at no later generation (a worker takes the roster's admission
/// as soon as it is later than its promise).
fn admissions(
    roster: &Roster,
    held: &BTreeMap<WorkerId, Generation>,
) -> BTreeMap<WorkerId, Admission> {
    (0..POOL)
        .map(pool_worker)
        .map(|worker| {
            let mut admission = roster.counted_admission_of(&worker);
            if let Some(promised) = held.get(&worker)
                && admission.current.is_none_or(|current| current < *promised)
            {
                admission.current = Some(*promised);
            }
            (worker, admission)
        })
        .collect()
}

fn is_quorum(
    configuration: &Configuration,
    admissions: &BTreeMap<WorkerId, Admission>,
    workers: &BTreeSet<WorkerId>,
) -> bool {
    let mut tally = Tally::against(configuration);
    for worker in workers {
        tally.record(worker.clone(), admissions[worker]);
    }
    tally.has_quorum()
}

/// Whether some quorum of `after` is disjoint from some quorum of
/// `before`. Quorums are upward closed, so it is enough to check, for each
/// quorum of `before`, whether its complement is a quorum of `after`.
fn quorums_miss(
    before: (&Configuration, &BTreeMap<WorkerId, Admission>),
    after: (&Configuration, &BTreeMap<WorkerId, Admission>),
    present: &BTreeSet<WorkerId>,
) -> Option<BTreeSet<WorkerId>> {
    (0u8..(1 << POOL))
        .map(chosen)
        .filter(|set| set.is_subset(present))
        .find(|set| {
            let complement: BTreeSet<WorkerId> = present.difference(set).cloned().collect();
            is_quorum(before.0, before.1, set) && is_quorum(after.0, after.1, &complement)
        })
}

proptest! {
    #[test]
    fn adjacent_configurations_share_a_majority(
        ops in proptest::collection::vec(roster_op_strategy(), 1..30),
    ) {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        let leader = pool_worker(0);
        let mut term = 1u64;
        let mut roster = Roster::genesis(leader.clone(), 0);
        let mut present: BTreeSet<WorkerId> = (0..POOL).map(pool_worker).collect();
        // The promises workers hold, which no election or change forgets.
        let mut held: BTreeMap<WorkerId, Generation> = BTreeMap::new();
        // Every promise ever held, across elections.
        let mut ever_held: BTreeSet<(WorkerId, Generation)> = BTreeSet::new();
        for op in ops {
            let before_configuration = roster.configuration().clone();
            let before = admissions(&roster, &held);
            match &op {
                RosterOp::Join(index) => roster.add_pending(pool_worker(*index)),
                RosterOp::Promise(bits) => {
                    roster.promise_admission(&chosen(*bits), term);
                }
                RosterOp::Hold(bits) => {
                    for worker in chosen(*bits) {
                        if let Some(promised) = roster.promised_admission_of(&worker) {
                            held.insert(worker.clone(), promised);
                            ever_held.insert((worker.clone(), promised));
                            roster.record_held_admission(&worker, promised);
                        }
                    }
                }
                RosterOp::Batch(with_leader) => {
                    roster.begin_batch(with_leader.then_some(&leader), term);
                }
                RosterOp::Confirm(bits) => {
                    let held = roster.configuration().generation();
                    for worker in chosen(*bits) {
                        roster.record_held_generation(&worker, held);
                    }
                    roster.commit_if_confirmed(&leader, term);
                }
                RosterOp::Remove(bits) => {
                    roster.remove_all(&chosen(*bits), term);
                    present.retain(|worker| !chosen(*bits).contains(worker));
                }
                RosterOp::Elect(bits) => {
                    let mut respondents = chosen(*bits);
                    respondents.insert(leader.clone());
                    if is_quorum(&before_configuration, &before, &respondents) {
                        term += 1;
                        let answered = respondents
                            .iter()
                            .map(|worker| (worker.clone(), before[worker]))
                            .collect();
                        roster = Roster::after_election(0, term, &before_configuration, &answered);
                    }
                }
            }
            let after = admissions(&roster, &held);
            let missed = quorums_miss(
                (&before_configuration, &before),
                (roster.configuration(), &after),
                &present,
            );
            let leader_left =
                matches!(op, RosterOp::Remove(bits) if chosen(bits).contains(&leader));
            prop_assert!(
                missed.is_none(),
                "{:?} took {:?} to {:?}, and {:?} is a quorum of the first whose complement \
                 among the workers present is a quorum of the second",
                op,
                before_configuration,
                roster.configuration(),
                missed
            );
            // A quorum counts a worker at the promise it holds, so the
            // configuration that exists at that generation must admit the
            // worker there: a batch that left it out, or took it at
            // another generation, would be counted as it is not.
            // A roster whose leader was removed is abandoned, not honoured.
            for (worker, promised) in &ever_held {
                if !leader_left && roster.configuration().generation() == *promised {
                    prop_assert_eq!(
                        roster.admission_of(worker),
                        Some(*promised),
                        "{:?} holds a promise of {:?}, which {:?} does not honour",
                        worker,
                        promised,
                        roster.configuration()
                    );
                }
            }
            if leader_left {
                break;
            }
        }
    }
}
