//! Property tests for the coalescing and composite invariants at one node:
//! random sequences of submissions (tasks, flows and groups), claims,
//! completions (some returning a step), failures, cancellations of tasks and
//! composites, and worker losses, checking after every step that
//!
//! - at most one generation of a key is claimed, running or continuing;
//! - a generation that was claimed is never superseded;
//! - a superseded payload is folded exactly once, into the generation that
//!   absorbed it, oldest first;
//! - a lost generation is replayed only if it is the newest for its key;
//! - a continuation exists only for a certified run, and keeps its key held
//!   until its composite is over;
//! - a flow's stage exists only once every stage before it succeeded, and no
//!   stage exists after a failed one;
//! - every composite finishes at most once, and a finished one changes no
//!   more;
//!
//! and, once everything is drained, that memory accounting returns to zero.

use std::collections::{BTreeMap, BTreeSet};

use crate::support::clock::FakeClock;
use crate::support::composites::{flow_of, group_of, task_stage};
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::scheduler::{lose_silent, state_of};
use crate::support::spy::Spy;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::composite::{CompositeKind, CompositeState, CompositeStatus, OnError, Step};
use kabudachi_core::scheduler::{Claim, Completion, Event, Scheduler, Submission};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

const KEYS: u8 = 2;

#[derive(Debug, Clone)]
enum Op {
    /// A generation of coalescing key `key`, or a plain task if `None`.
    Submit {
        key: Option<u8>,
        size: u8,
        retries: u8,
    },
    Claim {
        limit: u8,
    },
    Start {
        pick: u8,
    },
    Complete {
        pick: u8,
        continues: bool,
    },
    /// A flow of two bound stages, or a group of two bound members under
    /// either policy, submitted with a client.
    SubmitComposite {
        group: bool,
        collect_all: bool,
    },
    Fail {
        pick: u8,
    },
    Cancel {
        pick: u8,
    },
    CancelComposite {
        pick: u8,
    },
    WorkerFallsSilent,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (proptest::option::weighted(0.8, 0..KEYS), 1u8..8, 0u8..3)
            .prop_map(|(key, size, retries)| Op::Submit { key, size, retries }),
        3 => (1u8..4).prop_map(|limit| Op::Claim { limit }),
        2 => any::<u8>().prop_map(|pick| Op::Start { pick }),
        2 => (any::<u8>(), any::<bool>())
            .prop_map(|(pick, continues)| Op::Complete { pick, continues }),
        2 => (any::<bool>(), any::<bool>())
            .prop_map(|(group, collect_all)| Op::SubmitComposite { group, collect_all }),
        1 => any::<u8>().prop_map(|pick| Op::Fail { pick }),
        1 => any::<u8>().prop_map(|pick| Op::Cancel { pick }),
        1 => any::<u8>().prop_map(|pick| Op::CancelComposite { pick }),
        1 => Just(Op::WorkerFallsSilent),
    ]
}

/// What the test remembers about a task it submitted.
struct Submitted {
    key: Option<u8>,
    /// The number written into its payload, unique across the test and
    /// increasing with submission order.
    number: u32,
}

struct Model {
    scheduler: Scheduler<FakeClock, SequentialIds, Spy>,
    clock: FakeClock,
    spy: Spy,
    worker: WorkerId,
    next_number: u32,
    tasks: BTreeMap<TaskId, Submitted>,
    /// Claims handed out and not yet finished with, in the order handed out.
    claims: Vec<Claim>,
    /// Every composite submitted or committed, in order.
    composites: Vec<CompositeId>,
    /// How many times each composite was announced finished.
    finished: BTreeMap<CompositeId, usize>,
    /// What each finished composite looked like when it was announced.
    frozen: BTreeMap<CompositeId, CompositeStatus>,
    /// Tasks that were claimed at least once.
    ever_claimed: BTreeSet<TaskId>,
    /// The payload numbers each task folded when first claimed.
    folded_by: BTreeMap<TaskId, Vec<u32>>,
}

fn number_of(payload: &[u8]) -> u32 {
    u32::from_be_bytes(payload[..4].try_into().unwrap())
}

impl Model {
    fn new() -> Self {
        let spy = Spy::default();
        let clock = FakeClock::new();
        let mut scheduler =
            Scheduler::with_observer(clock.clone(), SequentialIds::new(), spy.clone());
        scheduler.set_leadership_grant(Some(unbounded_grant()));
        Model {
            scheduler,
            clock,
            spy,
            worker: WorkerId::new("w1"),
            next_number: 0,
            tasks: BTreeMap::new(),
            claims: Vec::new(),
            composites: Vec::new(),
            finished: BTreeMap::new(),
            frozen: BTreeMap::new(),
            ever_claimed: BTreeSet::new(),
            folded_by: BTreeMap::new(),
        }
    }

    fn pick_claim(&self, pick: u8) -> Option<usize> {
        (!self.claims.is_empty()).then(|| pick as usize % self.claims.len())
    }

    /// The state of `task`'s current run, or `None` once it is forgotten.
    fn state_of(&self, task: &TaskId) -> Option<TaskRunState> {
        (!self.scheduler.runs_of(task).is_empty()).then(|| state_of(&self.scheduler, &self.spy, task))
    }

    fn apply(&mut self, op: &Op) {
        match op {
            Op::Submit { key, size, retries } => {
                let number = self.next_number;
                self.next_number += 1;
                let mut payload = number.to_be_bytes().to_vec();
                payload.resize(4 + *size as usize, 0);
                let mut submission = Submission::new(
                    TaskDefinitionId::new(if key.is_some() {
                        "index.refresh"
                    } else {
                        "plain"
                    }),
                    0,
                    payload,
                    "default",
                )
                .with_retries(u32::from(*retries));
                if let Some(key) = key {
                    submission = submission.with_coalescing_key(format!("k{key}"));
                }
                let task = self.scheduler.submit(submission).unwrap();
                self.tasks.insert(task, Submitted { key: *key, number });
            }
            Op::SubmitComposite { group, collect_all } => {
                let step = if *group {
                    let on_error = if *collect_all { OnError::CollectAll } else { OnError::FailFast };
                    group_of(on_error, vec![task_stage("a", Some(b"a")), task_stage("b", Some(b"b"))])
                } else {
                    flow_of(vec![task_stage("first", Some(b"a")), task_stage("second", None)])
                };
                let minted = self.scheduler.mint_composite(step, Some(self.worker.clone()));
                let id = self.scheduler.submit_composite(minted).unwrap();
                self.composites.push(id);
            }
            Op::Claim { limit } => {
                let claims = self
                    .scheduler
                    .claim_oldest(&self.worker, *limit as usize)
                    .unwrap();
                for claim in claims {
                    self.check_fold(&claim);
                    self.ever_claimed.insert(claim.task.task_id());
                    self.claims.push(claim);
                }
            }
            Op::Start { pick } => {
                if let Some(i) = self.pick_claim(*pick) {
                    let _ = self
                        .scheduler
                        .report_started(&self.worker, &self.claims[i].task_run_id, None);
                }
            }
            Op::Complete { pick, continues } => {
                if let Some(i) = self.pick_claim(*pick) {
                    let run = self.claims[i].task_run_id.clone();
                    let done = if *continues {
                        let step = flow_of(vec![task_stage("stage", Some(b"s")), task_stage("next", None)]);
                        self.scheduler.complete(&self.worker, &run, step.digest(), Completion::Continues(step))
                    } else {
                        self.scheduler
                            .complete(&self.worker, &run, Digest::blake3(b"d"), Completion::Final)
                    };
                    if done.is_ok() {
                        if *continues {
                            self.composites.push(CompositeId::continuation_of(&run));
                        }
                        self.claims.remove(i);
                    }
                }
            }
            Op::Fail { pick } => {
                if let Some(i) = self.pick_claim(*pick) {
                    let run = self.claims[i].task_run_id.clone();
                    if self.scheduler.fail(&self.worker, &run, "E").is_ok() {
                        self.claims.remove(i);
                    }
                }
            }
            Op::Cancel { pick } => {
                if !self.tasks.is_empty() {
                    let task = self
                        .tasks
                        .keys()
                        .nth(*pick as usize % self.tasks.len())
                        .unwrap()
                        .clone();
                    let _ = self.scheduler.cancel(&task);
                }
            }
            Op::CancelComposite { pick } => {
                if !self.composites.is_empty() {
                    let id = self.composites[*pick as usize % self.composites.len()].clone();
                    let _ = self.scheduler.cancel_composite(&id);
                }
            }
            Op::WorkerFallsSilent => self.worker_falls_silent(),
        }
        // Whatever the worker held of a run that is over is no longer its to
        // report on.
        self.claims.retain(|claim| {
            self.scheduler
                .task_run(&claim.task_run_id)
                .is_some_and(|run| matches!(run.current_state(), TaskRunState::Claimed | TaskRunState::Running))
        });
        self.collect_events();
    }

    /// Counts each composite finish announced since the last call, and notes
    /// how the composite stood when it was.
    fn collect_events(&mut self) {
        for event in self.scheduler.take_events() {
            if let Event::CompositeFinished(finished) = event {
                let id = finished.composite_id;
                *self.finished.entry(id.clone()).or_default() += 1;
                if let Some(status) = self.scheduler.composite_status(&id) {
                    self.frozen.entry(id).or_insert(status);
                }
            }
        }
    }

    /// The worker falls silent past its runs' reconnect timeouts: a lost
    /// coalescing generation is replayed only if no newer one waits for its
    /// key.
    fn worker_falls_silent(&mut self) {
        let waiting_keys: BTreeSet<u8> = self
            .tasks
            .iter()
            .filter(|(task, _)| self.state_of(task) == Some(TaskRunState::Queued))
            .filter_map(|(_, submitted)| submitted.key)
            .collect();
        let lost = lose_silent(&mut self.scheduler, &self.clock, &self.worker);
        for run in lost.iter().filter(|run| self.tasks.contains_key(&run.task_id)) {
            let key = self.tasks[&run.task_id].key;
            match key {
                Some(key) if waiting_keys.contains(&key) => {
                    assert_eq!(run.replayed, None, "a stale generation was replayed");
                }
                _ => assert!(
                    run.replayed.is_some(),
                    "the newest generation was not replayed"
                ),
            }
        }
        // Every claim the worker held is over now; a replay is claimed anew.
        self.claims.clear();
    }

    /// What a claim folds is what its generation absorbed,
    /// oldest first, and no payload is folded twice.
    fn check_fold(&mut self, claim: &Claim) {
        let task = claim.task.task_id();
        // A composite's member has no key, so it folds nothing.
        let Some(own) = self.tasks.get(&task).map(|submitted| submitted.number) else {
            assert!(claim.chain.is_empty(), "a composite's member folded a payload");
            return;
        };
        let chain: Vec<u32> = claim.chain.iter().map(|p| number_of(p)).collect();
        assert!(chain.windows(2).all(|w| w[0] < w[1]), "chain out of order");
        assert!(chain.iter().all(|n| *n < own), "chain has a newer payload");
        for number in &chain {
            let key = self
                .tasks
                .values()
                .find(|s| s.number == *number)
                .and_then(|s| s.key);
            assert_eq!(
                key, self.tasks[&task].key,
                "folded a payload of another key"
            );
        }
        if claim.attempt_number == 1 {
            for other in self.folded_by.values() {
                assert!(
                    other.iter().all(|n| !chain.contains(n) && *n != own),
                    "a payload was folded into two generations"
                );
            }
            self.folded_by.insert(task, chain);
        } else {
            // A replay folds what the first attempt folded.
            assert_eq!(self.folded_by.get(&task), Some(&chain));
        }
    }

    /// Whether `task` holds its key past its own run: its newest run was
    /// certified with a continuation that is not over.
    fn continues(&self, task: &TaskId) -> bool {
        self.scheduler.runs_of(task).pop().is_some_and(|run| {
            self.scheduler
                .composite_status(&CompositeId::continuation_of(&run))
                .is_some_and(|status| status.state == CompositeState::Running)
        })
    }

    fn check_invariants(&self) {
        for key in 0..KEYS {
            let mut holders = 0;
            for (task, submitted) in &self.tasks {
                if submitted.key != Some(key) {
                    continue;
                }
                let holds = matches!(
                    self.state_of(task),
                    Some(TaskRunState::Claimed | TaskRunState::Running)
                ) || self.continues(task);
                holders += usize::from(holds);
            }
            assert!(
                holders <= 1,
                "{holders} generations of key {key} hold it at once"
            );
        }
        for task in &self.ever_claimed {
            let Some(first) = self.scheduler.runs_of(task).first().cloned() else {
                continue;
            };
            assert_ne!(
                self.scheduler.task_run(&first).unwrap().current_state(),
                TaskRunState::Superseded,
                "a claimed generation was superseded"
            );
        }
        for task in self.tasks.keys() {
            // A continuation only exists for a certified run.
            for run in self.scheduler.runs_of(task) {
                let running = self
                    .scheduler
                    .composite_status(&CompositeId::continuation_of(&run))
                    .is_some_and(|status| status.state == CompositeState::Running);
                if running {
                    assert_eq!(
                        self.scheduler.task_run(&run).unwrap().current_state(),
                        TaskRunState::Succeeded,
                        "a continuation runs for a run that was not certified"
                    );
                }
            }
        }
        for (id, times) in &self.finished {
            assert!(*times <= 1, "{id:?} finished {times} times");
            if let (Some(frozen), Some(now)) = (self.frozen.get(id), self.scheduler.composite_status(id)) {
                assert_eq!(frozen.state, now.state, "{id:?} changed state after it finished");
                assert_eq!(frozen.next_stage, now.next_stage, "{id:?} moved on after it finished");
            }
        }
        for id in &self.composites {
            if self.scheduler.composite_status(id).is_none() {
                continue;
            }
            let record = self.scheduler.composite_record(id).unwrap();
            let step = Step::try_from(record.description.as_ref().unwrap()).unwrap();
            if step.kind != CompositeKind::Flow {
                continue;
            }
            for index in 0..step.stages.len() as u64 {
                let exists = !self.scheduler.runs_of(&id.member_task(index)).is_empty();
                let before_succeeded = (0..index).all(|earlier| {
                    self.state_of(&id.member_task(earlier)) == Some(TaskRunState::Succeeded)
                });
                assert!(
                    !exists || before_succeeded,
                    "stage {index} of {id:?} exists before the ones before it succeeded"
                );
            }
        }
    }

    /// Finishes everything still open, so accounting can be checked at rest.
    fn drain(&mut self) {
        // Cancelling a continuation's composite finishes its task.
        for id in self.composites.clone() {
            let _ = self.scheduler.cancel_composite(&id);
            // A composite that failed fast left its running members alone.
            let stages = self
                .scheduler
                .composite_record(&id)
                .map_or(0, |record| record.description.map_or(0, |step| step.stages.len()));
            for index in 0..stages as u64 {
                let _ = self.scheduler.cancel(&id.member_task(index));
            }
        }
        for task in self.tasks.keys().cloned().collect::<Vec<_>>() {
            let _ = self.scheduler.cancel(&task);
        }
        self.collect_events();
        // What was claimed and is running was just cancelled with the rest.
        assert_eq!(self.scheduler.pending_len(), 0);
        assert_eq!(self.scheduler.memory_in_use(), 0, "memory accounting drifted");
        for (id, times) in &self.finished {
            assert!(*times <= 1, "{id:?} finished {times} times");
        }
    }
}

/// The seed every run draws its histories from unless `PROPTEST_RNG_SEED`
/// names another, so every run checks the same cases.
const RNG_SEED: u64 = 0;

/// 128 cases from the fixed seed, unless `PROPTEST_CASES` or
/// `PROPTEST_RNG_SEED` ask for another run.
fn config() -> ProptestConfig {
    let config = crate::proptest::config(128);
    match config.rng_seed {
        RngSeed::Random => ProptestConfig {
            rng_seed: RngSeed::Fixed(RNG_SEED),
            ..config
        },
        RngSeed::Fixed(_) => config,
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn the_coalescing_invariants_hold_over_random_histories(
        ops in proptest::collection::vec(op(), 1..80)
    ) {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        let mut model = Model::new();
        for op in &ops {
            model.apply(op);
            model.check_invariants();
        }
        model.drain();
    }
}

/// Runs `ops` as a history, checking every invariant after each.
fn replay(ops: &[Op]) {
    let mut model = Model::new();
    for op in ops {
        model.apply(op);
        model.check_invariants();
    }
    model.drain();
}

#[test]
fn a_second_generation_of_a_key_is_never_claimed_beside_the_first() {
    let keyed = || Op::Submit { key: Some(0), size: 1, retries: 0 };
    replay(&[keyed(), Op::Claim { limit: 1 }, keyed(), Op::Claim { limit: 1 }]);
}

#[test]
fn a_silent_worker_replays_only_the_newest_generation_of_a_key() {
    let plain = || Op::Submit { key: None, size: 1, retries: 0 };
    let keyed = || Op::Submit { key: Some(0), size: 1, retries: 0 };
    replay(&[plain(), plain(), keyed(), Op::Claim { limit: 3 }, keyed(), Op::WorkerFallsSilent]);
}
