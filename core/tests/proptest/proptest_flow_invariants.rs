//! Property tests for the coalescing invariants at one node: random sequences
//! of submissions, claims, completions, continuations, failures, cancellations
//! and worker losses over a few keys, checking after every step that
//!
//! - at most one generation of a key is claimed, running or continuing;
//! - a generation that was claimed is never superseded;
//! - a superseded payload is folded exactly once, into the
//!   generation that absorbed it, oldest first;
//! - a lost generation is replayed only if it is the newest for its key;
//! - a continuation only exists for a certified run, and keeps its key
//!   held until it ends;
//!
//! and, once everything is drained, that memory accounting returns to zero.

use std::collections::{BTreeMap, BTreeSet};

use crate::support::clock::FakeClock;
use crate::support::grant::unbounded_grant;
use crate::support::ids::SequentialIds;
use crate::support::scheduler::state_of;
use crate::support::spy::Spy;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::records::TaskRunRecord;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::{Claim, Completion, Scheduler, Submission};
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
    EndContinuation {
        pick: u8,
    },
    Fail {
        pick: u8,
    },
    Cancel {
        pick: u8,
    },
    LoseWorker,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (proptest::option::weighted(0.8, 0..KEYS), 1u8..8, 0u8..3)
            .prop_map(|(key, size, retries)| Op::Submit { key, size, retries }),
        3 => (1u8..4).prop_map(|limit| Op::Claim { limit }),
        2 => any::<u8>().prop_map(|pick| Op::Start { pick }),
        2 => (any::<u8>(), any::<bool>())
            .prop_map(|(pick, continues)| Op::Complete { pick, continues }),
        1 => any::<u8>().prop_map(|pick| Op::EndContinuation { pick }),
        1 => any::<u8>().prop_map(|pick| Op::Fail { pick }),
        1 => any::<u8>().prop_map(|pick| Op::Cancel { pick }),
        1 => Just(Op::LoseWorker),
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
    spy: Spy,
    worker: WorkerId,
    next_number: u32,
    tasks: BTreeMap<TaskId, Submitted>,
    /// Claims handed out and not yet finished with, in the order handed out.
    claims: Vec<Claim>,
    /// Tasks whose run was certified with a continuation that has not ended.
    continuing: BTreeSet<TaskId>,
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
        let mut scheduler =
            Scheduler::with_observer(FakeClock::new(), SequentialIds::new(), spy.clone());
        scheduler.set_leadership_grant(Some(unbounded_grant()));
        Model {
            scheduler,
            spy,
            worker: WorkerId::new("w1"),
            next_number: 0,
            tasks: BTreeMap::new(),
            claims: Vec::new(),
            continuing: BTreeSet::new(),
            ever_claimed: BTreeSet::new(),
            folded_by: BTreeMap::new(),
        }
    }

    fn pick_claim(&self, pick: u8) -> Option<usize> {
        (!self.claims.is_empty()).then(|| pick as usize % self.claims.len())
    }

    fn state_of(&self, task: &TaskId) -> TaskRunState {
        state_of(&self.scheduler, &self.spy, task)
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
                        .report_started(&self.worker, &self.claims[i].task_run_id);
                }
            }
            Op::Complete { pick, continues } => {
                if let Some(i) = self.pick_claim(*pick) {
                    let claim = &self.claims[i];
                    let run = &claim.task_run_id;
                    let done = if *continues {
                        self.scheduler.complete(
                            &self.worker,
                            run,
                            Digest::blake3(b"d"),
                            Completion::Continues,
                        )
                    } else {
                        self.scheduler
                            .complete(&self.worker, run, Digest::blake3(b"d"), Completion::Final)
                    };
                    if done.is_ok() {
                        let task = claim.task.task_id();
                        if *continues {
                            self.continuing.insert(task);
                        }
                        self.claims.remove(i);
                    }
                }
            }
            Op::EndContinuation { pick } => {
                if !self.continuing.is_empty() {
                    let task = self
                        .continuing
                        .iter()
                        .nth(*pick as usize % self.continuing.len())
                        .unwrap()
                        .clone();
                    assert_eq!(self.scheduler.end_continuation(&task), Ok(true));
                    self.continuing.remove(&task);
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
                    if self.scheduler.cancel(&task).is_ok() {
                        // Whatever the worker held of it is no longer its to report on.
                        self.claims.retain(|claim| claim.task.task_id() != task);
                    }
                }
            }
            Op::LoseWorker => self.lose_worker(),
        }
    }

    /// A lost coalescing generation is replayed only if no newer one
    /// waits for its key.
    fn lose_worker(&mut self) {
        let waiting_keys: BTreeSet<u8> = self
            .tasks
            .iter()
            .filter(|(task, _)| self.state_of(task) == TaskRunState::Queued)
            .filter_map(|(_, submitted)| submitted.key)
            .collect();
        let lost = self.scheduler.lose_worker(&self.worker).unwrap();
        for run in &lost {
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
        let own = self.tasks[&task].number;
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

    fn check_invariants(&self) {
        for key in 0..KEYS {
            let mut holders = 0;
            for (task, submitted) in &self.tasks {
                if submitted.key != Some(key) {
                    continue;
                }
                let holds = matches!(
                    self.state_of(task),
                    TaskRunState::Claimed | TaskRunState::Running
                ) || self.continuing.contains(task);
                holders += usize::from(holds);
            }
            assert!(
                holders <= 1,
                "{holders} generations of key {key} hold it at once"
            );
        }
        for task in &self.ever_claimed {
            let first = self.scheduler.runs_of(task)[0].clone();
            assert_ne!(
                self.scheduler.task_run(&first).unwrap().current_state(),
                TaskRunState::Superseded,
                "a claimed generation was superseded"
            );
        }
        for task in &self.continuing {
            // A continuation only exists for a certified run.
            assert_eq!(self.state_of(task), TaskRunState::Succeeded);
        }
    }

    /// Finishes everything still open, so accounting can be checked at rest.
    fn drain(&mut self) {
        for task in self.continuing.clone() {
            assert_eq!(self.scheduler.end_continuation(&task), Ok(true));
        }
        self.continuing.clear();
        for task in self.tasks.keys().cloned().collect::<Vec<_>>() {
            let _ = self.scheduler.cancel(&task);
        }
        // What was claimed and is running was just cancelled with the rest.
        assert_eq!(self.scheduler.pending_len(), 0);
        assert_eq!(self.scheduler.memory_in_use(), 0, "memory accounting drifted");
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
