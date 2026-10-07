//! Property test for compaction at one node: whatever mix of submissions,
//! compaction results and supersessions during a compaction a key goes
//! through, its newest generation's fold is the fold of every payload
//! submitted, in submission order, and memory accounting returns to zero.

use std::collections::BTreeSet;

use crate::support::scheduler::Fixture;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskDefinitionId, WorkerId};
use kabudachi_core::scheduler::{Completion, MemoryLimits, Submission};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

#[derive(Debug, Clone)]
enum Op {
    Submit { size: u8 },
    /// Folds the queued compaction, if any, and reports it.
    Compact,
    /// Takes the queued compaction, submits meanwhile, then reports it.
    SupersedeDuringCompaction { size: u8 },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (1u8..60).prop_map(|size| Op::Submit { size }),
        2 => Just(Op::Compact),
        2 => (1u8..60).prop_map(|size| Op::SupersedeDuringCompaction { size }),
    ]
}

/// Not associative, so a fold in the wrong order or grouping shows.
fn merge(older: &[u8], newer: &[u8]) -> Vec<u8> {
    [b"(".as_slice(), older, b">", newer, b")"].concat()
}

fn fold_all(payloads: &[Vec<u8>]) -> Vec<u8> {
    payloads[1..]
        .iter()
        .fold(payloads[0].clone(), |folded, next| merge(&folded, next))
}

fn runner() -> WorkerId {
    WorkerId::new("runner")
}

fn generation(payload: Vec<u8>) -> Submission {
    Submission::new(TaskDefinitionId::new("index.refresh"), 0, payload, "default")
        .with_coalescing_key("k")
}

struct Model {
    fixture: Fixture,
    submitted: Vec<Vec<u8>>,
}

impl Model {
    fn submit(&mut self, size: u8) {
        let number = self.submitted.len() as u16;
        let mut payload = number.to_be_bytes().to_vec();
        payload.resize(2 + size as usize, b'.');
        self.fixture
            .scheduler
            .submit(generation(payload.clone()))
            .unwrap();
        self.submitted.push(payload);
    }

    fn compaction(&mut self) -> Option<kabudachi_core::scheduler::Claim> {
        self.fixture
            .scheduler
            .claim_oldest(&runner(), 10)
            .unwrap()
            .into_iter()
            .find(|claim| claim.task.compacts.is_some())
    }

    fn report(&mut self, claim: &kabudachi_core::scheduler::Claim) {
        let folded = fold_all(&claim.chain);
        self.fixture
            .scheduler
            .complete_compaction(&runner(), &claim.task_run_id, folded)
            .unwrap();
    }

    fn apply(&mut self, op: &Op) {
        match op {
            Op::Submit { size } => self.submit(*size),
            Op::Compact => {
                if let Some(claim) = self.compaction() {
                    self.report(&claim);
                }
            }
            Op::SupersedeDuringCompaction { size } => {
                if let Some(claim) = self.compaction() {
                    self.submit(*size);
                    self.report(&claim);
                }
            }
        }
    }
}

const RNG_SEED: u64 = 0;

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
    fn the_fold_does_not_depend_on_where_the_chain_was_compacted(
        ops in proptest::collection::vec(op(), 1..60)
    ) {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        let mut fixture = Fixture::leading_with_limits(MemoryLimits { soft: 250, hard: 1_000_000 });
        fixture.scheduler.set_compaction_runners(BTreeSet::from([runner()]));
        let holder = fixture.scheduler.submit(generation(b"h".to_vec())).unwrap();
        let held = fixture.scheduler.request_claim(&WorkerId::new("w1"), &holder).unwrap();
        fixture.scheduler.report_started(&WorkerId::new("w1"), &held.task_run_id).unwrap();
        let mut model = Model { fixture, submitted: Vec::new() };
        // A key's waiting generation needs at least one payload to fold.
        model.submit(1);
        for op in &ops {
            model.apply(op);
        }

        // Compactions that are still queued or held keep running until the
        // chain is as short as it will get, while the holder still has the key.
        while let Some(claim) = model.compaction() {
            model.report(&claim);
        }
        model
            .fixture
            .scheduler
            .complete(&WorkerId::new("w1"), &held.task_run_id, Digest::blake3(b"d"), Completion::Final)
            .unwrap();
        let newest = model
            .fixture
            .scheduler
            .claim_oldest(&WorkerId::new("w2"), 1)
            .unwrap()
            .remove(0);
        let mut inputs = newest.chain.clone();
        inputs.push(newest.task.serialized_input.clone());
        prop_assert_eq!(fold_all(&inputs), fold_all(&model.submitted));
        let w2 = WorkerId::new("w2");
        model.fixture.scheduler.report_started(&w2, &newest.task_run_id).unwrap();
        model
            .fixture
            .scheduler
            .complete(&w2, &newest.task_run_id, Digest::blake3(b"d"), Completion::Final)
            .unwrap();
        prop_assert_eq!(model.fixture.scheduler.memory_in_use(), 0, "memory accounting drifted");
    }
}
