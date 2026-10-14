//! How a reduce is cut into steps. Evaluated with a reducer that appends
//! (associative, not commutative, `[]` its identity), every plan must give
//! the items back in order, each exactly once: the property that makes a
//! tree equal to its chain.

use kabudachi_core::protocol::ids::TaskDefinitionId;
use kabudachi_core::scheduler::composite::{
    BoundInput, BoundStep, Operand, ReduceNode, ReducePlan, ReduceRoot, ReduceShape, TaskOptions,
};
use proptest::prelude::*;

fn shape(tree: bool, chunk_size: u32, seeded: bool) -> ReduceShape {
    ReduceShape {
        reducer: BoundStep {
            definition: TaskDefinitionId::new("fold"),
            version: 0,
            options: TaskOptions::on_queue("default"),
            input: BoundInput::Previous,
            fields: Vec::new(),
        },
        tree,
        chunk_size,
        initial: seeded.then(|| BoundInput::Value(Vec::new())),
    }
}

/// Runs `plan` with an appending reducer; `None` for a reduce with nothing
/// to give. Also checks that a step takes only earlier results, and that
/// each result is taken once, by the step `consumer` names.
fn evaluate(plan: &ReducePlan) -> Option<Vec<u64>> {
    let mut results: Vec<Vec<u64>> = Vec::new();
    let mut taken_by = vec![None; plan.steps() as usize];
    for index in 0..plan.steps() {
        let step = plan.step(index).unwrap();
        assert_eq!(step.index, index);
        let mut values = step.operands.iter().map(|operand| match *operand {
            Operand::Item(item) => vec![item],
            Operand::Initial => Vec::new(),
            Operand::Result(before) => {
                assert!(before < index, "step {index} takes a later result {before}");
                assert_eq!(taken_by[before as usize].replace(index), None, "result {before} taken twice");
                results[before as usize].clone()
            }
        });
        let mut value = values.next().unwrap();
        values.for_each(|next| value.extend(next));
        results.push(value);
    }
    for (index, taker) in taken_by.iter().enumerate() {
        assert_eq!(plan.consumer(index as u64), *taker, "consumer of {index}");
    }
    assert_eq!(plan.consumer(plan.steps()), None);
    assert_eq!(plan.consumer(plan.steps() + 7), None);
    assert_eq!(plan.step(plan.steps()), None);
    match plan.root() {
        ReduceRoot::Empty => None,
        ReduceRoot::Initial => Some(Vec::new()),
        ReduceRoot::Item(item) => Some(vec![item]),
        ReduceRoot::Step(root) => Some(results[root as usize].clone()),
    }
}

/// What a leader may start, worked out from the operands alone.
fn startable_by_hand(plan: &ReducePlan, settled: impl Fn(u64) -> bool) -> Vec<u64> {
    let mut ready = Vec::new();
    for index in 0..plan.steps() {
        let step = plan.step(index).unwrap();
        let operands_settled = step.operands.iter().all(|operand| match *operand {
            Operand::Result(before) => settled(before),
            _ => true,
        });
        if !settled(index) && operands_settled {
            ready.push(index);
        }
    }
    ready
}

/// The seed enters a chain once, at its first step, and a tree at the head
/// of every chunk; an unseeded plan never mentions it.
fn assert_initial_placement(plan: &ReducePlan, tree: bool, seeded: bool) {
    for index in 0..plan.steps() {
        let step = plan.step(index).unwrap();
        let wants_initial = seeded && if tree { !step.combines() } else { index == 0 };
        assert_eq!(step.operands.first() == Some(&Operand::Initial), wants_initial, "step {index}");
        assert_eq!(step.operands.iter().filter(|o| **o == Operand::Initial).count(), usize::from(wants_initial));
    }
}

proptest! {
    #[test]
    fn every_plan_folds_every_item_once_in_order(
        items in 0u64..300,
        chunk_size in 0u32..10,
        tree: bool,
        seeded: bool,
        settled_mask in prop::collection::vec(any::<bool>(), 600),
        settled_prefix in 0u64..600,
    ) {
        let plan = ReducePlan::new(items, &shape(tree, chunk_size, seeded));
        assert_initial_placement(&plan, tree, seeded);
        let by_mask = |index: u64| settled_mask[index as usize];
        prop_assert_eq!(plan.startable(by_mask), startable_by_hand(&plan, by_mask));
        let by_prefix = |index: u64| index < settled_prefix;
        prop_assert_eq!(plan.startable(by_prefix), startable_by_hand(&plan, by_prefix));
        prop_assert_eq!(plan.startable(|_| false), startable_by_hand(&plan, |_| false));
        let expected = (items > 0 || seeded).then(|| (0..items).collect::<Vec<_>>());
        prop_assert_eq!(evaluate(&plan), expected);
        let steps = match (tree, seeded, items) {
            (_, false, 0 | 1) | (_, true, 0) => 0,
            (false, true, n) => n,
            (false, false, n) => n - 1,
            (true, _, n) => 2 * n.div_ceil(u64::from(chunk_size.max(1))) - 1,
        };
        prop_assert_eq!(plan.steps(), steps);
    }
}

#[test]
fn a_tree_numbers_its_steps_level_by_level_and_carries_an_unpaired_result_up() {
    // Five items in chunks of two: chunks 0-2; combine 3 = (0, 1) with
    // chunk 2 carried up; root 4 = (3, 2).
    let plan = ReducePlan::new(5, &shape(true, 2, false));
    let nodes: Vec<(ReduceNode, Vec<Operand>)> =
        (0..5).map(|index| plan.step(index).unwrap()).map(|step| (step.node, step.operands)).collect();
    let node = |level, position| ReduceNode { level, position };
    assert_eq!(
        nodes,
        [
            (node(0, 0), vec![Operand::Item(0), Operand::Item(1)]),
            (node(0, 1), vec![Operand::Item(2), Operand::Item(3)]),
            (node(0, 2), vec![Operand::Item(4)]),
            (node(1, 0), vec![Operand::Result(0), Operand::Result(1)]),
            (node(2, 0), vec![Operand::Result(3), Operand::Result(2)]),
        ]
    );
    assert!(plan.step(4).unwrap().combines() && !plan.step(2).unwrap().combines());
    assert_eq!(plan.root(), ReduceRoot::Step(4));
}
