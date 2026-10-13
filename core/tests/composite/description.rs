//! A step description is the one input the leader takes from a composite's
//! submitter: it must read back exactly, digest the same every time, and be
//! refused when it cannot be driven.

use kabudachi_core::manifest::Manifest;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{self, StepDescription};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskRunId};
use kabudachi_core::scheduler::composite::{
    BoundField, BoundInput, BoundStep, CompositeKind, EXPANSION_DEFINITION, FieldValue, INLINE_VALUE_BYTES, ItemInput,
    MalformedStep, MemberBatch, OnError, ReduceShape, Spilled, SpilledValue, Stage, Step, StreamedMember, TaskOptions,
};
use prost::Message;

fn bound(name: &str, input: BoundInput) -> BoundStep {
    BoundStep {
        definition: TaskDefinitionId::new(name),
        version: 0,
        options: TaskOptions::on_queue("default"),
        input,
        fields: Vec::new(),
    }
}

fn task(name: &str, input: BoundInput) -> Stage {
    Stage::Task(bound(name, input))
}

/// A flow of a bound task, a group of a field-bound task and an unbound
/// one, and a map template: every kind of stage C drives.
fn every_kind() -> Step {
    let overlaid = Stage::Task(BoundStep {
        fields: vec![
            BoundField { name: "count".into(), value: FieldValue::Int(0) },
            BoundField { name: "label".into(), value: FieldValue::Text("a".into()) },
        ],
        ..bound("shout", BoundInput::Previous)
    });
    Step {
        kind: CompositeKind::Flow,
        on_error: OnError::FailFast,
        stages: vec![
            task("load", BoundInput::Value(b"x".to_vec())),
            Stage::Composite(Step {
                kind: CompositeKind::Group,
                on_error: OnError::CollectAll,
                stages: vec![overlaid, task("wrap", BoundInput::Previous)],
                streamed: false,
                reduce: None,
            }),
            Stage::Composite(Step {
                kind: CompositeKind::Map,
                on_error: OnError::FailFast,
                stages: vec![task("each", BoundInput::Previous)],
                streamed: false,
                reduce: None,
            }),
        ],
        streamed: false,
        reduce: None,
    }
}

#[test]
fn a_described_step_reads_back_exactly_and_its_digest_is_that_of_its_encoding() {
    let step = every_kind();
    let wire = StepDescription::from(&step);
    let bytes = wire.encode_to_vec();
    let read = Step::try_from(&StepDescription::decode(&bytes[..]).unwrap()).unwrap();
    assert_eq!(read, step);
    assert_eq!(step.digest(), Digest::blake3(&bytes));
    // Built again, it encodes to the same bytes: the digest names the step.
    assert_eq!(StepDescription::from(&every_kind()).encode_to_vec(), bytes);
    // A bound zero survives: a field bound to its default is still bound.
    let Stage::Composite(group) = &read.stages[1] else { panic!("a group stage") };
    let Stage::Task(overlaid) = &group.stages[0] else { panic!("a task member") };
    assert_eq!(overlaid.fields[0].value, FieldValue::Int(0));
    assert!(read.stages[2].as_composite().unwrap().expands());
    assert!(!read.takes_input());
}

#[test]
fn a_step_that_cannot_be_driven_is_refused() {
    let refused = |wire: StepDescription| Step::try_from(&wire).unwrap_err();
    let flow = |stages| StepDescription {
        kind: generated::CompositeKind::Flow as i32,
        on_error: generated::OnError::FailFast as i32,
        stages,
        streamed: false,
        reduce: None,
    };
    assert_eq!(refused(flow(Vec::new())), MalformedStep::NoStages);
    assert_eq!(
        refused(flow(vec![generated::StageDescription { step: None }])),
        MalformedStep::EmptyStage
    );
    let mut unsorted = StepDescription::from(&every_kind());
    let generated::stage_description::Step::Composite(group) = unsorted.stages[1].step.as_mut().unwrap()
    else {
        panic!("a group stage")
    };
    let generated::stage_description::Step::Task(overlaid) = group.stages[0].step.as_mut().unwrap()
    else {
        panic!("a task member")
    };
    overlaid.fields.reverse();
    assert_eq!(refused(unsorted), MalformedStep::FieldsUnsorted);
    let mut two_templates = StepDescription::from(&every_kind());
    let generated::stage_description::Step::Composite(map) = two_templates.stages[2].step.as_mut().unwrap()
    else {
        panic!("a map stage")
    };
    map.stages.push(map.stages[0].clone());
    assert_eq!(refused(two_templates), MalformedStep::MapShape);
    // A map's one input-taking member must be a task: a composite there
    // has no task to run on each item.
    let mut composite_template = StepDescription::from(&every_kind());
    let generated::stage_description::Step::Composite(map) = composite_template.stages[2].step.as_mut().unwrap()
    else {
        panic!("a map stage")
    };
    map.stages = StepDescription::from(&every_kind()).stages[2..3].to_vec();
    assert_eq!(refused(composite_template), MalformedStep::MapShape);
    // An item is a map's template's whole input, so a template over a list
    // cannot also have fields set on it: it would reject every item.
    let with_field = |streamed| {
        let mut wire = StepDescription::from(&every_kind());
        let generated::stage_description::Step::Composite(map) = wire.stages[2].step.as_mut().unwrap() else {
            panic!("a map stage")
        };
        let generated::stage_description::Step::Task(template) = map.stages[0].step.as_mut().unwrap() else {
            panic!("a task template")
        };
        template.fields.push(generated::BoundField {
            name: "label".into(),
            value: Some(generated::bound_field::Value::StringValue("a".into())),
        });
        map.streamed = streamed;
        wire
    };
    assert_eq!(refused(with_field(false)), MalformedStep::MapShape);
    assert_eq!(refused(with_field(true)), MalformedStep::StreamedWithStages);
    // Only a root is fed in batches: a streamed map as a stage of a flow has
    // nothing to feed it.
    let mut nested = StepDescription::from(&every_kind());
    let generated::stage_description::Step::Composite(map) = nested.stages[2].step.as_mut().unwrap() else {
        panic!("a map stage")
    };
    map.streamed = true;
    assert_eq!(refused(nested), MalformedStep::StreamedBelowRoot);
}

/// A streamed map: one template that takes each item, no members.
fn streamed_map() -> Step {
    Step {
        kind: CompositeKind::Map,
        on_error: OnError::CollectAll,
        stages: vec![task("each", BoundInput::Previous)],
        streamed: true,
        reduce: None,
    }
}

#[test]
fn a_streamed_step_and_its_member_batches_read_back_exactly_and_a_streamed_flow_is_refused() {
    let map = streamed_map();
    let wire = StepDescription::from(&map);
    assert!(wire.streamed);
    assert_eq!(Step::try_from(&wire), Ok(map.clone()));
    assert_ne!(map.digest(), Step { streamed: false, ..map.clone() }.digest());
    assert!(!map.takes_input() && !map.expands(), "a streamed map is fed by batches, not by its input");

    let id = CompositeId::new("c1");
    let batch = MemberBatch {
        composite_id: id.clone(),
        start_index: 1024,
        members: vec![
            StreamedMember::Item(ItemInput::Value(b"a".to_vec())),
            StreamedMember::Stage(task("other", BoundInput::Value(b"b".to_vec()))),
        ],
        last: true,
    };
    let encoded = generated::SubmitMembers::from(&batch).encode_to_vec();
    let decoded = generated::SubmitMembers::decode(&encoded[..]).unwrap();
    assert_eq!(MemberBatch::try_from(&decoded), Ok(batch));

    for refused in [
        Step { kind: CompositeKind::Flow, stages: Vec::new(), ..streamed_map() },
        Step { kind: CompositeKind::Group, ..streamed_map() },
        Step { stages: Vec::new(), ..streamed_map() },
    ] {
        assert!(Step::try_from(&StepDescription::from(&refused)).is_err(), "{refused:?}");
    }
    assert_ne!(id.expansion_task(), id.member_task(0));
}

#[test]
fn a_task_stage_with_no_queue_or_the_reserved_expansion_definition_is_refused() {
    let refused = |step: &Step| Step::try_from(&StepDescription::from(step)).unwrap_err();
    let flow_of = |stage| Step { kind: CompositeKind::Flow, on_error: OnError::FailFast, stages: vec![stage], streamed: false, reduce: None };
    let mut no_queue = bound("work", BoundInput::Value(b"x".to_vec()));
    no_queue.options.queue = String::new();
    assert_eq!(refused(&flow_of(Stage::Task(no_queue))), MalformedStep::NoQueue);
    let expansion = bound(EXPANSION_DEFINITION, BoundInput::Value(b"x".to_vec()));
    assert_eq!(refused(&flow_of(Stage::Task(expansion))), MalformedStep::ReservedDefinition);
}

#[test]
fn derived_ids_differ_by_parent_index_and_kind_and_always_fit_an_id() {
    let flow = CompositeId::new("f".repeat(256));
    let ids = [
        flow.member_task(0).as_str().to_owned(),
        flow.member_task(1).as_str().to_owned(),
        flow.child(0).as_str().to_owned(),
        CompositeId::new("g").member_task(0).as_str().to_owned(),
        CompositeId::continuation_of(&TaskRunId::new("r")).as_str().to_owned(),
    ];
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(id.len(), 32, "a derived id is 32 hex digits");
        assert!(!id.contains('/'), "a derived id never looks like a composite key");
        assert!(ids[i + 1..].iter().all(|other| other != id), "{id} derived twice");
    }
    assert_eq!(flow.member_task(1), flow.member_task(1));
}

fn bound_group(values: Vec<Vec<u8>>) -> Step {
    Step {
        kind: CompositeKind::Group,
        on_error: OnError::FailFast,
        stages: values.into_iter().map(|value| task("each", BoundInput::Value(value))).collect(),
        streamed: false,
        reduce: None,
    }
}

/// A seeded tree over the one reducer stage that takes the list it is given.
fn tree_reduce() -> Step {
    let reducer = bound("fold", BoundInput::Previous);
    Step {
        kind: CompositeKind::Reduce,
        on_error: OnError::FailFast,
        stages: vec![Stage::Task(reducer.clone())],
        streamed: false,
        reduce: Some(ReduceShape {
            reducer,
            tree: true,
            chunk_size: 2,
            initial: Some(BoundInput::Value(Vec::new())),
        }),
    }
}

#[test]
fn a_large_step_spills_its_long_values_to_blobs_the_same_way_every_time_and_a_small_one_keeps_them() {
    let small = bound_group(vec![b"short".to_vec(), vec![7; 1024]]);
    assert_eq!(small.clone().spill(), Spilled { step: small, values: Vec::new() });

    let long = vec![9u8; 300 * 1024];
    let large = bound_group(vec![vec![1; INLINE_VALUE_BYTES], long.clone(), vec![2; INLINE_VALUE_BYTES + 1]]);
    let spilled = large.clone().spill();
    assert_eq!(spilled, large.clone().spill(), "deterministic");
    assert_eq!(
        spilled.values,
        [SpilledValue { bytes: long.clone() }, SpilledValue { bytes: vec![2; INLINE_VALUE_BYTES + 1] }]
    );
    let inputs: Vec<&BoundInput> = spilled
        .step
        .stages
        .iter()
        .map(|stage| match stage {
            Stage::Task(bound) => &bound.input,
            Stage::Composite(_) => unreachable!(),
        })
        .collect();
    assert_eq!(inputs[0], &BoundInput::Value(vec![1; INLINE_VALUE_BYTES]), "a short value stays inline");
    assert_eq!(inputs[1], &BoundInput::Chunked(Manifest::chunk(&long).manifest));
    assert!(spilled.step.encoded_len() < 1024, "only manifests are left of the values");

    // A nested composite's values spill too, depth first, for the root to own.
    let nested = Step {
        kind: CompositeKind::Flow,
        on_error: OnError::FailFast,
        stages: vec![task("first", BoundInput::Value(b"x".to_vec())), Stage::Composite(large)],
        streamed: false,
        reduce: None,
    };
    assert_eq!(nested.spill().values, spilled.values);
}

#[test]
fn a_streamed_member_spills_only_once_its_inline_bytes_pass_the_senders_bound() {
    let value = vec![5u8; 1000];
    let item = StreamedMember::Item(ItemInput::Value(value.clone()));
    assert_eq!(item.clone().spill(1000), (item.clone(), Vec::new()), "at the bound it stays inline");
    let (spilled, values) = item.spill(999);
    assert_eq!(spilled, StreamedMember::Item(ItemInput::Chunked(Manifest::chunk(&value).manifest)));
    assert_eq!(values, [SpilledValue { bytes: value.clone() }], "an item's one value goes whole");

    // A value no longer than a one-chunk manifest costs is never worth spilling.
    let short = StreamedMember::Item(ItemInput::Value(vec![5; INLINE_VALUE_BYTES]));
    assert_eq!(short.clone().spill(0), (short, Vec::new()), "a short item stays inline below any bound");

    let stage = StreamedMember::Stage(bound_group(vec![vec![1; INLINE_VALUE_BYTES], value.clone()]).stages.remove(1));
    let (spilled, values) = stage.clone().spill(10);
    assert_eq!(values, [SpilledValue { bytes: value.clone() }]);
    assert_ne!(spilled, stage);

    let mut nested = bound_group(vec![vec![1; INLINE_VALUE_BYTES], value.clone()]);
    nested.stages.insert(0, task("first", BoundInput::Value(vec![3; INLINE_VALUE_BYTES + 1])));
    let (_, values) = StreamedMember::Stage(Stage::Composite(nested)).spill(10);
    assert_eq!(
        values,
        [SpilledValue { bytes: vec![3; INLINE_VALUE_BYTES + 1] }, SpilledValue { bytes: value }],
        "a composite member's nested values spill in stage order"
    );
}

#[test]
fn a_reduce_description_reads_back_exactly_and_one_that_cannot_be_planned_is_refused() {
    let reduce = tree_reduce();
    let bytes = StepDescription::from(&reduce).encode_to_vec();
    let read = Step::try_from(&StepDescription::decode(&bytes[..]).unwrap()).unwrap();
    assert_eq!(read, reduce);
    assert_eq!(
        read.reduce.as_ref().unwrap().initial,
        Some(BoundInput::Value(Vec::new())),
        "an initial that encodes to no bytes is still given: the reduce is seeded"
    );
    assert!(read.takes_input());

    let refused = |step: Step| Step::try_from(&StepDescription::from(&step)).unwrap_err();
    let shape = reduce.reduce.clone().unwrap();
    let mut collect_all = reduce.clone();
    collect_all.on_error = OnError::CollectAll;
    let mut no_shape = reduce.clone();
    no_shape.reduce = None;
    let mut zero_chunks = reduce.clone();
    zero_chunks.reduce.as_mut().unwrap().chunk_size = 0;
    let mut bound_item = reduce.clone();
    bound_item.stages = vec![task("fold", BoundInput::Value(b"a".to_vec()))];
    let mut other_reducer = reduce.clone();
    other_reducer.stages[0] = Stage::Task(BoundStep { definition: TaskDefinitionId::new("other"), ..shape.reducer.clone() });
    let mut fields_bound = reduce.clone();
    let field = BoundField { name: "count".into(), value: FieldValue::Int(0) };
    fields_bound.reduce.as_mut().unwrap().reducer.fields = vec![field];
    fields_bound.stages = vec![Stage::Task(fields_bound.reduce.as_ref().unwrap().reducer.clone())];
    for step in [collect_all, no_shape, zero_chunks, bound_item, other_reducer, fields_bound] {
        assert_eq!(refused(step), MalformedStep::ReduceShape);
    }
    let mut shaped_flow = StepDescription::from(&reduce);
    shaped_flow.kind = generated::CompositeKind::Flow as i32;
    assert_eq!(Step::try_from(&shaped_flow).unwrap_err(), MalformedStep::ReduceShape, "only a reduce takes reduce options");
}
