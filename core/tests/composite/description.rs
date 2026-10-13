//! A step description is the one input the leader takes from a composite's
//! submitter: it must read back exactly, digest the same every time, and be
//! refused when it cannot be driven.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{self, StepDescription};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId, TaskRunId};
use kabudachi_core::scheduler::composite::{
    BoundField, BoundInput, BoundStep, CompositeKind, EXPANSION_DEFINITION, FieldValue, MalformedStep, OnError,
    MemberBatch, Stage, Step, StreamedMember, TaskOptions,
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
            }),
            Stage::Composite(Step {
                kind: CompositeKind::Map,
                on_error: OnError::FailFast,
                stages: vec![task("each", BoundInput::Previous)],
                streamed: false,
            }),
        ],
        streamed: false,
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
    let mut reduce = flow(StepDescription::from(&every_kind()).stages);
    reduce.kind = generated::CompositeKind::Reduce as i32;
    assert_eq!(refused(reduce), MalformedStep::UnsupportedKind);
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
}

/// A streamed map: one template that takes each item, no members.
fn streamed_map() -> Step {
    Step {
        kind: CompositeKind::Map,
        on_error: OnError::CollectAll,
        stages: vec![task("each", BoundInput::Previous)],
        streamed: true,
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
            StreamedMember::Item(BoundInput::Value(b"a".to_vec())),
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
    let flow_of = |stage| Step { kind: CompositeKind::Flow, on_error: OnError::FailFast, stages: vec![stage], streamed: false };
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
