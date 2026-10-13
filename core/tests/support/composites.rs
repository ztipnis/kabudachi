use kabudachi_core::protocol::generated::{self, CompositePage, CompositeRecord, RecordVersion};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId};
use kabudachi_core::scheduler::composite::{
    BoundInput, BoundStep, CompositeKind, OnError, Stage, Step, TaskOptions,
};

fn version(term: u64, revision: u64) -> Option<RecordVersion> {
    Some(RecordVersion {
        recovery_epoch: 0,
        recovery_lineage: 0,
        leader_term: term,
        revision,
    })
}

/// A flow of one task, the description every record below carries.
fn one_stage_flow() -> generated::StepDescription {
    let step = Step {
        kind: CompositeKind::Flow,
        on_error: OnError::FailFast,
        stages: vec![Stage::Task(BoundStep {
            definition: TaskDefinitionId::new("work"),
            version: 0,
            options: TaskOptions::on_queue("default"),
            input: BoundInput::Value(b"x".to_vec()),
            fields: Vec::new(),
        })],
    };
    generated::StepDescription::from(&step)
}

/// A composite's record at epoch (0, 0), with no placement.
pub fn composite_record(id: &CompositeId, term: u64, revision: u64) -> CompositeRecord {
    CompositeRecord {
        version: version(term, revision),
        composite_id: Some(id.clone().into()),
        description: Some(one_stage_flow()),
        ..Default::default()
    }
}

/// Page `index` of a composite's outcomes at epoch (0, 0), with no placement.
pub fn page(id: &CompositeId, index: u32, term: u64, revision: u64) -> CompositePage {
    CompositePage {
        version: version(term, revision),
        composite_id: Some(id.clone().into()),
        index,
        ..Default::default()
    }
}
