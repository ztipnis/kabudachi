//! Streamed composites at one leader: members arrive in batches, each taken
//! whole or refused whole, and the composite ends once its last batch is in
//! and every member has settled.

use kabudachi_core::blob::BlobKey;
use kabudachi_core::manifest::{Manifest, ManifestEntry};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::composite_entry;
use kabudachi_core::protocol::ids::CompositeId;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::composite::{
    BoundField, BoundInput, CompositeState, FieldValue, MemberBatch, MembersAccepted, OnError, PAGE_ENTRIES,
    Stage, StreamedMember,
};
use kabudachi_core::scheduler::{ClaimRejection, SubmitRejection};

use crate::support::composites::*;
use crate::support::scheduler::Fixture;

#[test]
fn a_streamed_map_runs_each_item_once_in_order_and_ends_once_its_last_batch_is_in() {
    let mut fixture = Fixture::leading();
    let map = submitted(&mut fixture, streamed_map(OnError::FailFast));
    assert_eq!(fixture.scheduler.pending_len(), 0, "a streamed map starts with no member");
    let first = fixture.scheduler.submit_members(item_batch(&map, 0, 2, false)).unwrap();
    assert_eq!(first, MembersAccepted { members: 2, slow_down: false });
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, 0, 2, false)),
        Ok(first),
        "a batch asked again creates nothing"
    );
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, 1, 2, false)).unwrap().members,
        3,
        "a batch overlapping the held members adds only its tail"
    );
    assert_eq!(fixture.scheduler.pending_len(), 3);
    for index in 0..3 {
        let claim = run_member(&mut fixture, &map, index, &[index as u8]);
        assert_eq!(claim.task.serialized_input, item(index), "member {index} is given item {index}");
    }
    assert!(finished(&mut fixture).is_empty(), "every member settled, but more may come");
    fixture.scheduler.submit_members(item_batch(&map, 3, 0, true)).unwrap();
    let finished = finished(&mut fixture);
    assert_eq!(finished[0].state, CompositeState::Succeeded);
    for index in 0..3 {
        assert_eq!(
            entry(&fixture, &map, index).unwrap().outcome,
            Some(composite_entry::Outcome::Value(Digest::blake3(&[index as u8]).into()))
        );
    }
    let record = fixture.spy.newest_composite(&map).unwrap();
    assert_eq!((record.members, record.sealed), (3, true));
}

#[test]
fn a_batch_is_refused_whole_past_the_hard_limit_and_after_the_composite_is_over() {
    let (mut fixture, map) = leading_with_room_for(streamed_map(OnError::FailFast), 16, 24);
    fixture.scheduler.submit_members(item_batch(&map, 0, 2, false)).unwrap();
    assert!(matches!(
        fixture.scheduler.submit_members(item_batch(&map, 2, 2, false)),
        Err(SubmitRejection::Backpressure { .. })
    ));
    assert_eq!(fixture.scheduler.pending_len(), 2, "no member of the refused batch exists");
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, 5, 1, false)),
        Err(SubmitRejection::MemberGap { held: 2, start: 5 })
    );
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, u64::MAX, 2, false)),
        Err(SubmitRejection::MemberGap { held: 2, start: u64::MAX }),
        "a start index whose batch would end past the largest index is a gap, not an overflow"
    );
    fail_member(&mut fixture, &map, 0, "ValueError");
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Failed);
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, 2, 1, false)),
        Err(SubmitRejection::CompositeFinished),
        "fail_fast ended it: no member is added"
    );
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, 0, 2, false)).unwrap().members,
        2,
        "a batch it already took is still answered"
    );
}

#[test]
fn a_chunked_item_weighs_its_whole_input_so_a_batch_that_cannot_hold_it_is_refused_whole() {
    let (mut fixture, map) = leading_with_room_for(streamed_map(OnError::FailFast), 16, 40);
    let mut batch = item_batch(&map, 0, 1, false);
    batch.members.push(StreamedMember::Item(BoundInput::Chunked(Manifest::chunk(&[7; 64]).manifest)));
    assert!(matches!(fixture.scheduler.submit_members(batch), Err(SubmitRejection::Backpressure { .. })));
    assert_eq!(fixture.scheduler.pending_len(), 0, "the first item of the refused batch was not started");
    assert_eq!(fixture.scheduler.submit_members(item_batch(&map, 0, 1, false)).unwrap().members, 1);
}

#[test]
fn a_sealed_composite_takes_no_new_member_but_answers_a_batch_it_took() {
    let mut fixture = Fixture::leading();
    let map = submitted(&mut fixture, streamed_map(OnError::FailFast));
    fixture.scheduler.submit_members(item_batch(&map, 0, 2, false)).unwrap();
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, 0, 2, true)).unwrap().members,
        2,
        "the last batch asked again seals the composite and adds nothing"
    );
    assert_eq!(fixture.scheduler.pending_len(), 2);
    assert!(fixture.spy.newest_composite(&map).unwrap().sealed);
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&map, 1, 2, true)),
        Err(SubmitRejection::MemberGap { held: 2, start: 3 }),
        "a sealed composite takes no member, even from a batch that overlaps the held ones"
    );
    assert_eq!(fixture.scheduler.submit_members(item_batch(&map, 0, 2, true)).unwrap().members, 2);
}

#[test]
fn cancelling_a_streamed_group_cancels_the_members_it_has_and_refuses_more() {
    let mut fixture = Fixture::leading();
    let group = submitted(&mut fixture, streamed_group(OnError::CollectAll));
    let members = MemberBatch {
        composite_id: group.clone(),
        start_index: 0,
        members: vec![
            StreamedMember::Stage(task_stage("a", Some(b"a"))),
            StreamedMember::Stage(Stage::Composite(flow_of(vec![task_stage("b", Some(b"b"))]))),
        ],
        last: false,
    };
    fixture.scheduler.submit_members(members).unwrap();
    let running = start_member(&mut fixture, &group, 0);
    assert!(fixture.scheduler.composite_status(&group.child(1)).is_some(), "a member that is a flow is a child");
    fixture.scheduler.cancel_composite(&group).unwrap();
    assert_eq!(fixture.run_state(&running.task_run_id), TaskRunState::Cancelled);
    assert_eq!(
        fixture.scheduler.composite_status(&group.child(1)).unwrap().state,
        CompositeState::Cancelled
    );
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&group, 2, 1, true)),
        Err(SubmitRejection::CompositeFinished)
    );
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Cancelled);
}

#[test]
fn a_batch_of_the_wrong_kind_or_for_a_composite_that_is_not_streamed_is_refused() {
    let mut fixture = Fixture::leading();
    let map = submitted(&mut fixture, streamed_map(OnError::FailFast));
    let mut wrong = item_batch(&map, 0, 1, false);
    wrong.members = vec![StreamedMember::Stage(task_stage("a", Some(b"a")))];
    assert_eq!(fixture.scheduler.submit_members(wrong), Err(SubmitRejection::MemberKind));
    let mut fielded = streamed_map(OnError::FailFast);
    let Stage::Task(template) = &mut fielded.stages[0] else { unreachable!("the template is a task") };
    template.fields.push(BoundField { name: "flag".into(), value: FieldValue::Bool(true) });
    let fielded = submitted(&mut fixture, fielded);
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&fielded, 0, 1, false)),
        Err(SubmitRejection::MemberKind),
        "an item is the whole input, so it has no fields set on it"
    );
    let group = submitted(&mut fixture, streamed_group(OnError::FailFast));
    let nested = MemberBatch {
        composite_id: group.clone(),
        start_index: 0,
        members: vec![StreamedMember::Stage(Stage::Composite(streamed_map(OnError::FailFast)))],
        last: false,
    };
    assert_eq!(
        fixture.scheduler.submit_members(nested),
        Err(SubmitRejection::MemberKind),
        "a member cannot itself wait for members"
    );
    let open = MemberBatch {
        composite_id: group.clone(),
        start_index: 0,
        members: vec![StreamedMember::Stage(task_stage("a", None))],
        last: true,
    };
    assert_eq!(
        fixture.scheduler.submit_members(open),
        Err(SubmitRejection::MemberKind),
        "a streamed group has no input for a member that takes one"
    );
    let whole = submitted(&mut fixture, flow_of(vec![task_stage("a", Some(b"a"))]));
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&whole, 0, 1, true)),
        Err(SubmitRejection::NotStreamed)
    );
    assert_eq!(
        fixture.scheduler.submit_members(item_batch(&CompositeId::new("nobody"), 0, 1, true)),
        Err(SubmitRejection::UnknownComposite)
    );
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &map.member_task(0)),
        Err(ClaimRejection::TaskUnknown)
    );
}

#[test]
fn each_accepted_batch_says_whether_slow_down_is_raised() {
    let (mut fixture, map) = leading_with_room_for(streamed_map(OnError::FailFast), 16, 1024);
    let first = fixture.scheduler.submit_members(item_batch(&map, 0, 2, false)).unwrap();
    assert!(!first.slow_down, "16 more bytes is not past the soft limit");
    let second = fixture.scheduler.submit_members(item_batch(&map, 2, 1, true)).unwrap();
    assert!(second.slow_down && fixture.scheduler.slow_down());
    for index in 0..3 {
        run_member(&mut fixture, &map, index, b"r");
    }
    assert!(!fixture.scheduler.slow_down(), "the map is over and weighs nothing");
}

#[test]
fn a_page_is_written_once_it_is_whole_or_its_composite_is_over_and_never_in_between() {
    let mut fixture = Fixture::leading();
    let map = submitted(&mut fixture, streamed_map(OnError::CollectAll));
    let total = PAGE_ENTRIES + 2;
    fixture.scheduler.submit_members(item_batch(&map, 0, PAGE_ENTRIES, false)).unwrap();
    fixture.scheduler.submit_members(item_batch(&map, PAGE_ENTRIES, 2, true)).unwrap();
    for index in 0..PAGE_ENTRIES - 1 {
        run_member(&mut fixture, &map, index, b"r");
    }
    assert!(fixture.spy.newest_page(&map, 0).is_none(), "page 0 is not whole yet");
    run_member(&mut fixture, &map, PAGE_ENTRIES - 1, b"r");
    assert_eq!(fixture.spy.page_revisions(&map, 0), 1, "page 0 went out once, whole");
    run_member(&mut fixture, &map, PAGE_ENTRIES, b"r");
    assert!(fixture.spy.newest_page(&map, 1).is_none());
    fail_member(&mut fixture, &map, PAGE_ENTRIES + 1, "ValueError");
    assert_eq!(fixture.spy.newest_page(&map, 1).unwrap().entries.len(), 2);
    let record = fixture.spy.newest_composite(&map).unwrap();
    assert_eq!((record.members, record.sealed, record.finished), (total, true, true));
    assert_eq!(record.counts.unwrap().failed, 1);
}

#[test]
fn a_streamed_item_held_as_blobs_reaches_its_task_as_the_manifest_its_budget_counted() {
    let mut fixture = Fixture::leading();
    let map = submitted(&mut fixture, streamed_map(OnError::FailFast));
    let manifest = Manifest::new(vec![ManifestEntry::Blob(BlobKey::of(b"a large item"))], 12);
    let batch = MemberBatch {
        composite_id: map.clone(),
        start_index: 0,
        members: vec![StreamedMember::Item(BoundInput::Chunked(manifest.clone()))],
        last: true,
    };
    fixture.scheduler.submit_members(batch).unwrap();
    let claim = start_member(&mut fixture, &map, 0);
    assert_eq!(claim.input_manifest().unwrap(), Some(manifest), "the Task names the blobs the budget weighed");
    assert!(claim.task.serialized_input.is_empty());
}
