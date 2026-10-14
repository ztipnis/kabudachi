//! Reduces one leader drives: a reduce takes its items in batches into its
//! own item pages before any step runs.

use prost::Message as _;

use kabudachi_core::blob::BlobKey;
use kabudachi_core::manifest::{Manifest, ManifestEntry};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{Expansion, composite_entry, expansion};
use kabudachi_core::protocol::ids::{CompositeId, TaskDefinitionId};
use kabudachi_core::protocol::messages::prelude::*;
use kabudachi_core::protocol::task::TaskRunState;
use kabudachi_core::scheduler::composite::{
    CompositeKind, CompositeState, EXPANSION_DEFINITION, ITEM_PAGES, ItemInput, MemberBatch, OnError, PAGE_ENTRIES,
    REDUCE_EMPTY_KIND, REDUCE_ITEMS_LOST_KIND, REDUCER_CONTINUED_KIND, ReduceNode, Stage, Step, StreamedMember,
};
use kabudachi_core::scheduler::{Claim, ClaimRejection, Completion, Event, SubmitRejection};
use kabudachi_core::task_record::{RecordKey, RecordVersion, ShardRecord};
use kabudachi_core::time::Duration;

use crate::support::composites::*;
use crate::support::scheduler::Fixture;

#[test]
fn a_reduces_items_fill_its_item_pages_in_order_once_each_and_its_last_batch_seals_it() {
    let mut fixture = Fixture::leading();
    let reduce = submitted(&mut fixture, streamed_reduce(None, None));
    let values: Vec<Vec<u8>> = (0..PAGE_ENTRIES + 2).map(item).collect();
    let values: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
    let split = (PAGE_ENTRIES - 1) as usize;
    let mark = fixture.spy.mark();
    let taken = fixture.scheduler.submit_members(items_batch(&reduce, 0, &values[..split], false)).unwrap();
    let published = fixture.spy.published_since(mark);
    let page_at = published.iter().position(|record| matches!(record, ShardRecord::Page(_)));
    let record_at = published.iter().position(|record| matches!(record, ShardRecord::Composite(_)));
    assert!(page_at.unwrap() < record_at.unwrap(), "the item page is published before the record that counts its items");
    assert_eq!(taken.members, PAGE_ENTRIES - 1, "a reduce answers with the items it holds");
    assert_eq!(items_on_page(&fixture, &reduce, 0).len(), split, "a partly filled item page is written with its batch");
    assert_eq!(fixture.scheduler.pending_len(), 0, "no step starts before the reduce is sealed");
    let again = fixture.scheduler.submit_members(items_batch(&reduce, 0, &values[..split], false)).unwrap();
    assert_eq!(again.members, PAGE_ENTRIES - 1, "a batch asked again adds nothing");
    // The last batch overlaps what is held: only its tail is new.
    let overlap = split - 2;
    fixture.scheduler.submit_members(items_batch(&reduce, overlap as u64, &values[overlap..], true)).unwrap();
    let first: Vec<(u64, Vec<u8>)> = (0..PAGE_ENTRIES).map(|index| (index, item(index))).collect();
    assert_eq!(items_on_page(&fixture, &reduce, 0), first);
    assert_eq!(
        items_on_page(&fixture, &reduce, 1),
        [(PAGE_ENTRIES, item(PAGE_ENTRIES)), (PAGE_ENTRIES + 1, item(PAGE_ENTRIES + 1))]
    );
    let record = fixture.spy.newest_composite(&reduce).unwrap();
    assert_eq!((record.reduce_items, record.sealed), (PAGE_ENTRIES + 2, true));
    assert_eq!(record.members, PAGE_ENTRIES + 1, "a seedless chain of n items runs n - 1 steps");
    assert_eq!(
        fixture.scheduler.submit_members(items_batch(&reduce, PAGE_ENTRIES + 2, &[b"late"], false)),
        Err(SubmitRejection::MemberGap { held: PAGE_ENTRIES + 2, start: PAGE_ENTRIES + 3 }),
        "a sealed reduce takes no more items"
    );
}

/// The items of item page `page` of a stream: `PAGE_ENTRIES` items of 8 bytes.
fn page_of_items(page: u64) -> Vec<Vec<u8>> {
    (page * PAGE_ENTRIES..(page + 1) * PAGE_ENTRIES).map(item).collect()
}

#[test]
fn a_reduce_keeps_a_full_item_page_until_its_write_is_stored_so_a_stream_twice_the_hard_limit_goes_through() {
    let page_bytes = PAGE_ENTRIES * 8;
    let (mut fixture, reduce) = leading_with_room_for(streamed_reduce(None, None), 2 * page_bytes, 2 * page_bytes);
    let in_use = fixture.scheduler.memory_in_use();
    let pages: Vec<Vec<Vec<u8>>> = (0..4).map(page_of_items).collect();
    let three: Vec<&[u8]> = pages[..3].iter().flatten().map(Vec::as_slice).collect();
    assert!(
        matches!(
            fixture.scheduler.submit_members(items_batch(&reduce, 0, &three, false)),
            Err(SubmitRejection::Backpressure { .. })
        ),
        "one batch past the hard limit is refused whole"
    );
    assert!(items_on_page(&fixture, &reduce, 0).is_empty(), "a refused batch writes no item");
    for (page, items) in pages.iter().enumerate() {
        let items: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
        let start = page as u64 * PAGE_ENTRIES;
        fixture.scheduler.submit_members(items_batch(&reduce, start, &items, page == 3)).unwrap();
        let key = RecordKey::Page(reduce.clone(), ITEM_PAGES + page as u32);
        assert_eq!(items_on_page(&fixture, &reduce, page as u32).len() as u64, PAGE_ENTRIES);
        assert!(fixture.scheduler.holds(&key), "a full page is held until its write is stored, for repair to move or retry");
        assert_eq!(fixture.scheduler.memory_in_use(), in_use + page_bytes, "and it still counts against memory");
        if page == 0 {
            // Its write was refused: repair asks for it again, and the leader still has it to give.
            assert_eq!(fixture.scheduler.republish(&[key.clone()]), 1);
            assert_eq!(items_on_page(&fixture, &reduce, 0).len() as u64, PAGE_ENTRIES, "the retry carries every item");
        }
        let stored = fixture.spy.newest_page(&reduce, ITEM_PAGES + page as u32).unwrap().version.unwrap();
        fixture.scheduler.record_stored(&key, RecordVersion::from(&stored));
        assert!(!fixture.scheduler.holds(&key), "a stored page is let go");
        assert_eq!(fixture.scheduler.memory_in_use(), in_use, "and gives its bytes back");
        let writes = if page == 0 { 2 } else { 1 };
        assert_eq!(fixture.spy.page_revisions(&reduce, ITEM_PAGES + page as u32), writes);
    }
    // Four pages, twice the hard limit, went in: each full page was stored
    // and let go before the next batch was admitted.
    let record = fixture.spy.newest_composite(&reduce).unwrap();
    assert_eq!((record.reduce_items, record.sealed), (4 * PAGE_ENTRIES, true));
    fixture.scheduler.cancel_composite(&reduce).unwrap();
    assert_eq!(
        fixture.scheduler.submit_members(items_batch(&reduce, 4 * PAGE_ENTRIES, &[b"late"], false)),
        Err(SubmitRejection::CompositeFinished)
    );
}

#[test]
fn slow_down_clears_when_a_stored_item_page_is_let_go_with_no_other_activity() {
    let page_bytes = PAGE_ENTRIES * 8;
    let (mut fixture, reduce) = leading_with_room_for(streamed_reduce(None, None), page_bytes - 1, 4 * page_bytes);
    let page = page_of_items(0);
    let page: Vec<&[u8]> = page.iter().map(Vec::as_slice).collect();
    assert!(
        fixture.scheduler.submit_members(items_batch(&reduce, 0, &page, false)).unwrap().slow_down,
        "a full page passes the soft limit"
    );
    let stored = fixture.spy.newest_page(&reduce, ITEM_PAGES).unwrap().version.unwrap();
    fixture.scheduler.record_stored(&RecordKey::Page(reduce.clone(), ITEM_PAGES), RecordVersion::from(&stored));
    assert!(!fixture.scheduler.slow_down(), "the page was let go, so SlowDown clears with no other activity");
    assert!(
        fixture.scheduler.take_events().iter().any(|event| matches!(event, Event::SlowDown { active: false })),
        "and the submitters are told"
    );
}

#[test]
fn an_item_page_holds_small_items_inline_or_as_blobs_and_refuses_a_batch_with_any_other_whole() {
    let mut fixture = Fixture::leading();
    let reduce = submitted(&mut fixture, streamed_reduce(None, None));
    let mut batch = items_batch(&reduce, 0, &[b"a", &[0; 65]], false);
    assert_eq!(fixture.scheduler.submit_members(batch.clone()), Err(SubmitRejection::MemberKind), "an inline item past 64 bytes");
    batch.members = vec![StreamedMember::Stage(task_stage("fold", Some(b"x")))];
    assert_eq!(fixture.scheduler.submit_members(batch), Err(SubmitRejection::MemberKind), "a stage is no item");
    assert!(items_on_page(&fixture, &reduce, 0).is_empty(), "a refused batch writes no item");

    let blobs = Manifest::chunk(&[7; 100]).manifest;
    let mixed = MemberBatch {
        members: vec![
            StreamedMember::Item(ItemInput::Value(b"a".to_vec())),
            StreamedMember::Item(ItemInput::Chunked(blobs.clone())),
        ],
        ..items_batch(&reduce, 0, &[], false)
    };
    let in_use = fixture.scheduler.memory_in_use();
    assert_eq!(fixture.scheduler.submit_members(mixed).unwrap().members, 2);
    assert_eq!(
        fixture.scheduler.memory_in_use() - in_use,
        1 + blobs.encoded_len() as u64,
        "an inline item costs its bytes and a stored one its manifest"
    );
    let page = fixture.spy.newest_page(&reduce, ITEM_PAGES).unwrap();
    let outcomes: Vec<_> = page.entries.iter().map(|entry| (entry.index, entry.outcome.clone())).collect();
    assert_eq!(
        outcomes,
        [
            (0, Some(composite_entry::Outcome::Item(b"a".to_vec()))),
            (1, Some(composite_entry::Outcome::ItemManifest(blobs.into()))),
        ]
    );
}

/// Where a claimed reduce step says it sits.
fn node_of(claim: &Claim) -> Option<ReduceNode> {
    claim.task.reduce_node.as_ref().map(ReduceNode::from)
}

fn node(level: u32, position: u64) -> Option<ReduceNode> {
    Some(ReduceNode { level, position })
}

/// The operand results a claimed step was given, as the digests they were reported with.
fn operands_of(claim: &Claim) -> Option<Vec<ManifestEntry>> {
    claim.input_manifest().unwrap().map(|manifest| manifest.entries().to_vec())
}

fn result_blob(result: &[u8]) -> ManifestEntry {
    ManifestEntry::Blob(BlobKey::from(&Digest::blake3(result)))
}

#[test]
fn a_chain_runs_one_step_per_item_each_on_its_item_and_the_result_before_it() {
    let mut fixture = Fixture::leading();
    let reduce = reduce_fed(&mut fixture, streamed_reduce(None, Some(b"")), &[b"a", b"b", b"c"]);
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &reduce.member_task(1)).err(),
        Some(ClaimRejection::TaskUnknown),
        "a chain's second step waits for its first"
    );
    let first = run_member(&mut fixture, &reduce, 0, b"a");
    assert_eq!(node_of(&first), node(0, 0));
    assert_eq!(operands_of(&first), None, "the first step folds the item into the initial value");
    let second = run_member(&mut fixture, &reduce, 1, b"ab");
    assert_eq!(node_of(&second), node(0, 1));
    assert_eq!(operands_of(&second), Some(vec![result_blob(b"a")]));
    run_member(&mut fixture, &reduce, 2, b"abc");
    let finished = finished(&mut fixture);
    assert_eq!(finished[0].state, CompositeState::Succeeded);
    assert_eq!(value_of(&fixture, &reduce, 2), Some(Digest::blake3(b"abc")));
    assert_eq!(fixture.spy.newest_composite(&reduce).unwrap().counts.unwrap().succeeded, 3);
    for step in 0..3 {
        assert_eq!(fixture.scheduler.runs_of(&reduce.member_task(step)).len(), 1, "step {step} is created and run once");
    }
}

#[test]
fn a_tree_folds_its_chunks_side_by_side_then_combines_adjacent_results_in_order() {
    let mut fixture = Fixture::leading();
    // Chunks 0-2 of two items; combine 3 = (0, 1); root 4 = (3, 2).
    let reduce = reduce_fed(&mut fixture, streamed_reduce(Some(2), None), &[b"a", b"b", b"c", b"d", b"e"]);
    assert_eq!(fixture.scheduler.pending_len(), 3, "every chunk starts at once");
    run_member(&mut fixture, &reduce, 0, b"ab");
    run_member(&mut fixture, &reduce, 1, b"cd");
    let combine = run_member(&mut fixture, &reduce, 3, b"abcd");
    assert_eq!(node_of(&combine), node(1, 0));
    assert_eq!(operands_of(&combine), Some(vec![result_blob(b"ab"), result_blob(b"cd")]));
    assert_eq!(
        fixture.scheduler.request_claim(&worker(), &reduce.member_task(4)).err(),
        Some(ClaimRejection::TaskUnknown),
        "the root waits for the carried chunk"
    );
    run_member(&mut fixture, &reduce, 2, b"e");
    let root = run_member(&mut fixture, &reduce, 4, b"abcde");
    assert_eq!(node_of(&root), node(2, 0));
    assert_eq!(
        operands_of(&root),
        Some(vec![result_blob(b"abcd"), result_blob(b"e")]),
        "left before right: the carried chunk is the right operand"
    );
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
    for step in 0..5 {
        assert_eq!(fixture.scheduler.runs_of(&reduce.member_task(step)).len(), 1, "step {step} is created and run once");
    }
}

#[test]
fn a_step_that_exhausts_its_retries_fails_the_reduce_and_no_later_step_starts() {
    let mut fixture = Fixture::leading();
    let mut step = streamed_reduce(Some(1), None);
    step.reduce.as_mut().unwrap().reducer.options.retries = 1;
    // Chunks 0-2; combine 3 = (0, 1); root 4 = (3, 2).
    let reduce = reduce_fed(&mut fixture, step, &[b"a", b"b", b"c"]);
    let slow = start_member(&mut fixture, &reduce, 1);
    fail_member(&mut fixture, &reduce, 0, "ValueError");
    assert!(finished(&mut fixture).is_empty(), "a failure with retries left settles nothing");
    assert_eq!(fixture.scheduler.runs_of(&reduce.member_task(0)).len(), 2, "the failed run and its queued retry");
    // The second call claims that retry; it has none left, so its failure is final.
    fail_member(&mut fixture, &reduce, 0, "ValueError");
    assert_eq!(fixture.scheduler.runs_of(&reduce.member_task(0)).len(), 2, "the retry was the last run");
    let ended = finished(&mut fixture);
    assert_eq!(ended[0].state, CompositeState::Failed);
    assert_eq!(ended[0].failure.as_ref().unwrap().kind, "ValueError");
    fixture
        .scheduler
        .complete(&worker(), &slow.task_run_id, Digest::blake3(b"b"), Completion::Final)
        .unwrap();
    run_member(&mut fixture, &reduce, 2, b"c");
    assert!(value_of(&fixture, &reduce, 1).is_some(), "a step still running when the reduce failed is recorded");
    for after in [3, 4] {
        assert_eq!(
            fixture.scheduler.request_claim(&worker(), &reduce.member_task(after)).err(),
            Some(ClaimRejection::TaskUnknown),
            "no step starts once the reduce has failed"
        );
    }
    assert!(finished(&mut fixture).is_empty(), "the reduce failed once");
}

/// A flow of a map of three tasks, a seedless reduce of two-item chunks of
/// their results, and a task after it; runs the map's tasks. Returns the flow
/// and the reduce.
fn flow_after_a_map(fixture: &mut Fixture) -> (CompositeId, CompositeId) {
    let map = Stage::Composite(Step {
        kind: CompositeKind::Map,
        on_error: OnError::FailFast,
        stages: [b"1", b"2", b"3"].iter().map(|item| task_stage("each", Some(*item))).collect(),
        streamed: false,
        reduce: None,
    });
    let flow = submitted(fixture, flow_of(vec![map, reduce_template(Some(2), None), task_stage("report", None)]));
    for index in 0..3 {
        run_member(fixture, &flow.child(0), index, &[index as u8]);
    }
    let reduce = flow.child(1);
    (flow, reduce)
}

#[test]
fn a_reduce_after_a_map_folds_the_maps_results_in_order_without_an_expansion_and_goes_with_its_flow() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_result_ttl(Some(Duration::from_ticks(100)));
    let (flow, reduce) = flow_after_a_map(&mut fixture);
    assert_eq!(fixture.spy.newest_composite(&reduce).unwrap().reduce_items, 3, "the map's three results are its items");
    let chunk = run_member(&mut fixture, &reduce, 0, b"01");
    assert_ne!(chunk.task.task_definition_id().as_str(), EXPANSION_DEFINITION);
    assert_eq!(node_of(&chunk), node(0, 0));
    run_member(&mut fixture, &reduce, 1, b"2");
    run_member(&mut fixture, &reduce, 2, b"012");
    let report = run_member(&mut fixture, &flow, 2, b"reported");
    assert_eq!(
        operands_of(&report),
        Some(vec![ManifestEntry::Page { composite: reduce.clone(), page: 0 }]),
        "the stage after a reduce takes the reduce's one result"
    );
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
    for step in 0..3 {
        assert_eq!(fixture.scheduler.runs_of(&reduce.member_task(step)).len(), 1, "step {step} is created and run once");
    }
    fixture.clock.advance(Duration::from_ticks(100));
    fixture.scheduler.catch_up();
    assert!(fixture.scheduler.composite_status(&reduce).is_none());
    for step in 0..3 {
        assert!(fixture.forgotten(&reduce.member_task(step)), "step {step} goes with its reduce");
    }
}

/// A flow of a task stage returning a list and a seedless reduce of it; runs
/// the stage and claims and starts the reduce's expansion. Returns the flow,
/// the reduce and the expansion's claim.
fn expanding(fixture: &mut Fixture) -> (CompositeId, CompositeId, Claim) {
    let flow = submitted(fixture, flow_of(vec![task_stage("load", Some(b"x")), reduce_template(None, None)]));
    run_member(fixture, &flow, 0, b"[...]");
    let reduce = flow.child(1);
    let expansion = fixture.scheduler.request_claim(&worker(), &reduce.expansion_task()).unwrap();
    fixture.scheduler.report_started(&worker(), &expansion.task_run_id).unwrap();
    let asked = Expansion::decode(&expansion.task.serialized_input[..]).unwrap();
    let Some(expansion::Then::Reduce(reducer)) = asked.then else {
        panic!("a reduce's expansion names its reducer");
    };
    assert_eq!(reducer.definition, Some(TaskDefinitionId::new("fold").into()));
    (flow, reduce, expansion)
}

/// As [`expanding`], then streams `items` into the reduce and completes the
/// expansion, which seals it.
fn expanded(fixture: &mut Fixture, items: &[&[u8]]) -> (CompositeId, CompositeId) {
    let (flow, reduce, expansion) = expanding(fixture);
    fixture.scheduler.submit_members(items_batch(&reduce, 0, items, false)).unwrap();
    assert_eq!(fixture.scheduler.pending_len(), 0, "no step starts before the expansion seals the reduce");
    fixture
        .scheduler
        .complete(&worker(), &expansion.task_run_id, Digest::blake3(b""), Completion::Final)
        .unwrap();
    (flow, reduce)
}

#[test]
fn a_reduce_after_a_task_stage_folds_what_its_expansion_streamed_and_a_seedless_one_of_nothing_fails_from_the_leader() {
    let mut fixture = Fixture::leading();
    fixture.scheduler.set_result_ttl(Some(Duration::from_ticks(100)));
    let (flow, reduce) = expanded(&mut fixture, &[b"a", b"b"]);
    let step = run_member(&mut fixture, &reduce, 0, b"ab");
    assert_eq!(node_of(&step), node(0, 0));
    assert_eq!(items_on_page(&fixture, &reduce, 0), [(0, b"a".to_vec()), (1, b"b".to_vec())]);
    let ended = finished(&mut fixture);
    let flow_end = ended.iter().find(|end| end.composite_id == flow).unwrap();
    assert_eq!(flow_end.state, CompositeState::Succeeded);
    fixture.clock.advance(Duration::from_ticks(100));
    fixture.scheduler.catch_up();
    assert!(fixture.forgotten(&reduce.expansion_task()), "the expansion run goes with its reduce");

    let (empty, _) = expanded(&mut fixture, &[]);
    let ended = finished(&mut fixture);
    let failed = ended.iter().find(|end| end.composite_id == empty).unwrap();
    assert_eq!(failed.state, CompositeState::Failed);
    assert_eq!(failed.failure.as_ref().unwrap().kind, REDUCE_EMPTY_KIND);

    // A reduce whose last batch sealed it runs on when its expansion completes.
    let (_, sealed, expansion) = expanding(&mut fixture);
    fixture.scheduler.submit_members(items_batch(&sealed, 0, &[b"a", b"b"], true)).unwrap();
    fixture
        .scheduler
        .complete(&worker(), &expansion.task_run_id, Digest::blake3(b""), Completion::Final)
        .unwrap();
    run_member(&mut fixture, &sealed, 0, b"ab");
    assert_eq!(fixture.scheduler.runs_of(&sealed.member_task(0)).len(), 1, "its step is made once");
    assert_eq!(fixture.scheduler.composite_status(&sealed).unwrap().state, CompositeState::Succeeded);
}

#[test]
fn a_finished_reduce_writes_every_item_page_finished_whether_the_leader_still_held_it_or_not() {
    let mut fixture = Fixture::leading();
    let reduce = submitted(&mut fixture, one_chunk_reduce(2));
    let values: Vec<Vec<u8>> = (0..PAGE_ENTRIES + 2).map(item).collect();
    let values: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
    fixture.scheduler.submit_members(items_batch(&reduce, 0, &values, true)).unwrap();
    // The first page's write was stored, so the leader let it go.
    let first = RecordKey::Page(reduce.clone(), ITEM_PAGES);
    let stored = fixture.spy.newest_page(&reduce, ITEM_PAGES).unwrap().version.unwrap();
    fixture.scheduler.record_stored(&first, RecordVersion::from(&stored));
    assert!(!fixture.scheduler.holds(&first));
    run_member(&mut fixture, &reduce, 0, b"folded");
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
    for page in 0..2 {
        let newest = fixture.spy.newest_page(&reduce, ITEM_PAGES + page).unwrap();
        assert!(newest.finished, "item page {page} is rewritten as finished so a holder can retire it");
    }
}

#[test]
fn a_new_leader_keeps_a_finished_reduce_fed_in_batches_as_the_record_says_though_its_full_item_pages_were_rewritten_empty() {
    let mut old = Fixture::leading();
    let reduce = submitted(&mut old, one_chunk_reduce(2));
    let values: Vec<Vec<u8>> = (0..PAGE_ENTRIES + 2).map(item).collect();
    let values: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
    old.scheduler.submit_members(items_batch(&reduce, 0, &values, true)).unwrap();
    // The full page's write was stored, so the leader let it go and rewrites it empty.
    let first = RecordKey::Page(reduce.clone(), ITEM_PAGES);
    let stored = old.spy.newest_page(&reduce, ITEM_PAGES).unwrap().version.unwrap();
    old.scheduler.record_stored(&first, RecordVersion::from(&stored));
    run_member(&mut old, &reduce, 0, b"folded");
    assert_eq!(finished(&mut old)[0].state, CompositeState::Succeeded);
    let rewritten = old.spy.newest_page(&reduce, ITEM_PAGES).unwrap();
    assert!(rewritten.finished && rewritten.entries.is_empty(), "the full item page was rewritten finished and empty");
    let new = took_over(&old, rebuild_losing(&old, 0, |_| false));
    assert_eq!(new.scheduler.composite_status(&reduce).unwrap().state, CompositeState::Succeeded);
    let record = new.scheduler.composite_record(&reduce).unwrap();
    assert_eq!(
        (record.reduce_items, record.sealed, record.members),
        (PAGE_ENTRIES + 2, true, 1),
        "the republished record still counts the items its result is planned from"
    );
}

#[test]
fn a_new_leader_drives_a_half_combined_tree_on_and_makes_each_step_once() {
    let mut old = Fixture::leading();
    // Chunks 0-3; combines 4 = (0, 1), 5 = (2, 3); root 6 = (4, 5).
    let reduce = reduce_fed(&mut old, streamed_reduce(Some(1), None), &[b"a", b"b", b"c", b"d"]);
    run_member(&mut old, &reduce, 0, b"a");
    let mark = old.spy.mark();
    run_member(&mut old, &reduce, 1, b"b"); // makes combine 4
    run_member(&mut old, &reduce, 2, b"c");
    // The old leader dies with its composite and page writes since `mark` lost:
    // the new one knows step 0 from the reduce's record, steps 1 and 2 only
    // from their own Task records.
    let mut new = took_over(&old, rebuild_losing(&old, mark, is_composite_or_page));
    run_member(&mut new, &reduce, 3, b"d");
    run_member(&mut new, &reduce, 4, b"ab");
    run_member(&mut new, &reduce, 5, b"cd");
    let root = run_member(&mut new, &reduce, 6, b"abcd");
    assert_eq!(operands_of(&root), Some(vec![result_blob(b"ab"), result_blob(b"cd")]));
    assert_eq!(finished(&mut new)[0].state, CompositeState::Succeeded);
    for step in 0..7 {
        let runs = new.scheduler.runs_of(&reduce.member_task(step)).len();
        assert_eq!(runs, 1, "step {step} was made and run once");
    }
}

#[test]
fn a_new_leader_fails_a_reduce_one_of_whose_steps_failed_in_the_old_one_once_and_starts_nothing() {
    let mut old = Fixture::leading();
    let reduce = reduce_fed(&mut old, streamed_reduce(None, Some(b"")), &[b"a", b"b", b"c"]);
    let mark = old.spy.mark();
    fail_member(&mut old, &reduce, 0, "ValueError");
    // The failure reached the step's own record but not the reduce's.
    let mut new = took_over(&old, rebuild_losing(&old, mark, is_composite_or_page));
    let ended = finished(&mut new);
    assert_eq!(ended.len(), 1, "the reduce failed once");
    assert_eq!(ended[0].state, CompositeState::Failed);
    assert_eq!(ended[0].failure.as_ref().unwrap().kind, "ValueError");
    assert_eq!(new.scheduler.pending_len(), 0, "no later step starts");
}

/// A reduce of `PAGE_ENTRIES + extra` items in one chunk, so one step.
fn one_chunk_reduce(extra: u64) -> Step {
    streamed_reduce(Some((PAGE_ENTRIES + extra) as u32), None)
}

#[test]
fn a_new_leader_finds_a_reduces_items_on_both_sides_of_a_page_boundary() {
    for extra in [0, 1] {
        let mut old = Fixture::leading();
        let reduce = submitted(&mut old, one_chunk_reduce(extra + 2));
        let held = page_of_items(0).into_iter().chain((0..extra).map(|more| item(PAGE_ENTRIES + more))).collect::<Vec<_>>();
        let held: Vec<&[u8]> = held.iter().map(Vec::as_slice).collect();
        let mark = old.spy.mark();
        old.scheduler.submit_members(items_batch(&reduce, 0, &held, false)).unwrap();
        let mut new = took_over(&old, rebuild_losing(&old, mark, |key| matches!(key, RecordKey::Composite(_))));
        // The client, unanswered, asks again from the first page's end, so
        // the batch repeats the items the old leader stored and adds the rest.
        let last: Vec<Vec<u8>> = (PAGE_ENTRIES..PAGE_ENTRIES + extra + 2).map(item).collect();
        let last: Vec<&[u8]> = last.iter().map(Vec::as_slice).collect();
        let taken = new.scheduler.submit_members(items_batch(&reduce, PAGE_ENTRIES, &last, true)).unwrap();
        assert_eq!(taken.members, PAGE_ENTRIES + extra + 2, "{extra} items past the first page, none taken twice");
        let second: Vec<(u64, Vec<u8>)> =
            (0..extra + 2).map(|more| (PAGE_ENTRIES + more, item(PAGE_ENTRIES + more))).collect();
        assert_eq!(items_on_page(&new, &reduce, 1), second, "{extra} items past the first page");
        assert_eq!(new.spy.page_revisions(&reduce, ITEM_PAGES), 0, "the full first page is not written again");
        run_member(&mut new, &reduce, 0, b"folded");
        assert_eq!(finished(&mut new)[0].state, CompositeState::Succeeded);
    }
}

#[test]
fn a_new_leader_counts_a_reduces_items_only_up_to_a_gap_in_its_item_pages_and_does_not_trust_a_record_that_counts_more() {
    let mut old = Fixture::leading();
    let total = PAGE_ENTRIES + 3;
    let reduce = submitted(&mut old, one_chunk_reduce(3));
    old.scheduler.submit_members(items_batch(&reduce, 0, &[b"a", b"b"], false)).unwrap();
    let mark = old.spy.mark();
    let rest: Vec<Vec<u8>> = (2..total).map(item).collect();
    let rest: Vec<&[u8]> = rest.iter().map(Vec::as_slice).collect();
    old.scheduler.submit_members(items_batch(&reduce, 2, &rest, true)).unwrap();
    assert_eq!(old.scheduler.pending_len(), 1, "the sealed reduce started its step");
    // The first page's later writes, and the step, were lost: the second page
    // is stored with items 2 to PAGE_ENTRIES - 1 missing before it, while the
    // reduce's record says it is sealed with all the items.
    let lost = |key: &RecordKey| matches!(key, RecordKey::Task(_)) || matches!(key, RecordKey::Page(_, page) if *page == ITEM_PAGES);
    let mut new = took_over(&old, rebuild_losing(&old, mark, lost));
    assert_eq!(new.scheduler.pending_len(), 0, "a reduce missing items is not sealed, so no step starts");
    let all: Vec<Vec<u8>> = (0..total).map(item).collect();
    let all: Vec<&[u8]> = all.iter().map(Vec::as_slice).collect();
    let taken = new.scheduler.submit_members(items_batch(&reduce, 0, &all, true)).unwrap();
    assert_eq!(taken.members, total, "the client's batch fills the gap");
    assert_eq!(items_on_page(&new, &reduce, 0).len() as u64, PAGE_ENTRIES);
    run_member(&mut new, &reduce, 0, b"folded");
    assert_eq!(finished(&mut new)[0].state, CompositeState::Succeeded);
}

#[test]
fn a_reduce_whose_stored_items_fall_short_of_its_record_fails_when_its_expansion_ends_instead_of_folding_fewer() {
    let mut old = Fixture::leading();
    let (flow, reduce, expansion) = expanding(&mut old);
    let mark = old.spy.mark();
    old.scheduler.submit_members(items_batch(&reduce, 0, &[b"a", b"b", b"c"], false)).unwrap();
    old.scheduler
        .complete(&worker(), &expansion.task_run_id, Digest::blake3(b""), Completion::Final)
        .unwrap();
    // The record counting three items and the expansion's end were stored; the item page was not.
    let mut new = took_over(&old, rebuild_losing(&old, mark, |key| matches!(key, RecordKey::Page(..))));
    let ended = finished(&mut new);
    let end = ended.iter().find(|end| end.composite_id == flow).unwrap();
    assert_eq!(end.state, CompositeState::Failed);
    assert_eq!(end.failure.as_ref().unwrap().kind, REDUCE_ITEMS_LOST_KIND, "not a reduce of the items that survived");
    assert_eq!(new.scheduler.composite_status(&reduce).unwrap().state, CompositeState::Failed);
    assert!(
        new.spy.newest_page(&reduce, ITEM_PAGES).is_some_and(|page| page.finished),
        "the item pages the record counted are rewritten finished though the leader held none of them"
    );
}

#[test]
fn a_reducer_that_returns_a_continuation_fails_its_reduce_without_folding_the_continuations_result() {
    let mut fixture = Fixture::leading();
    let reduce = reduce_fed(&mut fixture, streamed_reduce(None, Some(b"")), &[b"a", b"b"]);
    let claim = start_member(&mut fixture, &reduce, 0);
    let continuation = continue_with_a_stage(&mut fixture, &claim.task_run_id);
    end_continuation(&mut fixture, &continuation);
    let ended = finished(&mut fixture);
    let end = ended.iter().find(|end| end.composite_id == reduce).unwrap();
    assert_eq!(end.state, CompositeState::Failed);
    assert_eq!(end.failure.as_ref().unwrap().kind, REDUCER_CONTINUED_KIND);
    assert_eq!(fixture.scheduler.pending_len(), 0, "the step after it does not start");
}

#[test]
fn an_expansion_that_fails_fails_the_reduce_and_its_flow_and_one_that_is_cancelled_with_its_flow_stops_the_run() {
    let mut fixture = Fixture::leading();
    let (flow, reduce, expansion) = expanding(&mut fixture);
    fixture.scheduler.submit_members(items_batch(&reduce, 0, &[b"a"], false)).unwrap();
    fixture
        .scheduler
        .fail_with_message(&worker(), &expansion.task_run_id, "ListError", "the list could not be read")
        .unwrap();
    let ended = finished(&mut fixture);
    // Only the flow has a client to tell.
    let end = ended.iter().find(|end| end.composite_id == flow).unwrap();
    assert_eq!(end.state, CompositeState::Failed);
    assert_eq!(end.failure.as_ref().unwrap().kind, "ListError");
    assert_eq!(fixture.scheduler.composite_status(&reduce).unwrap().state, CompositeState::Failed);
    assert_eq!(fixture.scheduler.pending_len(), 0, "no step runs for the items it did send");
    assert_eq!(
        fixture.scheduler.submit_members(items_batch(&reduce, 1, &[b"b"], true)),
        Err(SubmitRejection::CompositeFinished)
    );

    let (flow, _, expansion) = expanding(&mut fixture);
    fixture.scheduler.cancel_composite(&flow).unwrap();
    assert_eq!(fixture.run_state(&expansion.task_run_id), TaskRunState::Cancelled);
}

#[test]
fn a_reduce_after_a_map_that_expanded_folds_the_members_the_expansion_streamed() {
    let mut fixture = Fixture::leading();
    let map = Stage::Composite(Step {
        kind: CompositeKind::Map,
        on_error: OnError::FailFast,
        stages: vec![task_stage("each", None)],
        streamed: false,
        reduce: None,
    });
    let flow = submitted(&mut fixture, flow_of(vec![task_stage("load", Some(b"x")), map, reduce_template(Some(2), None)]));
    run_member(&mut fixture, &flow, 0, b"[...]");
    let map = flow.child(1);
    let expansion = fixture.scheduler.request_claim(&worker(), &map.expansion_task()).unwrap();
    fixture.scheduler.report_started(&worker(), &expansion.task_run_id).unwrap();
    fixture.scheduler.submit_members(item_batch(&map, 0, 2, false)).unwrap();
    fixture
        .scheduler
        .complete(&worker(), &expansion.task_run_id, Digest::blake3(b""), Completion::Final)
        .unwrap();
    run_member(&mut fixture, &map, 0, b"x");
    run_member(&mut fixture, &map, 1, b"y");
    let reduce = flow.child(2);
    assert_eq!(
        fixture.spy.newest_composite(&reduce).unwrap().reduce_items,
        2,
        "the members the map's expansion streamed are the reduce's items"
    );
    run_member(&mut fixture, &reduce, 0, b"xy");
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
}
