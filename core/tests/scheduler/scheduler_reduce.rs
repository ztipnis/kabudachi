//! Reduces one leader drives: a reduce takes its items in batches into its
//! own item pages before any step runs.

use kabudachi_core::blob::BlobKey;
use kabudachi_core::manifest::{Manifest, ManifestEntry};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::composite_entry;
use kabudachi_core::scheduler::composite::{
    CompositeState, ITEM_PAGES, ItemInput, MemberBatch, PAGE_ENTRIES, REDUCE_EMPTY_KIND, ReduceNode, StreamedMember,
};
use kabudachi_core::scheduler::{Claim, ClaimRejection, Completion, SubmitRejection};
use kabudachi_core::task_record::ShardRecord;

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
fn a_reduce_keeps_only_its_partly_filled_item_page_so_a_stream_twice_the_hard_limit_goes_through() {
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
        assert_eq!(items_on_page(&fixture, &reduce, page as u32).len() as u64, PAGE_ENTRIES);
        assert_eq!(fixture.scheduler.memory_in_use(), in_use, "a full page is let go once it is written");
        for written in 0..=page as u32 {
            assert_eq!(
                fixture.spy.page_revisions(&reduce, ITEM_PAGES + written),
                1,
                "a full page is written once and never again"
            );
        }
    }
    // Four pages, twice the hard limit, went in: each full page was written
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
}

#[test]
fn a_reduce_with_nothing_to_fold_finishes_as_it_starts_and_a_seedless_empty_one_fails() {
    let cases = [
        (streamed_reduce(Some(4), Some(b"0")), &[][..], CompositeState::Succeeded, None),
        (streamed_reduce(None, None), &[&b"only"[..]][..], CompositeState::Succeeded, None),
        (streamed_reduce(None, None), &[][..], CompositeState::Failed, Some(REDUCE_EMPTY_KIND)),
    ];
    for (step, items, state, kind) in cases {
        let mut fixture = Fixture::leading();
        reduce_fed(&mut fixture, step, items);
        let finished = finished(&mut fixture);
        assert_eq!(fixture.scheduler.pending_len(), 0, "no step runs");
        assert_eq!(finished[0].state, state);
        assert_eq!(finished[0].failure.as_ref().map(|failure| failure.kind.as_str()), kind);
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
    fail_member(&mut fixture, &reduce, 0, "ValueError");
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

/// The value a seeded two-item chain ends with when its second step's worker
/// is lost after storing its result and before reporting it, or not at all.
fn chain_root(lose_the_second_step: bool) -> (Option<Digest>, usize) {
    let mut fixture = Fixture::leading();
    let reduce = reduce_fed(&mut fixture, streamed_reduce(None, Some(b"")), &[b"a", b"b"]);
    run_member(&mut fixture, &reduce, 0, b"a");
    if lose_the_second_step {
        let lost = start_member(&mut fixture, &reduce, 1);
        // Its result blob was stored; its COMPLETE never came.
        let gone = fixture.lose_silent(&worker());
        assert!(gone[0].replayed.is_some(), "the step is replayed");
        assert!(finished(&mut fixture).is_empty());
        let replay = run_member(&mut fixture, &reduce, 1, b"ab");
        assert_ne!(replay.task_run_id, lost.task_run_id);
        assert_eq!(operands_of(&replay), operands_of(&lost), "the replay folds the same operands");
    } else {
        run_member(&mut fixture, &reduce, 1, b"ab");
    }
    assert_eq!(finished(&mut fixture)[0].state, CompositeState::Succeeded);
    (value_of(&fixture, &reduce, 1), fixture.scheduler.runs_of(&reduce.member_task(1)).len())
}

#[test]
fn a_step_lost_after_storing_its_result_is_replayed_on_the_same_operands_and_the_reduce_ends_as_the_chain_does() {
    let (clean, _) = chain_root(false);
    let (replayed, runs) = chain_root(true);
    assert_eq!(replayed, clean);
    assert_eq!(runs, 2, "the lost run and its replay; only the replay is certified");
}
