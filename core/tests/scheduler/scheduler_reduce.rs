//! Reduces one leader drives: a reduce takes its items in batches into its
//! own item pages before any step runs.

use kabudachi_core::manifest::Manifest;
use kabudachi_core::protocol::generated::composite_entry;
use kabudachi_core::scheduler::SubmitRejection;
use kabudachi_core::scheduler::composite::{ITEM_PAGES, ItemInput, MemberBatch, PAGE_ENTRIES, StreamedMember};

use crate::support::composites::*;
use crate::support::scheduler::Fixture;

#[test]
fn a_reduces_items_fill_its_item_pages_in_order_once_each_and_its_last_batch_seals_it() {
    let mut fixture = Fixture::leading();
    let reduce = submitted(&mut fixture, streamed_reduce(None, None));
    let values: Vec<Vec<u8>> = (0..PAGE_ENTRIES + 2).map(item).collect();
    let values: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
    let split = (PAGE_ENTRIES - 1) as usize;
    let taken = fixture.scheduler.submit_members(items_batch(&reduce, 0, &values[..split], false)).unwrap();
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
    assert_eq!(fixture.scheduler.submit_members(mixed).unwrap().members, 2);
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
