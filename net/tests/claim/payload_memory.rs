//! The ledger of blob payloads a process holds whole while they cross the
//! network, and the split of a worker's memory limits it is given from.

use std::time::Duration;

use kabudachi_core::scheduler::MemoryLimits;
use kabudachi_net::payload_memory::{PayloadMemory, TooLarge, payload_shares};

#[tokio::test]
async fn payload_memory_waits_for_room_and_refuses_only_a_payload_alone_past_the_hard_limit() {
    let memory = PayloadMemory::new(Some(MemoryLimits {
        soft: 50,
        hard: 100,
    }));
    let first = memory.try_take(60).expect("fits alone").expect("fits now");
    assert_eq!(memory.room(), Some(40));
    assert!(
        memory.try_take(60).expect("fits alone").is_none(),
        "not now"
    );
    assert_eq!(
        memory.try_take(101).err(),
        Some(TooLarge {
            size: 101,
            limit: 100
        })
    );

    let waiting = tokio::spawn({
        let memory = memory.clone();
        async move { memory.take_when_room(60).await.map(drop) }
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished(), "it waits while the first is held");
    drop(first);
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the release wakes the waiter")
        .expect("joined")
        .expect("fits once the first is released");

    // Bytes already in memory are counted even past the limit, and leave no room.
    let held = memory.take_anyway(90).expect("fits alone");
    let more = memory.take_anyway(20).expect("fits alone");
    assert_eq!((memory.in_use(), memory.room()), (110, Some(0)));
    drop((held, more));
    assert_eq!(memory.in_use(), 0);

    let unlimited = PayloadMemory::new(None);
    assert!(
        unlimited
            .try_take(u64::MAX / 2)
            .expect("no limit")
            .is_some()
    );
    assert_eq!(unlimited.room(), None);

    // A process gives half its limits to payloads in flight and the rest of
    // its hard limit to the blobs it stores, so the two shares add up.
    let (in_flight, store) = payload_shares(Some(MemoryLimits {
        soft: 51,
        hard: 101,
    }));
    assert_eq!(in_flight, Some(MemoryLimits { soft: 25, hard: 50 }));
    assert_eq!(store, Some(51));
    assert_eq!(payload_shares(None), (None, None));
}
