//! Blobs over the network: stored by a quorum of the placement they are
//! written to; fetched from the holders of their owner's record first, and
//! by their digest from any peer of the shard only if those miss.

use std::time::Duration;

use kabudachi_core::blob::{BlobKey, BlobOwner, MAX_BLOB_BYTES};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{ShardId, TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{Placement, RecordVersion};
use kabudachi_net::messenger::{Net, PlacedWrite, PutBlobError};
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::net::connect_to;

const TEST_TIMEOUT: Duration = Duration::from_secs(20);

/// A writer, two holders and a reader of one shard. The writer dialed the
/// holders, and the holders dialed the reader, so the reader never dialed
/// either.
async fn writer_holders_reader() -> [Net; 4] {
    let shard = ShardId::new("shard-1");
    let [writer, first, second, reader] =
        std::array::from_fn(|_| Net::for_shard(shard.name(), None));
    let reader_address = reader.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
    for holder in [&first, &second] {
        let address = holder.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
        connect_to(holder, &address, &writer).await;
        connect_to(&reader, &reader_address, holder).await;
    }
    [writer, first, second, reader]
}

fn owned_by(task: &str) -> BlobOwner {
    BlobOwner::Task(TaskId::new(task))
}

/// `task`'s unfinished record, placed on `holders`.
fn record_on(task: &str, holders: &[WorkerId]) -> TaskRecord {
    TaskRecord {
        version: Some(
            RecordVersion { recovery_epoch: RecoveryEpoch::new(0, 0), leader_term: 1, revision: 0 }
                .into(),
        ),
        task: Some(Task {
            task_id: Some(TaskId::new(task).into()),
            task_definition_id: Some(TaskDefinitionId::new("definition").into()),
            ..Task::default()
        }),
        placement: holders.iter().cloned().map(Into::into).collect(),
        ..TaskRecord::default()
    }
}

async fn blob_requests(net: &Net) -> u64 {
    net.diagnostics().await.traffic.blob_requests_received
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blob_is_stored_by_a_quorum_of_its_placement_and_fetched_from_its_owners_holders_first() {
    within_deadline(async {
        let [writer, first, second, reader] = writer_holders_reader().await;
        let holders = vec![first.local_worker_id(), second.local_worker_id()];
        let chunk = b"a chunk of some payload".to_vec();
        let key = BlobKey::of(&chunk);
        let placement = Placement { holders: holders.clone(), quorum: 2 };

        let stored = timeout(
            TEST_TIMEOUT,
            writer.put_blob(owned_by("task-1"), chunk.clone(), placement.clone()),
        )
        .await
        .expect("the write ended within the timeout");
        assert_eq!(stored, Ok(Digest::blake3(&chunk)));
        for holder in [&first, &second, &writer] {
            assert_eq!(
                holder.held_records().blob(&key).map(|record| record.value),
                Some(chunk.clone()),
                "each holder, and the writer itself, keeps a copy"
            );
        }

        // The owner's record names the holders: the reader, which holds
        // nothing, asks them directly.
        writer.write_records(vec![PlacedWrite::new(record_on("task-1", &holders), 2)]);
        let task = TaskId::new("task-1");
        timeout(TEST_TIMEOUT, wait_until(|| first.held_records().get(&task).is_some()))
            .await
            .expect("the record reached a holder");
        let asked_before = blob_requests(&first).await + blob_requests(&second).await;
        let found = timeout(TEST_TIMEOUT, reader.get_blob(owned_by("task-1"), &key.digest()))
            .await
            .expect("ended");
        assert_eq!(found, Some(chunk.clone()));
        assert!(
            blob_requests(&first).await + blob_requests(&second).await > asked_before,
            "the holders were asked directly"
        );

        // An owner whose record no one holds: the kad lookup still finds the
        // blob, with no direct ask; a blob no one holds is not found.
        let other = b"held for an owner with no record".to_vec();
        writer.put_blob(owned_by("task-2"), other.clone(), placement.clone()).await.expect("stored");
        let asked_before = blob_requests(&first).await + blob_requests(&second).await;
        let found =
            timeout(TEST_TIMEOUT, reader.get_blob(owned_by("task-2"), &BlobKey::of(&other).digest()))
                .await
                .expect("ended");
        assert_eq!(found, Some(other));
        assert_eq!(blob_requests(&first).await + blob_requests(&second).await, asked_before);
        let unknown = BlobKey::of(b"no one holds this");
        assert_eq!(
            timeout(TEST_TIMEOUT, reader.get_blob(owned_by("task-1"), &unknown.digest()))
                .await
                .expect("ended"),
            None
        );

        // No quorum can be had of a placement naming a peer no one answers to.
        let unreachable = Placement {
            holders: vec![first.local_worker_id(), WorkerId::new("not-a-peer")],
            quorum: 2,
        };
        assert_eq!(
            timeout(
                TEST_TIMEOUT,
                writer.put_blob(owned_by("task-1"), b"another".to_vec(), unreachable)
            )
            .await
            .expect("ended"),
            Err(PutBlobError::NotStored)
        );

        // A writer whose own store has no room keeps nothing and sends nothing.
        let held_by_writer = writer.held_records();
        held_by_writer.set_blob_store_limit(Some(held_by_writer.blob_bytes()));
        let refused = b"no room for this one".to_vec();
        assert_eq!(
            writer.put_blob(owned_by("task-1"), refused.clone(), placement.clone()).await,
            Err(PutBlobError::StoreFull)
        );
        assert_eq!(first.held_records().blob(&BlobKey::of(&refused)), None);

        assert_eq!(
            writer.put_blob(owned_by("task-1"), vec![0; MAX_BLOB_BYTES + 1], placement).await,
            Err(PutBlobError::TooLarge { size: MAX_BLOB_BYTES + 1, limit: MAX_BLOB_BYTES })
        );
    })
    .await
}
