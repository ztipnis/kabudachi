//! A worker's record store over the network: what a peer acknowledges it
//! has stored, and it refuses older, conflicting, malformed and other shards'
//! records.

use std::time::Duration;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{ShardId, TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{MAX_RECORD_BYTES, RecordVersion, Write};
use kabudachi_net::messenger::{Net, PlacedWrite};
use kabudachi_net::task_store::{HeldRecords, TaskRecordStore, record_key};
use libp2p::kad::store::RecordStore;
use libp2p::kad::{Record, RecordKey};
use prost::Message as _;
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::net::connect_to;

const TEST_TIMEOUT: Duration = Duration::from_secs(20);

fn version(number: u64, lineage: u64, term: u64, revision: u64) -> RecordVersion {
    RecordVersion {
        recovery_epoch: RecoveryEpoch::new(number, lineage),
        leader_term: term,
        revision,
    }
}

fn record(task: &str, version: RecordVersion, queue: &str) -> TaskRecord {
    TaskRecord {
        version: Some(version.into()),
        task: Some(Task {
            task_id: Some(TaskId::new(task).into()),
            task_definition_id: Some(TaskDefinitionId::new("definition").into()),
            queue: queue.to_owned(),
            ..Task::default()
        }),
        ..TaskRecord::default()
    }
}

fn shard(name: &str) -> ShardId {
    ShardId::new(name)
}

/// A writer of shard `a` connected to a holder of shard `b`.
async fn two_connected(a: ShardId, b: ShardId) -> (Net, Net) {
    let writer = Net::for_shard(a, None);
    let holder = Net::for_shard(b, None);
    let address = holder.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
    connect_to(&holder, &address, &writer).await;
    (writer, holder)
}

/// A reader of `shard` connected to two more of its workers, which it
/// dialed: the reader, then the other two.
async fn three_connected(shard: ShardId) -> [Net; 3] {
    let reader = Net::for_shard(shard.clone(), None);
    let first = Net::for_shard(shard.clone(), None);
    let second = Net::for_shard(shard, None);
    for holder in [&first, &second] {
        let address = holder.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
        connect_to(holder, &address, &reader).await;
    }
    [reader, first, second]
}

/// `record` as a leader writes it to `holders`: naming them as its placement.
fn placed_at(mut record: TaskRecord, holders: &[&WorkerId]) -> TaskRecord {
    record.placement = holders.iter().map(|holder| (*holder).clone().into()).collect();
    record
}

/// Writes every one of `records` at once to `holders` from `writer`, and
/// says, in order, whether each write reached its quorum. A refused write is
/// only known to have failed once the writer's write timeout passes, so
/// writes a test expects to be refused go together.
async fn write_all(
    writer: &Net,
    records: Vec<TaskRecord>,
    holders: &[&WorkerId],
    quorum: usize,
) -> Vec<bool> {
    let records: Vec<TaskRecord> = records
        .into_iter()
        .map(|record| placed_at(record, holders))
        .collect();
    let writes: Vec<Write> = records.iter().map(Write::of).collect();
    writer.write_records(
        records
            .into_iter()
            .map(|record| PlacedWrite { record, quorum })
            .collect(),
    );
    let mut outcomes: Vec<Option<bool>> = vec![None; writes.len()];
    timeout(TEST_TIMEOUT, async {
        loop {
            for outcome in writer.take_write_outcomes() {
                let slot = writes
                    .iter()
                    .position(|write| *write == outcome.write)
                    .expect("an outcome of a write this test made");
                outcomes[slot] = Some(outcome.stored);
            }
            if outcomes.iter().all(Option::is_some) {
                return;
            }
            writer.wait_for_arrival().await;
        }
    })
    .await
    .expect("every write ended within the timeout");
    outcomes.into_iter().map(|stored| stored.expect("ended")).collect()
}

async fn write_one(writer: &Net, record: TaskRecord, holders: &[&WorkerId], quorum: usize) -> bool {
    write_all(writer, vec![record], holders, quorum).await[0]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_acknowledges_only_records_it_stored() {
    within_deadline(async {
        let (writer, holder) = two_connected(shard("shard-1"), shard("shard-1")).await;
        let holder_id = holder.local_worker_id();

        let newer = record("task-1", version(0, 0, 1, 5), "newer");
        assert!(write_one(&writer, newer.clone(), &[&holder_id], 1).await);
        let held = placed_at(newer.clone(), &[&holder_id]);
        assert_eq!(holder.held_records().get(&TaskId::new("task-1")), Some(held.clone()));

        let older = record("task-1", version(0, 0, 1, 4), "older");
        let conflicting = record("task-1", version(0, 0, 1, 5), "different");
        assert_eq!(
            write_all(&writer, vec![older, conflicting], &[&holder_id], 1).await,
            [false, false],
            "neither an older revision nor a different one of the same version is acknowledged"
        );
        assert!(
            write_one(&writer, newer.clone(), &[&holder_id], 1).await,
            "an identical republish is"
        );
        assert_eq!(holder.held_records().get(&TaskId::new("task-1")), Some(held));
    })
    .await
}

/// What `reader` finds when it looks `task` up among its peers and itself.
async fn lookup(reader: &Net, task: &str) -> Option<TaskRecord> {
    timeout(TEST_TIMEOUT, reader.get_record(TaskId::new(task)))
        .await
        .expect("the lookup ended within the timeout")
}

fn queue_of(found: Option<TaskRecord>) -> Option<String> {
    found.and_then(|record| record.task).map(|task| task.queue)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lookup_finds_the_newest_revision_among_the_peers_and_the_reader_itself() {
    within_deadline(async {
        // The newest revision any peer holds, though one holds only an old one,
        // and nothing at all for a task no one holds.
        let [reader, stale, fresh] = three_connected(shard("shard-1")).await;
        let (stale_id, fresh_id) = (stale.local_worker_id(), fresh.local_worker_id());
        assert!(
            write_one(&reader, record("task-1", version(0, 0, 1, 1), "old"), &[&stale_id, &fresh_id], 2)
                .await
        );
        assert!(write_one(&reader, record("task-1", version(0, 0, 2, 0), "new"), &[&fresh_id], 1).await);
        assert_eq!(queue_of(lookup(&reader, "task-1").await).as_deref(), Some("new"));
        assert_eq!(lookup(&reader, "task-2").await, None, "no one holds task-2");

        // The reader's own copy counts: a newer revision only it holds.
        let (reader, holder) = two_connected(shard("shard-1"), shard("shard-1")).await;
        let holders = [&reader.local_worker_id(), &holder.local_worker_id()];
        assert!(write_one(&reader, record("task-1", version(0, 0, 1, 0), "mine"), &holders, 2).await);
        assert!(
            write_one(&reader, record("task-1", version(0, 0, 1, 1), "newer"), &[holders[0]], 1).await
        );
        assert_eq!(queue_of(lookup(&reader, "task-1").await).as_deref(), Some("newer"));

        // Holders that dialed the reader are found too, though it never dialed them.
        let [reader, writer, first, second] = reader_dialed_by_holders(shard("shard-1")).await;
        let holders = [&first.local_worker_id(), &second.local_worker_id()];
        assert!(write_one(&writer, record("task-1", version(0, 0, 1, 0), "held"), &holders, 2).await);
        assert_eq!(queue_of(lookup(&reader, "task-1").await).as_deref(), Some("held"));
    })
    .await
}

/// A reader of `shard` that only its two holders dialed (it never dialed
/// them), and a writer that dialed the holders: the reader, then the writer
/// and the two holders.
async fn reader_dialed_by_holders(shard: ShardId) -> [Net; 4] {
    let reader = Net::for_shard(shard.clone(), None);
    let writer = Net::for_shard(shard.clone(), None);
    let first = Net::for_shard(shard.clone(), None);
    let second = Net::for_shard(shard, None);
    let reader_address = reader.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
    for holder in [&first, &second] {
        let address = holder.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
        connect_to(holder, &address, &writer).await;
        connect_to(&reader, &reader_address, holder).await;
    }
    [reader, writer, first, second]
}

/// Polls `condition` until it holds, panicking at the timeout.
async fn eventually(what: &str, mut condition: impl AsyncFnMut() -> bool) {
    timeout(TEST_TIMEOUT, async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within the timeout"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_never_lands_in_another_shard_and_its_peer_is_never_a_routing_candidate() {
    within_deadline(async {
        let host = Net::for_shard(shard("shard-1"), None);
        let same_shard = Net::for_shard(shard("shard-1"), None);
        let other_shard = Net::for_shard(shard("shard-2"), None);
        let address = host.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
        for dialer in [&same_shard, &other_shard] {
            // Only a peer that advertises a listen address can be added.
            dialer.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
            connect_to(&host, &address, dialer).await;
        }
        let (same_id, other_id) = (same_shard.local_worker_id(), other_shard.local_worker_id());

        let stored = write_one(&host, record("task-1", version(0, 0, 1, 0), "q"), &[&other_id], 1).await;

        assert!(!stored);
        assert_eq!(other_shard.held_records().get(&TaskId::new("task-1")), None);

        // Both were identified by the host, which only an Identify gives it an
        // address for: the dialers connected inbound.
        eventually("the host identified both peers", async || {
            host.dialable_address(&same_id).await.is_some()
                && host.dialable_address(&other_id).await.is_some()
        })
        .await;

        let routed = host.records_routing_peers().await;
        assert!(routed.contains(&same_id), "a peer of the same shard is a routing candidate");
        assert!(!routed.contains(&other_id), "a peer of another shard is not");
    })
    .await
}

// A store is what kad hands every record it is asked to keep, whatever a
// peer sent: it keeps only a decodable record, of the key it is stored
// under, with a version, no larger than the largest record.
#[test]
fn the_store_keeps_only_what_is_a_well_formed_record_of_its_own_key() {
    let held = HeldRecords::new(None);
    let mut store = TaskRecordStore::new(held.clone());
    let task = TaskId::new("task-1");
    let put = |store: &mut TaskRecordStore, key: RecordKey, value: Vec<u8>| {
        store.put(Record::new(key, value)).is_ok()
    };
    let stored = record("task-1", version(0, 0, 1, 1), "q");

    assert!(!put(&mut store, record_key(&task), b"not a record".to_vec()), "undecodable");
    assert!(
        !put(&mut store, RecordKey::new(&"other"), stored.encode_to_vec()),
        "a record stored under another task's key"
    );
    assert!(
        !put(&mut store, record_key(&task), vec![0; MAX_RECORD_BYTES as usize + 1]),
        "a value past the largest record"
    );
    let mut unversioned = stored.clone();
    unversioned.version = None;
    assert!(
        !put(&mut store, record_key(&task), unversioned.encode_to_vec()),
        "a record with no version"
    );
    assert!(held.task_ids().is_empty());

    assert!(put(&mut store, record_key(&task), stored.encode_to_vec()));
    assert_eq!(held.task_ids(), vec![task.clone()]);
    let served = store.get(&record_key(&task)).expect("the record is served");
    assert_eq!(TaskRecord::decode(&served.value[..]).unwrap(), stored);
}
