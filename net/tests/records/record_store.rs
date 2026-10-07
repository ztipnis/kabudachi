//! A worker's record store over the network: what a peer acknowledges it
//! has stored, and it refuses older, conflicting and other shards' records.

use std::time::Duration;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{ShardId, TaskDefinitionId, TaskId, WorkerId};
use kabudachi_core::task_record::{RecordVersion, Write};
use kabudachi_net::messenger::{Net, PlacedWrite};
use tokio::time::timeout;

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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_never_lands_in_another_shard() {
    let (writer, stranger) = two_connected(shard("shard-1"), shard("shard-2")).await;

    let stored = write_one(
        &writer,
        record("task-1", version(0, 0, 1, 0), "q"),
        &[&stranger.local_worker_id()],
        1,
    )
    .await;

    assert!(!stored);
    assert_eq!(stranger.held_records().get(&TaskId::new("task-1")), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lookup_returns_the_newest_revision_any_holder_has() {
    let [reader, stale, fresh] = three_connected(shard("shard-1")).await;
    let (stale_id, fresh_id) = (stale.local_worker_id(), fresh.local_worker_id());
    assert!(
        write_one(&reader, record("task-1", version(0, 0, 1, 1), "old"), &[&stale_id, &fresh_id], 2)
            .await
    );
    assert!(write_one(&reader, record("task-1", version(0, 0, 2, 0), "new"), &[&fresh_id], 1).await);

    let found = timeout(TEST_TIMEOUT, reader.get_record(TaskId::new("task-1")))
        .await
        .expect("the lookup ended within the timeout")
        .expect("two peers hold it");

    assert_eq!(found.task.unwrap().queue, "new");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lookup_counts_the_readers_own_copy() {
    let (reader, holder) = two_connected(shard("shard-1"), shard("shard-1")).await;
    let holders = [&reader.local_worker_id(), &holder.local_worker_id()];
    assert!(write_one(&reader, record("task-1", version(0, 0, 1, 0), "mine"), &holders, 2).await);
    // A newer revision only the reader holds: no peer has it.
    assert!(
        write_one(&reader, record("task-1", version(0, 0, 1, 1), "newer"), &[&holders[0]], 1).await
    );

    let found = timeout(TEST_TIMEOUT, reader.get_record(TaskId::new("task-1")))
        .await
        .expect("the lookup ended within the timeout")
        .expect("the reader holds it");

    assert_eq!(found.task.unwrap().queue, "newer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lookup_of_a_record_no_one_holds_finds_nothing() {
    let [reader, _, _] = three_connected(shard("shard-1")).await;

    let found = timeout(TEST_TIMEOUT, reader.get_record(TaskId::new("task-1")))
        .await
        .expect("the lookup ended within the timeout");

    assert_eq!(found, None);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lookup_finds_records_of_holders_that_dialed_the_reader() {
    let [reader, writer, first, second] = reader_dialed_by_holders(shard("shard-1")).await;
    let holders = [&first.local_worker_id(), &second.local_worker_id()];
    assert!(write_one(&writer, record("task-1", version(0, 0, 1, 0), "held"), &holders, 2).await);

    let found = timeout(TEST_TIMEOUT, reader.get_record(TaskId::new("task-1")))
        .await
        .expect("the lookup ended within the timeout")
        .expect("both holders hold it");

    assert_eq!(found.task.unwrap().queue, "held");
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
async fn a_peer_of_another_shard_is_never_added_to_the_records_kad() {
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
}
