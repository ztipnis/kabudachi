//! Blobs over the network: stored by a quorum of the placement they are
//! written to; fetched from the holders of their owner's record first, and
//! by their digest from any peer of the shard only if those miss.

use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;

use kabudachi_core::blob::{BlobKey, BlobOwner, MAX_BLOB_BYTES};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::manifest::{Manifest, ManifestEntry};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{ShardId, TaskDefinitionId, TaskId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::{TaskRejectReason, claim_response, task_response};
use kabudachi_core::scheduler::{Claim, MemoryLimits, Submission, Submitted, mint};
use kabudachi_core::task_record::{Placement, RecordVersion};
use kabudachi_core::time::RealClock;
use kabudachi_net::messenger::{Net, PutBlobError};
use kabudachi_net::task_store::{TaskRecordStore, record_key};
use libp2p::kad::Record;
use libp2p::kad::store::RecordStore as _;
use libp2p::PeerId;
use prost::Message as _;
use kabudachi_net::task_exchange::{LeaderRetry, NotStored, placement_of};
use kabudachi_net::task_store::placement::ReplicationFactor;
use tokio::time::timeout;

use crate::support::deadline::within_deadline;
use crate::support::election::wait_until;
use crate::support::net::connect_to;
use crate::support::records::{Voters, plain_with, submitted_with};

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

        // The owner's record names the holders. The outsider belongs to another
        // shard, so its kad lookup reaches no holder of this one: only the
        // holders' direct answers can give it the blob. It holds the owner's
        // record itself, handed to it as a draining holder would.
        let outsider = Net::for_shard(ShardId::new("shard-2").name(), None);
        let outsider_address = outsider.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).await;
        for holder in [&first, &second] {
            connect_to(&outsider, &outsider_address, holder).await;
        }
        let mut handed = Record::new(record_key(&TaskId::new("task-1")), record_on("task-1", &holders).encode_to_vec());
        handed.publisher = PeerId::from_str(first.local_worker_id().as_str()).ok();
        TaskRecordStore::new(outsider.held_records()).put(handed).expect("the outsider keeps the record");
        let asked_before = blob_requests(&first).await + blob_requests(&second).await;
        let found = timeout(TEST_TIMEOUT, outsider.get_blob(owned_by("task-1"), &key.digest()))
            .await
            .expect("ended");
        assert_eq!(found, Some(chunk.clone()), "the holders' direct answer is the only way to the bytes");
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

        // A writer whose own store has no room is told so, and no holder had
        // stored the blob by then.
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

const MIB: usize = 1024 * 1024;

/// `len` bytes that differ from chunk to chunk.
fn varied(len: usize) -> Vec<u8> {
    (0..len).map(|at| (at % 251) as u8).collect()
}

fn two_holders() -> ReplicationFactor {
    ReplicationFactor::new(NonZeroUsize::new(2).expect("two"))
}

/// Has `client` store `input` where `leader` places `minted`'s record, then
/// submit `minted` with the manifest instead of the bytes. The manifest.
async fn submitted_as_blobs(
    client: &Net,
    leader: &WorkerId,
    minted: Submitted,
    input: &[u8],
) -> Manifest {
    let task = minted.task_id.clone();
    let placed = client.place_records(leader.clone(), vec![task.clone()]).await.expect("answered");
    let placement = placement_of(&placed, &task).expect("the leader places the task");
    let manifest = client.store_payload(&task, input, placement).await.expect("stored");
    submitted_with(
        client,
        leader,
        (task.as_str(), minted.submitted_at),
        minted.submission.clone().with_input_manifest(manifest.clone()),
    )
    .await;
    manifest
}

/// `claimant` claims `task` from `leader`, asking again until it is due.
async fn claimed_when_due(claimant: &Net, leader: &WorkerId, task: &TaskId) -> Claim {
    loop {
        let answer = claimant.request_claim(leader.clone(), task.clone()).await.expect("answered");
        if let Some(claim_response::Result::Accept(wire)) = answer.result {
            return Claim::try_from(&wire).expect("a well-formed claim");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Why `leader` refused `submission` at once, as a submitter is told.
async fn refusal_of(client: &Net, leader: &WorkerId, submission: Submission) -> i32 {
    let minted = mint(submission, &Uuid7Ids, &RealClock::new());
    let answer = client.submit(leader.clone(), minted).await.expect("answered");
    let Some(task_response::Result::Reject(reject)) = answer.result else {
        panic!("expected a refusal, got {answer:?}");
    };
    reject.reason
}

/// With every store holding all it can, the shard cannot take a new input
/// now: `submitter` is told so, as backpressure, not as a failure.
async fn a_new_input_is_refused_while_every_store_is_full(stores: &[&Net], submitter: &Net) {
    for net in stores {
        net.held_records().set_blob_store_limit(Some(net.held_records().blob_bytes()));
    }
    let crowded = mint(plain_with(b""), &Uuid7Ids, &RealClock::new()).task_id;
    let quick = LeaderRetry {
        answer_within: Duration::from_secs(2),
        ask_again_after: Duration::from_millis(50),
    };
    assert_eq!(
        submitter.store_payload_at_leader(&crowded, &varied(MIB), quick).await,
        Err(NotStored::Refused)
    );
    for net in stores {
        net.held_records().set_blob_store_limit(None);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_input_reaches_its_claimant_and_a_drained_holder_hands_its_blobs_over() {
    within_deadline(async {
        let (mut shard, client) = Voters::start_with(4, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        // Small configured limits, never the machine's memory.
        let limits = MemoryLimits { soft: 4 * MIB as u64, hard: 8 * MIB as u64 };
        shard.with(leader, move |_, scheduler| scheduler.set_memory_limits(Some(limits))).await;
        shard.join_as_pending(&client, leader).await;
        let nets = shard.nets.clone();
        let handed_off = shard.handed_off();

        shard
            .drive_until(async {
                let input = varied(3 * MIB / 2);
                let minted = mint(plain_with(b""), &Uuid7Ids, &RealClock::new());
                let task = minted.task_id.clone();
                let manifest = submitted_as_blobs(&client, &leader_id, minted, &input).await;
                let owner = BlobOwner::Task(task.clone());
                let keys: Vec<BlobKey> = manifest.blob_keys().cloned().collect();

                // A voter other than the leader holds every blob: it drains
                // below, and some other voter claims.
                let drainer = (0..nets.len())
                    .find(|voter| {
                        let held = nets[*voter].held_records();
                        *voter != leader && keys.iter().all(|key| held.blob(key).is_some())
                    })
                    .expect("one of the two holders is not the leader");
                let claimant = (0..nets.len())
                    .find(|voter| *voter != leader && *voter != drainer)
                    .expect("a voter besides the leader and the drainer");

                // A claimant without room for the input is passed over, and
                // the task stays pending; one with room gets it.
                let room = Some(MIB as u64);
                let small = nets[claimant].claim_oldest(leader_id.clone(), 1, room).await;
                let small = small.expect("answered");
                assert!(
                    matches!(
                        &small.result,
                        Some(claim_response::Result::Batch(batch)) if batch.claims.is_empty()
                    ),
                    "{small:?}"
                );

                // Whichever voter claims it fetches the input back whole.
                let claim = claimed_when_due(&nets[claimant], &leader_id, &task).await;
                let carried = claim.input_manifest().expect("readable").expect("a manifest input");
                assert_eq!(carried, manifest);
                let resolved = nets[claimant].resolve_manifest(&owner, &carried).await;
                assert_eq!(resolved, Ok(input.clone()));

                // The leader refuses for good an input that alone is past its
                // hard memory limit, whatever the blobs, and one that sends
                // bytes beside its manifest.
                let unfetched = ManifestEntry::Blob(BlobKey::of(b"never fetched"));
                let oversized = Manifest::new(vec![unfetched], 8 * MIB as u64 + 1);
                let too_large = plain_with(b"").with_input_manifest(oversized);
                assert_eq!(
                    refusal_of(&client, &leader_id, too_large).await,
                    TaskRejectReason::TaskRejectPayloadTooLarge as i32
                );
                let mut both = plain_with(b"").with_input_manifest(manifest.clone());
                both.serialized_input = b"inline".to_vec();
                assert_eq!(
                    refusal_of(&client, &leader_id, both).await,
                    TaskRejectReason::TaskRejectMalformed as i32
                );

                let stores: Vec<&Net> =
                    nets.iter().map(|net| &**net).chain(std::iter::once(&client)).collect();
                a_new_input_is_refused_while_every_store_is_full(&stores, &nets[claimant]).await;

                // A voter holding the input's blobs drains: it hands each to
                // the owner's placement among the voters left, before its
                // driver returns.
                nets[drainer].request_drain();
                let handed = handed_off.returned(drainer).await;
                assert!(handed.blobs_abandoned.is_empty(), "{handed:?}");
                assert!(handed.blobs_stored >= keys.len(), "{handed:?}");
                wait_until(|| {
                    keys.iter().all(|key| {
                        (0..nets.len())
                            .filter(|voter| *voter != drainer)
                            .filter(|voter| nets[*voter].held_records().blob(key).is_some())
                            .count()
                            >= 2
                    })
                })
                .await;
            })
            .await;
    })
    .await
}
