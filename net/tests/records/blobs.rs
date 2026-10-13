//! Blobs over the network: stored by a quorum of the placement they are
//! written to; fetched from the holders of their owner's record first, and
//! by their digest from any peer of the shard only if those miss.

use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::blob::{BlobKey, BlobOwner, MAX_BLOB_BYTES};
use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::manifest::{BLOB_CHUNK_BYTES, Manifest, ManifestEntry};
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{Task, TaskRecord};
use kabudachi_core::protocol::ids::{ShardId, TaskDefinitionId, TaskId, Uuid7Ids, WorkerId};
use kabudachi_core::protocol::messages::{TaskRejectReason, claim_response, task_response};
use kabudachi_core::queues::Subscription;
use kabudachi_core::scheduler::{Claim, MemoryLimits, Submission, Submitted, mint};
use kabudachi_core::task_record::{Placement, RecordKey, RecordVersion};
use kabudachi_core::time::RealClock;
use kabudachi_net::blobs::BlobRole;
use kabudachi_net::messenger::{Net, PutBlobError};
use kabudachi_net::task_exchange::{LeaderRetry, NotStored, placement_of, placement_with_asker_of};
use kabudachi_net::task_store::placement::{ReplicationFactor, placement};
use kabudachi_net::task_store::TaskRecordStore;
use libp2p::PeerId;
use libp2p::kad::Record;
use libp2p::kad::store::RecordStore as _;
use prost::Message as _;
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
    let reader_address = reader
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .await;
    for holder in [&first, &second] {
        let address = holder
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .await;
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
            RecordVersion {
                recovery_epoch: RecoveryEpoch::new(0, 0),
                leader_term: 1,
                revision: 0,
            }
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
        let placement = Placement {
            holders: holders.clone(),
            quorum: 2,
        };

        let stored = timeout(
            TEST_TIMEOUT,
            writer.put_blob(owned_by("task-1"), chunk.clone(), placement.clone()),
        )
        .await
        .expect("the write ended within the timeout");
        assert_eq!(stored, Ok(Digest::blake3(&chunk)));
        for holder in [&first, &second] {
            assert_eq!(
                holder.held_records().blob(&key).map(|record| record.value),
                Some(chunk.clone()),
                "each holder keeps a copy"
            );
        }
        assert_eq!(
            writer.held_records().blob(&key),
            None,
            "a writer outside the placement keeps none: it would never be told the owner's record"
        );

        // The owner's record names the holders. The outsider belongs to another
        // shard, so its kad lookup reaches no holder of this one: only the
        // holders' direct answers can give it the blob. It holds the owner's
        // record itself, handed to it as a draining holder would.
        let outsider = Net::for_shard(ShardId::new("shard-2").name(), None);
        let outsider_address = outsider
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .await;
        for holder in [&first, &second] {
            connect_to(&outsider, &outsider_address, holder).await;
        }
        let mut handed = Record::new(
            libp2p::kad::RecordKey::new(&RecordKey::Task(TaskId::new("task-1")).kad_bytes()),
            record_on("task-1", &holders).encode_to_vec(),
        );
        handed.publisher = PeerId::from_str(first.local_worker_id().as_str()).ok();
        TaskRecordStore::new(outsider.held_records())
            .put(handed)
            .expect("the outsider keeps the record");
        let asked_before = blob_requests(&first).await + blob_requests(&second).await;
        let found = timeout(
            TEST_TIMEOUT,
            outsider.get_blob(owned_by("task-1"), &key.digest()),
        )
        .await
        .expect("ended");
        assert_eq!(
            found,
            Some(chunk.clone()),
            "the holders' direct answer is the only way to the bytes"
        );
        assert!(
            blob_requests(&first).await + blob_requests(&second).await > asked_before,
            "the holders were asked directly"
        );

        // An owner whose record no one holds: the kad lookup still finds the
        // blob, with no direct ask; a blob no one holds is not found.
        let other = b"held for an owner with no record".to_vec();
        writer
            .put_blob(owned_by("task-2"), other.clone(), placement.clone())
            .await
            .expect("stored");
        let asked_before = blob_requests(&first).await + blob_requests(&second).await;
        let found = timeout(
            TEST_TIMEOUT,
            reader.get_blob(owned_by("task-2"), &BlobKey::of(&other).digest()),
        )
        .await
        .expect("ended");
        assert_eq!(found, Some(other));
        assert_eq!(
            blob_requests(&first).await + blob_requests(&second).await,
            asked_before
        );
        let unknown = BlobKey::of(b"no one holds this");
        assert_eq!(
            timeout(
                TEST_TIMEOUT,
                reader.get_blob(owned_by("task-1"), &unknown.digest())
            )
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

        // A writer that is one of the holders keeps its own copy, which
        // counts towards the quorum: one more holder is enough for two.
        let among = Placement {
            holders: vec![writer.local_worker_id(), first.local_worker_id()],
            quorum: 2,
        };
        let mine = b"a blob its writer holds".to_vec();
        writer
            .put_blob(owned_by("task-3"), mine.clone(), among.clone())
            .await
            .expect("stored");
        assert_eq!(
            writer.held_records().blob(&BlobKey::of(&mine)).map(|record| record.value),
            Some(mine)
        );

        // A holder whose own store has no room is told so, and no other holder
        // had stored the blob by then.
        let held_by_writer = writer.held_records();
        held_by_writer.set_blob_store_limit(Some(held_by_writer.blob_bytes()));
        let refused = b"no room for this one".to_vec();
        assert_eq!(
            writer
                .put_blob(owned_by("task-1"), refused.clone(), among)
                .await,
            Err(PutBlobError::StoreFull)
        );
        assert_eq!(first.held_records().blob(&BlobKey::of(&refused)), None);

        assert_eq!(
            writer
                .put_blob(owned_by("task-1"), vec![0; MAX_BLOB_BYTES + 1], placement)
                .await,
            Err(PutBlobError::TooLarge {
                size: MAX_BLOB_BYTES + 1,
                limit: MAX_BLOB_BYTES
            })
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
    let placed = client
        .place_records(leader.clone(), vec![RecordKey::Task(task.clone())])
        .await
        .expect("answered");
    let placement = placement_with_asker_of(&placed, &RecordKey::Task(task.clone())).expect("the leader places the task");
    let manifest = client
        .store_payload(BlobRole::Input, &BlobOwner::Task(task.clone()), input, placement)
        .await
        .expect("stored");
    submitted_with(
        client,
        leader,
        (task.as_str(), minted.submitted_at),
        minted
            .submission
            .clone()
            .with_input_manifest(manifest.clone()),
    )
    .await;
    manifest
}

/// `claimant` claims `task` from `leader`, asking again until it is due.
async fn claimed_when_due(claimant: &Net, leader: &WorkerId, task: &TaskId) -> Claim {
    loop {
        let answer = claimant
            .request_claim(leader.clone(), task.clone())
            .await
            .expect("answered");
        if let Some(claim_response::Result::Accept(wire)) = answer.result {
            return Claim::try_from(&wire).expect("a well-formed claim");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Why `leader` refused `submission` at once, as a submitter is told.
async fn refusal_of(client: &Net, leader: &WorkerId, submission: Submission) -> i32 {
    let minted = mint(submission, &Uuid7Ids, &RealClock::new());
    let answer = client
        .submit(leader.clone(), minted)
        .await
        .expect("answered");
    let Some(task_response::Result::Reject(reject)) = answer.result else {
        panic!("expected a refusal, got {answer:?}");
    };
    reject.reason
}

/// With every store holding all it can, the shard cannot take a new input
/// now: `submitter` is told so, as backpressure, not as a failure.
async fn a_new_input_is_refused_while_every_store_is_full(stores: &[&Net], submitter: &Net) {
    for net in stores {
        net.held_records()
            .set_blob_store_limit(Some(net.held_records().blob_bytes()));
    }
    assert_eq!(new_input_stored(submitter).await, Err(NotStored::Refused));
    for net in stores {
        net.held_records().set_blob_store_limit(None);
    }
}

/// What `submitter` is told when it stores the blobs of a new task's input
/// at the leader, giving up after a short while.
async fn new_input_stored(submitter: &Net) -> Result<Manifest, NotStored> {
    let crowded = mint(plain_with(b""), &Uuid7Ids, &RealClock::new()).task_id;
    let quick = LeaderRetry {
        answer_within: Duration::from_secs(2),
        ask_again_after: Duration::from_millis(50),
    };
    // Bytes no holder has a chunk of already: a holder keeps a chunk it holds
    // for one more owner at no cost, however full it is.
    let unheld: Vec<u8> = (0..MIB).map(|at| (at % 241) as u8).collect();
    submitter
        .store_payload_at_leader(BlobRole::Input, &BlobOwner::Task(crowded.clone()), &unheld, quick)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_input_reaches_its_claimant_and_a_drained_holder_hands_its_blobs_over() {
    within_deadline(async {
        let (mut shard, client) = Voters::start_with(4, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        // Small configured limits, never the machine's memory.
        let limits = MemoryLimits {
            soft: 4 * MIB as u64,
            hard: 8 * MIB as u64,
        };
        shard
            .with(leader, move |_, scheduler| {
                scheduler.set_memory_limits(Some(limits))
            })
            .await;
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
                assert!(
                    keys.iter().all(|key| client.held_records().blob(key).is_none()),
                    "a submitter outside the placement keeps none of the blobs it stored"
                );

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
                let small = nets[claimant]
                    .claim_oldest(leader_id.clone(), 1, &Subscription::Every, room)
                    .await;
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
                let carried = claim
                    .input_manifest()
                    .expect("readable")
                    .expect("a manifest input");
                assert_eq!(carried, manifest);
                let resolved = nets[claimant].resolve_manifest(&owner, &carried).await;
                assert_eq!(resolved, Ok(input.clone()));

                // The leader refuses for good an input that alone is past its
                // hard memory limit, whatever the blobs, and one that sends
                // bytes beside its manifest.
                let oversized_bytes = 8 * MIB as u64 + 1;
                let unfetched: Vec<ManifestEntry> = (0..oversized_bytes.div_ceil(BLOB_CHUNK_BYTES as u64))
                    .map(|index| ManifestEntry::Blob(BlobKey::of(format!("never fetched {index}").as_bytes())))
                    .collect();
                let oversized = Manifest::new(unfetched, oversized_bytes);
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

                let stores: Vec<&Net> = nets
                    .iter()
                    .map(|net| &**net)
                    .chain(std::iter::once(&client))
                    .collect();
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

/// A new task whose `input` the client stores as blobs, then submits by
/// manifest, with the shard driven meanwhile: the task and its manifest.
async fn submit_blobs(shard: &mut Voters, client: &Net, leader: &WorkerId, input: &[u8]) -> (TaskId, Manifest) {
    let minted = mint(plain_with(b""), &Uuid7Ids, &RealClock::new());
    let task = minted.task_id.clone();
    let manifest = shard.drive_until(submitted_as_blobs(client, leader, minted, input)).await;
    (task, manifest)
}

fn shard_id(nets: &[Arc<Net>], voter: usize) -> WorkerId {
    nets[voter].local_worker_id()
}

/// The first voter of `count` not in `but`.
fn shard_other_than(but: &[usize], count: usize) -> usize {
    (0..count).find(|voter| !but.contains(voter)).expect("a voter left")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tasks_input_leaves_every_holder_when_it_finishes_and_follows_its_record_when_a_holder_dies() {
    within_deadline(async {
        let (mut shard, client) = Voters::start_with(4, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        shard.join_as_pending(&client, leader).await;
        let nets = shard.nets.clone();

        // A task whose input is stored as blobs, claimed and finished: its
        // input's blobs leave every holder at once, long before the record's
        // retention passes.
        let finished_input = varied(MIB + 1);
        let (finished, finished_manifest) = submit_blobs(&mut shard, &client, &leader_id, &finished_input).await;
        let finished_keys: Vec<BlobKey> = finished_manifest.blob_keys().cloned().collect();
        let claimant = shard_other_than(&[leader], nets.len());
        shard
            .drive_until(async {
                let claim = claimed_when_due(&nets[claimant], &leader_id, &finished).await;
                let carried = claim.input_manifest().expect("readable").expect("a manifest input");
                assert_eq!(
                    nets[claimant].resolve_manifest(&BlobOwner::Task(finished.clone()), &carried).await,
                    Ok(finished_input.clone())
                );

                // Every store has room for a chunk or two and no more: a new
                // input is refused, but this task's result, which is not
                // held to the share inputs are, lands.
                let stores: Vec<&Net> = nets.iter().map(|net| &**net).chain(std::iter::once(&client)).collect();
                for net in &stores {
                    net.held_records()
                        .set_blob_store_limit(Some(net.held_records().blob_bytes() + 300_000));
                }
                assert_eq!(new_input_stored(&nets[claimant]).await, Err(NotStored::Refused));
                let quick = LeaderRetry {
                    answer_within: Duration::from_secs(2),
                    ask_again_after: Duration::from_millis(50),
                };
                let result = varied(200_000);
                let stored = nets[claimant]
                    .store_payload_at_leader(BlobRole::Result, &BlobOwner::Task(finished.clone()), &result, quick)
                    .await
                    .expect("a result is stored where a new input is refused");
                let result_keys: Vec<BlobKey> = stored.blob_keys().cloned().collect();
                for net in &stores {
                    net.held_records().set_blob_store_limit(None);
                }

                nets[claimant]
                    .report_started(leader_id.clone(), claim.task_run_id.clone())
                    .await
                    .expect("answered");
                nets[claimant]
                    .complete(leader_id.clone(), claim.task_run_id.clone(), Digest::blake3(b"done"))
                    .await
                    .expect("answered");
                wait_until(|| {
                    finished_keys
                        .iter()
                        .all(|key| nets.iter().all(|net| net.held_records().blob(key).is_none()))
                })
                .await;
                // The result's blobs outlive the finish, which is what the
                // task's client fetches them by; only the input went.
                for key in &result_keys {
                    assert!(
                        nets.iter().any(|net| net.held_records().blob(key).is_some()),
                        "a result blob is kept after its owner finishes"
                    );
                }
            })
            .await;

        // Another task's input follows its record when a holder of it dies.
        let input = varied(3 * MIB / 2);
        let (task, manifest) = submit_blobs(&mut shard, &client, &leader_id, &input).await;
        let keys: Vec<BlobKey> = manifest.blob_keys().cloned().collect();

        // A holder of its record (not the leader) dies.
        let dead = (0..nets.len())
            .find(|voter| *voter != leader && nets[*voter].held_records().get(&task).is_some())
            .expect("a holder other than the leader");
        let dead_id = shard.id(dead);
        shard.kill(dead);
        let survivors: Vec<usize> = (0..nets.len()).filter(|voter| *voter != dead).collect();

        shard
            .drive_until(async {
                // The leader places the record off the dead holder, and the
                // blobs follow it: a majority of the new placement holds each.
                wait_until(|| {
                    survivors.iter().any(|voter| {
                        let Some(record) = nets[*voter].held_records().get(&task) else {
                            return false;
                        };
                        let placement: Vec<WorkerId> = record.placement.into_iter().map(WorkerId::from).collect();
                        !placement.is_empty()
                            && !placement.contains(&dead_id)
                            && keys.iter().all(|key| {
                                survivors
                                    .iter()
                                    .filter(|holder| placement.contains(&shard_id(&nets, **holder)))
                                    .filter(|holder| nets[**holder].held_records().blob(key).is_some())
                                    .count()
                                    >= placement.len() / 2 + 1
                            })
                    })
                })
                .await;

                // The submitter, which holds none of it, reads the input whole.
                assert!(keys.iter().all(|key| client.held_records().blob(key).is_none()));
                assert_eq!(
                    client.resolve_manifest(&BlobOwner::Task(task.clone()), &manifest).await,
                    Ok(input.clone())
                );
            })
            .await;
    })
    .await
}

/// A fresh task id whose record `voters` place on `member` (`member_holds`),
/// or not.
fn task_placed_with(voters: &[WorkerId], member: &WorkerId, member_holds: bool) -> TaskId {
    loop {
        let task = mint(plain_with(b""), &Uuid7Ids, &RealClock::new()).task_id;
        let real = placement(&RecordKey::Task(task.clone()), voters, two_holders()).expect("voters place a task");
        if real.holders.contains(member) == member_holds {
            return task;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_writer_among_the_nearest_voters_is_told_the_real_placement_and_keeps_a_copy_in_it() {
    within_deadline(async {
        let mut shard = Voters::start_without_client(4, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        let nets = shard.nets.clone();
        let voters: Vec<WorkerId> = (0..nets.len()).map(|voter| shard.id(voter)).collect();
        let writer = shard_other_than(&[leader], nets.len());
        let task = task_placed_with(&voters, &voters[writer], true);
        let real = placement(&RecordKey::Task(task.clone()), &voters, two_holders()).expect("placed");

        shard
            .drive_until(async {
                let answer = nets[writer]
                    .place_records(leader_id.clone(), vec![RecordKey::Task(task.clone())])
                    .await
                    .expect("answered");
                let told = placement_with_asker_of(&answer, &RecordKey::Task(task.clone())).expect("the leader places the task");
                assert_eq!(told.holders, real.holders, "the answer names the real holders");
                assert_eq!(told.quorum, real.quorum, "and their quorum");
                let others = placement_of(&answer, &RecordKey::Task(task.clone())).expect("placed without the writer");
                assert!(!others.holders.contains(&voters[writer]), "the writer left out of the plain placement");

                let payload = varied(MIB + 1);
                let manifest = nets[writer]
                    .store_payload(BlobRole::Input, &BlobOwner::Task(task.clone()), &payload, told)
                    .await
                    .expect("stored");
                let keys: Vec<BlobKey> = manifest.blob_keys().cloned().collect();
                assert!(
                    keys.iter().all(|key| nets[writer].held_records().blob(key).is_some()),
                    "the writer keeps its copy as one of the holders"
                );
                let holds_all = |voter: usize| keys.iter().all(|key| nets[voter].held_records().blob(key).is_some());
                let remote = |holder_ids: &[WorkerId]| -> Vec<usize> {
                    (0..nets.len()).filter(|voter| *voter != writer && holder_ids.contains(&voters[*voter])).collect()
                };
                let real_remote = remote(&real.holders);
                assert_eq!(real_remote.len(), real.holders.len() - 1);
                wait_until(|| real_remote.iter().filter(|voter| holds_all(**voter)).count() >= real.quorum - 1).await;
                for outside in (0..nets.len()).filter(|voter| *voter != writer && !real_remote.contains(voter)) {
                    assert!(
                        keys.iter().all(|key| nets[outside].held_records().blob(key).is_none()),
                        "a voter outside the placement holds none"
                    );
                }
            })
            .await;
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_writer_outside_the_nearest_voters_keeps_no_copy_and_a_lone_voter_places_itself() {
    within_deadline(async {
        let mut shard = Voters::start_without_client(4, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let leader_id = shard.id(leader);
        let nets = shard.nets.clone();
        let voters: Vec<WorkerId> = (0..nets.len()).map(|voter| shard.id(voter)).collect();
        let writer = shard_other_than(&[leader], nets.len());
        let task = task_placed_with(&voters, &voters[writer], false);
        shard
            .drive_until(async {
                let answer = nets[writer]
                    .place_records(leader_id.clone(), vec![RecordKey::Task(task.clone())])
                    .await
                    .expect("answered");
                let told = placement_with_asker_of(&answer, &RecordKey::Task(task.clone())).expect("the leader places the task");
                let manifest = nets[writer]
                    .store_payload(BlobRole::Input, &BlobOwner::Task(task.clone()), &varied(MIB + 1), told)
                    .await
                    .expect("stored");
                assert!(
                    manifest.blob_keys().all(|key| nets[writer].held_records().blob(key).is_none()),
                    "a writer outside the placement keeps none"
                );
            })
            .await;
    })
    .await;

    // A shard whose only voter is the writer: the placement is the writer
    // alone, which its own copy satisfies.
    within_deadline(async {
        let mut shard = Voters::start_without_client(1, two_holders()).await;
        let leader = shard.drive_until_a_leader().await;
        let net = shard.nets[leader].clone();
        let task = mint(plain_with(b""), &Uuid7Ids, &RealClock::new()).task_id;
        let quick = LeaderRetry {
            answer_within: Duration::from_secs(2),
            ask_again_after: Duration::from_millis(50),
        };
        let input = varied(MIB + 1);
        let manifest = shard
            .drive_until(timeout(
                TEST_TIMEOUT,
                net.store_payload_at_leader(BlobRole::Input, &BlobOwner::Task(task.clone()), &input, quick),
            ))
            .await
            .expect("the store ended within the timeout")
            .expect("a lone voter stores its own input");
        assert_eq!(net.resolve_manifest(&BlobOwner::Task(task), &manifest).await, Ok(input));
    })
    .await
}
