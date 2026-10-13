//! What a drained worker does with the records it holds (Tasks', composites'
//! and composite pages') before it exits: hand each to the holders its leader
//! would place it on now, wait
//! for them to store it, and write again what was refused, until a deadline.
//!
//! A drained follower knows no voters, so it asks the leader it followed
//! where each record goes (see `Net::place_records`); a drained leader reads
//! the answer from its own configuration. Each copy goes as the worker holds
//! it, naming the worker as its publisher, so a holder keeps it whatever
//! holders the record names (see `Net::hand_off`). A copy a holder refuses
//! because it holds a newer revision needs no handing over: the leader wrote
//! past it.
//!
//! Blobs go after the records, each to the placement of every task or
//! composite that owns it, by the same placement asks.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kabudachi_core::blob::{BlobKey, BlobOwner, BlobRecord};
use kabudachi_core::election::HandOffTo;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::task_response;
use kabudachi_core::task_record::{Placement, RecordKey, ShardRecord, VersionOrder};
use libp2p::futures::future::join_all;
use tokio::time::{Instant, sleep, timeout_at};

use crate::messenger::{HandOffWrite, Net, RecordLookup};
use crate::task_exchange::{MAX_PLACE_IDS, placed_record, record_key_of};
use crate::task_store::HeldRecords;
use crate::task_store::placement::{ReplicationFactor, placement};

/// The most copies handed over at once.
const IN_FLIGHT: usize = 64;

/// What a drained worker's hand-off did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HandedOff {
    /// Records enough holders stored.
    pub stored: usize,
    /// Records a holder already had a newer revision of: the leader wrote
    /// past this copy, and that write is the leader's to make last.
    pub superseded: usize,
    /// Records not stored by the deadline, or that no one could be asked to
    /// store.
    pub abandoned: Vec<RecordKey>,
    /// Blobs, counted once per owner, that a quorum of their owner's
    /// placement stored.
    pub blobs_stored: usize,
    /// Blobs not stored by the deadline for some owner, or whose owner no
    /// one could place.
    pub blobs_abandoned: Vec<BlobKey>,
}

/// Where a record goes: the holders and how many of them must store it.
type Placed = BTreeMap<RecordKey, (Vec<WorkerId>, usize)>;

/// What asking where `keys` go got.
#[derive(Default)]
struct Round {
    placed: Placed,
    /// Records whose placement no one answered: asked again after the delay.
    unanswered: Vec<RecordKey>,
    /// Records the answer left out: no voter could hold them.
    unplaceable: Vec<RecordKey>,
}

/// Hands every record this worker holds to the holders `to` says, writing
/// again after `retry_after` any a holder refused or no one answered for, and
/// stopping at `deadline`. Records still not stored then are abandoned and
/// logged, at error level, by id.
pub(crate) async fn hand_off_held_records(
    net: &Net,
    to: HandOffTo,
    factor: ReplicationFactor,
    retry_after: Duration,
    deadline: Instant,
) -> HandedOff {
    let mut done = HandedOff::default();
    let mut waiting = net.held_records().record_keys();
    while !waiting.is_empty() && Instant::now() < deadline {
        let Some(round) = place(net, &to, &waiting, factor, deadline).await else {
            break;
        };
        done.abandoned.extend(round.unplaceable);
        let mut again = round.unanswered;
        let placeable: Vec<RecordKey> = round.placed.keys().cloned().collect();
        for chunk in placeable.chunks(IN_FLIGHT) {
            again.extend(hand_over(net, chunk, &round.placed, deadline, &mut done).await);
        }
        waiting = again;
        if !waiting.is_empty() {
            sleep(retry_after.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    }
    done.abandoned.extend(waiting);
    if !done.abandoned.is_empty() {
        tracing::error!(
            count = done.abandoned.len(),
            records = ?done.abandoned,
            "this worker is leaving its shard with records no other worker took; they leave with it"
        );
    }
    hand_off_held_blobs(net, &to, factor, retry_after, deadline, &mut done).await;
    done
}

/// A blob this worker holds, and the task or composite that owns it.
type Owned = (BlobKey, BlobOwner);

/// A blob to write to the holders its owner's record would be placed on.
struct BlobHandOver {
    key: BlobKey,
    owner: BlobOwner,
    blob: BlobRecord,
    placement: Placement,
    /// Whether `owner` stored the blob as one of its results.
    result: bool,
}

/// Hands every blob this worker holds, for each owner it has, to the holders
/// the owner's record would be placed on, writing again after
/// `retry_after` what was not stored, until `deadline`.
async fn hand_off_held_blobs(
    net: &Net,
    to: &HandOffTo,
    factor: ReplicationFactor,
    retry_after: Duration,
    deadline: Instant,
    done: &mut HandedOff,
) {
    let held = net.held_records();
    let mut waiting = owned_blobs(&held);
    while !waiting.is_empty() && Instant::now() < deadline {
        let owners: BTreeSet<RecordKey> = waiting.iter().map(|(_, owner)| record_key_of(owner)).collect();
        let owners: Vec<RecordKey> = owners.into_iter().collect();
        let Some(round) = place(net, to, &owners, factor, deadline).await else {
            break;
        };
        let (handovers, mut again) = sorted(&held, waiting, &round, done);
        again.extend(hand_over_blobs(net, handovers, deadline, done).await);
        waiting = again;
        if !waiting.is_empty() {
            sleep(retry_after.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    }
    done.blobs_abandoned.extend(waiting.into_iter().map(|(key, _)| key));
    for key in &done.blobs_abandoned {
        tracing::warn!(blob = %key, "a drained worker could not hand a blob over");
    }
}

/// Each blob held, once for every owner it has.
fn owned_blobs(held: &HeldRecords) -> Vec<Owned> {
    let mut owned = Vec::new();
    for (key, owners) in held.blobs() {
        for owner in owners {
            owned.push((key.clone(), owner));
        }
    }
    owned
}

/// Splits `waiting` by what `round` says of each blob's owner: those to hand
/// over now, and those to ask about again. A blob no longer held needs
/// nothing, and one whose owner could not be placed is abandoned.
fn sorted(
    held: &HeldRecords,
    waiting: Vec<Owned>,
    round: &Round,
    done: &mut HandedOff,
) -> (Vec<BlobHandOver>, Vec<Owned>) {
    let mut handovers = Vec::new();
    let mut again = Vec::new();
    for (key, owner) in waiting {
        let Some(blob) = held.blob(&key) else {
            continue;
        };
        let record = record_key_of(&owner);
        match round.placed.get(&record) {
            Some((holders, quorum)) => {
                let placement = Placement { holders: holders.clone(), quorum: *quorum };
                let result = held.results_of(&owner).contains(&key);
                handovers.push(BlobHandOver { key, owner, blob, placement, result });
            }
            None if round.unanswered.contains(&record) => again.push((key, owner)),
            None => done.blobs_abandoned.push(key),
        }
    }
    (handovers, again)
}

/// Writes each of `handovers`, and returns those not stored by `deadline`.
async fn hand_over_blobs(
    net: &Net,
    handovers: Vec<BlobHandOver>,
    deadline: Instant,
    done: &mut HandedOff,
) -> Vec<Owned> {
    let mut unstored = Vec::new();
    for chunk in handovers.chunks(IN_FLIGHT) {
        let stored = join_all(chunk.iter().map(|handover| {
            let owner = handover.owner.clone();
            let (blob, placement) = (handover.blob.clone(), handover.placement.clone());
            timeout_at(deadline, net.put_blob_as_held(owner, blob, placement, handover.result))
        }))
        .await;
        for (handover, stored) in chunk.iter().zip(stored) {
            if matches!(stored, Ok(Ok(_))) {
                done.blobs_stored += 1;
            } else {
                unstored.push((handover.key.clone(), handover.owner.clone()));
            }
        }
    }
    unstored
}

/// Where each of `keys` goes, if anyone can say: the leader a follower
/// followed answers, a leader's own voters place locally. `None` when no one
/// can, which ends the hand-off.
async fn place(
    net: &Net,
    to: &HandOffTo,
    keys: &[RecordKey],
    factor: ReplicationFactor,
    deadline: Instant,
) -> Option<Round> {
    let mut round = Round::default();
    match to {
        HandOffTo::Nobody => return None,
        HandOffTo::Voters(voters) => {
            for key in keys {
                match placement(key, voters, factor) {
                    Some(placed) => {
                        round.placed.insert(key.clone(), (placed.holders, placed.quorum));
                    }
                    None => round.unplaceable.push(key.clone()),
                }
            }
        }
        HandOffTo::Leader(leader) => {
            for page in keys.chunks(MAX_PLACE_IDS) {
                let asked = timeout_at(deadline, net.place_records(leader.clone(), page.to_vec())).await;
                // A leader that has not answered, or does not lead yet, is
                // asked again after the delay.
                let Ok(Ok(response)) = asked else {
                    round.unanswered.extend_from_slice(page);
                    continue;
                };
                let Some(task_response::Result::Placements(answer)) = response.result else {
                    round.unanswered.extend_from_slice(page);
                    continue;
                };
                let mut placed = Placed::new();
                for key in answer.placements {
                    // An answer with no holder but the asker places the record
                    // nowhere a drained worker can hand it.
                    if let Some(record) = placed_record(&key).filter(|_| !key.holders.is_empty()) {
                        placed.insert(
                            record,
                            (
                                key.holders.into_iter().map(WorkerId::from).collect(),
                                usize::try_from(key.quorum).unwrap_or(usize::MAX),
                            ),
                        );
                    }
                }
                for key in page {
                    match placed.remove(key) {
                        Some(placement) => {
                            round.placed.insert(key.clone(), placement);
                        }
                        None => round.unplaceable.push(key.clone()),
                    }
                }
            }
        }
    }
    Some(round)
}

/// Hands each of `keys` to the holders `placed` names, and returns those
/// that were neither stored nor superseded. A record no longer held (a
/// finished one past its retention) needs nothing.
async fn hand_over(
    net: &Net,
    keys: &[RecordKey],
    placed: &Placed,
    deadline: Instant,
    done: &mut HandedOff,
) -> Vec<RecordKey> {
    let held = net.held_records();
    let mut sent = Vec::new();
    for key in keys {
        let (Some(record), Some((holders, quorum))) = (held.get_record(key), placed.get(key)) else {
            continue;
        };
        let stored = net.hand_off(HandOffWrite {
            record: record.clone(),
            holders: holders.clone(),
            quorum: *quorum,
        });
        sent.push((key.clone(), record, stored));
    }
    let mut unstored = Vec::new();
    for (key, record, stored) in sent {
        if timeout_at(deadline, stored).await.unwrap_or(false) {
            done.stored += 1;
        } else {
            unstored.push((key, record));
        }
    }
    // Which refusals mean the leader wrote past the copy, looked up together
    // and no later than the deadline.
    let superseded = join_all(unstored.iter().map(|(key, record)| async move {
        timeout_at(deadline, is_superseded(net, key, record)).await.unwrap_or(false)
    }))
    .await;
    let mut refused = Vec::new();
    for ((key, _), superseded) in unstored.into_iter().zip(superseded) {
        if superseded {
            done.superseded += 1;
        } else {
            refused.push(key);
        }
    }
    refused
}

/// Whether a newer revision of the record `key` names than `record` is held
/// anywhere the lookup reaches.
async fn is_superseded(net: &Net, key: &RecordKey, record: &ShardRecord) -> bool {
    let Ok((_, ours)) = record.identify() else {
        return false;
    };
    let RecordLookup::Found(found) = net.find_shard_record(key.clone()).await else {
        return false;
    };
    found.identify().is_ok_and(|(_, theirs)| ours.order(&theirs) == VersionOrder::Newer)
}
