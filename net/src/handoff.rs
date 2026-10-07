//! What a drained worker does with the Task records it holds before it
//! exits: hand each to the holders its leader would place it on now, wait
//! for them to store it, and write again what was refused, until a deadline.
//!
//! A drained follower knows no voters, so it asks the leader it followed
//! where each record goes (see `Net::place_records`); a drained leader reads
//! the answer from its own configuration. Each copy goes as the worker holds
//! it, naming the worker as its publisher, so a holder keeps it whatever
//! holders the record names (see `Net::hand_off`). A copy a holder refuses
//! because it holds a newer revision needs no handing over: the leader wrote
//! past it.

use std::collections::BTreeMap;
use std::time::Duration;

use kabudachi_core::election::HandOffTo;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::task_response;
use kabudachi_core::task_record::{VersionOrder, identify};
use libp2p::futures::future::join_all;
use tokio::time::{Instant, sleep, timeout_at};

use crate::messenger::{HandOffWrite, Net};
use crate::task_exchange::MAX_PLACE_IDS;
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
    pub abandoned: Vec<TaskId>,
}

/// Where a record goes: the holders and how many of them must store it.
type Placed = BTreeMap<TaskId, (Vec<WorkerId>, usize)>;

/// What asking where `tasks` go got.
#[derive(Default)]
struct Round {
    placed: Placed,
    /// Tasks whose placement no one answered: asked again after the delay.
    unanswered: Vec<TaskId>,
    /// Tasks the answer left out: no voter could hold them.
    unplaceable: Vec<TaskId>,
}

/// Hands every record this worker holds to the holders `to` says, writing
/// again after `retry_after` any a holder refused or no one answered for, and
/// stopping at `deadline`. Records still not stored then are abandoned and
/// logged.
pub(crate) async fn hand_off_held_records(
    net: &Net,
    to: HandOffTo,
    factor: ReplicationFactor,
    retry_after: Duration,
    deadline: Instant,
) -> HandedOff {
    let mut done = HandedOff::default();
    let mut waiting = net.held_records().task_ids();
    while !waiting.is_empty() && Instant::now() < deadline {
        let Some(round) = place(net, &to, &waiting, factor, deadline).await else {
            break;
        };
        done.abandoned.extend(round.unplaceable);
        let mut again = round.unanswered;
        let placeable: Vec<TaskId> = round.placed.keys().cloned().collect();
        for chunk in placeable.chunks(IN_FLIGHT) {
            again.extend(hand_over(net, chunk, &round.placed, deadline, &mut done).await);
        }
        waiting = again;
        if !waiting.is_empty() {
            sleep(retry_after.min(deadline.saturating_duration_since(Instant::now()))).await;
        }
    }
    done.abandoned.extend(waiting);
    for task in &done.abandoned {
        tracing::warn!(task = task.as_str(), "a drained worker could not hand a record over");
    }
    done
}

/// Where each of `tasks` goes, if anyone can say: the leader a follower
/// followed answers, a leader's own voters place locally. `None` when no one
/// can, which ends the hand-off.
async fn place(
    net: &Net,
    to: &HandOffTo,
    tasks: &[TaskId],
    factor: ReplicationFactor,
    deadline: Instant,
) -> Option<Round> {
    let mut round = Round::default();
    match to {
        HandOffTo::Nobody => return None,
        HandOffTo::Voters(voters) => {
            for task in tasks {
                match placement(task, voters, factor) {
                    Some(placed) => {
                        round.placed.insert(task.clone(), (placed.holders, placed.quorum));
                    }
                    None => round.unplaceable.push(task.clone()),
                }
            }
        }
        HandOffTo::Leader(leader) => {
            for page in tasks.chunks(MAX_PLACE_IDS) {
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
                    if let Some(task) = key.task_id {
                        placed.insert(
                            TaskId::from(task),
                            (
                                key.holders.into_iter().map(WorkerId::from).collect(),
                                usize::try_from(key.quorum).unwrap_or(usize::MAX),
                            ),
                        );
                    }
                }
                for task in page {
                    match placed.remove(task) {
                        Some(placement) => {
                            round.placed.insert(task.clone(), placement);
                        }
                        None => round.unplaceable.push(task.clone()),
                    }
                }
            }
        }
    }
    Some(round)
}

/// Hands each of `tasks` to the holders `placed` names, and returns those
/// that were neither stored nor superseded. A record no longer held (a
/// finished one past its retention) needs nothing.
async fn hand_over(
    net: &Net,
    tasks: &[TaskId],
    placed: &Placed,
    deadline: Instant,
    done: &mut HandedOff,
) -> Vec<TaskId> {
    let held = net.held_records();
    let mut sent = Vec::new();
    for task in tasks {
        let (Some(record), Some((holders, quorum))) = (held.get(task), placed.get(task)) else {
            continue;
        };
        let stored = net.hand_off(HandOffWrite {
            record: record.clone(),
            holders: holders.clone(),
            quorum: *quorum,
        });
        sent.push((task.clone(), record, stored));
    }
    let mut unstored = Vec::new();
    for (task, record, stored) in sent {
        if timeout_at(deadline, stored).await.unwrap_or(false) {
            done.stored += 1;
        } else {
            unstored.push((task, record));
        }
    }
    // Which refusals mean the leader wrote past the copy, looked up together
    // and no later than the deadline.
    let superseded = join_all(unstored.iter().map(|(task, record)| async move {
        timeout_at(deadline, is_superseded(net, task, record)).await.unwrap_or(false)
    }))
    .await;
    let mut refused = Vec::new();
    for ((task, _), superseded) in unstored.into_iter().zip(superseded) {
        if superseded {
            done.superseded += 1;
        } else {
            refused.push(task);
        }
    }
    refused
}

/// Whether a newer revision of `task`'s record than `record` is held
/// anywhere the lookup reaches.
async fn is_superseded(net: &Net, task: &TaskId, record: &TaskRecord) -> bool {
    let Ok((_, ours)) = identify(record) else {
        return false;
    };
    let Some(found) = net.get_record(task.clone()).await else {
        return false;
    };
    identify(&found).is_ok_and(|(_, theirs)| ours.order(&theirs) == VersionOrder::Newer)
}
