//! The steal exchange: how a worker with nothing to run asks a shard peer
//! which tasks the peer holds records of that look claimable, and how a worker
//! answers.
//!
//! [`codec`] frames the `/kabudachi/steal/1` messages. The asking side is
//! [`Net::steal`], to the peer the caller names; [`Net::steal_targets`] says
//! which peers to ask, nearest first, from the records `kad`'s routing table.
//! That table is read afresh on every call and kept by no one: it routes the
//! asks and says nothing about who belongs to the shard. The answering side
//! is [`candidates_for_steal`], which every driver applies to each inbound
//! request: answering needs no leadership and no executor. An answer decides
//! nothing, because every task in it must still be claimed from the leader,
//! who may refuse one this worker's records showed as waiting.

use std::str::FromStr;
use std::time::Duration;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::{StealRequest, StealResponse};
use kabudachi_core::queues::{QueuePolicy, Subscription};
use kabudachi_core::time::WallTime;
use libp2p::PeerId;

use crate::discovery::{Takes, waited};
use crate::exchange::Asked;
use crate::messenger::Net;
use crate::peers::worker_id_of;
use crate::steal::codec::StealCodec;
use crate::task_store::{Candidate, HeldRecords};

pub mod codec;

/// The most task ids one steal answer carries, whatever the asker's limit:
/// enough for any batch a worker claims at once, small enough that a holder
/// of many records answers cheaply.
pub(crate) const MAX_STEAL_IDS: usize = 256;

/// How long [`Net::steal`] waits for a peer's answer. A peer that does not
/// answer (one whose driver is busy or gone) must not stall the whole stage
/// that asks its neighbours too, and a peer's answer is a read of its own
/// records, so one that takes longer is not worth waiting for.
pub(crate) const STEAL_TIMEOUT: Duration = Duration::from_secs(2);

/// An unanswered inbound `/kabudachi/steal/1` request, returned by
/// [`Net::poll_steal_requests`]. Answer it with [`Net::respond_steal`];
/// dropping it unanswered just lets the asker's substream eventually fail on
/// their side, the same contract as `ClaimRequestHandle`.
pub struct StealRequestHandle(Asked<StealCodec>);

impl StealRequestHandle {
    /// The `WorkerId` of whoever sent this request.
    pub fn from(&self) -> WorkerId {
        worker_id_of(&self.0.from)
    }

    /// The most task ids to answer with: what the request asks, capped at
    /// [`MAX_STEAL_IDS`].
    pub fn limit(&self) -> usize {
        usize::try_from(self.0.request.limit)
            .unwrap_or(MAX_STEAL_IDS)
            .min(MAX_STEAL_IDS)
    }

    /// Whether the asker runs compaction, so a compaction run may be offered.
    pub fn runs_compaction(&self) -> bool {
        self.0.request.runs_compaction
    }

    /// The queues the asker takes from, so only tasks of those are offered.
    pub fn queues(&self) -> Subscription {
        Subscription::from_names(&self.0.request.queues)
    }
}

impl Net {
    /// Asks `peer` which tasks it holds records of that look claimable, at
    /// most `limit` of them, oldest submission first. `None` if no answer
    /// came within [`STEAL_TIMEOUT`]: the peer could not be reached or did not
    /// answer, or this `Net` is the peer. A compaction run is offered only if
    /// `takes` says this worker runs them, and a task only if its queue is one
    /// `takes` names.
    pub async fn steal(&self, peer: WorkerId, limit: usize, takes: &Takes) -> Option<Vec<TaskId>> {
        if peer == self.local_worker_id() {
            return None;
        }
        let to = PeerId::from_str(peer.as_str()).ok()?;
        let limit = u32::try_from(limit).unwrap_or(u32::MAX);
        let asked = self.ask::<StealCodec>(
            to,
            StealRequest {
                limit,
                runs_compaction: takes.runs_compaction,
                queues: takes.queues.names(),
            },
        );
        let response = tokio::time::timeout(STEAL_TIMEOUT, asked).await.ok()??;
        Some(response.task_ids.into_iter().map(TaskId::from).collect())
    }

    /// Drains every inbound `/kabudachi/steal/1` request not yet answered.
    /// Answer each with [`Self::respond_steal`].
    pub fn poll_steal_requests(&self) -> Vec<StealRequestHandle> {
        self.take_asked::<StealCodec>()
            .into_iter()
            .map(StealRequestHandle)
            .collect()
    }

    /// Answers a request obtained from [`Self::poll_steal_requests`].
    /// Fire-and-forget like `respond_claim`.
    pub fn respond_steal(&self, handle: StealRequestHandle, task_ids: Vec<TaskId>) {
        let response = StealResponse {
            task_ids: task_ids.into_iter().map(Into::into).collect(),
        };
        self.answer::<StealCodec>(handle.0.channel, response);
    }
}

/// The answer to a steal request: the tasks of `held` that look claimable at
/// `now` in the asker's `queues`, most urgent first by `policy`, then oldest
/// submission first, at most `limit` and [`MAX_STEAL_IDS`]. A compaction run is
/// among them only when the asker `runs_compaction`.
pub(crate) fn candidates_for_steal(
    held: &HeldRecords,
    now: WallTime,
    limit: usize,
    runs_compaction: bool,
    queues: &Subscription,
    policy: &QueuePolicy,
) -> Vec<TaskId> {
    let mut found = held.claimable(now, runs_compaction, queues);
    found.sort_by(|a, b| {
        let rank = |candidate: &Candidate| {
            policy.rank(&candidate.queue, waited(now, candidate.submitted_at))
        };
        rank(b)
            .cmp(&rank(a))
            .then(
                a.submitted_at
                    .as_unix_millis()
                    .cmp(&b.submitted_at.as_unix_millis()),
            )
            .then(a.task.cmp(&b.task))
    });
    found
        .into_iter()
        .take(limit.min(MAX_STEAL_IDS))
        .map(|candidate| candidate.task)
        .collect()
}
