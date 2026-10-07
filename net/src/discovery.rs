//! How a worker with room to run more finds work: first in the records it
//! holds itself, nearest keys first; then by asking shard peers, the nearest
//! distance class first and widening one class at a time; last by asking the
//! leader for the oldest pending tasks. Every task found is claimed from the
//! leader, which may refuse one this worker's view showed as waiting: that
//! view can be stale, and a refusal only moves discovery on.
//!
//! A worker keeps no list of peers for this. The peers to steal from are read
//! from the records `kad` routing table ([`Net::steal_targets`]) each time
//! [`Net::discover`] runs, and forgotten when it returns.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::time::Duration;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::{
    Claim, ClaimRejectReason, ClaimResponse, claim_response,
};
use kabudachi_core::time::WallTime;
use libp2p::PeerId;
use libp2p::futures::future::join_all;
use libp2p::kad::KBucketKey;

use crate::claim::ClaimFailure;
use crate::messenger::Net;
use crate::steal::MAX_STEAL_IDS;
use crate::task_store::record_key;

/// Where a claimed task was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// In this worker's own records.
    Own,
    /// In a peer's records; `class` counts the distance classes outward from
    /// this worker that held peers, 0 being the nearest.
    Peer { class: usize },
    /// The leader's oldest pending tasks.
    Oldest,
}

/// Why a discovery stopped before it tried every stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryStop {
    /// The leader named is this worker: its own claims are its scheduler's,
    /// so nothing was asked.
    ThisWorkerLeads,
    /// The leader refused as not leading, refused this worker as no member,
    /// or did not answer sensibly: asking more would get the same.
    LeaderRefused,
}

/// What one discovery found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Found {
    /// The claims granted, in the order they were granted, with where each
    /// task was found.
    pub claims: Vec<(Stage, Claim)>,
    /// Candidates the leader refused because this worker's view was stale:
    /// the task is taken, over, not yet due or unknown to the leader.
    pub stale: usize,
    /// Why discovery ended early, if it did. What was claimed before that is
    /// kept in `claims`.
    pub stopped: Option<DiscoveryStop>,
}

impl Net {
    /// Claims up to `limit` tasks from `leader`, looking in the stages above
    /// in order and stopping as soon as `limit` are claimed. Each candidate is
    /// tried once per call. `now` is the wall-clock time by which this worker's
    /// own records are judged due: a `Net` keeps no clock, so the caller passes
    /// the one its driver answers steal requests by, and every worker of a
    /// process then judges by the same time.
    ///
    /// A compaction run is a candidate only if `runs_compaction` says this
    /// worker runs them.
    pub async fn discover(
        &self,
        leader: WorkerId,
        limit: usize,
        now: WallTime,
        runs_compaction: bool,
    ) -> Found {
        let mut found = Found::default();
        if leader == self.local_worker_id() {
            found.stopped = Some(DiscoveryStop::ThisWorkerLeads);
            return found;
        }
        let mut tried = BTreeSet::new();
        let own = self.own_candidates(now, runs_compaction);
        if self
            .claim_each(&leader, Stage::Own, own, limit, &mut tried, &mut found)
            .await
            .is_break()
        {
            return found;
        }
        for (class, peers) in self.steal_targets().await.into_iter().enumerate() {
            if found.claims.len() >= limit {
                return found;
            }
            // Ids already tried take answer slots without being candidates, so
            // ask for as many more as were tried.
            let asked = (limit - found.claims.len() + tried.len()).min(MAX_STEAL_IDS);
            let answers = join_all(peers.into_iter().map(|peer| self.steal(peer, asked, runs_compaction))).await;
            let mut offered = BTreeSet::new();
            let candidates: Vec<TaskId> = answers
                .into_iter()
                .flatten()
                .flatten()
                .filter(|task| offered.insert(task.clone()))
                .collect();
            let stage = Stage::Peer { class };
            if self
                .claim_each(&leader, stage, candidates, limit, &mut tried, &mut found)
                .await
                .is_break()
            {
                return found;
            }
        }
        if found.claims.len() < limit {
            self.claim_oldest_into(&leader, limit, &mut found).await;
        }
        found
    }

    /// The tasks of this worker's own records that look claimable now,
    /// nearest first by the distance between a task's key and this worker's.
    fn own_candidates(&self, now: WallTime, runs_compaction: bool) -> Vec<TaskId> {
        let mut candidates: Vec<TaskId> = self
            .held_records()
            .claimable(now, runs_compaction)
            .into_iter()
            .map(|(_, task)| task)
            .collect();
        if let Ok(local) = PeerId::from_str(self.local_worker_id().as_str()) {
            let local = KBucketKey::from(local);
            candidates.sort_by_key(|task| KBucketKey::new(record_key(task)).distance(&local));
        }
        candidates
    }

    /// Asks the leader for each of `candidates` not yet tried, until `limit`
    /// are claimed. Breaks if the leader's answer means asking more is
    /// pointless.
    async fn claim_each(
        &self,
        leader: &WorkerId,
        stage: Stage,
        candidates: Vec<TaskId>,
        limit: usize,
        tried: &mut BTreeSet<TaskId>,
        found: &mut Found,
    ) -> ControlFlow<()> {
        for task in candidates {
            if found.claims.len() >= limit {
                break;
            }
            if !tried.insert(task.clone()) {
                continue;
            }
            match self.request_claim(leader.clone(), task).await {
                Ok(ClaimResponse {
                    result: Some(claim_response::Result::Accept(claim)),
                }) => found.claims.push((stage, claim)),
                Ok(ClaimResponse {
                    result: Some(claim_response::Result::Reject(reject)),
                }) => match ClaimRejectReason::try_from(reject.reason) {
                    Ok(
                        ClaimRejectReason::ClaimRejectTaskUnknown
                        | ClaimRejectReason::ClaimRejectNotReady
                        | ClaimRejectReason::ClaimRejectAlreadySelected
                        | ClaimRejectReason::ClaimRejectFinished
                        | ClaimRejectReason::ClaimRejectSuperseded
                        | ClaimRejectReason::ClaimRejectKeyBusy
                        | ClaimRejectReason::ClaimRejectCannotRun,
                    ) => found.stale += 1,
                    Ok(
                        ClaimRejectReason::ClaimRejectNotLeader
                        | ClaimRejectReason::ClaimRejectNotMember
                        | ClaimRejectReason::Unspecified,
                    )
                    | Err(_) => return stop(found, DiscoveryStop::LeaderRefused),
                },
                Ok(_) => return stop(found, DiscoveryStop::LeaderRefused),
                Err(failure) => return stop(found, stop_for(failure)),
            }
        }
        ControlFlow::Continue(())
    }

    /// The last stage: the leader's oldest pending tasks, as many as are
    /// still wanted.
    async fn claim_oldest_into(&self, leader: &WorkerId, limit: usize, found: &mut Found) {
        let wanted = u32::try_from(limit - found.claims.len()).unwrap_or(u32::MAX);
        match self.claim_oldest(leader.clone(), wanted).await {
            Ok(ClaimResponse {
                result: Some(claim_response::Result::Batch(batch)),
            }) => found
                .claims
                .extend(batch.claims.into_iter().map(|claim| (Stage::Oldest, claim))),
            Ok(_) => found.stopped = Some(DiscoveryStop::LeaderRefused),
            Err(failure) => found.stopped = Some(stop_for(failure)),
        }
    }
}

fn stop(found: &mut Found, why: DiscoveryStop) -> ControlFlow<()> {
    found.stopped = Some(why);
    ControlFlow::Break(())
}

fn stop_for(failure: ClaimFailure) -> DiscoveryStop {
    match failure {
        ClaimFailure::ThisWorkerLeads => DiscoveryStop::ThisWorkerLeads,
        ClaimFailure::Unanswered => DiscoveryStop::LeaderRefused,
    }
}

/// The wait before the next discovery after one that found nothing: doubling
/// from [`Self::MIN`] up to [`Self::MAX`], and back to none once one finds
/// work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IdleBackoff {
    next: Option<Duration>,
}

impl IdleBackoff {
    /// The wait after the first discovery that finds nothing.
    pub const MIN: Duration = Duration::from_millis(100);
    /// The longest wait.
    pub const MAX: Duration = Duration::from_millis(5_000);

    /// A discovery just claimed `claimed` tasks; how long to wait before the
    /// next.
    pub fn after(&mut self, claimed: usize) -> Duration {
        if claimed > 0 {
            self.next = None;
            return Duration::ZERO;
        }
        let wait = self.next.unwrap_or(Self::MIN);
        self.next = Some((wait * 2).min(Self::MAX));
        wait
    }
}
