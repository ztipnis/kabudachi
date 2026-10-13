//! How a worker with room to run more finds work: first in the records it
//! holds itself, nearest keys first; then by asking shard peers, the nearest
//! distance class first and widening one class at a time; last by asking the
//! leader for the oldest pending tasks. Only tasks of the queues this worker
//! takes are candidates, and each stage tries the most urgent first. Every
//! task found is claimed from the leader, which may refuse one this worker's
//! view showed as waiting: that view can be stale, and a refusal only moves
//! discovery on. A task of its own records whose input is newer than the
//! worker runs is not claimed: the worker tells the leader, which then hands
//! it that task no more.
//!
//! A worker keeps no list of peers for this. The peers to steal from are read
//! from the records `kad` routing table ([`Net::steal_targets`]) each time
//! [`Net::discover`] runs, and forgotten when it returns.

use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::time::Duration;

use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::{
    Claim, ClaimRejectReason, ClaimResponse, claim_response,
};
use kabudachi_core::queues::{QueuePolicy, Subscription};
use kabudachi_core::task_record::RecordKey;
use kabudachi_core::time::{Duration as CoreDuration, WallTime};
use kabudachi_core::versions::RunnableVersions;
use libp2p::PeerId;
use libp2p::futures::future::join_all;
use libp2p::kad::KBucketKey;

use crate::messenger::Net;
use crate::steal::MAX_STEAL_IDS;
use crate::task_store::Candidate;

/// What a worker takes when it looks for work, and how it ranks what it finds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Takes {
    /// Whether it runs compaction runs.
    pub runs_compaction: bool,
    /// The queues it takes from.
    pub queues: Subscription,
    /// How it ranks candidates (its own copy of the shard's queue policy).
    pub policy: QueuePolicy,
    /// The input version it runs of each task definition: its own-records
    /// stage skips, and reports to the leader, a task whose input is newer.
    pub runnable: RunnableVersions,
}

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
    /// The leader refused as not leading, refused this worker as no member,
    /// did not answer sensibly, or did not answer at all (a leader that is
    /// this worker is never asked): asking more would get the same.
    LeaderRefused,
}

/// What one discovery found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Found {
    /// The claims granted, in the order they were granted, with where each
    /// task was found.
    pub claims: Vec<(Stage, Claim)>,
    /// Candidates the leader refused because this worker's view was stale:
    /// the task is taken, over, not yet due or unknown to the leader, or held
    /// to queues this worker's view did not know.
    pub stale: usize,
    /// Candidates the leader refused because their queue had as many tasks
    /// running as it allows.
    pub full: usize,
    /// Why discovery ended early, if it did. What was claimed before that is
    /// kept in `claims`.
    pub stopped: Option<DiscoveryStop>,
}

/// What one discovery has done so far.
struct Search {
    found: Found,
    /// The candidates already asked for.
    tried: BTreeSet<TaskId>,
    /// The blob input bytes this worker can still take; `None` for no bound.
    room: Option<u64>,
}

impl Search {
    /// Whether a candidate known to carry `input` bytes as blobs is past the
    /// room left. A candidate whose input is not known is never.
    fn is_past_room(&self, input: Option<u64>) -> bool {
        matches!((input, self.room), (Some(input), Some(left)) if input > left)
    }

    /// Keeps `claim`, found at `stage`, and takes its blob input from the
    /// room left.
    fn granted(&mut self, stage: Stage, claim: Claim) {
        let input = claim.task.as_ref().and_then(|task| task.input_manifest.as_ref());
        let input = input.map_or(0, |manifest| manifest.total_bytes);
        self.room = self.room.map(|left| left.saturating_sub(input));
        self.found.claims.push((stage, claim));
    }
}

impl Net {
    /// Claims up to `limit` tasks from `leader`, looking in the stages above
    /// in order and stopping as soon as `limit` are claimed. Each candidate is
    /// tried once per call. `now` is the wall-clock time by which this worker's
    /// own records are judged due: a `Net` keeps no clock, so the caller passes
    /// the one its driver answers steal requests by, and every worker of a
    /// process then judges by the same time.
    ///
    /// A compaction run is a candidate only if `takes` says this worker runs
    /// them, and a task only if its queue is one `takes` names.
    ///
    /// `input_room` bounds the input bytes, carried as blobs, the claims may
    /// add up to (`None` for no bound). A candidate of this worker's own
    /// records whose input is past the room left is skipped without asking
    /// the leader. A peer's candidate carries no size, so it is asked for,
    /// and what it carries lowers the room left once it is granted; the last
    /// stage sends the room left to the leader, which passes over a task
    /// that does not fit it.
    pub async fn discover(
        &self,
        leader: WorkerId,
        limit: usize,
        now: WallTime,
        takes: &Takes,
        input_room: Option<u64>,
    ) -> Found {
        let mut search =
            Search { found: Found::default(), tried: BTreeSet::new(), room: input_room };
        let (own, newer) = self.own_candidates(now, takes);
        // A leader cannot send itself a request: its scheduler is told in
        // process. An error is ignored: the record then stays unblocked, so
        // the next round reports the task again, or another worker claims it.
        if leader != self.local_worker_id() {
            join_all(newer.into_iter().map(|(task, reason)| self.report_blocked(leader.clone(), task, reason, false)))
                .await;
        }
        if self.claim_each(&leader, Stage::Own, own, limit, &mut search).await.is_break() {
            return search.found;
        }
        for (class, peers) in self.steal_targets().await.into_iter().enumerate() {
            if search.found.claims.len() >= limit {
                return search.found;
            }
            // Ids already tried take answer slots without being candidates, so
            // ask for as many more as were tried.
            let asked = (limit - search.found.claims.len() + search.tried.len()).min(MAX_STEAL_IDS);
            let answers = join_all(peers.into_iter().map(|peer| self.steal(peer, asked, takes))).await;
            let mut offered = BTreeSet::new();
            let candidates: Vec<(TaskId, Option<u64>)> = answers
                .into_iter()
                .flatten()
                .flatten()
                .filter(|task| offered.insert(task.clone()))
                .map(|task| (task, None))
                .collect();
            let stage = Stage::Peer { class };
            if self.claim_each(&leader, stage, candidates, limit, &mut search).await.is_break() {
                return search.found;
            }
        }
        if search.found.claims.len() < limit {
            self.claim_oldest_into(&leader, limit, &takes.queues, &mut search).await;
        }
        search.found
    }

    /// The tasks of this worker's own records, in its queues, that look
    /// claimable now: most urgent first, then nearest (by the distance between
    /// a task's key and this worker's), then oldest; each with the bytes of
    /// input it carries as blobs.
    ///
    /// A task whose input is newer than `takes.runnable` is no candidate. The
    /// ones whose record does not yet show the leader was told they are
    /// blocked are returned beside, each with its reason, for the caller to
    /// report: this reports nothing itself.
    fn own_candidates(&self, now: WallTime, takes: &Takes) -> (Vec<(TaskId, Option<u64>)>, Vec<(TaskId, String)>) {
        let held = self.held_records();
        let mut candidates = held.claimable(now, takes.runs_compaction, &takes.queues);
        let mut newer = Vec::new();
        candidates.retain(|candidate| match held.newer_than(&candidate.task, &takes.runnable) {
            None => true,
            Some(input) => {
                if !input.reported {
                    newer.push((candidate.task.clone(), input.reason));
                }
                false
            }
        });
        let local = PeerId::from_str(self.local_worker_id().as_str())
            .ok()
            .map(KBucketKey::from);
        candidates.sort_by_key(|candidate: &Candidate| {
            (
                Reverse(
                    takes
                        .policy
                        .rank(&candidate.queue, waited(now, candidate.submitted_at)),
                ),
                local
                    .as_ref()
                    .map(|local| KBucketKey::new(RecordKey::Task(candidate.task.clone()).kad_bytes()).distance(local)),
                candidate.submitted_at.as_unix_millis(),
            )
        });
        let candidates = candidates
            .into_iter()
            .map(|candidate| {
                let input = held.blob_input_bytes(&candidate.task);
                (candidate.task, Some(input))
            })
            .collect();
        (candidates, newer)
    }

    /// Asks the leader for each of `candidates` not yet tried and not past
    /// the room left, until `limit` are claimed. Breaks if the leader's
    /// answer means asking more is pointless.
    async fn claim_each(
        &self,
        leader: &WorkerId,
        stage: Stage,
        candidates: Vec<(TaskId, Option<u64>)>,
        limit: usize,
        search: &mut Search,
    ) -> ControlFlow<()> {
        for (task, input) in candidates {
            if search.found.claims.len() >= limit {
                break;
            }
            if !search.tried.insert(task.clone()) || search.is_past_room(input) {
                continue;
            }
            match self.request_claim(leader.clone(), task).await {
                Ok(ClaimResponse {
                    result: Some(claim_response::Result::Accept(claim)),
                }) => search.granted(stage, claim),
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
                        | ClaimRejectReason::ClaimRejectCannotRun
                        | ClaimRejectReason::ClaimRejectNotSubscribed,
                    ) => search.found.stale += 1,
                    Ok(ClaimRejectReason::ClaimRejectQueueFull) => search.found.full += 1,
                    Ok(
                        ClaimRejectReason::ClaimRejectNotLeader
                        | ClaimRejectReason::ClaimRejectNotMember
                        | ClaimRejectReason::Unspecified,
                    )
                    | Err(_) => return stop(&mut search.found, DiscoveryStop::LeaderRefused),
                },
                Ok(_) => return stop(&mut search.found, DiscoveryStop::LeaderRefused),
                Err(_) => return stop(&mut search.found, DiscoveryStop::LeaderRefused),
            }
        }
        ControlFlow::Continue(())
    }

    /// The last stage: the leader's oldest pending tasks, as many as are
    /// still wanted and as fit the room left.
    async fn claim_oldest_into(
        &self,
        leader: &WorkerId,
        limit: usize,
        queues: &Subscription,
        search: &mut Search,
    ) {
        let wanted = u32::try_from(limit - search.found.claims.len()).unwrap_or(u32::MAX);
        match self.claim_oldest(leader.clone(), wanted, queues, search.room).await {
            Ok(ClaimResponse {
                result: Some(claim_response::Result::Batch(batch)),
            }) => {
                for claim in batch.claims {
                    search.granted(Stage::Oldest, claim);
                }
            }
            Ok(_) => search.found.stopped = Some(DiscoveryStop::LeaderRefused),
            Err(_) => search.found.stopped = Some(DiscoveryStop::LeaderRefused),
        }
    }
}

/// How long a task submitted at `submitted_at` has waited by `now`; zero if
/// the clocks disagree the other way.
pub(crate) fn waited(now: WallTime, submitted_at: WallTime) -> CoreDuration {
    CoreDuration::from_millis(
        now.as_unix_millis()
            .saturating_sub(submitted_at.as_unix_millis()),
    )
}

fn stop(found: &mut Found, why: DiscoveryStop) -> ControlFlow<()> {
    found.stopped = Some(why);
    ControlFlow::Break(())
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
