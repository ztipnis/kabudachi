use std::collections::BTreeMap;

use crate::protocol::digest::Digest;
use crate::protocol::ids::WorkerId;
use crate::time::{Duration, Instant};

/// What is known of one worker's disagreement with its leader.
#[derive(Debug, Clone, Default)]
struct Watched {
    /// When the heartbeats began to differ; none while they agree.
    differing_since: Option<Instant>,
    asked_at: Option<Instant>,
}

/// Decides when a worker's heartbeats disagree with its leader about the
/// runs it holds for long enough to ask it again. A difference that lasts
/// under two heartbeat intervals is only an answer still on its way.
#[derive(Debug, Clone)]
pub struct DriftWatch {
    persist: Duration,
    spacing: Duration,
    workers: BTreeMap<WorkerId, Watched>,
}

impl DriftWatch {
    /// `heartbeat_interval` sets how long a difference must last (two of
    /// them); `suspect_timeout` how far apart two re-reports of one worker
    /// must be.
    pub fn new(heartbeat_interval: Duration, suspect_timeout: Duration) -> Self {
        DriftWatch {
            persist: Duration::from_ticks(heartbeat_interval.as_ticks().saturating_mul(2)),
            spacing: suspect_timeout,
            workers: BTreeMap::new(),
        }
    }

    /// `worker`'s heartbeat said `heard`; the leader believes `expected`.
    /// Whether `worker` is due to be asked for its runs again now. A heartbeat
    /// that carries no digest says nothing, so it never differs. Asking is
    /// the caller's to record with [`DriftWatch::asked`]: a worker due but not
    /// asked stays due.
    pub fn heard(&mut self, worker: &WorkerId, heard: &[u8], expected: &Digest, now: Instant) -> bool {
        if heard.is_empty() || heard == expected.value() {
            if let Some(watched) = self.workers.get_mut(worker) {
                watched.differing_since = None;
            }
            return false;
        }
        let watched = self.workers.entry(worker.clone()).or_default();
        let since = *watched.differing_since.get_or_insert(now);
        let lasted = now - since >= self.persist;
        let spaced = watched
            .asked_at
            .is_none_or(|asked| now - asked >= self.spacing);
        lasted && spaced
    }

    /// `worker` was asked for its runs at `now`: not again for a suspicion
    /// timeout.
    pub fn asked(&mut self, worker: &WorkerId, now: Instant) {
        self.workers.entry(worker.clone()).or_default().asked_at = Some(now);
    }

    /// `worker` left the configuration, or the office ended: forget it.
    pub fn forget(&mut self, worker: &WorkerId) {
        self.workers.remove(worker);
    }
}
