//! A node's request to leave its shard gracefully, from when it is asked
//! until the node drains.
//!
//! A node asked to drain in a state that cannot drain yet keeps the request
//! and drains when it next reaches `Active`, `LeaderReconciling` or
//! `Leader`. A node that leads does not leave at once: it keeps leading
//! until every other voter has reported a routing crawl since its
//! admission, so no worker is left knowing only this leader, or until its
//! drain wait limit passes. A leader that loses office while it waits keeps
//! the request, and waits afresh once it leads again.
//!
//! This part decides only when the node drains; the node carries the
//! departure out.

use crate::protocol::worker_state::WorkerState;
use crate::time::{Clock, Duration, Instant};

/// Where a node's request to drain stands.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainRequest {
    /// No drain asked for, or the node has drained.
    #[default]
    NotAsked,
    /// Asked in a state that cannot drain yet.
    Kept,
    /// The node leads and was asked to drain: it leaves once its other
    /// voters have crawled, or at `until` regardless.
    Waiting { until: Instant },
}

/// What the node does about a drain just asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Asked {
    /// Drain now.
    DrainNow,
    /// A wait began: see at once whether the node is already free to leave
    /// (see [`DrainRequest::leave_if_free`]).
    WaitForCrawls,
    /// Nothing now: the request is kept, already waited on, or moot.
    Nothing,
}

impl DrainRequest {
    /// A drain was asked for while the node is in `state`. An `Active` node
    /// drains at once. A node that leads starts waiting, unless it already
    /// waits. A node already draining, or one that can never drain again,
    /// ignores it; any other keeps it.
    pub(crate) fn ask(
        &mut self,
        state: WorkerState,
        clock: &impl Clock,
        wait_limit: Duration,
    ) -> Asked {
        match state {
            WorkerState::Active => Asked::DrainNow,
            WorkerState::LeaderReconciling | WorkerState::Leader => {
                if matches!(self, DrainRequest::Waiting { .. }) {
                    return Asked::Nothing;
                }
                *self = DrainRequest::Waiting {
                    until: clock.now() + wait_limit,
                };
                Asked::WaitForCrawls
            }
            WorkerState::Draining | WorkerState::Stopped => Asked::Nothing,
            _ => {
                *self = DrainRequest::Kept;
                Asked::Nothing
            }
        }
    }

    /// Whether a waiting node leaves now: once `others_crawled` says every
    /// other voter has crawled, or once its wait has run out. `false`, and
    /// nothing is asked, unless it waits.
    pub(crate) fn leave_if_free(
        &mut self,
        clock: &impl Clock,
        others_crawled: impl FnOnce() -> bool,
    ) -> bool {
        let DrainRequest::Waiting { until } = *self else {
            return false;
        };
        let free = others_crawled();
        if free || clock.now() >= until {
            *self = DrainRequest::NotAsked;
            return true;
        }
        false
    }

    /// The node lost office: a wait in progress becomes a kept request.
    pub(crate) fn office_lost(&mut self) {
        if let DrainRequest::Waiting { .. } = self {
            *self = DrainRequest::Kept;
        }
    }

    /// The node reached `reached`. Returns whether a kept request now
    /// applies, clearing it; the node then asks again in that state.
    pub(crate) fn take_kept_for(&mut self, reached: WorkerState) -> bool {
        let applies = *self == DrainRequest::Kept
            && matches!(
                reached,
                WorkerState::Active | WorkerState::LeaderReconciling | WorkerState::Leader
            );
        if applies {
            *self = DrainRequest::NotAsked;
        }
        applies
    }

    /// When a waiting node next has to look again: the end of its wait.
    pub(crate) fn wakes_at(&self) -> Option<Instant> {
        match self {
            DrainRequest::Waiting { until } => Some(*until),
            DrainRequest::NotAsked | DrainRequest::Kept => None,
        }
    }
}
