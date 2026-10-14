//! A node's request to leave its shard gracefully, from when it is asked
//! until the node drains.
//!
//! A node asked to drain in a state that cannot drain yet keeps the request
//! and drains when it next reaches `Active`, `LeaderReconciling` or
//! `Leader`. A node that leads does not leave at once: it keeps leading
//! until every other voter has reported a routing crawl since its
//! admission, so no worker is left knowing only this leader, or until its
//! drain wait limit passes, and then hands its leadership over. It leaves
//! through the leader that takes over once that leader vouches for hearing
//! it. What happens when the hand-over does not go through depends on where
//! the node stands:
//!
//! - In office, with no successor having taken up the hand-over, it departs
//!   alone when the hand-over ends, a suspicion timeout after it began,
//!   whatever the drain wait limit is.
//! - Deposed, following the successor, it leaves through it at the drain wait
//!   limit after the hand-over began, if the successor has not vouched for
//!   hearing it by then.
//! - In `LeaderSuspect`, `RollCall` or `NoQuorum`, with no leader, it leaves
//!   without one at that limit.
//!
//! A leader that loses office while it waits keeps the request, and waits
//! afresh once it leads again.
//!
//! This part decides only when the node drains; the node carries the
//! departure out.

use crate::protocol::worker_state::WorkerState;
use crate::time::{Duration, Instant};

/// Where a node's request to drain stands.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainRequest {
    /// No drain asked for, or the node has drained.
    #[default]
    NotAsked,
    /// Asked in a state that cannot drain yet; a node still without a leader
    /// at `until` leaves without one.
    Kept { until: Instant },
    /// The node leads: it hands over once its other voters have crawled, or
    /// at `until` regardless; with no voter to hand to by then it leaves alone.
    Waiting { until: Instant },
    /// It handed its leadership over to leave: it leaves through the leader
    /// that took over once that leader vouches for hearing it, or at `until`
    /// regardless.
    HandingOver { until: Instant },
}

/// What the node does about a drain just asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Asked {
    /// Drain now.
    DrainNow,
    /// A wait began: see at once whether the node is already free to hand
    /// its leadership over.
    WaitToHandOver,
    /// The node has no leader to drain through: the request is kept until one
    /// appears, or the wait limit passes.
    WaitForALeader,
    /// Nothing now: the request is kept, already waited on, or moot.
    Nothing,
}

impl DrainRequest {
    /// A drain was asked for in `state` at `now`. An `Active` node drains at
    /// once. A node that leads starts waiting to hand over, unless it
    /// already waits or hands over. A node in `LeaderSuspect`, `RollCall` or
    /// `NoQuorum` keeps the request until a leader appears or `wait_limit`
    /// passes. A node already draining, or one that can
    /// never drain again, ignores it; any other keeps it.
    pub(crate) fn ask(&mut self, state: WorkerState, now: Instant, wait_limit: Duration) -> Asked {
        match state {
            WorkerState::Active => Asked::DrainNow,
            WorkerState::LeaderReconciling | WorkerState::Leader => {
                if matches!(
                    self,
                    DrainRequest::Waiting { .. } | DrainRequest::HandingOver { .. }
                ) {
                    return Asked::Nothing;
                }
                *self = DrainRequest::Waiting {
                    until: now + wait_limit,
                };
                Asked::WaitToHandOver
            }
            WorkerState::Draining | WorkerState::Stopped => Asked::Nothing,
            WorkerState::LeaderSuspect | WorkerState::RollCall | WorkerState::NoQuorum => {
                *self = DrainRequest::Kept {
                    until: now + wait_limit,
                };
                Asked::WaitForALeader
            }
            _ => {
                *self = DrainRequest::Kept {
                    until: now + wait_limit,
                };
                Asked::Nothing
            }
        }
    }

    /// Whether the node handed its leadership over to leave.
    pub(crate) fn hands_over(&self) -> bool {
        matches!(self, DrainRequest::HandingOver { .. })
    }

    /// Whether a node waiting for a leader, or for the leader it handed over
    /// to, has waited as long as it may.
    pub(crate) fn gave_up_waiting(&self, now: Instant) -> bool {
        match self {
            DrainRequest::Kept { until } | DrainRequest::HandingOver { until } => now >= *until,
            DrainRequest::NotAsked | DrainRequest::Waiting { .. } => false,
        }
    }

    /// The node lost office at `now`: a wait to hand over becomes a request
    /// kept for `wait_limit` more; a hand-over goes on.
    pub(crate) fn office_lost(&mut self, now: Instant, wait_limit: Duration) {
        if let DrainRequest::Waiting { .. } = self {
            *self = DrainRequest::Kept {
                until: now + wait_limit,
            };
        }
    }

    /// The node reached `reached`. Returns whether a kept request now
    /// applies, clearing it; the node then asks again in that state. A
    /// hand-over applies again only if the node leads again.
    pub(crate) fn take_kept_for(&mut self, reached: WorkerState) -> bool {
        let applies = match self {
            DrainRequest::Kept { .. } => matches!(
                reached,
                WorkerState::Active | WorkerState::LeaderReconciling | WorkerState::Leader
            ),
            DrainRequest::HandingOver { .. } => {
                matches!(reached, WorkerState::LeaderReconciling | WorkerState::Leader)
            }
            DrainRequest::NotAsked | DrainRequest::Waiting { .. } => false,
        };
        if applies {
            *self = DrainRequest::NotAsked;
        }
        applies
    }

    /// When the node next has to look again: the end of its wait.
    pub(crate) fn wakes_at(&self) -> Option<Instant> {
        match self {
            DrainRequest::Kept { until }
            | DrainRequest::Waiting { until }
            | DrainRequest::HandingOver { until } => Some(*until),
            DrainRequest::NotAsked => None,
        }
    }
}
