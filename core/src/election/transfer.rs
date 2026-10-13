//! A leader's hand-over of its leadership, from the step it asked a voter
//! to stand until it leaves office, leads again, or departs.

use crate::time::Instant;

/// The hand-over a leader has under way: it asked its successor to stand,
/// and holds no grant until `until`, when, if it still holds office, it leads
/// again (or, draining, departs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Transfer {
    pub(crate) until: Instant,
}
