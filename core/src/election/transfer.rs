//! A leader's hand-over of its leadership, from the step it asked a voter
//! to stand until it leaves office, leads again, or departs.

use crate::protocol::ids::WorkerId;
use crate::time::Instant;

/// The hand-over a leader has under way: it asked `to` to stand, and holds
/// no grant until `until`, when, if it still holds office, it leads again
/// (or, draining, departs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Transfer {
    pub(crate) to: WorkerId,
    pub(crate) until: Instant,
}
