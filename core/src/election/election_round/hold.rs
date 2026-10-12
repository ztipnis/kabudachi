//! The hold a roll call of another worker, once answered, puts on this
//! node's own roll calls.

use crate::time::{Duration, Instant};

/// A roll call of another worker that this node answered, and until when it
/// holds this node's own roll calls back if nothing resolves it first.
#[derive(Debug, Clone)]
struct AnsweredCall {
    /// When the first answer of the hold episode this belongs to was made.
    episode_start: Instant,
    release_at: Instant,
    /// Whether this answer's window reached the episode's end. Once such a
    /// hold has run out the node is owed a call: see
    /// [`Self::is_owed_a_call`].
    capped: bool,
}

impl AnsweredCall {
    /// Whether this hold ended at or, by the minimum, just past its episode's
    /// cap by `now` and the node has not called since: until it does, answers
    /// no longer hold it, so that callers retrying in step with the episode
    /// cannot starve it.
    fn is_owed_a_call(&self, now: Instant) -> bool {
        self.capped && self.release_at <= now
    }
}

/// How long a roll call of another worker that this node answered holds
/// this node's own roll calls back (see `ElectionRound::roll_call_due`).
#[derive(Debug, Clone, Default)]
pub(super) struct RollCallHold {
    answered: Option<AnsweredCall>,
}

impl RollCallHold {
    /// This node answered a roll call at `now`: the hold moves to that answer,
    /// within its episode's limits.
    pub(super) fn answered_at(
        &mut self,
        now: Instant,
        roll_call_deadline: Duration,
        suspect_timeout: Duration,
    ) {
        // A call of base width closes within a roll-call deadline of
        // this answer, and its candidate's vote within another, but
        // a slow candidate's vote request and the leader's ack can
        // arrive later still: a call of this node's own before the
        // ack would only contest the next term against the worker
        // it is helping elect. So the hold lasts until that ack
        // (see `ElectionRound::follow_leader_of`), or, if the candidate
        // died, a suspicion timeout past the window it needed.
        //
        // Every answer made within the episode holds the node for at
        // least two base roll-call deadlines (a call's census and
        // vote), however much of the hold episode (the answers made
        // while a hold is live) has gone; one made after its end
        // is held no further than one minimum past it, and once that
        // hold runs out the next answer finds the node owed a call:
        // a caller retrying a call this node already answered must
        // not find it free to contest the retry's election.
        //
        // Callers taking turns would chain holds so that this node
        // never gets a window of its own, so the episode ends two
        // windows after its first answer. The minimum may carry a
        // late answer's hold past that end, but never past the end
        // plus one minimum: each retry of one caller comes later than
        // a minimum after its last, so a lone caller never chains it,
        // and the bound stops several from doing so. That gives a
        // lone slow caller its whole window, and a node a window of
        // its own to call in, after at most two.
        //
        // A hold that ran out short of the cap is no part of an
        // episode: the next answer starts a fresh one. One that the
        // cap ended leaves the node owed a call: until it starts one
        // of its own or follows a leader, further answers hold it
        // no more, or a caller delivered at the very instant the cap
        // ends could hold it again before its tick runs, and callers
        // retrying in step could do so without end. The node still
        // answers; it is only not held. A caller answered late in
        // another caller's episode may also be contested early,
        // which costs extra calls, never safety.
        let round_span = Duration::from_ticks(roll_call_deadline.as_ticks().saturating_mul(2));
        let window = Duration::from_ticks(
            round_span
                .as_ticks()
                .saturating_add(suspect_timeout.as_ticks()),
        );
        if self
            .answered
            .as_ref()
            .is_some_and(|held| held.is_owed_a_call(now))
        {
            return;
        }
        // The gap after a hold that ran out uncapped relies on
        // the node's suspicion jitter: a call answered at the
        // exact instant of that expiry, before the node's tick,
        // starts a new episode.
        let live = self.answered.as_ref().filter(|held| held.release_at > now);
        let episode_start = live.map_or(now, |held| held.episode_start);
        let episode_end = episode_start + Duration::from_ticks(window.as_ticks().saturating_mul(2));
        let release_at = (now + window)
            .min(episode_end)
            .max(now + round_span)
            .min(episode_end + round_span);
        self.answered = Some(AnsweredCall {
            episode_start,
            release_at,
            capped: now + window >= episode_end,
        });
    }

    /// When the hold releases this node; `None` with no answer held.
    pub(super) fn release_at(&self) -> Option<Instant> {
        self.answered.as_ref().map(|answered| answered.release_at)
    }

    /// The answered call resolved, or no longer matters.
    pub(super) fn clear(&mut self) {
        self.answered = None;
    }
}
