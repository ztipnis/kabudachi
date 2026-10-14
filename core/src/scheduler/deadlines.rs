//! What time does to tasks, and when it next will: delayed tasks become due,
//! pending ones expire, and finished ones are forgotten.

use crate::protocol::ids::IdGenerator;
use crate::protocol::records::TaskRunRecord;
use crate::protocol::task::TaskRunState;
use crate::time::{Clock, Instant, WallTime};

use super::{Observer, Scheduler};

/// What one call to [`Scheduler::catch_up`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CaughtUp {
    /// Delayed tasks that became due and are now pending (only while leading).
    pub queued: usize,
    /// Pending tasks that expired (only while leading).
    pub expired: usize,
    /// Finished tasks forgotten because their result TTL had passed.
    pub forgotten: usize,
    /// Blocked tasks that failed because they waited past the block TTL
    /// (only while leading).
    pub migration_failed: usize,
}

impl<C: Clock, I: IdGenerator, O: Observer> Scheduler<C, I, O> {
    /// Lets time take effect, and is the only call a runtime makes for time's
    /// sake: it forgets every task that finished at least `result_ttl` ago,
    /// and, only while this scheduler leads, makes due delayed tasks pending
    /// and expires pending tasks past their expiry (reported through
    /// [`Self::take_events`]), and loses the runs of silent workers whose
    /// reconnect timeouts have passed (see [`Self::note_silence`]). Call it
    /// when [`Self::next_deadline`] comes.
    pub fn catch_up(&mut self) -> CaughtUp {
        let mut caught_up = if self.check_leader() {
            let released = self.release_due();
            self.lose_overdue();
            released
        } else {
            CaughtUp::default()
        };
        caught_up.forgotten = self.end_call();
        caught_up
    }

    /// Delayed tasks that are due become pending, and pending tasks past
    /// their expiry expire. Only a leader decides this, so its callers check.
    /// Claims do this themselves first, so a task that ran out of time is
    /// never handed out.
    pub(super) fn release_due(&mut self) -> CaughtUp {
        let mut advanced = CaughtUp::default();
        let now = self.clock.now();
        let stamped_at = WallTime::now(&self.clock);
        for task_id in self.waiting.take_expired(now) {
            self.expire(&task_id, now);
            advanced.expired += 1;
        }
        advanced.migration_failed = self.fail_blocked_past_ttl(now);
        for task_id in self.waiting.release_due(now) {
            let run_id = self.current_run[&task_id].clone();
            self.runs
                .get_mut(&run_id)
                .expect("every current run is stored")
                .transition_to(TaskRunState::Queued, stamped_at)
                .expect("a Scheduled run can always be queued");
            self.mark_decided(&run_id);
            advanced.queued += 1;
        }
        advanced
    }

    /// When [`Self::catch_up`] next has something to do, or `None`: the next
    /// forgetting time always, and the next delay or expiry only while this
    /// scheduler leads. Also the end of a bounded lease while this scheduler
    /// has found itself leading: that end stays in the answer until a call
    /// finds it passed, since finding the lapse is then `catch_up`'s to do.
    /// An unbounded lease adds nothing. Also the next replay of a silent
    /// worker's run, only while this scheduler leads.
    pub fn next_deadline(&self) -> Option<Instant> {
        let forgetting = [
            self.retention.next_due(),
            self.retention
                .ttl()
                .and_then(|ttl| self.composites.next_due(ttl)),
        ]
        .into_iter()
        .flatten()
        .min();
        let lease_end = self.leadership.noticed_lease_end();
        let waiting = if self.is_leader() {
            self.waiting.next_deadline()
        } else {
            None
        };
        let replays = if self.is_leader() {
            self.replays_due().into_iter().map(|(at, _)| at).min()
        } else {
            None
        };
        let block_ttl = if self.is_leader() {
            self.next_block_deadline()
        } else {
            None
        };
        [waiting, forgetting, lease_end, replays, block_ttl]
            .into_iter()
            .flatten()
            .min()
    }
}
