use crate::hashing::Field;
use crate::protocol::worker_state::WorkerState;
use crate::time::{Clock, Duration, Instant};

use super::super::{WorkerNode, lengthen_by_less_than_half};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Checks this node's timers against its clock.
    ///
    /// - `Active`: moves to `LeaderSuspect` once no leader ack has arrived
    ///   within its jittered suspicion timeout.
    /// - `LeaderSuspect` and `NoQuorum`: starts a roll call, if it can (see
    ///   [`Self::can_start_roll_call`]). Each transition takes its own
    ///   `Tick`, so a single one never goes from `Active` to `RollCall`.
    /// - `RollCall`: at its roll call's deadline, stands as the candidate or
    ///   goes `NoQuorum` (see [`ElectionRound::on_deadline`]).
    /// - `Candidate`: at its vote's deadline, not having won, suspects its
    ///   leader again (see [`Self::suspect_again`]).
    /// - `Leader`: reports every worker it has not heard from for too long
    ///   as lost (see [`Self::report_lost_workers`]). Moving to `NoQuorum`
    ///   once its quorum-contact lease has run out happens on every step,
    ///   before the input (see [`Self::step`]).
    ///
    /// Every other state is a no-op — deliberately so for `Bootstrapping` and
    /// `Joining`: nothing times out a
    /// stalled join here, since the transition out of those states happens
    /// once via [`Input::JoinAnswer`], driven by something outside `core`
    /// that learns who leads the shard, not by a timer.
    pub(super) fn tick(&mut self) {
        match self.state {
            WorkerState::Active => {
                if self.clock.now() - self.last_leader_contact > self.jittered_suspect_timeout() {
                    self.round.may_call_from(self.clock.now());
                    self.transition_to(WorkerState::LeaderSuspect);
                }
            }
            WorkerState::LeaderSuspect | WorkerState::NoQuorum => {
                if self.can_start_roll_call() {
                    let now = self.clock.now();
                    let timestamp_millis = self.clock.wall_clock_millis();
                    self.decide(|round, view| round.begin_roll_call(view, timestamp_millis, now));
                }
            }
            WorkerState::RollCall | WorkerState::Candidate => {
                let now = self.clock.now();
                self.decide(|round, view| round.on_deadline(view, now));
            }
            WorkerState::LeaderReconciling | WorkerState::Leader => {
                self.drain_once_free();
                if self.holds_office() {
                    self.report_lost_workers();
                }
            }
            _ => {}
        }
    }

    /// Whether a `LeaderSuspect` or `NoQuorum` node starts a roll call on
    /// its next `Tick`: once [`Self::next_roll_call_due`] has come.
    fn can_start_roll_call(&self) -> bool {
        self.next_roll_call_due()
            .is_some_and(|due| self.clock.now() >= due)
    }

    /// When a `LeaderSuspect` or `NoQuorum` node may start a roll call (see
    /// [`ElectionRound::roll_call_due`]).
    ///
    /// `None` for a node with no configuration: it has nothing to count a
    /// quorum against. It stays `LeaderSuspect`, still heartbeating its
    /// leader, until an ack from a leader returns it to `Active`.
    ///
    /// `None` too for a member that may not stand yet: with an authority, it
    /// stands only once its latest read of the authority's epoch confirms its
    /// own (see [`Self::may_stand`]). Its deadline is then its next read, so
    /// a `Tick` is never due that moves nothing.
    pub(super) fn next_roll_call_due(&self) -> Option<Instant> {
        self.standing
            .configuration()
            .filter(|_| self.may_stand())
            .map(|_| self.round.roll_call_due(self.clock.now()))
    }

    /// Whether this node may stand for election. With no authority, always.
    /// With one, only while its latest read of the authority's recovery epoch
    /// confirms this node's own epoch, or found the authority holding none:
    /// a node at an epoch the authority does not hold must rejoin, not elect
    /// a leader there that would pull the others off the authority's epoch.
    fn may_stand(&self) -> bool {
        match (self.authority.as_ref(), self.standing.epoch()) {
            (Some(authority), Some(own)) => authority.permits_standing(own),
            _ => true,
        }
    }

    /// How long this node, while `Active`, goes without an accepted leader
    /// ack before it suspects its leader:
    /// `suspect_timeout` lengthened by less than a half, by a share hashed
    /// from this node's `WorkerId` and the latest term it knows of. Workers
    /// thus rarely suspect at the same instant, and a worker waits a new
    /// time once the term has moved on. Leader stickiness and a leader's
    /// lease keep the unjittered `suspect_timeout`, the shortest this can
    /// be.
    pub(super) fn jittered_suspect_timeout(&self) -> Duration {
        let share = self.hash_function.hash_to_u64(&[
            Field::Text(self.my_id.as_str()),
            Field::Number(self.latest_term()),
        ]);
        lengthen_by_less_than_half(self.timings.suspect_timeout, share)
    }

    pub(super) fn current_leader_still_valid(&self) -> bool {
        self.clock.now() - self.last_leader_contact <= self.timings.suspect_timeout
    }

    /// The latest term this node knows of: the highest it has seen, or that
    /// of the latest roll call it accepted, its own included, if later.
    fn latest_term(&self) -> u64 {
        self.round.latest_term(self.standing.highest_term_seen())
    }
}
