use std::collections::BTreeSet;

use crate::protocol::ids::WorkerId;
use crate::protocol::worker_state::WorkerState;
use crate::time::{Clock, Duration};

use super::super::leader_office::Duties;
use super::super::lease::LeaseChange;
use super::super::{Output, WorkerNode};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Moves to `NoQuorum`: the node's quorum is out
    /// of reach, so it waits for its peers to return. Every jittered
    /// suspicion timeout it tries a roll call again; meanwhile it answers
    /// the roll calls and grants the votes of others, and an ack from a
    /// leader returns it to `Active`. With an authority, the authority path
    /// can take it out too (see [`AuthorityStanding::begin_recovery`]).
    /// A leader that has not heard from a quorum within its quorum-contact
    /// lease gives up leading, to `NoQuorum`.
    pub(in crate::election) fn lose_quorum_if_its_lease_ended(&mut self) {
        if !self.holds_office() {
            return;
        }
        let now = self.clock.now();
        let Some(office) = self.office.as_mut() else {
            return;
        };
        let duties = Duties {
            me: &self.my_id,
            lease: &self.lease,
            timings: &self.timings,
            now,
        };
        if office.has_lost_quorum(&duties) {
            self.lose_quorum();
        }
    }

    /// The driver names the workers that hold runs and fell silent, and those
    /// that answered this office's rebuild (see [`Input::WatchWorkers`]).
    pub(in crate::election) fn on_watch_workers(
        &mut self,
        silent_holders: BTreeSet<WorkerId>,
        answered: BTreeSet<WorkerId>,
    ) {
        let now = self.clock.now();
        if let Some(office) = self.office.as_mut() {
            office.watch(silent_holders, &self.my_id, now);
            office.heard_answers(answered, &self.my_id, now);
        }
    }

    /// Reports this node's grant and abort deadline where they differ from
    /// the ones last reported.
    pub(in crate::election) fn report_lease_changes(&mut self) {
        // The lease runs from the win, exactly as a leader's, but the
        // scheduler is handed a grant only once the node leads.
        let grant = self.lease.grant(self.office().as_ref(), &self.timings);
        let changes = self.lease.report(
            grant,
            self.state == WorkerState::Leader,
            &self.timings,
            self.clock.now(),
        );
        for change in changes {
            self.outputs.push(match change {
                LeaseChange::Grant(grant) => Output::Grant(grant),
                LeaseChange::AbortDeadline(deadline) => Output::AbortDeadline(deadline),
            });
        }
    }

    /// How long a leader goes without hearing from a worker before it
    /// reports that worker lost: a suspicion timeout and then a reconnect
    /// timeout. Membership only: a run's replay is timed by its own
    /// reconnect timeout (see [`Output::WorkerSilence`]).
    pub(in crate::election) fn lost_after(&self) -> Duration {
        Duration::from_ticks(
            self.timings
                .suspect_timeout
                .as_ticks()
                .saturating_add(self.timings.reconnect_timeout.as_ticks()),
        )
    }

    /// Reports every worker this leader has not heard from for a suspicion
    /// timeout and a reconnect timeout as lost, once each. Silences go
    /// first, so a worker lost at the instant it falls silent (a reconnect
    /// timeout of zero) still has its silence reported.
    pub(in crate::election) fn report_lost_workers(&mut self) {
        self.report_silences();
        let now = self.clock.now();
        let lost_after = self.lost_after();
        let suspect_timeout = self.timings.suspect_timeout;
        let Some(office) = self.office.as_mut() else {
            return;
        };
        for worker in office.lost_by(
            now,
            lost_after,
            suspect_timeout,
        ) {
            // Its cancelled runs stay listed: its own runs are decided by its
            // silence, each at its own reconnect timeout, so a worker heard
            // again may still hold one and must still be told.
            self.outputs.push(Output::WorkerLost(worker));
        }
    }

    /// Reports each change in which workers this leader counts silent (see
    /// [`Output::WorkerSilence`]).
    pub(in crate::election) fn report_silences(&mut self) {
        if !self.holds_office() {
            return;
        }
        let now = self.clock.now();
        let suspect_timeout = self.timings.suspect_timeout;
        let Some(office) = self.office.as_mut() else {
            return;
        };
        for (worker, reconnect_from) in office.silence_changes(now, suspect_timeout) {
            self.outputs.push(Output::WorkerSilence { worker, reconnect_from });
        }
    }
}
