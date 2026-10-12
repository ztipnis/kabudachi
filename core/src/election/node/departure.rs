use crate::protocol::checked::Checked;
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::prelude::*;
use crate::protocol::messages::{SelfRemove, election_message};
use crate::protocol::worker_state::WorkerState;
use crate::time::Clock;

use super::super::drain::Asked;
use super::super::leader_office::Departure;
use super::super::{HandOffTo, Output, StopReason, WorkerNode};

impl<C> WorkerNode<C>
where
    C: Clock,
{
    /// Drains at once from `Active`, and from `LeaderReconciling` or `Leader`
    /// once every other voter has crawled or the wait limit has passed. From a
    /// state that will reach one of them later it keeps the request for
    /// [`Self::transition_to`] to apply; a node already draining, or one
    /// that can never drain again, ignores it.
    pub(super) fn request_drain(&mut self) {
        let asked =
            self.drain_request
                .ask(self.state, &self.clock, self.timings.drain_wait_limit);
        match asked {
            Asked::DrainNow => self.drain(),
            Asked::WaitForCrawls => self.drain_once_free(),
            Asked::Nothing => {}
        }
    }

    /// A leader asked to drain leaves once every other voter has reported a
    /// routing crawl since its admission, so no worker is left knowing only
    /// this leader, or once its drain wait has run out. Until then it keeps
    /// leading.
    pub(super) fn drain_once_free(&mut self) {
        let leaves = self.drain_request.leave_if_free(&self.clock, || {
            self.office
                .as_mut()
                .is_some_and(|office| office.remaining_voters_have_crawled(&self.my_id))
        });
        if leaves {
            self.drain();
        }
    }

    /// Gracefully shuts an `Active` or `Leader` node down and ends in `Stopped`.
    ///
    /// A follower sends `SelfRemove` to its leader alone, carrying the
    /// highest term it has seen, for that leader's term guard (see
    /// [`Self::on_self_remove`]).
    /// Followers learn of the removal from the generation the leader then
    /// announces. A node that knows no leader tells no one: the next
    /// founding leaves it out, or the authority path counts it out.
    ///
    /// A leader applies its own removal, which no other leader can,
    /// and sends every connected peer a final ack announcing the
    /// configuration without it, together with every removal it has
    /// accepted and not yet applied, so the survivors elect under the
    /// shrunk one. A leader whose removal, with those, would leave no voter
    /// announces nothing (see [`Roster::remove_all`](crate::configuration::Roster::remove_all)).
    fn drain(&mut self) {
        // Leaving `Leader` withdraws the grant first, so no departure
        // message goes out while this node still holds one.
        let was_leader = self.holds_office();
        let hand_off = if was_leader {
            HandOffTo::Voters(
                self.placeable_voters()
                    .into_iter()
                    .filter(|voter| *voter != self.my_id)
                    .collect(),
            )
        } else {
            match &self.leader {
                Some((leader, _)) if *leader != self.my_id => HandOffTo::Leader(leader.clone()),
                _ => HandOffTo::Nobody,
            }
        };
        let departure = self
            .office
            .take()
            .and_then(|office| office.depart(&self.my_id, &mut self.standing));
        self.transition_to(WorkerState::Draining);
        if was_leader {
            self.announce_own_departure(departure);
        } else {
            self.tell_leader_of_departure();
        }
        self.outputs.push(Output::HandOff(hand_off));

        // There is no outstanding work to wait for yet, so draining finishes
        // immediately. The transition table has no direct `Active -> Stopped`
        // edge, hence the two transitions.
        self.stop_reason = Some(StopReason::Drained);
        self.transition_to(WorkerState::Stopped);
    }

    /// The draining follower's half of [`Self::drain`]: one `SelfRemove`
    /// to the leader it follows, if it knows one other than itself.
    fn tell_leader_of_departure(&mut self) {
        let Some((leader, leader_term)) = self.leader.clone() else {
            return;
        };
        if leader == self.my_id {
            return;
        }
        let msg = SelfRemove {
            worker_id: Some(self.my_id.clone().into()),
            incarnation_id: Some(self.incarnation_id.clone().into()),
            shard_id: Some(self.shard_id.clone().into()),
            configuration_generation: self
                .standing
                .configuration()
                .map(|configuration| configuration.generation().into()),
            term_seen: self.standing.highest_term_seen(),
            leader_term,
        };
        self.send(leader, election_message::Payload::SelfRemove(msg));
    }

    /// The draining leader's half of [`Self::drain`]: if taking itself out
    /// of its roster announced a change (`departure`), acks every connected
    /// peer with it.
    fn announce_own_departure(&mut self, departure: Option<Departure>) {
        let Some(departure) = departure else {
            return;
        };
        for peer in self.connected.clone() {
            // Unasked, and sent after the grant is withdrawn: no heartbeat
            // token to vouch for.
            let content = departure.ack_for(&peer);
            self.send_ack_with(peer, content, None);
        }
    }

    /// Accepts a departing worker's SELF_REMOVE, to take it out of this leader's roster with every
    /// other one accepted since the last change, in one next generation and
    /// with no commit round (see [`LeaderOffice::take_removal`](crate::election::leader_office::LeaderOffice::take_removal)).
    ///
    /// It accepts only a removal addressed to this leadership: to this
    /// node, as the leader of this term. And the term guard: it accepts the removal only if the
    /// worker has seen no term later than this leader's. A worker that has may
    /// have voted in an election of that later term, whose quorum was
    /// counted with it; shrinking N here as well could let two quorums of
    /// one term miss each other. Granting a vote raises the voter's highest
    /// term seen before the grant goes out, and a worker grants nothing
    /// once it has stopped, so the term it sends covers every vote it can
    /// have cast; a roll call it only answered counts for no quorum, so it
    /// need not (and does not) raise it. Such a worker stays in the roster
    /// until the next founding leaves it out, or the authority path counts
    /// it out.
    ///
    /// Only a `Leader` honours it: every other node ignores it, and a
    /// candidate keeps counting against its roll call's configuration.
    pub(super) fn on_self_remove(&mut self, departing: &WorkerId, msg: &Checked<SelfRemove>) {
        if !self.accepts_removal(msg) {
            return;
        }
        if let Some(office) = self.office.as_mut() {
            office.take_removal(departing.clone());
            self.cancelled_runs.forget(departing);
        }
        self.drain_once_free();
    }

    /// Whether this leader accepts `msg`: it holds office, `msg` is for its
    /// shard, and it is addressed to this leadership by a worker that has seen
    /// no later term (the term guard; see [`Self::on_self_remove`]).
    fn accepts_removal(&self, msg: &Checked<SelfRemove>) -> bool {
        let to_this_leadership = msg.term_seen <= self.term && msg.leader_term == self.term;
        self.holds_office() && msg.shard_id() == self.shard_id && to_this_leadership
    }
}
