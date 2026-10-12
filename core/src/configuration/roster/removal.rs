use std::collections::BTreeSet;

use crate::protocol::ids::WorkerId;

use crate::configuration::{Admission, Configuration, Joint, OldSide, Single};
use super::Roster;

impl Roster {
    /// Takes the departing `workers` out together (every pending
    /// SELF_REMOVE in the next generation), as the leader
    /// elected in `leader_term` announces it, with no commit round.
    ///
    /// If any of them is a voter here, on either side of a joint
    /// configuration, the configuration moves once, to the next generation
    /// that leader announces (see [`crate::configuration::Generation::next_change`]), re-based
    /// there: every member left on the new side is re-admitted at it, and
    /// the new side counts exactly those. A joint configuration keeps its
    /// old side, less the departing workers it counted: a batch or founding
    /// in flight is re-announced with shrunk counts. When that empties the
    /// old side, the joint configuration collapses to its new side alone: an
    /// empty side could never supply a majority, so every quorum would stall.
    ///
    /// A member that is no voter, or a pending joiner, is only forgotten. A
    /// worker the roster does not hold changes nothing, so a repeated
    /// removal counts once. The configuration never shrinks below one
    /// voter: a removal that would leave the new side empty only forgets.
    /// Whoever is taken out is never held again (see
    /// [`Roster::add_pending`]).
    ///
    /// Which removals a leader may apply at all is the caller's to judge: the
    /// leader refuses a removal from a worker that has seen a later term than
    /// its own (see `WorkerNode`'s handling of `SelfRemove`), so such a
    /// worker never reaches here.
    ///
    /// # Panics
    ///
    /// If `leader_term` is below the term of the configuration's generation
    /// (see [`crate::configuration::Generation::next_change`]).
    pub fn remove_all(&mut self, workers: &BTreeSet<WorkerId>, leader_term: u64) {
        self.take_out(workers, leader_term, true);
    }

    /// Takes `worker` out as [`Self::remove_all`] does, as a leader does
    /// one that confirms none of its acks, but without keeping it from
    /// returning: it is not recorded as departed, so its next heartbeat
    /// holds it as a pending joiner again. Only one worker, so a single
    /// change shrinks the electorate by one and every quorum of the
    /// configuration before it shares a voter with every quorum after it.
    pub(crate) fn remove_lost(&mut self, worker: &WorkerId, leader_term: u64) {
        self.take_out(&BTreeSet::from([worker.clone()]), leader_term, false);
    }

    fn take_out(&mut self, workers: &BTreeSet<WorkerId>, leader_term: u64, depart: bool) {
        let old_side = self.configuration.old_electorate();
        let new_side = self.configuration.voters();
        let (mut counted_anywhere, mut counted_on_old_side) = (false, 0);
        for worker in workers {
            let Some(admission) = self.forget_departing(worker, depart) else {
                continue;
            };
            let on_old_side = old_side.is_some_and(|old| old.counted().counts(admission));
            counted_anywhere |= new_side.counts(admission) || on_old_side;
            counted_on_old_side += usize::from(on_old_side);
        }
        if !counted_anywhere || self.new_side_member_count() == 0 {
            return;
        }
        self.reconfigure_without(leader_term, old_side, counted_on_old_side);
    }

    /// Forgets `worker` as one taken out (as departed when `depart`): every
    /// record of it goes, and a promise round naming it is void. Its admission
    /// generations if it was a member.
    fn forget_departing(&mut self, worker: &WorkerId, depart: bool) -> Option<Admission> {
        if depart {
            self.departed.insert(worker.clone());
        }
        self.pending.remove(worker);
        self.promises.void_if_naming(worker);
        self.echoes.forget(worker);
        let prior = self.prior_admissions.remove(worker);
        let current = self.members.remove(worker)?;
        Some(Admission {
            current: Some(current),
            prior,
        })
    }

    /// Moves the configuration past a removal, as `leader_term`'s leader
    /// announces it: re-based at the next generation, the new side counting
    /// the members re-admitted there, and the old side, if any, less the
    /// `counted_on_old_side` voters taken out of it. An emptied old side
    /// collapses the configuration to its new side alone.
    fn reconfigure_without(
        &mut self,
        leader_term: u64,
        old_side: Option<OldSide>,
        counted_on_old_side: usize,
    ) {
        let changed = self.next_generation(leader_term);
        let new_voter_count = self.re_admit_new_side_at(changed);
        let old_side_left = old_side.map(|old| OldSide {
            voter_count: old.voter_count.saturating_sub(counted_on_old_side),
            ..old
        });
        self.configuration = match old_side_left {
            Some(old) if old.voter_count >= 1 => Configuration::joint(Joint {
                generation: changed,
                base: changed,
                batch_generation: changed,
                old_base: old.base,
                old_generation: old.generation,
                old_voter_count: old.voter_count,
                new_voter_count,
            })
            .expect("a removal keeps the old side it had, and both sides have a voter"),
            _ => {
                self.prior_admissions.clear();
                Configuration::single(Single {
                    generation: changed,
                    base: changed,
                    voter_count: new_voter_count,
                })
                .expect("a removal leaves at least one voter")
            }
        };
    }
}
