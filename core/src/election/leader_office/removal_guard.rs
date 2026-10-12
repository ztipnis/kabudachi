use std::collections::{BTreeMap, BTreeSet};

use crate::configuration::{Generation, Roster, Side};
use crate::protocol::ids::WorkerId;

/// What keeps the removal of voters that confirm no ack safe: the voters
/// taken out, each with the generation it last echoed, and the voters of each
/// generation the office may still need to reason about.
#[derive(Default)]
pub(super) struct RemovalGuard {
    /// The voters taken out for confirming no ack, each with the generation
    /// it last echoed before (see [`Self::may_remove_one`]).
    removed: Vec<(WorkerId, Option<Generation>)>,
    /// The voters of each generation this office may still need to reason
    /// about: the current one, those a member last echoed, and those a
    /// record of `removed` points at.
    voters_at: BTreeMap<Generation, Vec<Side>>,
}

impl RemovalGuard {
    /// Notes the voters of `generation`, the one a roll call was called under.
    pub(super) fn seed(&mut self, generation: Generation, sides: Vec<Side>) {
        self.voters_at.insert(generation, sides);
    }

    /// `worker`, last echoing `last_echoed`, was taken out.
    pub(super) fn record(&mut self, worker: WorkerId, last_echoed: Option<Generation>) {
        self.removed.push((worker, last_echoed));
    }

    /// Whether a queued unconfirming member may be taken out now: the
    /// configuration is committed, and the voters that hold it, the leader
    /// among them, outside the queue, are a majority of the voters of every
    /// configuration a muted or removed voter may still stand at: the one
    /// each queued voter last echoed, and the one each voter removed
    /// before last echoed. Any majority of such a configuration then holds a
    /// voter that, holding a later generation, refuses its call as stale, so
    /// those voters, the removed ones among them, cannot elect a second
    /// leader. A voter whose echoed generation is unknown, as any is until it
    /// echoes in this office, blocks removal.
    pub(super) fn may_remove_one(
        &self,
        roster: &Roster,
        unconfirming: &BTreeSet<WorkerId>,
        me: &WorkerId,
    ) -> bool {
        if unconfirming.is_empty() || roster.configuration().is_joint() {
            return false;
        }
        let counted = roster.counted_voters();
        let mut anchors: BTreeSet<Option<Generation>> = unconfirming
            .iter()
            .filter(|worker| counted.contains(*worker))
            .map(|worker| roster.held_generation_of(worker))
            .collect();
        anchors.extend(
            self.removed
                .iter()
                .filter(|(worker, _)| !counted.contains(worker))
                .map(|(_, anchor)| *anchor),
        );
        let current = roster.configuration().generation();
        let holders: BTreeSet<&WorkerId> = counted
            .iter()
            .filter(|worker| *worker == me || roster.held_generation_of(worker) == Some(current))
            .filter(|worker| !unconfirming.contains(*worker))
            .collect();
        anchors.into_iter().all(|anchor| {
            let Some(voters) = anchor.and_then(|generation| self.voters_at.get(&generation)) else {
                return false;
            };
            voters.iter().all(|(known, total)| {
                let holding = known.iter().filter(|voter| holders.contains(voter)).count();
                2 * holding > *total
            })
        })
    }

    /// Forgets the voters removed that can no longer elect behind the
    /// leader's back: one readmitted, or one a majority of whose anchor
    /// configuration now holds a later generation, which is permanent. The
    /// leader holds the current generation, later than any anchor but it.
    pub(super) fn drop_settled(&mut self, roster: &Roster, me: &WorkerId) {
        let counted = roster.counted_voters();
        let current = roster.configuration().generation();
        let voters_at = &self.voters_at;
        self.removed.retain(|(worker, anchor)| {
            if counted.contains(worker) {
                return false;
            }
            let Some(anchor) = anchor else { return true };
            let Some(voters) = voters_at.get(anchor) else {
                return true;
            };
            // Settled once a majority of every side holds a later generation.
            !voters.iter().all(|(known, total)| {
                let later = known
                    .iter()
                    .filter(|voter| {
                        counted.contains(*voter)
                            && if *voter == me {
                                current > *anchor
                            } else {
                                roster
                                    .held_generation_of(voter)
                                    .is_some_and(|held| held > *anchor)
                            }
                    })
                    .count();
                2 * later > *total
            })
        });
    }

    /// Notes the voters of the current generation, and forgets those of
    /// generations nothing points at any more.
    pub(super) fn note_voters(&mut self, roster: &Roster) {
        let current = roster.configuration().generation();
        if !self.voters_at.contains_key(&current) {
            self.voters_at.insert(current, roster.sides());
        }
        let removed = &self.removed;
        self.voters_at.retain(|generation, _| {
            *generation == current
                || removed
                    .iter()
                    .any(|(_, anchor)| *anchor == Some(*generation))
                || roster
                    .members()
                    .keys()
                    .any(|member| roster.held_generation_of(member) == Some(*generation))
        });
    }
}
