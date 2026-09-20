//! Ring-ordered peer membership (README §11) and `SELF_REMOVE` handling
//! (README §12.3). Ring order is ascending `WorkerId` among the active members.

use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

use crate::hashing::{Field, HashFunction};
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::SelfRemove;
use crate::protocol::messages::prelude::*;

/// Default ring neighbors per direction (README §11: "around three").
pub const DEFAULT_RING_FANOUT: usize = 3;

pub trait MembershipView {
    fn effective_electorate(&self) -> BTreeSet<WorkerId>;

    /// Up to the ring fanout workers after `me` in ring order, wrapping past
    /// the highest member. `me` is excluded and need not be a member.
    fn ring_successors(&self, me: WorkerId) -> Vec<WorkerId>;

    /// Like [`Self::ring_successors`], walking backward from `me`.
    fn ring_predecessors(&self, me: WorkerId) -> Vec<WorkerId>;

    /// Starts at `0`; increases by one for each member `apply_self_remove`
    /// actually removes and for every `rebuild`.
    fn membership_generation(&self) -> u64;

    /// An 8-byte digest of the electorate, stable across builds so different
    /// processes can compare views.
    fn membership_digest(&self) -> [u8; 8];

    /// Irrevocably removes the sender from the electorate; idempotent. Removal
    /// is per `WorkerId`, not per incarnation.
    fn apply_self_remove(&mut self, msg: &SelfRemove);

    /// Replaces the electorate wholesale for forced recovery (README §14.3),
    /// which can add members. Always bumps the generation, even if the set is
    /// unchanged.
    fn rebuild(&mut self, new_members: BTreeSet<WorkerId>);
}

pub struct RingMembership {
    members: BTreeSet<WorkerId>,
    generation: u64,
    ring_fanout: usize,
    hash_function: HashFunction,
}

impl RingMembership {
    pub fn new(initial_members: BTreeSet<WorkerId>) -> Self {
        RingMembership {
            members: initial_members,
            generation: 0,
            ring_fanout: DEFAULT_RING_FANOUT,
            hash_function: HashFunction::default(),
        }
    }

    pub fn with_ring_fanout(mut self, ring_fanout: usize) -> Self {
        self.ring_fanout = ring_fanout;
        self
    }

    /// Every worker in the shard must use the same hash function for digests
    /// to be comparable.
    pub fn with_hash_function(mut self, hash_function: HashFunction) -> Self {
        self.hash_function = hash_function;
        self
    }
}

impl MembershipView for RingMembership {
    fn effective_electorate(&self) -> BTreeSet<WorkerId> {
        self.members.clone()
    }

    fn ring_successors(&self, me: WorkerId) -> Vec<WorkerId> {
        let after_me = self.members.range((Excluded(&me), Unbounded));
        let before_me = self.members.range(..&me);
        after_me
            .chain(before_me)
            .take(self.ring_fanout)
            .cloned()
            .collect()
    }

    fn ring_predecessors(&self, me: WorkerId) -> Vec<WorkerId> {
        let before_me = self.members.range(..&me).rev();
        let after_me = self.members.range((Excluded(&me), Unbounded)).rev();
        before_me
            .chain(after_me)
            .take(self.ring_fanout)
            .cloned()
            .collect()
    }

    fn membership_generation(&self) -> u64 {
        self.generation
    }

    fn membership_digest(&self) -> [u8; 8] {
        let mut fields = vec![Field::Number(self.members.len() as u64)];
        fields.extend(
            self.members
                .iter()
                .map(|member| Field::Text(member.as_str())),
        );
        self.hash_function.hash_to_prefix(&fields)
    }

    fn apply_self_remove(&mut self, msg: &SelfRemove) {
        if self.members.remove(&msg.worker_id()) {
            self.generation += 1;
        }
    }

    fn rebuild(&mut self, new_members: BTreeSet<WorkerId>) {
        self.members = new_members;
        self.generation += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::generated;

    fn worker(label: &str) -> WorkerId {
        WorkerId::new(label)
    }

    fn ring(labels: &[&str]) -> RingMembership {
        RingMembership::new(labels.iter().map(|l| worker(l)).collect())
    }

    fn self_remove_for(worker_id: &WorkerId) -> SelfRemove {
        SelfRemove {
            worker_id: Some(generated::WorkerId {
                value: worker_id.as_str().to_string(),
            }),
            incarnation_id: Some(generated::IncarnationId {
                value: "incarnation-1".into(),
            }),
            shard_id: Some(generated::ShardId {
                value: "shard-1".into(),
            }),
            membership_generation: 0,
        }
    }

    #[test]
    fn highest_member_successors_wrap_to_lowest() {
        let m = ring(&["w0", "w1", "w2", "w3", "w4"]);
        assert_eq!(
            m.ring_successors(worker("w4")),
            vec![worker("w0"), worker("w1"), worker("w2")]
        );
    }

    #[test]
    fn lowest_member_predecessors_wrap_to_highest() {
        let m = ring(&["w0", "w1", "w2", "w3", "w4"]);
        assert_eq!(
            m.ring_predecessors(worker("w0")),
            vec![worker("w4"), worker("w3"), worker("w2")]
        );
    }

    #[test]
    fn middle_member_non_wrapping_neighbors() {
        let m = ring(&["w0", "w1", "w2", "w3", "w4"]);
        assert_eq!(
            m.ring_successors(worker("w2")),
            vec![worker("w3"), worker("w4"), worker("w0")]
        );
        assert_eq!(
            m.ring_predecessors(worker("w2")),
            vec![worker("w1"), worker("w0"), worker("w4")]
        );
    }

    #[test]
    fn three_member_ring_returns_exactly_two_neighbors_each_direction() {
        let m = ring(&["w0", "w1", "w2"]);
        assert_eq!(
            m.ring_successors(worker("w0")),
            vec![worker("w1"), worker("w2")]
        );
        assert_eq!(
            m.ring_predecessors(worker("w0")),
            vec![worker("w2"), worker("w1")]
        );
    }

    #[test]
    fn two_member_ring_returns_same_two_members_in_opposite_order() {
        let m = ring(&["w0", "w1"]);
        // With one other member, both directions return that same singleton.
        assert_eq!(m.ring_successors(worker("w0")), vec![worker("w1")]);
        assert_eq!(m.ring_predecessors(worker("w0")), vec![worker("w1")]);
        assert_eq!(m.ring_successors(worker("w1")), vec![worker("w0")]);
        assert_eq!(m.ring_predecessors(worker("w1")), vec![worker("w0")]);
    }

    #[test]
    fn single_member_ring_has_no_neighbors_in_either_direction() {
        let m = ring(&["w0"]);
        assert_eq!(m.ring_successors(worker("w0")), Vec::<WorkerId>::new());
        assert_eq!(m.ring_predecessors(worker("w0")), Vec::<WorkerId>::new());
    }

    #[test]
    fn apply_self_remove_shrinks_electorate_and_increments_generation_once() {
        let mut m = ring(&["w0", "w1", "w2"]);
        assert_eq!(m.membership_generation(), 0);

        m.apply_self_remove(&self_remove_for(&worker("w1")));
        assert_eq!(
            m.effective_electorate(),
            [worker("w0"), worker("w2")].into_iter().collect()
        );
        assert_eq!(m.membership_generation(), 1);

        m.apply_self_remove(&self_remove_for(&worker("w1")));
        assert_eq!(
            m.effective_electorate(),
            [worker("w0"), worker("w2")].into_iter().collect()
        );
        assert_eq!(m.membership_generation(), 1);

        let mut other_message = self_remove_for(&worker("w1"));
        other_message.incarnation_id = Some(generated::IncarnationId {
            value: "incarnation-2".into(),
        });
        m.apply_self_remove(&other_message);
        assert_eq!(
            m.effective_electorate(),
            [worker("w0"), worker("w2")].into_iter().collect()
        );
        assert_eq!(m.membership_generation(), 1);

        assert!(!m.effective_electorate().contains(&worker("w1")));
    }

    #[test]
    fn rebuild_wholesale_replaces_electorate_and_always_increments_generation() {
        let mut m = ring(&["w0", "w1", "w2"]);
        assert_eq!(m.membership_generation(), 0);

        let new_members: BTreeSet<WorkerId> = ["w2", "w3"].iter().map(|l| worker(l)).collect();
        m.rebuild(new_members.clone());

        assert_eq!(
            m.effective_electorate(),
            new_members,
            "rebuild must replace the electorate wholesale, not merge with the old set"
        );
        assert!(!m.effective_electorate().contains(&worker("w0")));
        assert!(!m.effective_electorate().contains(&worker("w1")));
        assert!(m.effective_electorate().contains(&worker("w3")));
        assert_eq!(m.membership_generation(), 1);

        // A forced reconfiguration is a distinct event even when the set is unchanged.
        m.rebuild(new_members.clone());
        assert_eq!(m.effective_electorate(), new_members);
        assert_eq!(m.membership_generation(), 2);
    }

    #[test]
    fn digest_changes_on_removal_and_is_stable_on_idempotent_replay() {
        let mut m = ring(&["w0", "w1", "w2"]);
        let before = m.membership_digest();

        m.apply_self_remove(&self_remove_for(&worker("w1")));
        let after_removal = m.membership_digest();
        assert_ne!(before, after_removal);

        m.apply_self_remove(&self_remove_for(&worker("w1")));
        let after_replay = m.membership_digest();
        assert_eq!(after_removal, after_replay);
    }

    #[test]
    fn digest_is_stable_across_builds() {
        // Computed with Python's `hashlib` (SHA-256) over the big-endian
        // member count followed by each length-prefixed ID.
        let m = ring(&["worker-0", "worker-1", "worker-2"]);
        assert_eq!(m.membership_digest(), [85, 187, 159, 144, 41, 72, 249, 228]);
    }

    #[test]
    fn digest_follows_the_configured_hash_function() {
        let m = ring(&["worker-0", "worker-1", "worker-2"])
            .with_hash_function(HashFunction::new::<sha3::Sha3_256>());
        assert_eq!(
            m.membership_digest(),
            [105, 162, 217, 196, 74, 58, 181, 211]
        );
    }

    #[test]
    fn ring_fanout_is_configurable() {
        let m = ring(&["w0", "w1", "w2", "w3", "w4"]).with_ring_fanout(1);
        assert_eq!(m.ring_successors(worker("w4")), vec![worker("w0")]);
        assert_eq!(m.ring_predecessors(worker("w0")), vec![worker("w4")]);
    }

    #[test]
    fn neighbors_of_a_non_member_start_at_its_sort_position() {
        let m = ring(&["w0", "w2", "w4"]);
        assert_eq!(
            m.ring_successors(worker("w3")),
            vec![worker("w4"), worker("w0"), worker("w2")]
        );
        assert_eq!(
            m.ring_predecessors(worker("w3")),
            vec![worker("w2"), worker("w0"), worker("w4")]
        );
    }

    #[test]
    fn fresh_construction_electorate_matches_initial_members() {
        let initial: BTreeSet<WorkerId> = ["w0", "w1", "w2"].iter().map(|l| worker(l)).collect();
        let m = RingMembership::new(initial.clone());
        assert_eq!(m.effective_electorate(), initial);
        assert_eq!(m.membership_generation(), 0);
    }
}
