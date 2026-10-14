use std::collections::BTreeMap;

use crate::protocol::ids::WorkerId;

use crate::configuration::Generation;

/// What each member has said of the configuration it holds: the highest
/// generation it ever named, as a respondent of the winning roll call or in a
/// heartbeat, and the generation its latest heartbeat named.
#[derive(Debug, Clone, Default)]
pub(super) struct Echoes {
    highest: BTreeMap<WorkerId, Generation>,
    /// The generation each member's latest echo named, whatever came before:
    /// a joint configuration commits on those that name exactly its own.
    latest: BTreeMap<WorkerId, Generation>,
}

impl Echoes {
    /// Raises what `member` is known to hold to `generation`; never lowers it.
    pub(super) fn raise(&mut self, member: &WorkerId, generation: Generation) {
        let newest = self.highest.entry(member.clone()).or_insert(generation);
        *newest = (*newest).max(generation);
    }

    /// `member`'s heartbeat named `held`: raises its highest and records it as
    /// its latest.
    pub(super) fn record(&mut self, member: &WorkerId, held: Generation) {
        self.raise(member, held);
        self.latest.insert(member.clone(), held);
    }

    pub(super) fn highest(&self, member: &WorkerId) -> Option<Generation> {
        self.highest.get(member).copied()
    }

    /// The members whose latest echo named exactly `generation`, in id order.
    pub(super) fn naming(&self, generation: Generation) -> impl Iterator<Item = &WorkerId> {
        self.latest
            .iter()
            .filter(move |(_, held)| **held == generation)
            .map(|(member, _)| member)
    }

    pub(super) fn forget(&mut self, member: &WorkerId) {
        self.highest.remove(member);
        self.latest.remove(member);
    }
}
