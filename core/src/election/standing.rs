//! What a node knows of its shard, and how recovery epochs order.
//!
//! [`ShardStanding`] holds a node's recovery epoch, the highest term it has
//! seen, and the configuration and admission generations it follows. It
//! changes only through the transitions named on it, so no site writes one of
//! those fields beside the others. [`order`] and [`order_numbers`] are the
//! one place two recovery epochs are compared: every site that reads an epoch
//! off a message, an ack or the authority matches on the [`EpochOrder`] they
//! return.

use std::cmp::Ordering;

use crate::configuration::{Admission, Configuration, Generation, Roster};
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::ids::WorkerId;

/// How another node's, or the authority's, recovery epoch compares with this
/// node's own (ADR-0001 decision 1's amendment: an epoch is its number and
/// its lineage).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochOrder {
    /// The same epoch: same number, and the same lineage or none named.
    Mine,
    /// A later epoch of the same lineage, or of an unnamed one.
    Later,
    /// An earlier epoch of the same lineage, or of an unnamed one.
    Stale,
    /// Another lineage's epoch: another shard's, whatever its number. The
    /// number ordering is kept for `on_leader_ack`, which adopts a
    /// higher-numbered foreign epoch and ignores an equal or lower one
    /// (E13-U1, E13-R10(c)).
    Foreign(Ordering),
}

/// A recovery epoch as a message or the authority names it: its number, and
/// its lineage when the message carries one (`RecoveryEpoch` always does; a
/// `LeaderHeartbeatAck` may; a heartbeat, roll call, vote, refusal or
/// certificate never does).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeardEpoch {
    pub(crate) number: u64,
    pub(crate) lineage: Option<u64>,
}

impl From<RecoveryEpoch> for HeardEpoch {
    fn from(epoch: RecoveryEpoch) -> Self {
        HeardEpoch {
            number: epoch.number,
            lineage: Some(epoch.lineage),
        }
    }
}

/// Where `other` stands against `own`. With no lineage named, only numbers
/// are compared (see [`order_numbers`]).
pub(crate) fn order(own: &RecoveryEpoch, other: HeardEpoch) -> EpochOrder {
    match other.lineage {
        Some(lineage) if lineage != own.lineage => {
            EpochOrder::Foreign(other.number.cmp(&own.number))
        }
        _ => order_numbers(own.number, other.number),
    }
}

/// [`order`] where neither lineage is in play: `Mine`, `Later` or `Stale` by
/// number, never `Foreign`.
pub(crate) fn order_numbers(own: u64, other: u64) -> EpochOrder {
    match other.cmp(&own) {
        Ordering::Equal => EpochOrder::Mine,
        Ordering::Greater => EpochOrder::Later,
        Ordering::Less => EpochOrder::Stale,
    }
}

/// What a node knows of its shard: its recovery epoch (`None` only before
/// its first join), the highest term it has seen, and the configuration and
/// admission generations it follows. It changes only through the named
/// transitions below.
#[derive(Debug, Clone)]
pub(crate) struct ShardStanding {
    epoch: Option<RecoveryEpoch>,
    highest_term_seen: u64,
    /// `None` for a joiner until it accepts its first leader ack.
    configuration: Option<Configuration>,
    /// The generation at which the node became a voter; `None` for a pending
    /// member.
    admission: Option<Generation>,
    /// While the node holds a joint configuration an election founded, the
    /// admission generation it held before that election admitted it.
    prior_admission: Option<Generation>,
}

/// What [`ShardStanding::accept_ack`] changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AckChange {
    /// The ack's epoch was a later one: the standing moved to it and forgot
    /// the old epoch's configuration and admissions.
    pub(crate) epoch_moved: bool,
    /// The configuration generation the standing holds differs from the one
    /// it held before the ack's configuration was offered.
    pub(crate) generation_changed: bool,
}

impl ShardStanding {
    /// The standing of a node that has joined no shard yet.
    pub(crate) fn unjoined() -> Self {
        ShardStanding {
            epoch: None,
            highest_term_seen: 0,
            configuration: None,
            admission: None,
            prior_admission: None,
        }
    }

    /// The standing of a node started inside `known`, at its configuration's
    /// recovery epoch of `lineage`.
    pub(crate) fn known(known: crate::election::KnownConfiguration, lineage: u64) -> Self {
        ShardStanding {
            epoch: Some(RecoveryEpoch::new(
                known.configuration.generation().recovery_epoch(),
                lineage,
            )),
            highest_term_seen: 0,
            configuration: Some(known.configuration),
            admission: known.admission,
            prior_admission: None,
        }
    }

    pub(crate) fn epoch(&self) -> Option<RecoveryEpoch> {
        self.epoch
    }

    /// The recovery epoch number; 0 before the first join.
    pub(crate) fn epoch_number(&self) -> u64 {
        self.epoch.map_or(0, |epoch| epoch.number)
    }

    /// Where `other` stands against this standing's epoch; `None` before the
    /// first join, when it has none.
    pub(crate) fn order(&self, other: HeardEpoch) -> Option<EpochOrder> {
        self.epoch.map(|own| order(&own, other))
    }

    pub(crate) fn highest_term_seen(&self) -> u64 {
        self.highest_term_seen
    }

    pub(crate) fn configuration(&self) -> Option<&Configuration> {
        self.configuration.as_ref()
    }

    /// Both admission generations a quorum counts this node by.
    pub(crate) fn counted_admission(&self) -> Admission {
        Admission {
            current: self.admission,
            prior: self.prior_admission,
        }
    }

    /// Raises the highest term seen to `term`; never lowers it.
    pub(crate) fn saw_term(&mut self, term: u64) {
        self.highest_term_seen = self.highest_term_seen.max(term);
    }

    /// The JOIN answer was taken: the node now stands at `epoch`, and has
    /// seen `leader_term`.
    pub(crate) fn joined(&mut self, epoch: RecoveryEpoch, leader_term: u64) {
        self.epoch = Some(epoch);
        self.saw_term(leader_term);
    }

    /// Accepts `term` and what an ack of `heard`'s epoch carries. An ack of a
    /// later epoch (or a higher foreign one) first moves this standing to
    /// that epoch, forgetting the old one's configuration: the epochs' terms
    /// are not comparable, so `term` becomes the highest seen. The epoch
    /// takes `heard`'s lineage when the ack names one: a recovery usually
    /// keeps the lineage, but an epoch recovered from a shard founded afresh
    /// does not, and a node that kept its old lineage would not recognise the
    /// epoch as its own when it next reconnected. Then the offered
    /// configuration is taken on as [`Self::adopt`] says.
    pub(crate) fn accept_ack(
        &mut self,
        heard: HeardEpoch,
        term: u64,
        offered: Configuration,
        admission: Option<Generation>,
        prior: Option<Generation>,
    ) -> AckChange {
        let epoch_moved = matches!(
            self.order(heard),
            Some(EpochOrder::Later | EpochOrder::Foreign(Ordering::Greater))
        );
        if epoch_moved {
            self.forget();
            let lineage = heard
                .lineage
                .or(self.epoch.map(|epoch| epoch.lineage))
                .unwrap_or_default();
            self.epoch = Some(RecoveryEpoch::new(heard.number, lineage));
            self.highest_term_seen = term;
        }
        self.saw_term(term);
        let held = self.configuration.as_ref().map(Configuration::generation);
        self.adopt(offered, admission, prior);
        AckChange {
            epoch_moved,
            generation_changed: self.configuration.as_ref().map(Configuration::generation) != held,
        }
    }

    /// Accepts an election certificate's `term` and the configuration it
    /// carries, as [`Self::adopt`] says.
    pub(crate) fn adopt_certificate(
        &mut self,
        term: u64,
        offered: Configuration,
        admission: Option<Generation>,
        prior: Option<Generation>,
    ) {
        self.saw_term(term);
        self.adopt(offered, admission, prior);
    }

    /// Adopts `offered`, a refuser's configuration, when it is the commit of
    /// the joint configuration this node holds and this node is on that
    /// one's new side: it is admitted at the commit's generation, as the
    /// commit's own ack would have admitted it (see
    /// [`Configuration::admission_after_commit`]). Returns whether it did.
    pub(crate) fn adopt_relayed_commit(&mut self, offered: Configuration) -> bool {
        let Some(admission) = self
            .configuration
            .as_ref()
            .and_then(|own| own.admission_after_commit(&offered, self.admission))
        else {
            return false;
        };
        self.configuration = Some(offered);
        self.admission = Some(admission);
        self.prior_admission = None;
        true
    }

    /// Takes on, as the leader `me`'s own, the configuration `roster` leads
    /// and `me`'s admission generations there.
    pub(crate) fn take_on_roster(&mut self, roster: &Roster, me: &WorkerId) {
        self.configuration = Some(roster.configuration().clone());
        self.admission = roster.admission_of(me);
        self.prior_admission = roster.prior_admission_of(me);
    }

    /// The authority path swapped the recovery epoch to `epoch`: the node
    /// stands there, has seen `term`, and takes on the configuration its
    /// recovery `founded` there, if it did.
    pub(crate) fn recovered_to(
        &mut self,
        epoch: RecoveryEpoch,
        term: u64,
        founded: Option<&Roster>,
        me: &WorkerId,
    ) {
        self.epoch = Some(epoch);
        self.saw_term(term);
        if let Some(roster) = founded {
            self.configuration = Some(roster.configuration().clone());
            self.admission = roster.admission_of(me);
            self.prior_admission = None;
        }
    }

    /// Leaves the shard for `epoch`, the floor the node rejoins at: forgets
    /// the configuration and admissions, and the terms seen.
    pub(crate) fn rejoin_at(&mut self, epoch: RecoveryEpoch) {
        self.forget();
        self.epoch = Some(epoch);
        self.highest_term_seen = 0;
    }

    /// Adopts `offered`, a configuration a leader announced, when the node
    /// has none or `offered` is newer than its own, and with it `admission`
    /// and `prior`, its admission generations there, when an admission is
    /// given. When the node already holds `offered`, it adopts only the
    /// admission generations, which repairs ones it missed. Admission
    /// generations offered with an older configuration than its own are
    /// ignored: they belong to a configuration the node has moved past,
    /// where they would make it no voter of its own.
    fn adopt(
        &mut self,
        offered: Configuration,
        admission: Option<Generation>,
        prior: Option<Generation>,
    ) {
        let is_newer = self
            .configuration
            .as_ref()
            .is_none_or(|own| offered.generation() > own.generation());
        if !is_newer && self.configuration.as_ref() != Some(&offered) {
            return;
        }
        if let Some(admission) = admission {
            self.admission = Some(admission);
            self.prior_admission = prior;
        }
        self.configuration = Some(offered);
    }

    /// Forgets the configuration and admissions held under the epoch left
    /// behind.
    fn forget(&mut self) {
        self.configuration = None;
        self.admission = None;
        self.prior_admission = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_order_reads_lineage_then_number() {
        let own = RecoveryEpoch::new(3, 5);
        let heard = |number, lineage| HeardEpoch { number, lineage };

        assert_eq!(order(&own, heard(3, Some(5))), EpochOrder::Mine);
        assert_eq!(order(&own, heard(4, Some(5))), EpochOrder::Later);
        assert_eq!(order(&own, heard(2, Some(5))), EpochOrder::Stale);

        assert_eq!(order(&own, heard(3, None)), EpochOrder::Mine);
        assert_eq!(order(&own, heard(4, None)), EpochOrder::Later);
        assert_eq!(order(&own, heard(2, None)), EpochOrder::Stale);

        assert_eq!(
            order(&own, heard(2, Some(6))),
            EpochOrder::Foreign(Ordering::Less)
        );
        assert_eq!(
            order(&own, heard(3, Some(6))),
            EpochOrder::Foreign(Ordering::Equal)
        );
        assert_eq!(
            order(&own, heard(4, Some(6))),
            EpochOrder::Foreign(Ordering::Greater)
        );
    }
}
