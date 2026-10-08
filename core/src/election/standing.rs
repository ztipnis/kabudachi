//! What a node knows of its shard, and how recovery epochs order.
//!
//! [`ShardStanding`] holds a node's recovery epoch, the highest term it has
//! seen, and the configuration and admission generations it follows. It
//! changes only through the transitions named on it, so no site writes one of
//! those fields beside the others. [`order`] is the one place two recovery
//! epochs are compared: every site that reads an epoch off a message, an ack
//! or the authority matches on the [`EpochOrder`] it returns.

use std::cmp::Ordering;

use crate::configuration::{Admission, Configuration, Generation, Roster};
use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::ids::WorkerId;
use crate::protocol::messages::JoinResponse;

/// How another node's, or the authority's, recovery epoch compares with this
/// node's own. Epochs are totally ordered, by number and then lineage, so any
/// two nodes agree which of two epochs is newer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochOrder {
    /// The same epoch: number and lineage.
    Mine,
    /// A newer epoch.
    Later,
    /// An older epoch.
    Stale,
}

/// Where `heard` stands against `own`.
pub(crate) fn order(own: RecoveryEpoch, heard: RecoveryEpoch) -> EpochOrder {
    match heard.cmp(&own) {
        Ordering::Equal => EpochOrder::Mine,
        Ordering::Greater => EpochOrder::Later,
        Ordering::Less => EpochOrder::Stale,
    }
}

/// The recovery epoch a node rejoins at, and the one place JOIN pointers are
/// judged against it: which a node takes, and which of those is newest. It is
/// a value: `net` takes the node's current floor for each pass of its search
/// for a leader, and asks it instead of comparing epochs itself.
///
/// A pointer is accepted when its epoch is the floor's own or later (see
/// [`EpochOrder`]). A floor of `None`, a node that never joined, accepts every
/// pointer. Among accepted pointers a later epoch is newer, and of one epoch a
/// later term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinFloor {
    epoch: Option<RecoveryEpoch>,
}

impl JoinFloor {
    /// The floor of a node that has joined no shard: it accepts every pointer.
    pub fn none() -> Self {
        JoinFloor { epoch: None }
    }

    /// The floor at `epoch`.
    pub fn at(epoch: RecoveryEpoch) -> Self {
        JoinFloor { epoch: Some(epoch) }
    }

    /// The epoch the floor stands at, `None` for a node that never joined.
    pub fn epoch(&self) -> Option<RecoveryEpoch> {
        self.epoch
    }

    /// Whether the floor takes a pointer to a leader of `pointer`'s epoch.
    pub fn accepts(&self, pointer: &JoinResponse) -> bool {
        self.accepts_epoch(pointer_epoch(pointer))
    }

    fn accepts_epoch(&self, named: RecoveryEpoch) -> bool {
        self.epoch
            .is_none_or(|floor| order(floor, named) != EpochOrder::Stale)
    }

    /// The newest of the pointers this floor accepts; of equally new ones,
    /// the first. `None` when it accepts none.
    pub fn newest<'a>(
        &self,
        pointers: impl IntoIterator<Item = &'a JoinResponse>,
    ) -> Option<&'a JoinResponse> {
        self.newest_first(pointers).into_iter().next()
    }

    /// The pointers this floor accepts, newest first: by epoch, then term;
    /// equally new ones keep the order they were given in.
    pub fn newest_first<'a>(
        &self,
        pointers: impl IntoIterator<Item = &'a JoinResponse>,
    ) -> Vec<&'a JoinResponse> {
        let mut ranked: Vec<_> = pointers
            .into_iter()
            .filter(|pointer| self.accepts(pointer))
            .collect();
        ranked.sort_by_key(|pointer| std::cmp::Reverse((pointer_epoch(pointer), pointer.term)));
        ranked
    }

    /// Takes `held`, the epoch the coordination authority holds, as the floor
    /// when it is of another lineage than the floor's. The floor may move
    /// down: the held epoch's number can be below it. A floor of the same
    /// lineage stays, and so does one that is `None`.
    pub fn refresh(&mut self, held: RecoveryEpoch) {
        if self.epoch.is_some_and(|floor| floor.lineage != held.lineage) {
            self.epoch = Some(held);
        }
    }
}

fn pointer_epoch(pointer: &JoinResponse) -> RecoveryEpoch {
    RecoveryEpoch::new(pointer.recovery_epoch, pointer.recovery_epoch_lineage)
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
    /// The admission generation the standing holds differs from the one it
    /// held before the ack: it took up a promise, which its leader waits to
    /// hear it hold.
    pub(crate) admission_changed: bool,
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
    /// recovery epoch.
    pub(crate) fn known(known: crate::election::KnownConfiguration) -> Self {
        ShardStanding {
            epoch: Some(known.configuration.generation().recovery_epoch()),
            highest_term_seen: 0,
            configuration: Some(known.configuration),
            admission: known.admission,
            prior_admission: None,
        }
    }

    pub(crate) fn epoch(&self) -> Option<RecoveryEpoch> {
        self.epoch
    }

    /// The floor this standing's epoch is, for a node rejoining.
    pub(crate) fn join_floor(&self) -> JoinFloor {
        JoinFloor { epoch: self.epoch }
    }

    /// The recovery epoch number; 0 before the first join.
    pub(crate) fn epoch_number(&self) -> u64 {
        self.epoch.map_or(0, |epoch| epoch.number)
    }

    /// Where `heard` stands against this standing's epoch; `None` before the
    /// first join, when it has none.
    pub(crate) fn order(&self, heard: RecoveryEpoch) -> Option<EpochOrder> {
        self.epoch.map(|own| order(own, heard))
    }

    pub(crate) fn highest_term_seen(&self) -> u64 {
        self.highest_term_seen
    }

    pub(crate) fn configuration(&self) -> Option<&Configuration> {
        self.configuration.as_ref()
    }

    /// The admission generation the node holds, a promise ahead of its
    /// configuration included.
    pub(crate) fn admission(&self) -> Option<Generation> {
        self.admission
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
    /// newer epoch (see [`EpochOrder::Later`]) first moves this standing to
    /// that epoch, forgetting the old one's configuration: the epochs' terms
    /// are not comparable, so `term` becomes the highest seen. The epoch is
    /// `heard` whole, lineage included: a recovery usually keeps the lineage,
    /// but a shard founded afresh does not, and a node that kept its old
    /// lineage would not recognise the epoch as its own when it next
    /// reconnected. Then the offered configuration is taken on as
    /// [`Self::adopt`] says.
    pub(crate) fn accept_ack(
        &mut self,
        heard: RecoveryEpoch,
        term: u64,
        offered: Configuration,
        admission: Option<Generation>,
        prior: Option<Generation>,
    ) -> AckChange {
        let epoch_moved = self.order(heard) == Some(EpochOrder::Later);
        if epoch_moved {
            self.forget();
            self.epoch = Some(heard);
            self.highest_term_seen = term;
        }
        self.saw_term(term);
        let held = self.configuration.as_ref().map(Configuration::generation);
        let admitted = self.admission;
        self.adopt(offered, admission, prior);
        AckChange {
            epoch_moved,
            generation_changed: self.configuration.as_ref().map(Configuration::generation) != held,
            admission_changed: self.admission != admitted,
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
    /// [`Configuration::admission_after_commit`]). Or when it is the batch
    /// the leader of the single configuration this node holds started on it,
    /// whose start this node missed: it is admitted at the batch's
    /// generation, its old admission the prior (see
    /// [`Configuration::admission_after_batch_start`]). Or when it is the
    /// batch this node was promised admission in, the joint configuration at
    /// exactly the promised generation: this node already holds that
    /// admission (see [`Self::adopt`]), and takes up the configuration it
    /// counts in, with no prior admission (see
    /// [`Configuration::is_the_batch_promised`]). Returns whether it adopted.
    pub(crate) fn adopt_relayed_configuration(&mut self, offered: Configuration) -> bool {
        let Some(own) = self.configuration.as_ref() else {
            return false;
        };
        if let Some(admission) = own.admission_after_commit(&offered, self.admission) {
            self.configuration = Some(offered);
            self.admission = Some(admission);
            self.prior_admission = None;
            return true;
        }
        if let Some(admission) = own.admission_after_batch_start(&offered, self.admission) {
            self.configuration = Some(offered);
            self.prior_admission = self.admission;
            self.admission = Some(admission);
            return true;
        }
        if own.is_the_batch_promised(&offered, self.admission) {
            self.configuration = Some(offered);
            self.prior_admission = None;
            return true;
        }
        false
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

    /// Leaves the shard for `epoch`, the floor the node rejoins at: a JOIN
    /// pointer older than it (see [`EpochOrder::Stale`]) is refused. Forgets
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
    ///
    /// An admission later than `offered`'s generation is a promise: the
    /// leader will admit the node at that generation in a batch it has yet
    /// to start. The node holds it, and so counts at that generation, only if
    /// it raises the admission it holds and the node is no voter of the
    /// configuration it ends up holding; its prior admission stays.
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
        match admission {
            Some(promised) if promised > offered.generation() => {
                if self.admission.is_none_or(|held| held < promised)
                    && !offered.is_voter(self.counted_admission())
                {
                    self.admission = Some(promised);
                }
            }
            Some(admission) => {
                self.admission = Some(admission);
                self.prior_admission = prior;
            }
            None => {}
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
