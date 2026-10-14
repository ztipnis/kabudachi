//! A shard's voter configuration: which
//! workers count toward a quorum, and whether a given set of them reaches one.
//!
//! Followers never hold the member list. A follower knows its configuration
//! only as a generation, a base generation and a voter count, plus its own
//! admission generation. A worker is a voter when its admission generation
//! lies between the base generation and the generation, inclusive, so the
//! counts alone are enough to judge a majority. Only the leader holds the
//! member list, in a [`Roster`].
//!
//! A configuration can be joint: the configuration an election or an
//! admission batch moves to (the new side), together with the one it moves
//! from (the old side), each with its own voter count. Every quorum of a
//! joint configuration needs a majority of both sides, so it shares a
//! majority with every quorum of either. An
//! election founds a joint configuration (see [`Roster::after_election`]),
//! and its leader commits it to the new side alone once a majority of each
//! side holds it (see [`Roster::commit_if_confirmed`]).
//!
//! Every change a leader makes (founding a joint configuration, re-stamping
//! one at its own term, committing it, removing a voter) moves the
//! configuration to a generation of the leader's term, re-bases it there, and
//! re-admits there every member it counts on the new side. So the range from
//! base to generation is one generation, which only that term's one leader
//! mints, and never takes in a worker a rival election admitted. The cost: a
//! member that misses the ack carrying its re-admission is no voter of the
//! change until a later ack repairs it.

use std::collections::BTreeMap;

use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::generated;
use crate::protocol::ids::WorkerId;

mod roster;

pub use roster::Roster;
pub(crate) use roster::Side;

/// Why a configuration breaks one of its rules, whether built here or decoded
/// from a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidConfiguration {
    #[error("a generation's counter is u64::MAX")]
    CounterAtMax,
    #[error("Configuration.generation is required but was absent")]
    MissingGeneration,
    #[error("Configuration.base is required but was absent")]
    MissingBase,
    #[error("Configuration.electorate is required but was absent")]
    MissingElectorate,
    #[error("JointElectorate.batch_generation is required but was absent")]
    MissingBatchGeneration,
    #[error("JointElectorate.old_base is required but was absent")]
    MissingOldBase,
    #[error("JointElectorate.old_generation is required but was absent")]
    MissingOldGeneration,
    #[error("base is later than generation")]
    BaseAfterGeneration,
    #[error("a joint electorate's base is later than its batch generation")]
    BaseAfterBatchGeneration,
    #[error("a joint electorate's batch generation is later than generation")]
    BatchGenerationAfterGeneration,
    #[error("a joint electorate's old base is later than its old generation")]
    OldBaseAfterOldGeneration,
    #[error("a joint electorate's old generation is not earlier than its batch generation")]
    OldGenerationNotBeforeBatchGeneration,
    #[error("a joint electorate's old base is later than its base")]
    OldBaseAfterBase,
    #[error("a voter count is zero")]
    ZeroVoterCount,
    #[error("a voter count does not fit in usize")]
    VoterCountTooLarge,
}

/// The identity of a configuration: the triple (recovery epoch, term,
/// counter), compared lexicographically in that order.
///
/// The term is that of the election that founded the configuration, or of the
/// leader that announced the change, so configurations announced under
/// different leaders never share a generation. The recovery epoch leads so the
/// order stays right whether or not a forced reconfiguration resets the term.
/// The counter rises by one with every change and never resets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Generation {
    // The derived `Ord` compares fields in declaration order, so this order is
    // the comparison order: recovery epoch (number, then lineage), term,
    // counter.
    recovery_epoch: RecoveryEpoch,
    term: u64,
    counter: u64,
}

impl Generation {
    pub fn new(recovery_epoch: RecoveryEpoch, term: u64, counter: u64) -> Self {
        Generation {
            recovery_epoch,
            term,
            counter,
        }
    }

    /// A new shard's first generation: term 0, counter 0. It is the genesis
    /// configuration's generation and base generation, and its creator's
    /// admission generation.
    pub fn genesis(recovery_epoch: RecoveryEpoch) -> Self {
        Generation::new(recovery_epoch, 0, 0)
    }

    pub fn recovery_epoch(&self) -> RecoveryEpoch {
        self.recovery_epoch
    }

    /// The term of the election that founded this generation's
    /// configuration, or of the leader that announced it.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// Whether this generation and `other` were minted in the same recovery
    /// epoch and term.
    fn is_in_term_of(&self, other: Generation) -> bool {
        self.recovery_epoch == other.recovery_epoch && self.term == other.term
    }

    /// The generation of the next change a leader elected in `leader_term`
    /// announces after this one: the same recovery epoch, the announcing
    /// leader's term, and the counter one higher (every
    /// change a leader announces carries the term it was elected in). So two
    /// leaders of different terms that each change the same configuration
    /// announce distinct generations, the later term's ordered after the
    /// earlier's. A commit and a removal use it, and so will each phase of
    /// an admission batch, with the same leader term.
    ///
    /// # Panics
    ///
    /// If `leader_term` is below this generation's term: a leader's term is
    /// never below the term of a configuration it holds, and the result would
    /// order before this generation. Also if the counter is already
    /// `u64::MAX`. The counter starts at 0 at
    /// genesis and rises by one per change, and decode
    /// ([`crate::protocol::configuration`]) rejects any generation whose
    /// counter is `u64::MAX`, so reaching this edge needs a peer bug that
    /// first gets a counter to `u64::MAX - 1`, plus one more local change.
    /// Wrapping instead would silently invert generation order, which is
    /// worse than a panic.
    pub fn next_change(self, leader_term: u64) -> Self {
        assert!(
            leader_term >= self.term,
            "a leader's term {leader_term} is below the term {} of a configuration it holds",
            self.term
        );
        Generation::new(
            self.recovery_epoch,
            leader_term,
            self.counter
                .checked_add(1)
                .expect("Generation counter overflowed u64::MAX"),
        )
    }

    /// The generation of the configuration an election founds: the current
    /// recovery epoch, the election's term, and the counter one past that of
    /// the configuration the roll call ran under. It is also the founded
    /// configuration's base generation and every respondent's new admission
    /// generation.
    ///
    /// # Panics
    ///
    /// See [`Generation::next_change`]: the roll call generation's counter
    /// reaching `u64::MAX` needs the same peer bug.
    pub fn founded_by_election(
        recovery_epoch: RecoveryEpoch,
        election_term: u64,
        roll_call_generation: Generation,
    ) -> Self {
        Generation::new(
            recovery_epoch,
            election_term,
            roll_call_generation
                .counter
                .checked_add(1)
                .expect("Generation counter overflowed u64::MAX"),
        )
    }
}

/// Infallible: every `Generation` this crate can build is valid on the wire.
/// A wire `Generation` that is not a valid domain `Generation` (its counter is
/// `u64::MAX`) is decoded through the checked
/// [`TryFrom<&generated::Generation>`](crate::protocol::configuration) in the
/// protocol layer instead.
impl From<Generation> for generated::Generation {
    fn from(generation: Generation) -> Self {
        generated::Generation {
            recovery_epoch: generation.recovery_epoch.number,
            recovery_epoch_lineage: generation.recovery_epoch.lineage,
            term: generation.term,
            counter: generation.counter,
        }
    }
}

/// The admission generations a quorum counts a worker by: the one it holds
/// (`None` for a pending member), and, while it is a voter of a joint
/// configuration an election founded, the one it held before that election
/// admitted it (see [`Joint`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Admission {
    pub current: Option<Generation>,
    pub prior: Option<Generation>,
}

/// A worker known by its admission generation alone, with no prior one.
impl From<Option<Generation>> for Admission {
    fn from(current: Option<Generation>) -> Self {
        Admission {
            current,
            prior: None,
        }
    }
}

/// The voter set a shard's quorums are counted against, as every worker knows
/// it: a generation, a base generation and how many voters there are.
///
/// The voters are the workers admitted from the base generation through the
/// generation, inclusive. The base generation is that of the latest change
/// a leader made to the configuration, which re-admitted there every voter
/// it counted (see the module docs), so a worker that change left out holds
/// an older admission generation and is not a voter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    generation: Generation,
    base: Generation,
    electorate: Electorate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Electorate {
    Single {
        voter_count: usize,
    },
    /// A move from one configuration to another is in flight (see
    /// [`Joint`]).
    Joint {
        batch_generation: Generation,
        old_base: Generation,
        old_generation: Generation,
        old_voter_count: usize,
        new_voter_count: usize,
    },
}

/// The fields of a single-sided [`Configuration`], for [`Configuration::single`].
pub struct Single {
    pub generation: Generation,
    pub base: Generation,
    pub voter_count: usize,
}

/// The fields of a joint [`Configuration`], for [`Configuration::joint`]:
/// the configuration a shard moves to (the new side) together with the one
/// it moves from (the old side).
///
/// The new side is the voters admitted from `base` through `generation`,
/// `new_voter_count` of them. The old side is the voters of the
/// configuration moved from, whose base and generation were `old_base` and
/// `old_generation`: a worker whose admission generation, or prior admission
/// generation (see [`Admission`]), lies from `old_base` through
/// `old_generation`, `old_voter_count` of them. Workers joining are admitted
/// at `batch_generation`.
///
/// An election founds one whose new side is its roll call's respondents,
/// admitted at `batch_generation`, which is also `base` and `generation`, and
/// whose old side is the configuration the roll call ran under: each
/// respondent keeps the admission it held before as its prior one, so it
/// still counts there. An election won under one not yet committed
/// re-stamps it: the same old side, with `generation`, `base` and
/// `batch_generation` all moved to a generation of the winner's term, and a
/// new side of the respondents re-admitted there. An admission batch has
/// `old_base` and `old_generation` the base and generation of the single
/// configuration it started from, and the batch generation its base and
/// generation. A removal re-announces any of these at a later generation,
/// re-based there, with shrunk counts.
pub struct Joint {
    pub generation: Generation,
    pub base: Generation,
    pub batch_generation: Generation,
    pub old_base: Generation,
    pub old_generation: Generation,
    pub old_voter_count: usize,
    pub new_voter_count: usize,
}

/// The old side of a joint configuration: the configuration it moves from,
/// by base, generation and voter count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OldSide {
    base: Generation,
    generation: Generation,
    voter_count: usize,
}

impl OldSide {
    /// Whether this side has exactly this base, generation and voter count.
    fn is(&self, base: Generation, generation: Generation, voter_count: usize) -> bool {
        self.voter_count == voter_count && self.base == base && self.generation == generation
    }

    /// The workers this side counts, by current or prior admission.
    fn counted(&self) -> Counted {
        Counted::OnceAdmittedWithin(self.base, self.generation)
    }
}

impl Joint {
    /// The first rule of [`Configuration::joint`] this breaks, in the order
    /// that constructor documents; `None` when it breaks none.
    fn first_broken_rule(&self) -> Option<InvalidConfiguration> {
        [
            (
                self.base > self.generation,
                InvalidConfiguration::BaseAfterGeneration,
            ),
            (
                self.base > self.batch_generation,
                InvalidConfiguration::BaseAfterBatchGeneration,
            ),
            (
                self.batch_generation > self.generation,
                InvalidConfiguration::BatchGenerationAfterGeneration,
            ),
            (
                self.old_base > self.old_generation,
                InvalidConfiguration::OldBaseAfterOldGeneration,
            ),
            (
                self.old_generation >= self.batch_generation,
                InvalidConfiguration::OldGenerationNotBeforeBatchGeneration,
            ),
            (
                self.old_base > self.base,
                InvalidConfiguration::OldBaseAfterBase,
            ),
            (
                self.old_voter_count == 0 || self.new_voter_count == 0,
                InvalidConfiguration::ZeroVoterCount,
            ),
        ]
        .into_iter()
        .find_map(|(broken, rule)| broken.then_some(rule))
    }
}

impl Configuration {
    /// A new shard's first configuration: its creator alone, at the genesis
    /// generation, which is also the base generation.
    pub fn genesis(recovery_epoch: RecoveryEpoch) -> Self {
        let genesis = Generation::genesis(recovery_epoch);
        Configuration::single(Single {
            generation: genesis,
            base: genesis,
            voter_count: 1,
        })
        .expect("a genesis configuration's base is its generation, with one voter")
    }

    /// A configuration of `single.voter_count` voters: the workers admitted
    /// from `single.base` through `single.generation`. Refuses a base later
    /// than the generation, and no voters.
    ///
    /// A configuration decoded from a peer is built through here too (see
    /// [`crate::protocol::configuration`], which checks only field presence),
    /// so no configuration exists that breaks these rules.
    pub fn single(single: Single) -> Result<Self, InvalidConfiguration> {
        if single.base > single.generation {
            return Err(InvalidConfiguration::BaseAfterGeneration);
        }
        if single.voter_count == 0 {
            return Err(InvalidConfiguration::ZeroVoterCount);
        }
        Ok(Configuration {
            generation: single.generation,
            base: single.base,
            electorate: Electorate::Single {
                voter_count: single.voter_count,
            },
        })
    }

    /// A joint configuration (see [`Joint`]), whose quorums need a majority
    /// of the old side and a majority of the new side. Refuses, in this order:
    /// `BaseAfterGeneration`, `BaseAfterBatchGeneration`,
    /// `BatchGenerationAfterGeneration`, `OldBaseAfterOldGeneration`,
    /// `OldGenerationNotBeforeBatchGeneration`, `OldBaseAfterBase`, and
    /// `ZeroVoterCount` (old side, then new).
    pub fn joint(joint: Joint) -> Result<Self, InvalidConfiguration> {
        if let Some(broken) = joint.first_broken_rule() {
            return Err(broken);
        }
        Ok(Configuration {
            generation: joint.generation,
            base: joint.base,
            electorate: Electorate::Joint {
                batch_generation: joint.batch_generation,
                old_base: joint.old_base,
                old_generation: joint.old_generation,
                old_voter_count: joint.old_voter_count,
                new_voter_count: joint.new_voter_count,
            },
        })
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    /// The generation of the latest change a leader made to this
    /// configuration, where it re-admitted every voter of the (new side of
    /// the) configuration.
    pub fn base(&self) -> Generation {
        self.base
    }

    /// How many voters a committed configuration has; `None` while joint.
    pub fn voter_count(&self) -> Option<usize> {
        match self.electorate {
            Electorate::Single { voter_count } => Some(voter_count),
            Electorate::Joint { .. } => None,
        }
    }

    /// Whether this is a joint configuration, still moving from one
    /// configuration to another.
    pub fn is_joint(&self) -> bool {
        matches!(self.electorate, Electorate::Joint { .. })
    }

    /// Whether a worker admitted at `admission` is a voter here, on either
    /// side of a joint configuration. A pending member has no admission
    /// generation and is never a voter.
    pub fn is_voter(&self, admission: impl Into<Admission>) -> bool {
        let admission = admission.into();
        match self.electorate {
            Electorate::Single { .. } => self.voters().counts(admission),
            Electorate::Joint {
                old_base,
                old_generation,
                ..
            } => {
                self.voters().counts(admission)
                    || Counted::OnceAdmittedWithin(old_base, old_generation).counts(admission)
            }
        }
    }

    /// The admission generation that a worker admitted at `admission` in
    /// this joint configuration holds in `committed`, when `committed` is
    /// this configuration's own commit: a single configuration at the next
    /// change of this one's generation, in the same recovery epoch and term,
    /// re-based there. Every change from a joint configuration to a single
    /// one at its next generation (a commit, or a removal that empties the
    /// old side) re-admits there every member its new side counted, except
    /// one that removal takes out, which has stopped. So a new-side member
    /// that missed that change is admitted at its generation. `None` otherwise: this
    /// configuration is single, `committed` is not its commit, or the worker
    /// is not on its new side.
    pub fn admission_after_commit(
        &self,
        committed: &Configuration,
        admission: Option<Generation>,
    ) -> Option<Generation> {
        let next = Generation::new(
            self.generation.recovery_epoch,
            self.generation.term,
            self.generation.counter.checked_add(1)?,
        );
        (self.is_joint()
            && !committed.is_joint()
            && committed.generation == next
            && committed.base == next
            && self.voters().counts(admission.into()))
        .then_some(next)
    }

    /// The admission generation that a worker admitted at `admission` in this
    /// single configuration takes up in `batch`, when `batch` is the joint
    /// configuration its own leader started directly on it: at a later
    /// generation of the same recovery epoch and term (the leader promised
    /// the joiners that generation, which its own changes then skip past),
    /// based and batched there, moving from this configuration's voters (its
    /// old side has as many). `batch`'s own start
    /// admits the voters here that echoed this configuration at its generation
    /// (see [`Self::admission_after_commit`]), so a voter that missed the
    /// start is admitted there. `None` otherwise: this configuration is
    /// joint, `batch` is not such a batch, or the worker is not a voter here.
    ///
    /// Only the same term qualifies. An election's founding is shaped like
    /// a batch but in a later term, and its respondents are not every voter
    /// of the configuration it moves from, so a voter it left out must not
    /// count as admitted there.
    pub(crate) fn admission_after_batch_start(
        &self,
        batch: &Configuration,
        admission: Option<Generation>,
    ) -> Option<Generation> {
        (self.is_batch_started_on_this(batch) && self.voters().counts(admission.into()))
            .then_some(batch.generation)
    }

    /// Whether `batch` is the joint configuration this single one's own leader
    /// started directly on it (see [`Self::admission_after_batch_start`]).
    fn is_batch_started_on_this(&self, batch: &Configuration) -> bool {
        let Some(old) = batch.old_electorate() else {
            return false;
        };
        let Electorate::Single { voter_count } = self.electorate else {
            return false;
        };
        old.is(self.base, self.generation, voter_count)
            && batch.is_fresh_batch_after(self.generation)
    }

    /// Whether this is a joint configuration based and batched at its own
    /// generation, minted after `earlier` in `earlier`'s epoch and term: the
    /// shape a leader's batch takes, started on a configuration at `earlier`.
    fn is_fresh_batch_after(&self, earlier: Generation) -> bool {
        let Electorate::Joint {
            batch_generation, ..
        } = self.electorate
        else {
            return false;
        };
        let based_at_its_batch =
            self.base == self.generation && batch_generation == self.generation;
        self.generation > earlier && self.generation.is_in_term_of(earlier) && based_at_its_batch
    }

    /// A joint configuration's old side; `None` for a single one.
    fn old_electorate(&self) -> Option<OldSide> {
        match self.electorate {
            Electorate::Single { .. } => None,
            Electorate::Joint {
                old_base,
                old_generation,
                old_voter_count,
                ..
            } => Some(OldSide {
                base: old_base,
                generation: old_generation,
                voter_count: old_voter_count,
            }),
        }
    }

    /// Whether a worker holding this configuration, and the promise of
    /// admission at `promised` (a generation later than this one that it
    /// does not count in), holds the configuration `batch` the promise is
    /// for: a joint configuration at exactly `promised`, the only one that
    /// admits the workers promised it. The worker is then admitted there, as
    /// its promise says. (A promise lies beyond this configuration's range,
    /// so the worker is no voter of it.)
    pub(crate) fn is_the_batch_promised(
        &self,
        batch: &Configuration,
        promised: Option<Generation>,
    ) -> bool {
        promised.is_some_and(|promised| {
            batch.is_joint()
                && batch.generation == promised
                && promised > self.generation
        })
    }

    /// The voters of a single configuration, or of a joint one's new side.
    fn voters(&self) -> Counted {
        Counted::AdmittedWithin(self.base, self.generation)
    }

    /// This joint configuration re-stamped at `generation`, which is also
    /// its base and batch generation, with the same old side: the new
    /// side's voters are then exactly the workers admitted at `generation`,
    /// `new_voter_count` of them.
    ///
    /// # Panics
    ///
    /// If this configuration is single: only an uncommitted joint one is
    /// re-stamped (see [`Roster::after_election`]).
    fn re_stamped_at(&self, generation: Generation, new_voter_count: usize) -> Configuration {
        let Electorate::Joint {
            old_base,
            old_generation,
            old_voter_count,
            ..
        } = self.electorate
        else {
            unreachable!("only a joint configuration is re-stamped");
        };
        debug_assert!(
            generation > self.generation,
            "a change moves a configuration to a later generation"
        );
        Configuration::joint(Joint {
            generation,
            base: generation,
            batch_generation: generation,
            old_base,
            old_generation,
            old_voter_count,
            new_voter_count,
        })
        .expect("a re-stamped configuration keeps its old side, and its new side has a voter")
    }
}

/// Infallible: every `Configuration` this crate can build is valid on the
/// wire. Written here rather than in the protocol layer so the private
/// `Electorate` enum never has to leave this module; a wire `Configuration`
/// that is not a valid domain `Configuration` is decoded through the checked
/// [`TryFrom<&generated::Configuration>`](crate::protocol::configuration) in
/// the protocol layer instead.
impl From<&Configuration> for generated::Configuration {
    fn from(configuration: &Configuration) -> Self {
        let electorate = match &configuration.electorate {
            Electorate::Single { voter_count } => {
                generated::configuration::Electorate::Single(generated::SingleElectorate {
                    voter_count: *voter_count as u64,
                })
            }
            Electorate::Joint {
                batch_generation,
                old_base,
                old_generation,
                old_voter_count,
                new_voter_count,
            } => generated::configuration::Electorate::Joint(generated::JointElectorate {
                batch_generation: Some((*batch_generation).into()),
                old_voter_count: *old_voter_count as u64,
                new_voter_count: *new_voter_count as u64,
                old_base: Some((*old_base).into()),
                old_generation: Some((*old_generation).into()),
            }),
        };
        generated::Configuration {
            generation: Some(configuration.generation.into()),
            base: Some(configuration.base.into()),
            electorate: Some(electorate),
        }
    }
}

/// Counts workers toward one or more majorities and says whether every one of
/// them is reached.
///
/// Open a tally against a [`Configuration`] (a majority of its voters, or of
/// both sides of a joint one) or against a plain count (a majority of it,
/// where every worker fed counts). [`Tally::and`] joins two tallies into one
/// that needs both, such as an election's win rule: a majority of the roll
/// call's configuration and a majority of its respondents.
#[derive(Debug, Clone)]
pub struct Tally {
    majorities: Vec<Majority>,
    fed: BTreeMap<WorkerId, Admission>,
}

/// More than half of `voter_count`, counting only the fed workers `counted`
/// admits.
#[derive(Debug, Clone)]
struct Majority {
    counted: Counted,
    voter_count: usize,
}

/// Which fed workers count toward a majority.
#[derive(Debug, Clone, Copy)]
enum Counted {
    EveryWorker,
    /// Workers whose admission generation lies from the first bound through
    /// the second. A pending member has none and never counts.
    AdmittedWithin(Generation, Generation),
    /// Workers whose admission generation, or prior admission generation,
    /// lies from the first bound through the second: the voters of the
    /// configuration a joint one moves from.
    OnceAdmittedWithin(Generation, Generation),
}

impl Tally {
    /// A tally that needs a majority of `configuration`'s voters, or for a
    /// joint configuration a majority of each side. Workers that are not
    /// voters there can be fed but never count.
    pub fn against(configuration: &Configuration) -> Self {
        let majorities = match configuration.electorate {
            Electorate::Single { voter_count } => vec![Majority {
                counted: configuration.voters(),
                voter_count,
            }],
            Electorate::Joint {
                old_base,
                old_generation,
                old_voter_count,
                new_voter_count,
                ..
            } => vec![
                Majority {
                    counted: Counted::OnceAdmittedWithin(old_base, old_generation),
                    voter_count: old_voter_count,
                },
                Majority {
                    counted: configuration.voters(),
                    voter_count: new_voter_count,
                },
            ],
        };
        Tally::needing(majorities)
    }

    /// A tally that needs a majority of `voter_count`, counting every worker
    /// fed whatever its admission generation. The caller decides who is
    /// eligible by what it feeds.
    pub fn against_count(voter_count: usize) -> Self {
        Tally::needing(vec![Majority {
            counted: Counted::EveryWorker,
            voter_count,
        }])
    }

    /// Joins two tallies into one that has a quorum only when both would.
    /// Workers already fed to either are kept; one fed to both keeps the
    /// admission generation `self` was fed.
    pub fn and(mut self, other: Tally) -> Self {
        self.majorities.extend(other.majorities);
        for (worker, admission) in other.fed {
            self.fed.entry(worker).or_insert(admission);
        }
        self
    }

    /// Feeds one worker, admitted at `admission` (`None` for a pending
    /// member, or an [`Admission`] with a prior generation). A worker already
    /// fed is ignored, so it counts once, with the admission it was first fed.
    pub fn record(&mut self, worker: WorkerId, admission: impl Into<Admission>) {
        self.fed.entry(worker).or_insert(admission.into());
    }

    pub fn has_quorum(&self) -> bool {
        self.majorities
            .iter()
            .all(|majority| majority.is_reached_by(self.fed.values().copied()))
    }

    /// Whether every voter of every majority it needs has been fed: all of a
    /// configuration's voters, of both sides for a joint one.
    pub fn is_unanimous(&self) -> bool {
        self.majorities.iter().all(|majority| {
            let counted = self
                .fed
                .values()
                .filter(|admission| majority.counted.counts(**admission))
                .count();
            counted >= majority.voter_count
        })
    }

    fn needing(majorities: Vec<Majority>) -> Self {
        Tally {
            majorities,
            fed: BTreeMap::new(),
        }
    }
}

impl Majority {
    fn is_reached_by(&self, admissions: impl Iterator<Item = Admission>) -> bool {
        let counted = admissions
            .filter(|admission| self.counted.counts(*admission))
            .count();
        counted > self.voter_count / 2
    }
}

impl Counted {
    fn counts(&self, admission: Admission) -> bool {
        let within = |from: &Generation, to: &Generation, generation: Option<Generation>| {
            generation.is_some_and(|generation| *from <= generation && generation <= *to)
        };
        match self {
            Counted::EveryWorker => true,
            Counted::AdmittedWithin(from, to) => within(from, to, admission.current),
            Counted::OnceAdmittedWithin(from, to) => {
                within(from, to, admission.current) || within(from, to, admission.prior)
            }
        }
    }
}
