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

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::generated;
use crate::protocol::ids::WorkerId;

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
    // the comparison order.
    recovery_epoch: u64,
    term: u64,
    counter: u64,
}

impl Generation {
    pub fn new(recovery_epoch: u64, term: u64, counter: u64) -> Self {
        Generation {
            recovery_epoch,
            term,
            counter,
        }
    }

    /// A new shard's first generation: term 0, counter 0. It is the genesis
    /// configuration's generation and base generation, and its creator's
    /// admission generation.
    pub fn genesis(recovery_epoch: u64) -> Self {
        Generation::new(recovery_epoch, 0, 0)
    }

    pub fn recovery_epoch(&self) -> u64 {
        self.recovery_epoch
    }

    /// The term of the election that founded this generation's
    /// configuration, or of the leader that announced it.
    pub fn term(&self) -> u64 {
        self.term
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
        recovery_epoch: u64,
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
            recovery_epoch: generation.recovery_epoch,
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

impl Configuration {
    /// A new shard's first configuration: its creator alone, at the genesis
    /// generation, which is also the base generation.
    pub fn genesis(recovery_epoch: u64) -> Self {
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
        if joint.base > joint.generation {
            return Err(InvalidConfiguration::BaseAfterGeneration);
        }
        if joint.base > joint.batch_generation {
            return Err(InvalidConfiguration::BaseAfterBatchGeneration);
        }
        if joint.batch_generation > joint.generation {
            return Err(InvalidConfiguration::BatchGenerationAfterGeneration);
        }
        if joint.old_base > joint.old_generation {
            return Err(InvalidConfiguration::OldBaseAfterOldGeneration);
        }
        if joint.old_generation >= joint.batch_generation {
            return Err(InvalidConfiguration::OldGenerationNotBeforeBatchGeneration);
        }
        if joint.old_base > joint.base {
            return Err(InvalidConfiguration::OldBaseAfterBase);
        }
        if joint.old_voter_count == 0 || joint.new_voter_count == 0 {
            return Err(InvalidConfiguration::ZeroVoterCount);
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

/// What only the leader holds: the configuration it leads, every member with
/// its admission generation (and, while the configuration is joint, the
/// prior admission generation of each member an election or a batch
/// re-admitted), the pending joiners, which claim work but have no admission
/// generation until an admission batch or an election admits them, the
/// configuration generation each member last said it holds, and the
/// workers it has taken out.
///
/// A `WorkerId` names one process incarnation: a restarted worker
/// comes back under a new one, as a pending joiner, and the old one only
/// ever leaves. So a worker once taken out is never held again.
#[derive(Debug, Clone)]
pub struct Roster {
    configuration: Configuration,
    members: BTreeMap<WorkerId, Generation>,
    prior_admissions: BTreeMap<WorkerId, Generation>,
    pending: BTreeSet<WorkerId>,
    held_generations: BTreeMap<WorkerId, Generation>,
    departed: BTreeSet<WorkerId>,
}

impl Roster {
    /// A roster leading `configuration`, holding `members` at their admission
    /// generations and `pending` joiners. A worker given as both is a member.
    pub fn new(
        configuration: Configuration,
        members: BTreeMap<WorkerId, Generation>,
        mut pending: BTreeSet<WorkerId>,
    ) -> Self {
        pending.retain(|worker| !members.contains_key(worker));
        Roster {
            configuration,
            members,
            prior_admissions: BTreeMap::new(),
            pending,
            held_generations: BTreeMap::new(),
            departed: BTreeSet::new(),
        }
    }

    /// The roster of the winner of the election for `term`, at
    /// `recovery_epoch`, whose roll call ran under `roll_call_configuration`
    /// and drew `respondents`, each with the admission it answered with.
    ///
    /// Under a single configuration C0, the respondents found the next one:
    /// a joint configuration whose new side is the respondents, every one
    /// admitted at g′ = (`recovery_epoch`, `term`, C0's counter + 1), which
    /// is also its base, and whose old side is C0's voters. Each respondent
    /// keeps the admission it answered with as its prior one, so a voter of
    /// C0 still counts there. A worker that did not answer is no member: a
    /// voter of C0 among them still counts on the old side by its own
    /// admission, and on the new side of nothing.
    ///
    /// Under a joint configuration, which no majority of each side has
    /// committed yet, the election founds nothing new: its winner re-stamps
    /// that configuration at g′, a generation of its own term, with the same
    /// old side, and re-bases the new side there. Each respondent the new side counted, by the
    /// admission it answered with, is re-admitted at g′, and the new side
    /// counts exactly those: a
    /// new-side voter that did not answer
    /// holds no admission at g′, so counting it would leave a phantom voter
    /// no quorum could ever include. Every other respondent is a member at
    /// the admission it answered with (a later batch admits it), a pending
    /// one stays pending, and each keeps the prior admission it answered
    /// with, by which the old side counts it.
    ///
    /// Either way the new side's range is the one generation g′, which only
    /// this term's winner mints, so it never takes in a worker another
    /// election admitted (see [`Roster::commit_if_confirmed`]).
    ///
    /// # Panics
    ///
    /// See [`Generation::founded_by_election`].
    pub fn after_election(
        recovery_epoch: u64,
        term: u64,
        roll_call_configuration: &Configuration,
        respondents: &BTreeMap<WorkerId, Admission>,
    ) -> Self {
        let founded = Generation::founded_by_election(
            recovery_epoch,
            term,
            roll_call_configuration.generation,
        );
        let Electorate::Single { voter_count } = roll_call_configuration.electorate else {
            return Roster::re_stamped(roll_call_configuration, founded, respondents);
        };
        let mut roster = Roster::new(
            Configuration::joint(Joint {
                generation: founded,
                base: founded,
                batch_generation: founded,
                old_base: roll_call_configuration.base,
                old_generation: roll_call_configuration.generation,
                old_voter_count: voter_count,
                new_voter_count: respondents.len(),
            })
            .expect("a roll call's respondents are at least one, and its old side is earlier than the founded generation"),
            respondents
                .keys()
                .map(|respondent| (respondent.clone(), founded))
                .collect(),
            BTreeSet::new(),
        );
        roster.prior_admissions = respondents
            .iter()
            .filter_map(|(respondent, admission)| {
                admission
                    .current
                    .map(|current| (respondent.clone(), current))
            })
            .collect();
        roster
    }

    /// The roster of a win under `joint`, a joint configuration not yet
    /// committed: `joint` re-stamped and re-based at `restamped`, its new
    /// side counting the respondents re-admitted there (see
    /// [`Roster::after_election`]).
    fn re_stamped(
        joint: &Configuration,
        restamped: Generation,
        respondents: &BTreeMap<WorkerId, Admission>,
    ) -> Self {
        let mut roster = Roster::new(
            joint.clone(),
            respondents
                .iter()
                .filter_map(|(respondent, admission)| {
                    admission
                        .current
                        .map(|current| (respondent.clone(), current))
                })
                .collect(),
            respondents
                .iter()
                .filter(|(_, admission)| admission.current.is_none())
                .map(|(respondent, _)| respondent.clone())
                .collect(),
        );
        roster.prior_admissions = respondents
            .iter()
            .filter_map(|(respondent, admission)| {
                admission.prior.map(|prior| (respondent.clone(), prior))
            })
            .collect();
        let re_admitted = roster.re_admit_new_side_at(restamped);
        roster.configuration = joint.re_stamped_at(restamped, re_admitted);
        roster
    }

    /// Re-admits at `generation` every member the current configuration
    /// counts, by its admission alone, as a voter of its new side (of a
    /// single configuration, as a voter at all): the members a change this
    /// leader announces at `generation`, re-based there, counts on its new
    /// side. Prior admissions are left as they are. Returns how many it
    /// re-admitted, which is the new side's voter count after the change.
    fn re_admit_new_side_at(&mut self, generation: Generation) -> usize {
        let new_side = self.configuration.voters();
        let mut re_admitted = 0;
        for admission in self.members.values_mut() {
            if new_side.counts(Admission::from(Some(*admission))) {
                *admission = generation;
                re_admitted += 1;
            }
        }
        re_admitted
    }

    /// How many members the current configuration counts, by their
    /// admission alone, on its new side.
    fn new_side_member_count(&self) -> usize {
        let new_side = self.configuration.voters();
        self.members
            .values()
            .filter(|admission| new_side.counts(Admission::from(Some(**admission))))
            .count()
    }

    /// A new shard's roster: its creator as the only member, admitted at the
    /// genesis generation.
    pub fn genesis(creator: WorkerId, recovery_epoch: u64) -> Self {
        Roster {
            configuration: Configuration::genesis(recovery_epoch),
            members: BTreeMap::from([(creator, Generation::genesis(recovery_epoch))]),
            prior_admissions: BTreeMap::new(),
            pending: BTreeSet::new(),
            held_generations: BTreeMap::new(),
            departed: BTreeSet::new(),
        }
    }

    pub fn configuration(&self) -> &Configuration {
        &self.configuration
    }

    /// A member's admission generation; `None` for a pending joiner or a
    /// worker the roster does not hold.
    pub fn admission_of(&self, worker: &WorkerId) -> Option<Generation> {
        self.members.get(worker).copied()
    }

    /// The admission generation a member held before the election that
    /// founded this joint configuration re-admitted it; `None` for any
    /// other worker, and once the configuration is committed.
    pub fn prior_admission_of(&self, worker: &WorkerId) -> Option<Generation> {
        self.prior_admissions.get(worker).copied()
    }

    /// Both admission generations a quorum counts `worker` by.
    pub fn counted_admission_of(&self, worker: &WorkerId) -> Admission {
        Admission {
            current: self.admission_of(worker),
            prior: self.prior_admission_of(worker),
        }
    }

    /// Records that `member` holds a configuration at `held`, as its
    /// heartbeat said. Only the latest said counts; a worker that is no
    /// member is ignored.
    pub fn record_held_generation(&mut self, member: &WorkerId, held: Generation) {
        if self.members.contains_key(member) {
            self.held_generations.insert(member.clone(), held);
        }
    }

    /// Commits a joint configuration once `leader` and the members that said
    /// they hold exactly its generation are a quorum of it: a majority of
    /// each side. The
    /// configuration becomes its new side alone at the next generation
    /// `leader`, elected in `leader_term`, announces, re-based there: every
    /// member the new side counted is re-admitted at it, the committed
    /// configuration counts exactly those, and prior admissions
    /// are dropped.
    /// Returns whether it committed. A single configuration has nothing to
    /// commit.
    ///
    /// The leader of a term mints the joint configuration it leads at a
    /// generation of that term (see [`Roster::after_election`]), and no
    /// other leader mints one there. So a majority of the old side then
    /// holds that configuration or a later one, and refuses a roll call
    /// under any configuration of an earlier generation: the one the joint
    /// one moved from, and a rival joint one founded from it. A generation
    /// another configuration is at, though later, names no configuration
    /// this leader leads, so a member saying it holds one does not count.
    ///
    /// # Panics
    ///
    /// See [`Generation::next_change`].
    pub fn commit_if_confirmed(&mut self, leader: &WorkerId, leader_term: u64) -> bool {
        let Electorate::Joint {
            new_voter_count, ..
        } = self.configuration.electorate
        else {
            return false;
        };
        let generation = self.configuration.generation;
        let mut tally = Tally::against(&self.configuration);
        tally.record(leader.clone(), self.counted_admission_of(leader));
        for (member, held) in &self.held_generations {
            if *held == generation {
                tally.record(member.clone(), self.counted_admission_of(member));
            }
        }
        if !tally.has_quorum() {
            return false;
        }
        let committed = generation.next_change(leader_term);
        let re_admitted = self.re_admit_new_side_at(committed);
        debug_assert!(re_admitted >= 1, "a commit's quorum re-admits some member");
        self.configuration = Configuration::single(Single {
            generation: committed,
            base: committed,
            // The quorum above holds a majority of the new side, so some
            // member was re-admitted; the fallback only keeps a local bug
            // from announcing no voters in a release build.
            voter_count: if re_admitted >= 1 {
                re_admitted
            } else {
                new_voter_count
            },
        })
        .expect("a commit's base is its generation, and it has a voter");
        self.prior_admissions.clear();
        self.held_generations.clear();
        true
    }

    /// Holds a joiner as pending. A worker that is already a member stays one:
    /// only a configuration change may take a voter out. A worker this
    /// roster took out is ignored: it never comes back under the same
    /// `WorkerId` (see [`Roster`]), so only a message of its old incarnation
    /// still in flight can name it.
    pub fn add_pending(&mut self, worker: WorkerId) {
        if !self.members.contains_key(&worker) && !self.departed.contains(&worker) {
            self.pending.insert(worker);
        }
    }

    /// Whether `worker` could join in an admission batch: it is pending, or
    /// a member the configuration does not count on its new side (a
    /// respondent a re-stamp left at its old admission, say).
    pub fn is_admissible(&self, worker: &WorkerId) -> bool {
        self.pending.contains(worker)
            || self.members.get(worker).is_some_and(|admission| {
                !self
                    .configuration
                    .voters()
                    .counts(Admission::from(Some(*admission)))
            })
    }

    /// Starts an admission batch admitting those of
    /// `joiners` that are admissible (see [`Roster::is_admissible`]), as
    /// the leader elected in `leader_term` announces it. Returns whether it
    /// started one.
    ///
    /// Only a single configuration takes a batch: one change at a time, so
    /// a joint configuration admits no one until it commits, and joiners
    /// arriving meanwhile wait for the next batch. The batch is a joint
    /// configuration at the next generation this leader announces, j, re-based
    /// there: its old side is the single configuration (its base, generation
    /// and voter count), and its new side the voters re-admitted at j, each
    /// keeping its old admission as the prior one by which the old side
    /// counts it, plus the joiners, admitted at j with no prior one. It
    /// commits like a founding (see [`Roster::commit_if_confirmed`]).
    ///
    /// # Panics
    ///
    /// See [`Generation::next_change`].
    pub fn begin_batch(&mut self, joiners: &BTreeSet<WorkerId>, leader_term: u64) -> bool {
        let Electorate::Single { voter_count } = self.configuration.electorate else {
            return false;
        };
        let admitted: Vec<WorkerId> = joiners
            .iter()
            .filter(|joiner| self.is_admissible(joiner))
            .cloned()
            .collect();
        if admitted.is_empty() {
            return false;
        }
        let batch = self.configuration.generation.next_change(leader_term);
        let new_side = self.configuration.voters();
        let mut new_voter_count = 0;
        for (member, admission) in self.members.iter_mut() {
            if new_side.counts(Admission::from(Some(*admission))) {
                self.prior_admissions.insert(member.clone(), *admission);
                *admission = batch;
                new_voter_count += 1;
            }
        }
        for joiner in admitted {
            self.pending.remove(&joiner);
            self.prior_admissions.remove(&joiner);
            self.members.insert(joiner, batch);
            new_voter_count += 1;
        }
        self.configuration = Configuration::joint(Joint {
            generation: batch,
            base: batch,
            batch_generation: batch,
            old_base: self.configuration.base,
            old_generation: self.configuration.generation,
            old_voter_count: voter_count,
            new_voter_count,
        })
        .expect("a batch's old generation is its base's, before the batch, and both sides have a voter");
        true
    }

    pub fn is_pending(&self, worker: &WorkerId) -> bool {
        self.pending.contains(worker)
    }

    /// Every member, with its admission generation.
    pub fn members(&self) -> &BTreeMap<WorkerId, Generation> {
        &self.members
    }

    pub fn pending(&self) -> &BTreeSet<WorkerId> {
        &self.pending
    }

    /// Takes the departing `workers` out together (every pending
    /// SELF_REMOVE in the next generation), as the leader
    /// elected in `leader_term` announces it, with no commit round.
    ///
    /// If any of them is a voter here, on either side of a joint
    /// configuration, the configuration moves once, to the next generation
    /// that leader announces (see [`Generation::next_change`]), re-based
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
    /// (see [`Generation::next_change`]).
    pub fn remove_all(&mut self, workers: &BTreeSet<WorkerId>, leader_term: u64) {
        let old_side = match self.configuration.electorate {
            Electorate::Single { .. } => None,
            Electorate::Joint {
                old_base,
                old_generation,
                old_voter_count,
                ..
            } => Some((old_base, old_generation, old_voter_count)),
        };
        let new_side = self.configuration.voters();
        let (mut counted_anywhere, mut counted_on_old_side) = (false, 0);
        for worker in workers {
            self.departed.insert(worker.clone());
            self.pending.remove(worker);
            self.held_generations.remove(worker);
            let prior = self.prior_admissions.remove(worker);
            let Some(current) = self.members.remove(worker) else {
                continue;
            };
            let admission = Admission {
                current: Some(current),
                prior,
            };
            let on_old_side = old_side.is_some_and(|(old_base, old_generation, _)| {
                Counted::OnceAdmittedWithin(old_base, old_generation).counts(admission)
            });
            counted_anywhere |= new_side.counts(admission) || on_old_side;
            counted_on_old_side += usize::from(on_old_side);
        }
        if !counted_anywhere || self.new_side_member_count() == 0 {
            return;
        }

        let changed = self.configuration.generation.next_change(leader_term);
        let new_voter_count = self.re_admit_new_side_at(changed);
        let old_side_left = old_side.map(|(old_base, old_generation, old_voter_count)| {
            (
                old_base,
                old_generation,
                old_voter_count.saturating_sub(counted_on_old_side),
            )
        });
        self.configuration = match old_side_left {
            Some((old_base, old_generation, old_voter_count)) if old_voter_count >= 1 => {
                Configuration::joint(Joint {
                    generation: changed,
                    base: changed,
                    batch_generation: changed,
                    old_base,
                    old_generation,
                    old_voter_count,
                    new_voter_count,
                })
                .expect("a removal keeps the old side it had, and both sides have a voter")
            }
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
