mod admission;
mod echoes;
mod founding;
mod removal;

use std::collections::{BTreeMap, BTreeSet};

use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::ids::WorkerId;

use crate::configuration::{Admission, Configuration, Counted, Electorate, Generation};
use admission::Promises;
use echoes::Echoes;

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
    /// The configuration an election this roster's leader won was called
    /// under, with its voters (see [`Roster::sides`]); `None` for a roster
    /// no election made.
    called_under: Option<(Generation, Vec<Side>)>,
    configuration: Configuration,
    members: BTreeMap<WorkerId, Generation>,
    prior_admissions: BTreeMap<WorkerId, Generation>,
    pending: BTreeSet<WorkerId>,
    echoes: Echoes,
    departed: BTreeSet<WorkerId>,
    promises: Promises,
}

/// One side of a configuration's electorate: the voters known by id and how
/// many there are in all. A side an election was called under counts voters
/// that never answered, whom nobody knows by id.
pub(crate) type Side = (BTreeSet<WorkerId>, usize);

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
            called_under: None,
            configuration,
            members,
            prior_admissions: BTreeMap::new(),
            pending,
            echoes: Echoes::default(),
            departed: BTreeSet::new(),
            promises: Promises::default(),
        }
    }

    /// A new shard's roster: its creator as the only member, admitted at the
    /// genesis generation.
    pub fn genesis(creator: WorkerId, recovery_epoch: RecoveryEpoch) -> Self {
        Roster {
            called_under: None,
            configuration: Configuration::genesis(recovery_epoch),
            members: BTreeMap::from([(creator, Generation::genesis(recovery_epoch))]),
            prior_admissions: BTreeMap::new(),
            pending: BTreeSet::new(),
            echoes: Echoes::default(),
            departed: BTreeSet::new(),
            promises: Promises::default(),
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
    /// heartbeat said. Only the highest said counts, so a heartbeat that
    /// arrives late never takes back a newer one; a worker that is no member
    /// is ignored.
    pub fn record_held_generation(&mut self, member: &WorkerId, held: Generation) {
        if self.members.contains_key(member) {
            self.echoes.record(member, held);
        }
    }

    /// Whether every member the configuration counts, `leader` aside, has
    /// said it holds the configuration's generation since it took it.
    pub(crate) fn is_held_by_every_voter(&self, leader: &WorkerId) -> bool {
        let voters = self.configuration.voters();
        self.members
            .iter()
            .filter(|(member, admission)| {
                *member != leader && voters.counts(Admission::from(Some(**admission)))
            })
            .all(|(member, _)| {
                self.echoes
                    .highest(member)
                    .is_some_and(|held| held >= self.configuration.generation())
            })
    }

    /// The highest generation `member` has said it holds, in this office or
    /// as a respondent of the roll call that made its leader; `None` until it
    /// says so.
    pub(crate) fn held_generation_of(&self, member: &WorkerId) -> Option<Generation> {
        self.echoes.highest(member)
    }

    /// The members the configuration counts as voters now, the leader
    /// included.
    pub(crate) fn counted_voters(&self) -> BTreeSet<WorkerId> {
        let voters = self.configuration.voters();
        self.members
            .iter()
            .filter(|(_, admission)| voters.counts(Admission::from(Some(**admission))))
            .map(|(member, _)| member.clone())
            .collect()
    }

    /// The sides of the current configuration: of a single one, its counted
    /// voters; of a joint one, its new side and its old side.
    pub(crate) fn sides(&self) -> Vec<Side> {
        let counted = self.counted_voters();
        match self.configuration.electorate {
            Electorate::Single { voter_count } => vec![(counted, voter_count)],
            Electorate::Joint {
                old_voter_count,
                new_voter_count,
                ..
            } => {
                let mut sides = vec![(counted, new_voter_count)];
                if let Some((_, known, _)) = self.old_side() {
                    sides.push((known, old_voter_count));
                }
                sides
            }
        }
    }

    /// While the configuration is joint: the generation of the configuration
    /// it moves from, the members its old side counts (by the admissions
    /// they held then, those the roster knows), and how many voters that old
    /// side has in all.
    pub(crate) fn old_side(&self) -> Option<(Generation, BTreeSet<WorkerId>, usize)> {
        let Electorate::Joint {
            old_base,
            old_generation,
            old_voter_count,
            ..
        } = self.configuration.electorate
        else {
            return None;
        };
        let old_side = Counted::OnceAdmittedWithin(old_base, old_generation);
        let counted = self
            .members
            .iter()
            .filter(|(member, current)| {
                old_side.counts(Admission {
                    current: Some(**current),
                    prior: self.prior_admissions.get(*member).copied(),
                })
            })
            .map(|(member, _)| member.clone())
            .collect();
        Some((old_generation, counted, old_voter_count))
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

    /// Whether the roster holds `worker`, admitted or pending.
    pub(crate) fn holds(&self, worker: &WorkerId) -> bool {
        self.members.contains_key(worker) || self.pending.contains(worker)
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
}
