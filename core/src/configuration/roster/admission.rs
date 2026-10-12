use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::ids::WorkerId;

use crate::configuration::{Admission, Configuration, Electorate, Generation, Joint};
use super::Roster;

/// The admissions a leader has promised joiners, ahead of the batch that
/// admits them: a joiner told it will be admitted at a generation holds that
/// as its admission before the batch exists, so a quorum counts it there
/// whether or not the leader lives to start the batch.
#[derive(Debug, Clone, Default)]
pub(super) struct Promises {
    /// The latest generation promised: every change the leader announces
    /// moves past it, so no joiner holding a promise is ever counted at a
    /// generation it was not admitted in.
    latest: Option<Generation>,
    /// The workers promised `latest`, each with whether it has said it holds
    /// the promise. Earlier rounds are forgotten: a worker left out of the
    /// latest holds a promise no batch honours.
    round: BTreeMap<WorkerId, bool>,
}

impl Promises {
    /// The latest round, while no change announced since reached its
    /// generation (`current` is the configuration's) and it names someone.
    fn standing(&self, current: Generation) -> Option<(Generation, &BTreeMap<WorkerId, bool>)> {
        let latest = self.latest?;
        (latest > current && !self.round.is_empty()).then_some((latest, &self.round))
    }

    /// The generation the next change starts from: `current`, or the latest
    /// promise if that is later.
    fn floor(&self, current: Generation) -> Generation {
        self.latest.filter(|latest| *latest > current).unwrap_or(current)
    }

    /// `worker` said it holds an admission at `held`: confirms its promise if
    /// that is the generation promised.
    fn confirm(&mut self, worker: &WorkerId, held: Generation) {
        if self.latest == Some(held)
            && let Some(confirmed) = self.round.get_mut(worker)
        {
            *confirmed = true;
        }
    }

    /// A round a taken-out worker was promised in is void: a batch at its
    /// generation without it would leave it holding a promise the
    /// configuration there does not honour. The joiners still waiting are
    /// promised again, past it.
    pub(super) fn void_if_naming(&mut self, worker: &WorkerId) {
        if self.round.contains_key(worker) {
            self.round.clear();
        }
    }
}

impl Roster {
    /// The generation of the next change `leader_term`'s leader announces:
    /// the next past the configuration's, or past the latest admission it
    /// promised, if that is later. A promise names a generation only the
    /// batch admitting the joiners promised it may announce; any other change
    /// announced there would count a joiner that holds the promise as a voter
    /// it never admitted.
    ///
    /// # Panics
    ///
    /// See [`Generation::next_change`].
    pub(super) fn next_generation(&self, leader_term: u64) -> Generation {
        self.promises
            .floor(self.configuration.generation)
            .next_change(leader_term)
    }

    /// The latest promise round, while a batch has yet to honour it: nothing
    /// announced since reached its generation, and it names someone.
    fn standing_promises(&self) -> Option<(Generation, &BTreeMap<WorkerId, bool>)> {
        self.promises.standing(self.configuration.generation)
    }

    /// The generation `worker` is promised admission at, while a batch has
    /// yet to honour the promise.
    pub fn promised_admission_of(&self, worker: &WorkerId) -> Option<Generation> {
        let (latest, round) = self.standing_promises()?;
        round.contains_key(worker).then_some(latest)
    }

    /// The workers promised admission in the round a batch has yet to
    /// honour, each with whether it has said it holds the promise.
    pub(crate) fn promised_workers(&self) -> impl Iterator<Item = (&WorkerId, bool)> {
        self.standing_promises()
            .into_iter()
            .flat_map(|(_, round)| round.iter().map(|(worker, held)| (worker, *held)))
    }

    /// Whether a round of promises stands and every worker it names has said
    /// it holds its promise.
    pub(crate) fn is_every_promise_held(&self) -> bool {
        self.standing_promises()
            .is_some_and(|(_, round)| round.values().all(|held| *held))
    }

    /// Records that `worker` holds an admission at `held`, as its heartbeat
    /// said: confirms its promise if that is the generation promised it.
    pub fn record_held_admission(&mut self, worker: &WorkerId, held: Generation) {
        self.promises.confirm(worker, held);
    }

    /// Promises the admissible `joiners` (see [`Roster::is_admissible`]) an
    /// admission at a generation of the leader's term, in a new round that
    /// replaces any earlier one: each is admitted there by the batch that
    /// follows once every one has confirmed, so each already holds that
    /// admission when the batch exists, and a quorum counts it even if the
    /// leader is lost before the batch reaches it. The generation is past
    /// every change announced and every earlier promise. Only a single
    /// configuration takes promises. Returns whether a round stands.
    ///
    /// # Panics
    ///
    /// See [`Generation::next_change`].
    pub fn promise_admission(&mut self, joiners: &BTreeSet<WorkerId>, leader_term: u64) -> bool {
        self.promises.round.clear();
        if self.configuration.is_joint() {
            return false;
        }
        let promised: BTreeMap<WorkerId, bool> = joiners
            .iter()
            .filter(|joiner| self.is_admissible(joiner))
            .map(|joiner| (joiner.clone(), false))
            .collect();
        if promised.is_empty() {
            return false;
        }
        self.promises.latest = Some(self.next_generation(leader_term));
        self.promises.round = promised;
        true
    }

    /// Starts an admission batch (see [`Roster::promise_admission`]) of the
    /// workers promised admission, once every one has confirmed its promise,
    /// and `leader`, if it is admissible and given: the leader needs no
    /// promise, for it holds the batch itself. Without a round of promises
    /// the leader alone is admitted. Returns whether it started one.
    ///
    /// Only a single configuration takes a batch: one change at a time, so
    /// a joint configuration admits no one until it commits, and joiners
    /// arriving meanwhile wait for the next batch. The batch is a joint
    /// configuration at the generation promised, j, re-based there: its old
    /// side is the single configuration (its base, generation and voter
    /// count), and its new side the voters re-admitted at j, each keeping its
    /// old admission as the prior one by which the old side counts it, plus
    /// the joiners, admitted at j with no prior one. It commits like a
    /// founding (see [`Roster::commit_if_confirmed`]).
    ///
    /// # Panics
    ///
    /// See [`Generation::next_change`].
    pub fn begin_batch(&mut self, leader: Option<&WorkerId>, leader_term: u64) -> bool {
        let Electorate::Single { voter_count } = self.configuration.electorate else {
            return false;
        };
        let (batch, promised) = match self.standing_promises() {
            Some((latest, round)) => {
                if !round.values().all(|held| *held) {
                    return false;
                }
                (latest, round.keys().cloned().collect::<BTreeSet<_>>())
            }
            None => (self.next_generation(leader_term), BTreeSet::new()),
        };
        let admitted: BTreeSet<WorkerId> = promised
            .into_iter()
            .chain(leader.cloned())
            .filter(|joiner| self.is_admissible(joiner))
            .collect();
        if admitted.is_empty() {
            return false;
        }
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
        self.promises.round.clear();
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
}
