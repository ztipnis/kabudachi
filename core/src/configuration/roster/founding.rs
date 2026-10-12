use std::collections::{BTreeMap, BTreeSet};

use crate::coordination_authority::RecoveryEpoch;
use crate::protocol::ids::WorkerId;

use crate::configuration::{Admission, Configuration, Counted, Electorate, Generation, Joint, Single, Tally};
use super::{Roster, Side};

impl Roster {
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
        recovery_epoch: RecoveryEpoch,
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
        let called_under = (
            roll_call_configuration.generation,
            Roster::sides_called_under(roll_call_configuration, respondents),
        );
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
        roster.called_under = Some(called_under);
        roster
    }

    /// Seeds what each of `held`'s members last echoed with the generation
    /// it said it holds, in answering the roll call this roster's leader won
    /// on. A voter refuses a call older than its own configuration, so the
    /// generation a respondent reports is a lower bound on the generation of
    /// any call it grants from then on: the leader can anchor the removal of
    /// one that then echoes nothing in this office to it, and a respondent
    /// that reported nothing, or an older generation than the call's, is
    /// anchored to no configuration the leader knows the voters of. Never
    /// lowers what a member has already echoed.
    pub(crate) fn seed_held_generations(&mut self, held: &BTreeMap<WorkerId, Generation>) {
        for (member, generation) in held {
            if self.members.contains_key(member) {
                self.echoes.raise(member, *generation);
            }
        }
    }

    /// The sides of `call`, as far as its `respondents` show them: each
    /// respondent the side counts, by the admission it answered with, and
    /// the side's voter count.
    fn sides_called_under(
        call: &Configuration,
        respondents: &BTreeMap<WorkerId, Admission>,
    ) -> Vec<Side> {
        let known = |counted: Counted| -> BTreeSet<WorkerId> {
            respondents
                .iter()
                .filter(|(_, admission)| counted.counts(**admission))
                .map(|(respondent, _)| respondent.clone())
                .collect()
        };
        match call.electorate {
            Electorate::Single { voter_count } => vec![(known(call.voters()), voter_count)],
            Electorate::Joint {
                old_base,
                old_generation,
                old_voter_count,
                new_voter_count,
                ..
            } => vec![
                (known(call.voters()), new_voter_count),
                (
                    known(Counted::OnceAdmittedWithin(old_base, old_generation)),
                    old_voter_count,
                ),
            ],
        }
    }

    /// The configuration an election this roster's leader won was called
    /// under, and its sides, as the respondents showed them.
    pub(crate) fn called_under(&self) -> Option<(Generation, Vec<Side>)> {
        self.called_under.clone()
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
        roster.called_under = Some((
            joint.generation,
            Roster::sides_called_under(joint, respondents),
        ));
        roster
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
        for member in self.echoes.naming(generation) {
            tally.record(member.clone(), self.counted_admission_of(member));
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
        true
    }
}
