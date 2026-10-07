//! Property tests for the election: random
//! sequences of faults against a `Cluster` of 4 to 7 voters, or of 3 to 5
//! voters and 1 or 2 pending members, each node with its own scheduler,
//! checking these invariants after every event:
//!
//! - L1: at most one node leads any one term. Two nodes may both be
//!   `Leader` at once: a stalled old leader stays `Leader` until its driver
//!   hands it what happened meanwhile. L2 is what keeps them from acting at
//!   once.
//! - L2: at most one node's scheduler holds a valid leadership grant at any
//!   instant (the harness checks after every step, see
//!   `Cluster::first_grant_overlap`): an old leader's grant ends before any
//!   new leader's first one, even while the old leader's driver is stalled
//!   and it never hears that it lost.
//! - L3: no election is won, and so no certificate sent, unless the voters
//!   that granted the winner are a returning quorum of its roll call's
//!   configuration: counting only those admitted from that configuration's
//!   base generation through its generation, a majority of its voter count;
//!   for a joint configuration, also a majority of its old side, counting
//!   those admitted, or admitted before, from the old side's base through
//!   its generation. Counted from the messages the nodes sent, which is at
//!   least what the winner received.
//! - L4: no node holds an admission it did not come by: every admission
//!   generation a node holds, or answers a roll call with, is the genesis
//!   generation, or one of a term that node led, or one that term's leader
//!   sent it (in a certificate or an ack) as a respondent of its winning
//!   roll call, or at a batch's generation, or having heard it confirm one
//!   of its acks of the term (a joiner, which a batch
//!   takes only once it has confirmed one). Every generation of a term is minted by
//!   that term's one leader (L1). A worker an election left out can come
//!   back only through that: a batch, which admits it on the batch's new
//!   side alone, never on the side of the configuration the batch moved
//!   from.
//!
//! A batch's joiners never count as returning voters of a configuration
//! founded from the one the batch moved from, should the batch be abandoned
//! before it commits: that is L3, whose oracle counts each side by its
//! generation range, and a joiner's only admission is the batch generation,
//! past the moved-from configuration's range. The pending-member run checks
//! that some cases start batches and win elections under a batch, or under
//! the configuration a batch moved from.
//! - L5: every node's configuration generation has a term no later than the
//!   highest term the node has seen, and the node's own recovery epoch.
//! - L6: a node's `term()` and `recovery_epoch()` never decrease.
//! - L7: a node that has drained (reached `Stopped`) is never `Leader`
//!   later. A drain asked for outside `Active`/`Leader` waits until the node
//!   gets there, so a node counts as drained from the first time it is seen
//!   `Stopped`, whichever event got it there.
//! - L8: a `Heal` alone changes no node's `term()` or `recovery_epoch()`.
//!
//! L3 and L4 are checked by an oracle of their own over the wire messages,
//! comparing generations as (recovery epoch, term, counter) tuples, so a
//! fault in the crate's own generation order or tally cannot hide itself.
//!
//! Events: `Advance` (1..=15 ticks), `Partition` (a random 2-way split of
//! the nodes), `Heal`, `Drain` and `Stall` (a random node, for 1..=20
//! ticks), and changes to the network's drop rate (up to 30%) and delay (up
//! to 3 ticks). Each case also draws the network's seed, its duplicate rate
//! and whether it reorders deliveries. Sequences are 1..150 events, and a
//! run checks 256 cases of each cluster shape (see `CASES`).
//!
//! No node joins later: a pending member is one from the start, holding the
//! voters' configuration with no admission generation (see
//! `Cluster::bootstrap_with_pending`), as a joiner does until a batch or an
//! election admits it. Admissions (batches) and removals (`Drain`)
//! interleave with elections as the events fall. Rival foundings from one configuration need one: the
//! shortest traces with two leaders electing under one configuration have
//! 3 voters and 1 pending member. A worker an election left out plays the
//! same part in the next election: no voter of the founded configuration,
//! it answers roll calls and grants votes as a new voter.
//!
//! Those clusters have no coordination authority. A third property,
//! `authority_invariants_hold_after_every_event`, runs clusters whose nodes
//! all have one, through its faults as well, checking its own invariants
//! (A1-A3, see there).


use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::configuration::Generation;
use kabudachi_core::election::Output;
use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::election_message::Payload;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;
use proptest::prelude::*;
use proptest::test_runner::{RngSeed, TestRunner};
use crate::support::harness::{Cluster, StepRecord};

const MAX_NODES: usize = 7;

/// The seed every run draws its cases from unless `PROPTEST_RNG_SEED` names
/// another, so that a run is reproducible: every run of the suite checks the
/// same cases, and a failure one run finds, every run finds. A deeper check
/// takes other seeds and more cases (`PROPTEST_CASES`); run one before
/// closing a change to the election, since a fixed seed covers only its own
/// cases.
const RNG_SEED: u64 = 0;

/// The configuration of a run: the fixed seed unless `PROPTEST_RNG_SEED`
/// names another, and 256 cases unless `PROPTEST_CASES` names another count.
fn config() -> ProptestConfig {
    seeded(crate::proptest::config(CASES))
}

/// How many cases a run checks unless `PROPTEST_CASES` says otherwise.
const CASES: u32 = 256;

fn seeded(config: ProptestConfig) -> ProptestConfig {
    match config.rng_seed {
        RngSeed::Random => ProptestConfig {
            rng_seed: RngSeed::Fixed(RNG_SEED),
            ..config
        },
        RngSeed::Fixed(_) => config,
    }
}

#[derive(Debug, Clone)]
enum ScenarioEvent {
    Advance(Duration),
    /// For each node by index, whether it is on the first side; entries
    /// past the cluster's size are ignored.
    Partition([bool; MAX_NODES]),
    Heal,
    /// A node by index, modulo the cluster's size.
    Drain(usize),
    Stall(usize, Duration),
    SetDropRate(f64),
    SetDelay(Duration),
}

#[derive(Debug, Clone)]
struct NetworkFaults {
    seed: u64,
    duplicate_rate: f64,
    reorder: bool,
}

/// The share of deliveries the network holds back, and by how many ticks at
/// most (see `FakeNetwork::set_late_delivery`).
#[derive(Debug, Clone)]
struct LateDelivery {
    rate: f64,
    at_most: Duration,
}

fn scenario_event_strategy() -> impl Strategy<Value = ScenarioEvent> {
    prop_oneof![
        24 => (1u64..=15).prop_map(|ticks| ScenarioEvent::Advance(Duration::from_ticks(ticks))),
        5 => any::<[bool; MAX_NODES]>().prop_map(ScenarioEvent::Partition),
        3 => Just(ScenarioEvent::Heal),
        1 => (0..MAX_NODES).prop_map(ScenarioEvent::Drain),
        3 => (0..MAX_NODES, 1u64..=20)
            .prop_map(|(node, ticks)| ScenarioEvent::Stall(node, Duration::from_ticks(ticks))),
        2 => prop_oneof![Just(0.0), Just(0.05), Just(0.1), Just(0.3)]
            .prop_map(ScenarioEvent::SetDropRate),
        2 => (0u64..=3).prop_map(|ticks| ScenarioEvent::SetDelay(Duration::from_ticks(ticks))),
    ]
}

fn network_faults_strategy() -> impl Strategy<Value = NetworkFaults> {
    (
        any::<u64>(),
        prop_oneof![Just(0.0), Just(0.1)],
        any::<bool>(),
    )
        .prop_map(|(seed, duplicate_rate, reorder)| NetworkFaults {
            seed,
            duplicate_rate,
            reorder,
        })
}

/// Late deliveries up to 6 suspicion timeouts late: a certificate or ack
/// that reaches its recipient a few terms after it was sent, which the
/// model's two-leader traces need.
fn late_delivery_strategy() -> impl Strategy<Value = LateDelivery> {
    (
        prop_oneof![Just(0.0), Just(0.1), Just(0.2), Just(0.4)],
        1u64..=60,
    )
        .prop_map(|(rate, ticks)| LateDelivery {
            rate,
            at_most: Duration::from_ticks(ticks),
        })
}

/// A generation as the tuple it is ordered by.
type Rank = (u64, u64, u64);

fn rank(generation: Generation) -> Rank {
    wire_rank(&generation.into())
}

fn wire_rank(generation: &generated::Generation) -> Rank {
    (
        generation.recovery_epoch,
        generation.term,
        generation.counter,
    )
}

/// A worker's admission generation and prior admission generation, as
/// ranks.
type Admitted = (Option<Rank>, Option<Rank>);

/// A configuration's majorities, read off the wire: one for a single
/// configuration, one per side for a joint one.
struct CountedConfiguration {
    sides: Vec<Side>,
}

/// Workers whose admission generation (or, for `by_prior`, prior admission
/// generation too) lies from `from` through `through`, `voter_count` of
/// them.
struct Side {
    from: Rank,
    through: Rank,
    by_prior: bool,
    voter_count: u64,
}

impl CountedConfiguration {
    fn of(configuration: &generated::Configuration) -> Self {
        let rank_of = |generation: &Option<generated::Generation>| {
            wire_rank(generation.as_ref().expect("a generation"))
        };
        let new_side = |voter_count| Side {
            from: rank_of(&configuration.base),
            through: rank_of(&configuration.generation),
            by_prior: false,
            voter_count,
        };
        let sides = match &configuration.electorate {
            Some(generated::configuration::Electorate::Single(single)) => {
                vec![new_side(single.voter_count)]
            }
            Some(generated::configuration::Electorate::Joint(joint)) => vec![
                Side {
                    from: rank_of(&joint.old_base),
                    through: rank_of(&joint.old_generation),
                    by_prior: true,
                    voter_count: joint.old_voter_count,
                },
                new_side(joint.new_voter_count),
            ],
            None => panic!("a configuration's electorate"),
        };
        CountedConfiguration { sides }
    }

    /// Whether `admitted` is a majority of every side.
    fn is_quorum(&self, admitted: &[Admitted]) -> bool {
        self.sides.iter().all(|side| {
            let counted = admitted
                .iter()
                .filter(|(current, prior)| {
                    side.counts(*current) || (side.by_prior && side.counts(*prior))
                })
                .count() as u64;
            counted > side.voter_count / 2
        })
    }
}

impl Side {
    fn counts(&self, admission: Option<Rank>) -> bool {
        admission.is_some_and(|admission| self.from <= admission && admission <= self.through)
    }
}

/// The batch generation of `configuration`, and the generation of the
/// configuration the batch moved from, if it is a joint configuration an
/// admission batch announced: one whose old side is a configuration of its
/// own recovery epoch and term. A founding's or a re-stamp's old side is of
/// an earlier term or epoch, since only a term's own leader mints its
/// generations, and only after its win.
fn batch_generation(configuration: &generated::Configuration) -> Option<(Rank, Rank)> {
    let Some(generated::configuration::Electorate::Joint(joint)) = &configuration.electorate else {
        return None;
    };
    let old = wire_rank(joint.old_generation.as_ref()?);
    let batch = wire_rank(joint.batch_generation.as_ref()?);
    (old.0 == batch.0 && old.1 == batch.1).then_some((batch, old))
}

/// An admission a node was sent: by whom, and the batch generation of the
/// configuration it came with, if that was a batch.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Give {
    sender: WorkerId,
    batch: Option<Rank>,
}

/// What the oracle for L1, L3 and L4 has seen the nodes send and do.
#[derive(Default)]
struct Ledger {
    /// Per (initiator, term), the configuration its roll call ran under and
    /// the initiator's own admission generations then.
    roll_calls: BTreeMap<(WorkerId, u64), (generated::Configuration, Admitted)>,
    /// Per (initiator, term), each responder's admission generations in its
    /// first reply.
    replies: BTreeMap<(WorkerId, u64), BTreeMap<WorkerId, Admitted>>,
    /// Per (candidate, term), the voters that sent it their grant.
    grants: BTreeMap<(WorkerId, u64), BTreeSet<WorkerId>>,
    /// Per term, the respondents of the roll call that won it.
    respondents_by_term: BTreeMap<u64, BTreeSet<WorkerId>>,
    /// Every node that became `Leader`, by the term it leads.
    leaders_by_term: BTreeMap<u64, BTreeSet<WorkerId>>,
    /// Replies that named an admission generation, to check against
    /// `given` once every step of the event is in.
    admissions_answered: Vec<(WorkerId, Rank)>,
    /// Every admission of its own term a node sent another, in a
    /// certificate or an ack, by `(recipient, admission)`.
    given: BTreeMap<(WorkerId, Rank), BTreeSet<Give>>,
    /// The batch generations of the joint configurations leaders announced
    /// in batches, each with the generation the batch moved from (see
    /// `batch_generation`).
    batches: BTreeMap<Rank, Rank>,
    /// Every configuration generation a node announced on an ack or a
    /// certificate, and whether that configuration is joint.
    announced_joint: BTreeMap<Rank, bool>,
    /// The (worker, leader, term) triples where the worker heartbeated that
    /// leader confirming one of its acks of that term: what a batch needs
    /// of a joiner.
    confirmed: BTreeSet<(WorkerId, WorkerId, u64)>,
    /// How many elections were won under a batch's joint configuration, or
    /// under the configuration a batch moved from.
    wins_beside_a_batch: usize,
    /// How many nodes the cluster has.
    node_count: usize,
    /// The nodes that started as pending members.
    pending: BTreeSet<WorkerId>,
    /// How many elections were won, and how many of them had fewer
    /// respondents than the cluster has nodes.
    wins: usize,
    wins_leaving_a_worker_out: usize,
    /// How many elections a node that started pending answered.
    wins_admitting_a_pending_member: usize,
}

impl Ledger {
    fn take_in(&mut self, record: StepRecord) -> Result<(), TestCaseError> {
        let node = record.node;
        let won = record
            .outputs
            .iter()
            .any(|output| matches!(output, Output::StateChanged(WorkerState::Leader)));
        let mut certified = BTreeSet::new();
        for output in &record.outputs {
            let message = match output {
                Output::Send { message, .. } | Output::Publish { message } => message,
                _ => continue,
            };
            match &message.payload {
                Some(Payload::RollCall(call)) => {
                    // A node whose earlier call for this term a leader's ack
                    // outlived calls it again: the replies and grants so far
                    // belonged to the earlier call.
                    self.replies.remove(&(node.clone(), call.term));
                    self.grants.remove(&(node.clone(), call.term));
                    self.roll_calls.insert(
                        (node.clone(), call.term),
                        (
                            call.configuration.expect("a roll call's configuration"),
                            (record.admission.map(rank), record.prior_admission.map(rank)),
                        ),
                    );
                }
                Some(Payload::RollCallReply(reply)) => {
                    let admission = reply.admission.as_ref().map(wire_rank);
                    let prior = reply.prior_admission.as_ref().map(wire_rank);
                    self.replies
                        .entry((
                            reply.initiator_id.clone().expect("a reply's initiator").into(),
                            reply.term,
                        ))
                        .or_default()
                        .entry(node.clone())
                        .or_insert((admission, prior));
                    if let Some(admission) = admission {
                        self.admissions_answered.push((node.clone(), admission));
                    }
                }
                Some(Payload::Heartbeat(heartbeat)) => {
                    if let (Output::Send { to, .. }, Some(echo)) =
                        (output, heartbeat.newest_accepted_ack)
                    {
                        self.confirmed.insert((node.clone(), to.clone(), echo.term));
                    }
                }
                Some(Payload::VoteGrant(grant)) => {
                    self.grants
                        .entry((
                            grant.candidate_id.clone().expect("a grant's candidate").into(),
                            grant.term,
                        ))
                        .or_default()
                        .insert(node.clone());
                }
                Some(Payload::HeartbeatAck(ack)) => {
                    self.note_announced(ack.configuration.as_ref());
                    let batch = ack.configuration.as_ref().and_then(batch_generation);
                    if let Some((batch, moved_from)) = batch {
                        self.batches.insert(batch, moved_from);
                    }
                    if let Output::Send { to, .. } = output {
                        self.note_given(
                            &node,
                            to,
                            ack.term,
                            ack.recipient_admission.as_ref(),
                            batch.map(|(batch, _)| batch),
                        );
                    }
                }
                Some(Payload::ElectionCertificate(certificate)) => {
                    self.note_announced(certificate.configuration.as_ref());
                    if let Output::Send { to, .. } = output {
                        certified.insert(to.clone());
                        self.note_given(
                            &node,
                            to,
                            certificate.term,
                            certificate.recipient_admission.as_ref(),
                            None,
                        );
                    }
                    prop_assert_eq!(
                        certificate.term,
                        record.term,
                        "a certificate names the term its sender won"
                    );
                }
                _ => {}
            }
        }
        if won {
            self.check_win(&node, record.term, certified)?;
        }
        Ok(())
    }

    /// L1 and L3 for `leader`'s win of `term`, and records its roll call's
    /// respondents (`certified` and `leader`) for L4.
    fn check_win(
        &mut self,
        leader: &WorkerId,
        term: u64,
        certified: BTreeSet<WorkerId>,
    ) -> Result<(), TestCaseError> {
        let leaders = self.leaders_by_term.entry(term).or_default();
        leaders.insert(leader.clone());
        prop_assert!(
            leaders.len() == 1,
            "L1 violated: term {} has more than one leader: {:?}",
            term,
            leaders
        );

        let key = (leader.clone(), term);
        let (configuration, own_admission) = self
            .roll_calls
            .get(&key)
            .unwrap_or_else(|| panic!("{leader:?} won term {term} with no roll call of its own"));
        let configuration = CountedConfiguration::of(configuration);
        let replies = self.replies.get(&key).cloned().unwrap_or_default();
        let granted = self.grants.get(&key).cloned().unwrap_or_default();
        let granters: Vec<Admitted> = std::iter::once(*own_admission)
            .chain(
                granted
                    .iter()
                    .filter_map(|voter| replies.get(voter).copied()),
            )
            .collect();
        prop_assert!(
            configuration.is_quorum(&granters),
            "L3 violated: {:?} won term {} though its granters {:?} are no returning quorum \
             of every side of its roll call's configuration; grants from {:?}, replies {:?}",
            leader,
            term,
            granters,
            granted,
            replies
        );

        // The win rule's other clause: the granters
        // are a majority of the respondents, the winner included. The
        // winner certifies every respondent but itself, so those are the
        // respondents; a grant counted here was at least sent.
        let respondents = certified.len() as u64 + 1;
        let granting_respondents = 1 + granted
            .iter()
            .filter(|voter| certified.contains(*voter))
            .count() as u64;
        prop_assert!(
            granting_respondents > respondents / 2,
            "L3 violated: {:?} won term {} with {} of its {} respondents granting",
            leader,
            term,
            granting_respondents,
            respondents
        );
        self.wins += 1;
        if let Some((roll_call_configuration, _)) = self.roll_calls.get(&key)
            && let Some(roll_call_generation) = roll_call_configuration.generation.as_ref()
        {
            let roll_call_generation = wire_rank(roll_call_generation);
            if self.batches.contains_key(&roll_call_generation)
                || self.batches.values().any(|moved_from| *moved_from == roll_call_generation)
                || batch_generation(roll_call_configuration).is_some()
            {
                self.wins_beside_a_batch += 1;
            }
        }
        if (respondents as usize) < self.node_count {
            self.wins_leaving_a_worker_out += 1;
        }

        let mut respondents = certified;
        respondents.insert(leader.clone());
        if !respondents.is_disjoint(&self.pending) {
            self.wins_admitting_a_pending_member += 1;
        }
        self.respondents_by_term.insert(term, respondents);
        Ok(())
    }

    /// Records that `sender` sent `to` the admission `admission` with a
    /// message of `term`, if it is one of that term (a leader also acks
    /// members at admissions minted earlier, which it did not give), and
    /// the configuration's batch generation if it came with a batch.
    fn note_given(
        &mut self,
        sender: &WorkerId,
        to: &WorkerId,
        term: u64,
        admission: Option<&generated::Generation>,
        batch: Option<Rank>,
    ) {
        if let Some(admission) = admission.map(wire_rank)
            && admission.1 == term
        {
            self.given
                .entry((to.clone(), admission))
                .or_default()
                .insert(Give {
                    sender: sender.clone(),
                    batch,
                });
        }
    }

    /// Whether `node` came by `admission` as L4 allows: that term's leader
    /// gave it to a respondent of its winning roll call, or at a batch's
    /// generation (a joiner), or to a node that had confirmed one of its
    /// acks of the term, which a batch requires of every joiner. Every
    /// member a later change of the term re-admits joined in one of those
    /// ways, and a joiner whose batch ack was lost learns its admission
    /// from such a change. A node that missed a commit takes up its
    /// admission from a refusal instead: that term's leader announced a change at `admission`,
    /// and the node came by the generation just before it, which the change
    /// re-admits it from.
    fn came_by(&self, node: &WorkerId, admission: Rank) -> bool {
        self.given_directly(node, admission) || self.relayed(node, admission)
    }

    fn note_announced(&mut self, configuration: Option<&generated::Configuration>) {
        if let Some(configuration) = configuration
            && let Some(generation) = configuration.generation.as_ref()
        {
            let joint = matches!(
                configuration.electorate,
                Some(generated::configuration::Electorate::Joint(_))
            );
            self.announced_joint.insert(wire_rank(generation), joint);
        }
    }

    /// Whether `admission` is the commit of a joint configuration at the
    /// generation just before it, which `node` came by, and that term's
    /// leader announced it to some node.
    fn relayed(&self, node: &WorkerId, admission: Rank) -> bool {
        let (epoch, term, counter) = admission;
        let Some(previous) = counter.checked_sub(1) else {
            return false;
        };
        let commits_a_joint = self.announced_joint.get(&(epoch, term, previous)) == Some(&true)
            && self.announced_joint.get(&admission) == Some(&false);
        let leaders = self.leaders_by_term.get(&term);
        let announced = self.given.iter().any(|((_, given), gives)| {
            *given == admission
                && gives
                    .iter()
                    .any(|give| leaders.is_some_and(|leaders| leaders.contains(&give.sender)))
        });
        commits_a_joint && announced && self.given_directly(node, (epoch, term, previous))
    }

    fn given_directly(&self, node: &WorkerId, admission: Rank) -> bool {
        let (_, term, _) = admission;
        let leaders = self.leaders_by_term.get(&term);
        let respondent = self
            .respondents_by_term
            .get(&term)
            .is_some_and(|respondents| respondents.contains(node));
        self.given
            .get(&(node.clone(), admission))
            .into_iter()
            .flatten()
            .filter(|give| leaders.is_some_and(|leaders| leaders.contains(&give.sender)))
            .any(|give| {
                respondent
                    || give.batch == Some(admission)
                    || self
                        .confirmed
                        .contains(&(node.clone(), give.sender.clone(), term))
            })
    }

    /// L4 for `node` holding or answering with `admission`.
    fn check_admission(&self, node: &WorkerId, admission: Rank) -> Result<(), TestCaseError> {
        if admission == rank(Generation::genesis(0)) {
            return Ok(());
        }
        let (_, term, _) = admission;
        let led_it = self
            .leaders_by_term
            .get(&term)
            .is_some_and(|leaders| leaders.contains(node));
        prop_assert!(
            led_it || self.came_by(node, admission),
            "L4 violated: {:?} holds admission generation {:?}, which term {}'s leader {:?} \
             never gave it as a respondent of its win, in a batch, or at a change \
             (its winning roll call drew {:?}; given {:?})",
            node,
            admission,
            term,
            self.leaders_by_term.get(&term),
            self.respondents_by_term.get(&term),
            self.given.get(&(node.clone(), admission))
        );
        Ok(())
    }
}

fn check_configurations(cluster: &Cluster, ids: &[WorkerId]) -> Result<(), TestCaseError> {
    for id in ids {
        let node = cluster.node(id);
        let Some(configuration) = node.configuration() else {
            continue;
        };
        let generation = configuration.generation();
        prop_assert!(
            generation.term() <= node.highest_term_seen(),
            "L5 violated: {:?} holds a configuration at {:?} past its highest term seen {}",
            id,
            generation,
            node.highest_term_seen()
        );
        prop_assert_eq!(
            generation.recovery_epoch(),
            node.recovery_epoch(),
            "L5 violated: {:?} holds a configuration at {:?} of another recovery epoch",
            id,
            generation
        );
    }
    Ok(())
}

/// What `event` does to the cluster.
fn apply(cluster: &mut Cluster, ids: &[WorkerId], event: &ScenarioEvent) {
    let node = |index: &usize| &ids[index % ids.len()];
    match event {
        ScenarioEvent::Advance(dt) => cluster.advance(*dt),
        ScenarioEvent::Partition(sides) => {
            let (first, second): (Vec<_>, Vec<_>) =
                ids.iter().zip(sides).partition(|(_, first)| **first);
            cluster.partition(
                first.into_iter().map(|(id, _)| id.clone()).collect(),
                second.into_iter().map(|(id, _)| id.clone()).collect(),
            );
        }
        ScenarioEvent::Heal => cluster.heal(),
        ScenarioEvent::Drain(index) => cluster.drain(node(index)),
        ScenarioEvent::Stall(index, dt) => cluster.stall(node(index), *dt),
        ScenarioEvent::SetDropRate(rate) => cluster.network().set_drop_rate(*rate),
        ScenarioEvent::SetDelay(delay) => cluster.network().set_delay(*delay),
    }
}

/// One case's cluster shape, network and events.
#[derive(Debug, Clone)]
struct Case {
    voters: usize,
    pending: usize,
    faults: NetworkFaults,
    late: LateDelivery,
    events: Vec<ScenarioEvent>,
}

fn events_strategy() -> impl Strategy<Value = Vec<ScenarioEvent>> {
    proptest::collection::vec(scenario_event_strategy(), 1..150)
}

/// Runs random event sequences on fresh clusters of 4 to 7 voters, checking
/// L1-L8 after every event, then checks the run exercised what they guard:
/// some elections won, some leaving a worker out. A run whose time budget
/// was spent skips the coverage check, since it truncated the cases.
#[test]
fn leadership_invariants_hold_after_every_event() {
    // The strategy draws in the order it always has, so the regression file's
    // cases replay as they were found.
    let strategy = (4..=MAX_NODES, network_faults_strategy(), events_strategy()).prop_map(
        |(voters, faults, events)| Case {
            voters,
            pending: 0,
            faults,
            late: LateDelivery {
                rate: 0.0,
                at_most: Duration::from_ticks(0),
            },
            events,
        },
    );
    let coverage = run_cases(strategy);
    if crate::proptest::budget_was_spent() {
        return;
    }
    assert!(
        coverage.wins > 0,
        "no case won an election, so L1, L3 and L4 checked nothing: {coverage:?}"
    );
    assert!(
        coverage.wins_leaving_a_worker_out > 0,
        "no election left a worker out, so L4 checked nothing: {coverage:?}"
    );
}

/// The same, on clusters of 3 to 5 voters and 1 or 2 pending members, whose
/// elections found configurations that admit them, on a network that may
/// also deliver messages late; then checks some elections admitted one,
/// unless the time budget was spent and truncated the run.
#[test]
fn leadership_invariants_hold_with_pending_members() {
    let strategy = (
        3..=MAX_NODES - 2,
        1..=2usize,
        network_faults_strategy(),
        late_delivery_strategy(),
        events_strategy(),
    )
        .prop_map(|(voters, pending, faults, late, events)| Case {
            voters,
            pending,
            faults,
            late,
            events,
        });
    let coverage = run_cases(strategy);
    if crate::proptest::budget_was_spent() {
        return;
    }
    assert!(
        coverage.wins_admitting_a_pending_member > 0,
        "no election a pending member answered was won, so nothing was founded with one: \
         {coverage:?}"
    );
    assert!(
        coverage.batches > 0 && coverage.wins_beside_a_batch > 0,
        "no batch was started, or no election won under one or under what it moved from, \
         so L3 and L4 checked no batch: {coverage:?}"
    );
}

/// Checks every case `strategy` draws (see `config`), and returns what the
/// run exercised.
fn run_cases(strategy: impl Strategy<Value = Case>) -> Coverage {
    let coverage = RefCell::new(Coverage::default());
    let mut runner = TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        ..config()
    });

    let outcome = runner.run(&strategy, |case| {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        check_case(case, &mut coverage.borrow_mut())
    });

    if let Err(failure) = outcome {
        panic!("{failure}\n{runner}");
    }
    coverage.into_inner()
}

/// What the run exercised, across every case.
#[derive(Debug, Default)]
struct Coverage {
    wins: usize,
    wins_leaving_a_worker_out: usize,
    wins_admitting_a_pending_member: usize,
    batches: usize,
    wins_beside_a_batch: usize,
}

/// One case: a cluster of `case.voters` voters and `case.pending` pending
/// members on a network with `case.faults` and `case.late`, put through
/// `case.events`.
fn check_case(case: Case, coverage: &mut Coverage) -> Result<(), TestCaseError> {
    let Case {
        voters,
        pending,
        faults,
        late,
        events,
    } = case;
    let mut cluster = Cluster::bootstrap_with_pending(voters, pending, Duration::from_ticks(10));
    cluster.network().seed(faults.seed);
    cluster.network().set_duplicate_rate(faults.duplicate_rate);
    cluster.network().set_reorder(faults.reorder);
    cluster.network().set_late_delivery(late.rate, late.at_most);
    cluster.record_steps();
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();

    let mut ledger = Ledger {
        node_count: ids.len(),
        pending: cluster.pending_members().clone(),
        ..Ledger::default()
    };
    let mut last_term: BTreeMap<WorkerId, u64> = ids.iter().cloned().map(|id| (id, 0)).collect();
    let mut last_epoch: BTreeMap<WorkerId, u64> = ids.iter().cloned().map(|id| (id, 0)).collect();
    let mut ever_drained: BTreeSet<WorkerId> = BTreeSet::new();
    let mut last_admission: BTreeMap<WorkerId, Rank> = BTreeMap::new();

    for event in events {
        let before: BTreeMap<WorkerId, (u64, u64)> = ids
            .iter()
            .map(|id| {
                let node = cluster.node(id);
                (id.clone(), (node.term(), node.recovery_epoch()))
            })
            .collect();

        apply(&mut cluster, &ids, &event);

        for record in cluster.take_steps() {
            ledger.take_in(record)?;
        }
        for (node, admission) in std::mem::take(&mut ledger.admissions_answered) {
            ledger.check_admission(&node, admission)?;
        }
        for id in &ids {
            // A node's admission, once checked, stays valid: check it only
            // when it changes.
            let admission = cluster.node(id).admission().map(rank);
            if let Some(admission) = admission
                && last_admission.get(id) != Some(&admission)
            {
                ledger.check_admission(id, admission)?;
                last_admission.insert(id.clone(), admission);
            }
        }

        prop_assert_eq!(
            cluster.first_grant_overlap(),
            None,
            "L2 violated: more than one node held a valid grant at once"
        );

        check_configurations(&cluster, &ids)?;

        let states = cluster.states();
        for drained_id in &ever_drained {
            prop_assert_ne!(
                states[drained_id],
                WorkerState::Leader,
                "L7 violated: previously-drained node {:?} is now Leader",
                drained_id
            );
        }
        // Only a node that reached `Stopped` counts as drained for L7.
        for (id, state) in &states {
            if *state == WorkerState::Stopped {
                ever_drained.insert(id.clone());
            }
        }

        for id in &ids {
            let node = cluster.node(id);
            let (term, epoch) = (node.term(), node.recovery_epoch());
            prop_assert!(
                term >= last_term[id],
                "L6 violated: node {:?}'s term() decreased from {} to {}",
                id,
                last_term[id],
                term
            );
            prop_assert!(
                epoch >= last_epoch[id],
                "L6 violated: node {:?}'s recovery_epoch() decreased from {} to {}",
                id,
                last_epoch[id],
                epoch
            );
            last_term.insert(id.clone(), term);
            last_epoch.insert(id.clone(), epoch);

            if matches!(event, ScenarioEvent::Heal) {
                prop_assert_eq!(
                    (term, epoch),
                    before[id],
                    "L8 violated: node {:?}'s term or recovery epoch changed across a bare Heal",
                    id
                );
            }
        }
    }
    coverage.wins += ledger.wins;
    coverage.wins_leaving_a_worker_out += ledger.wins_leaving_a_worker_out;
    coverage.wins_admitting_a_pending_member += ledger.wins_admitting_a_pending_member;
    coverage.batches += ledger.batches.len();
    coverage.wins_beside_a_batch += ledger.wins_beside_a_batch;
    Ok(())
}

/// How many cases the authority property checks unless `PROPTEST_CASES`
/// says otherwise: each case runs many TTLs of heartbeats, so far fewer
/// than `CASES` keep the run to a few seconds.
const AUTHORITY_CASES: u32 = 40;

/// The suspicion timeout of the authority property's clusters: seconds, as
/// in production, against the authority's 30 s TTL, so a registration
/// lapses only after many heartbeats and roll calls.
const AUTHORITY_SUSPECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
enum AuthorityEvent {
    Network(ScenarioEvent),
    /// Cuts a node, by index, off from the authority, or reconnects it.
    SetReachable(usize, bool),
    /// Takes the whole authority down, or brings it back.
    SetAvailable(bool),
    /// The authority loses all its data.
    Flush,
}

fn authority_event_strategy() -> impl Strategy<Value = AuthorityEvent> {
    prop_oneof![
        24 => (100u64..=12_000)
            .prop_map(|ticks| AuthorityEvent::Network(ScenarioEvent::Advance(
                Duration::from_ticks(ticks)
            ))),
        5 => any::<[bool; MAX_NODES]>()
            .prop_map(|sides| AuthorityEvent::Network(ScenarioEvent::Partition(sides))),
        3 => Just(AuthorityEvent::Network(ScenarioEvent::Heal)),
        3 => (0..MAX_NODES, 100u64..=45_000).prop_map(|(node, ticks)| {
            AuthorityEvent::Network(ScenarioEvent::Stall(node, Duration::from_ticks(ticks)))
        }),
        2 => prop_oneof![Just(0.0), Just(0.05), Just(0.3)]
            .prop_map(|rate| AuthorityEvent::Network(ScenarioEvent::SetDropRate(rate))),
        1 => (0u64..=300).prop_map(|ticks| AuthorityEvent::Network(ScenarioEvent::SetDelay(
            Duration::from_ticks(ticks)
        ))),
        5 => (0..MAX_NODES, any::<bool>())
            .prop_map(|(node, reachable)| AuthorityEvent::SetReachable(node, reachable)),
        2 => any::<bool>().prop_map(AuthorityEvent::SetAvailable),
        1 => Just(AuthorityEvent::Flush),
    ]
}

/// What the authority property's run exercised, across every case.
#[derive(Debug, Default)]
struct AuthorityCoverage {
    grants: usize,
    orphaned: usize,
    recovered: usize,
    rejoined: usize,
}

/// Random interleavings of partitions, drops, stalls, per-node authority
/// cuts, whole-authority outages, flushes and time, against clusters of 4
/// to 7 voters whose nodes all have the authority, checking after every
/// event:
///
/// - A1: at most one node's scheduler holds a valid grant at any instant
///   (the harness checks after every step). With an authority every grant
///   also needs the recovery fence, which the authority grants to one
///   worker at a time, across flushes, outages and epoch swaps.
/// - A2: at most one node leads any one (recovery epoch, term): every
///   authority path swaps to an epoch no other has, and every election
///   within an epoch contests a term of its own.
/// - A3: every node's configuration generation is of its own recovery
///   epoch, and its recovery epoch never decreases while it stays a member
///   of one shard. It may fall only as the node leaves its shard for the
///   one the authority holds (a fenced node reconnecting,
///   a `NoQuorum` recovery or a leader's fence finding an epoch lower in
///   its lineage, such as one a leader republished after a flush, or of
///   another lineage), and so only in an event in which it went back to
///   `Bootstrapping`, and to an epoch whose lineage it then knows.
///
/// Then checks the run exercised what they guard: grants held, workers
/// orphaned, shards recovered through the authority path, and orphans
/// rejoined, unless the time budget was spent and truncated the run.
#[test]
fn authority_invariants_hold_after_every_event() {
    let strategy = (
        4..=MAX_NODES,
        network_faults_strategy(),
        proptest::collection::vec(authority_event_strategy(), 1..60),
    );
    let coverage = RefCell::new(AuthorityCoverage::default());
    let mut runner = TestRunner::new(ProptestConfig {
        source_file: Some(file!()),
        ..seeded(crate::proptest::config(AUTHORITY_CASES))
    });
    let outcome = runner.run(&strategy, |(voters, faults, events)| {
        if crate::proptest::budget_spent() {
            return Ok(());
        }
        check_authority_case(voters, faults, events, &mut coverage.borrow_mut())
    });
    if let Err(failure) = outcome {
        panic!("{failure}\n{runner}");
    }
    if crate::proptest::budget_was_spent() {
        return;
    }
    let coverage = coverage.into_inner();
    assert!(
        coverage.grants > 0
            && coverage.orphaned > 0
            && coverage.recovered > 0
            && coverage.rejoined > 0,
        "the run left an authority invariant unexercised: {coverage:?}"
    );
}

fn check_authority_case(
    voters: usize,
    faults: NetworkFaults,
    events: Vec<AuthorityEvent>,
    coverage: &mut AuthorityCoverage,
) -> Result<(), TestCaseError> {
    let mut cluster = Cluster::bootstrap_with_authority(voters, 0, AUTHORITY_SUSPECT_TIMEOUT);
    cluster.network().seed(faults.seed);
    cluster.network().set_duplicate_rate(faults.duplicate_rate);
    cluster.network().set_reorder(faults.reorder);
    cluster.record_steps();
    let ids: Vec<WorkerId> = cluster.node_ids().into_iter().collect();

    let mut leaders: BTreeMap<(u64, u64), BTreeSet<WorkerId>> = BTreeMap::new();
    let mut last_epoch: BTreeMap<WorkerId, u64> = ids.iter().cloned().map(|id| (id, 0)).collect();
    for event in events {
        let mut rejoined_now: BTreeSet<WorkerId> = BTreeSet::new();
        match &event {
            AuthorityEvent::Network(event) => apply(&mut cluster, &ids, event),
            AuthorityEvent::SetReachable(index, reachable) => cluster
                .node_authority(&ids[index % ids.len()])
                .set_reachable(*reachable),
            AuthorityEvent::SetAvailable(available) => {
                cluster.authority().set_available(*available)
            }
            AuthorityEvent::Flush => cluster.authority().flush(),
        }

        for record in cluster.take_steps() {
            for output in &record.outputs {
                match output {
                    Output::StateChanged(WorkerState::Leader) => {
                        let holders = leaders
                            .entry((record.recovery_epoch, record.term))
                            .or_default();
                        holders.insert(record.node.clone());
                        prop_assert!(
                            holders.len() == 1,
                            "A2 violated: (epoch, term) {:?} has more than one leader: {:?}",
                            (record.recovery_epoch, record.term),
                            holders
                        );
                    }
                    Output::StateChanged(WorkerState::Fenced) => coverage.orphaned += 1,
                    Output::StateChanged(WorkerState::Bootstrapping) => {
                        coverage.rejoined += 1;
                        rejoined_now.insert(record.node.clone());
                    }
                    Output::Grant(Some(_)) => coverage.grants += 1,
                    _ => {}
                }
            }
        }

        prop_assert_eq!(
            cluster.first_grant_overlap(),
            None,
            "A1 violated: more than one node held a valid grant at once"
        );
        for id in &ids {
            let node = cluster.node(id);
            if let Some(configuration) = node.configuration() {
                prop_assert_eq!(
                    configuration.generation().recovery_epoch(),
                    node.recovery_epoch(),
                    "A3 violated: {:?} holds a configuration at {:?} of another recovery epoch",
                    id,
                    configuration.generation()
                );
            }
            prop_assert!(
                node.recovery_epoch() >= last_epoch[id]
                    || (rejoined_now.contains(id) && node.recovery_lineage().is_some()),
                "A3 violated: {:?}'s recovery epoch fell from {} to {} without its rejoining \
                 the authority's",
                id,
                last_epoch[id],
                node.recovery_epoch()
            );
            last_epoch.insert(id.clone(), node.recovery_epoch());
        }
    }
    if last_epoch.values().any(|epoch| *epoch > 0) {
        coverage.recovered += 1;
    }
    Ok(())
}
