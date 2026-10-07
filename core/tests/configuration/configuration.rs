//! Interface tests for `configuration`:
//! generation identity, the voter test, the quorum tally and the leader's
//! roster.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::configuration::{
    Admission, Configuration, Generation, InvalidConfiguration, Joint, Roster, Single, Tally,
};
use kabudachi_core::protocol::ids::WorkerId;

fn generation(recovery_epoch: u64, term: u64, counter: u64) -> Generation {
    Generation::new(recovery_epoch, term, counter)
}

fn admitted(recovery_epoch: u64, term: u64, counter: u64) -> Option<Generation> {
    Some(generation(recovery_epoch, term, counter))
}

fn worker(label: &str) -> WorkerId {
    WorkerId::new(label)
}

/// Takes the worker labelled `label` out of `roster` alone, as the leader
/// elected in `leader_term`.
fn remove(roster: &mut Roster, label: &str, leader_term: u64) {
    roster.remove_all(&BTreeSet::from([worker(label)]), leader_term);
}

/// Feeds each (worker label, admission generation) pair to `tally`.
fn fed(mut tally: Tally, workers: &[(&str, Option<Generation>)]) -> Tally {
    for (label, admission) in workers {
        tally.record(worker(label), *admission);
    }
    tally
}

#[test]
fn recovery_epoch_outranks_term_and_counter() {
    assert!(generation(1, 0, 0) > generation(0, 9, 9));
    assert!(generation(1, 2, 0) > generation(1, 1, 9));
    assert!(generation(1, 2, 4) > generation(1, 2, 3));
}

#[test]
fn next_change_carries_the_announcing_leaders_term() {
    assert_eq!(generation(3, 0, 11).next_change(9), generation(3, 9, 12));
}

#[test]
#[should_panic(expected = "below")]
fn next_change_by_a_leader_of_an_older_term_panics() {
    generation(3, 7, 11).next_change(6);
}

#[test]
fn next_change_at_the_highest_representable_counter_reaches_u64_max() {
    assert_eq!(
        generation(3, 7, u64::MAX - 1).next_change(7),
        generation(3, 7, u64::MAX)
    );
}

#[test]
#[should_panic(expected = "overflowed")]
fn next_change_at_u64_max_panics() {
    generation(3, 7, u64::MAX).next_change(7);
}

#[test]
fn founded_by_election_at_the_highest_representable_counter_reaches_u64_max() {
    let roll_call = generation(2, 5, u64::MAX - 1);

    assert_eq!(
        Generation::founded_by_election(2, 6, roll_call),
        generation(2, 6, u64::MAX)
    );
}

#[test]
#[should_panic(expected = "overflowed")]
fn founded_by_election_at_u64_max_panics() {
    let roll_call = generation(2, 5, u64::MAX);
    Generation::founded_by_election(2, 6, roll_call);
}

#[test]
fn an_election_founds_the_roll_calls_next_counter_under_its_own_term() {
    let roll_call = generation(2, 5, 7);

    let founded = Generation::founded_by_election(2, 6, roll_call);

    assert_eq!(founded, generation(2, 6, 8));
    assert!(founded > roll_call);
    // An election on the authority path founds it in the new recovery epoch.
    assert_eq!(
        Generation::founded_by_election(3, 6, roll_call),
        generation(3, 6, 8)
    );
}

#[test]
fn a_worker_is_a_voter_exactly_when_admitted_from_the_base_through_the_current_generation() {
    let base = generation(1, 2, 0);
    let current = generation(1, 3, 4);
    let configuration = Configuration::single(Single {
        generation: current,
        base,
        voter_count: 3,
    }).expect("valid");

    let cases = [
        (None, false, "pending member"),
        (admitted(0, 9, 9), false, "older recovery epoch"),
        (admitted(1, 1, 5), false, "left out by the election"),
        (Some(base), true, "at the base"),
        (admitted(1, 2, 7), true, "between base and current"),
        (Some(current), true, "at the current"),
        (admitted(1, 3, 5), false, "after the current"),
        (admitted(2, 0, 0), false, "later recovery epoch"),
    ];
    for (admission, is_voter, case) in cases {
        assert_eq!(configuration.is_voter(admission), is_voter, "{case}");
    }
}

#[test]
fn during_an_admission_batch_both_sides_are_voters() {
    let base = generation(1, 2, 0);
    let batch = generation(1, 2, 3);
    let current = generation(1, 2, 5);
    let configuration = Configuration::joint(Joint {
        generation: current,
        base,
        batch_generation: batch,
        old_base: base,
        old_generation: generation(1, 2, 2),
        old_voter_count: 3,
        new_voter_count: 5,
    }).expect("valid");

    let cases = [
        (None, false, "pending member"),
        (admitted(1, 1, 9), false, "left out by the election"),
        (Some(base), true, "at the base"),
        (admitted(1, 2, 2), true, "just before the batch"),
        (Some(batch), true, "admitted by the batch"),
        (Some(current), true, "at the current"),
        (admitted(1, 2, 6), false, "after the current"),
    ];
    for (admission, is_voter, case) in cases {
        assert_eq!(configuration.is_voter(admission), is_voter, "{case}");
    }
}

#[test]
fn a_single_configuration_needs_more_than_half_of_its_voters() {
    let base = generation(1, 2, 0);
    // (voter count, fewest voters that reach quorum)
    let thresholds = [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3)];
    for (voter_count, quorum) in thresholds {
        let mut tally = Tally::against(&Configuration::single(Single {
            generation: base,
            base,
            voter_count,
        }).expect("valid"));
        assert!(!tally.has_quorum(), "no voters of {voter_count}");
        for fed_count in 1..=voter_count {
            tally.record(worker(&format!("voter-{fed_count}")), Some(base));
            assert_eq!(
                tally.has_quorum(),
                fed_count >= quorum,
                "{fed_count} of {voter_count} voters"
            );
        }
    }
}

#[test]
fn a_worker_fed_twice_counts_once_with_its_first_admission_generation() {
    let base = generation(1, 2, 0);
    let configuration = Configuration::single(Single {
        generation: base,
        base,
        voter_count: 3,
    }).expect("valid");

    let tally = fed(
        Tally::against(&configuration),
        &[("voter", Some(base)), ("voter", Some(base))],
    );
    assert!(!tally.has_quorum());
    assert!(fed(tally, &[("other-voter", Some(base))]).has_quorum());

    let first_fed_pending = fed(
        Tally::against(&Configuration::single(Single {
            generation: base,
            base,
            voter_count: 1,
        }).expect("valid")),
        &[("joiner", None), ("joiner", Some(base))],
    );
    assert!(!first_fed_pending.has_quorum());
}

#[test]
fn workers_that_are_not_voters_never_count() {
    let base = generation(1, 2, 0);
    let current = generation(1, 2, 4);
    let configuration = Configuration::single(Single {
        generation: current,
        base,
        voter_count: 3,
    }).expect("valid");

    let tally = fed(
        Tally::against(&configuration),
        &[
            ("pending", None),
            ("left-out", admitted(1, 1, 9)),
            ("from-a-later-generation", admitted(1, 2, 5)),
            ("from-a-later-epoch", admitted(2, 0, 0)),
            ("voter", Some(base)),
        ],
    );
    assert!(!tally.has_quorum());
    assert!(fed(tally, &[("other-voter", Some(current))]).has_quorum());
}

#[test]
fn during_an_admission_batch_a_quorum_needs_a_majority_of_both_sides() {
    let base = generation(1, 2, 0);
    let batch = generation(1, 2, 3);
    let current = generation(1, 2, 5);
    // Three voters before the batch, two joiners admitted by it.
    let configuration = Configuration::joint(Joint {
        generation: current,
        base,
        batch_generation: batch,
        old_base: base,
        old_generation: generation(1, 2, 2),
        old_voter_count: 3,
        new_voter_count: 5,
    }).expect("valid");

    let old_side_only = fed(
        Tally::against(&configuration),
        &[("old-1", Some(base)), ("old-2", Some(base))],
    );
    assert!(!old_side_only.has_quorum(), "2 of 5 on the new side");
    assert!(fed(old_side_only, &[("joiner-1", Some(batch))]).has_quorum());

    let joiners_carry_the_new_side = fed(
        Tally::against(&configuration),
        &[
            ("old-1", Some(base)),
            ("joiner-1", Some(batch)),
            ("joiner-2", Some(batch)),
        ],
    );
    assert!(
        !joiners_carry_the_new_side.has_quorum(),
        "1 of 3 on the old side"
    );
}

#[test]
fn in_an_admission_batch_the_old_side_is_the_voters_admitted_before_it() {
    let base = generation(1, 2, 0);
    let batch = generation(1, 2, 3);
    // A removal re-announced the batch at a later generation; the side
    // boundary stays at the batch generation.
    let current = generation(1, 2, 6);
    // With one voter on each side, a single worker reaches quorum exactly
    // when it is on the old side.
    let one_per_side = Configuration::joint(Joint {
        generation: current,
        base,
        batch_generation: batch,
        old_base: base,
        old_generation: generation(1, 2, 2),
        old_voter_count: 1,
        new_voter_count: 1,
    }).expect("valid");

    let cases = [
        (None, false, "pending member"),
        (admitted(1, 1, 9), false, "left out by the election"),
        (Some(base), true, "at the base"),
        (admitted(1, 2, 2), true, "just before the batch"),
        (Some(batch), false, "admitted by the batch"),
        (admitted(1, 2, 4), false, "after the batch"),
        (Some(current), false, "at the current"),
    ];
    for (admission, on_old_side, case) in cases {
        let tally = fed(Tally::against(&one_per_side), &[("worker", admission)]);
        assert_eq!(tally.has_quorum(), on_old_side, "{case}");
    }
}

#[test]
fn a_tally_against_a_plain_count_counts_every_worker_fed() {
    let tally = fed(
        Tally::against_count(3),
        &[("pending", None), ("pending", None)],
    );
    assert!(!tally.has_quorum());
    assert!(fed(tally, &[("left-out", admitted(0, 0, 0))]).has_quorum());
}

#[test]
fn the_win_rule_needs_a_returning_quorum_and_a_majority_of_respondents() {
    let base = generation(1, 2, 0);
    let left_out = admitted(1, 1, 0);
    // The roll call ran under a 3-voter configuration and drew 5
    // respondents, the initiator included.
    let roll_call = Configuration::single(Single {
        generation: base,
        base,
        voter_count: 3,
    }).expect("valid");
    let win_rule = || Tally::against(&roll_call).and(Tally::against_count(5));

    let returning_only = fed(
        win_rule(),
        &[("returning-1", Some(base)), ("returning-2", Some(base))],
    );
    assert!(!returning_only.has_quorum(), "2 of 5 respondents");
    assert!(fed(returning_only, &[("new-1", left_out)]).has_quorum());

    let mostly_new = fed(
        win_rule(),
        &[
            ("returning-1", Some(base)),
            ("new-1", left_out),
            ("new-2", left_out),
            ("new-3", left_out),
        ],
    );
    assert!(!mostly_new.has_quorum(), "1 of 3 returning voters");
}

#[test]
fn a_worker_fed_to_both_joined_tallies_counts_with_the_first_tallys_admission() {
    let base = generation(1, 2, 0);
    let one_voter = Configuration::single(Single {
        generation: base,
        base,
        voter_count: 1,
    }).expect("valid");
    let returning = fed(Tally::against(&one_voter), &[("w1", Some(base))]);
    let respondents = fed(Tally::against_count(1), &[("w1", None)]);
    assert!(returning.and(respondents).has_quorum());

    let as_pending = fed(Tally::against_count(1), &[("w1", None)]);
    let as_voter = fed(Tally::against(&one_voter), &[("w1", Some(base))]);
    assert!(
        !as_pending.and(as_voter).has_quorum(),
        "fed pending first, it is no voter of the configuration"
    );
}

#[test]
fn joining_tallies_keeps_the_workers_either_was_already_fed() {
    let base = generation(1, 2, 0);
    let returning = fed(
        Tally::against(&Configuration::single(Single {
            generation: base,
            base,
            voter_count: 3,
        }).expect("valid")),
        &[("returning-1", Some(base)), ("returning-2", Some(base))],
    );
    let respondents = fed(Tally::against_count(5), &[("new-1", admitted(1, 1, 0))]);

    assert!(returning.and(respondents).has_quorum());
}

#[test]
fn a_genesis_roster_holds_its_creator_as_the_only_voter() {
    let roster = Roster::genesis(worker("creator"), 4);
    let creator_admission = roster.admission_of(&worker("creator"));

    assert_eq!(roster.configuration(), &Configuration::genesis(4));
    assert_eq!(
        Configuration::genesis(4),
        Configuration::single(Single {
            generation: Generation::genesis(4),
            base: Generation::genesis(4),
            voter_count: 1,
        }).expect("valid")
    );
    assert_eq!(creator_admission, Some(Generation::genesis(4)));
    let creator_alone = fed(
        Tally::against(roster.configuration()),
        &[("creator", creator_admission)],
    );
    assert!(creator_alone.has_quorum());
}

#[test]
fn a_pending_joiner_is_held_without_an_admission_generation() {
    let mut roster = Roster::genesis(worker("creator"), 4);

    roster.add_pending(worker("joiner"));

    assert!(roster.is_pending(&worker("joiner")));
    assert_eq!(roster.admission_of(&worker("joiner")), None);
    assert_eq!(roster.configuration(), &Configuration::genesis(4));
}

#[test]
fn adding_a_member_as_pending_leaves_it_a_member() {
    let mut roster = Roster::genesis(worker("creator"), 4);

    roster.add_pending(worker("creator"));

    assert!(!roster.is_pending(&worker("creator")));
    assert_eq!(
        roster.admission_of(&worker("creator")),
        Some(Generation::genesis(4))
    );
}

fn three_voters_at(counter: u64) -> Configuration {
    Configuration::single(Single {
        generation: generation(0, 1, counter),
        base: generation(0, 1, 1),
        voter_count: 3,
    }).expect("valid")
}

/// A roster of three voters admitted at (0, 1, 1), plus one pending joiner.
fn roster_of_three_and_a_joiner() -> Roster {
    Roster::new(
        three_voters_at(1),
        BTreeMap::from([
            (worker("a"), generation(0, 1, 1)),
            (worker("b"), generation(0, 1, 1)),
            (worker("c"), generation(0, 1, 1)),
        ]),
        BTreeSet::from([worker("joiner")]),
    )
}

#[test]
fn a_worker_built_as_both_member_and_pending_is_a_member() {
    let roster = Roster::new(
        three_voters_at(1),
        BTreeMap::from([(worker("a"), generation(0, 1, 1))]),
        BTreeSet::from([worker("a")]),
    );

    assert!(!roster.is_pending(&worker("a")));
    assert_eq!(roster.admission_of(&worker("a")), Some(generation(0, 1, 1)));
}

#[test]
fn removing_a_voter_announces_one_fewer_voter_at_the_next_generation() {
    let mut roster = roster_of_three_and_a_joiner();

    remove(&mut roster, "b", 1);

    assert_eq!(roster.admission_of(&worker("b")), None);
    assert!(!roster.is_pending(&worker("b")));
    let shrunk = generation(0, 1, 2);
    assert_eq!(
        roster.configuration(),
        &Configuration::single(Single {
            generation: shrunk,
            base: shrunk,
            voter_count: 2,
        }).expect("valid"),
        "re-based at the removal's generation"
    );
    assert_eq!(roster.admission_of(&worker("a")), Some(shrunk));
    assert_eq!(roster.admission_of(&worker("c")), Some(shrunk));
    assert!(roster.is_pending(&worker("joiner")));
}

/// A removal re-admits only the voters; a member that was no voter
/// keeps its admission, and is no voter of the shrunk configuration either.
#[test]
fn a_removal_re_admits_only_the_remaining_voters() {
    let mut roster = Roster::new(
        three_voters_at(1),
        BTreeMap::from([
            (worker("a"), generation(0, 1, 1)),
            (worker("b"), generation(0, 1, 1)),
            (worker("left-out"), generation(0, 0, 0)),
        ]),
        BTreeSet::new(),
    );

    remove(&mut roster, "b", 1);

    assert_eq!(roster.admission_of(&worker("a")), Some(generation(0, 1, 2)));
    assert_eq!(
        roster.admission_of(&worker("left-out")),
        Some(generation(0, 0, 0))
    );
    assert!(!roster.configuration().is_voter(Some(generation(0, 0, 0))));
}

#[test]
fn removing_the_same_voter_twice_removes_it_once() {
    let mut roster = roster_of_three_and_a_joiner();

    remove(&mut roster, "b", 1);
    remove(&mut roster, "b", 1);

    assert_eq!(roster.configuration().generation(), generation(0, 1, 2));
    let two_voters = fed(
        Tally::against(roster.configuration()),
        &[("a", admitted(0, 1, 2)), ("c", admitted(0, 1, 2))],
    );
    assert!(two_voters.has_quorum());
    let one_voter = fed(
        Tally::against(roster.configuration()),
        &[("a", admitted(0, 1, 2))],
    );
    assert!(!one_voter.has_quorum());
}

#[test]
fn removing_a_pending_joiner_forgets_it_and_changes_no_count() {
    let mut roster = roster_of_three_and_a_joiner();

    remove(&mut roster, "joiner", 1);

    assert!(!roster.is_pending(&worker("joiner")));
    assert_eq!(roster.configuration(), &three_voters_at(1));
}

#[test]
fn removing_a_member_that_is_no_voter_changes_no_count() {
    let mut roster = Roster::new(
        three_voters_at(1),
        BTreeMap::from([
            (worker("a"), generation(0, 1, 1)),
            (worker("left-out"), generation(0, 0, 0)),
        ]),
        BTreeSet::new(),
    );

    remove(&mut roster, "left-out", 1);

    assert_eq!(roster.admission_of(&worker("left-out")), None);
    assert_eq!(roster.configuration(), &three_voters_at(1));
}

/// During a founding or a batch, a removal re-announces the joint
/// configuration at the next generation with shrunk counts: the new side
/// counts the members left on it, re-admitted there, and the old side one
/// fewer when it counted the departing worker.
#[test]
fn removing_a_voter_from_a_joint_configuration_re_announces_it_with_shrunk_counts() {
    let g0 = generation(0, 0, 0);
    let mut roster = founded_roster();

    remove(&mut roster, "b", 1);

    let shrunk = generation(0, 1, 2);
    assert_eq!(
        roster.configuration(),
        &Configuration::joint(Joint {
            generation: shrunk,
            base: shrunk,
            batch_generation: shrunk,
            old_base: g0,
            old_generation: g0,
            old_voter_count: 2,
            new_voter_count: 2,
        }).expect("valid")
    );
    assert_eq!(
        roster.counted_admission_of(&worker("a")),
        re_admitted(shrunk, Some(g0))
    );
    assert_eq!(
        roster.counted_admission_of(&worker("joiner")),
        re_admitted(shrunk, None)
    );
    assert_eq!(
        roster.counted_admission_of(&worker("b")),
        Admission::default()
    );
}

/// A removal that empties the old side of a joint configuration collapses
/// it to its new side alone: an empty side can never supply a
/// majority, so keeping it would stall every quorum.
#[test]
fn a_removal_that_empties_the_old_side_collapses_the_joint_configuration() {
    let g0 = generation(0, 0, 0);
    // C0 of one voter, "old"; its election in term 1 drew "old", p and q.
    let one_voter = Configuration::single(Single {
        generation: g0,
        base: g0,
        voter_count: 1,
    }).expect("valid");
    let mut roster = Roster::after_election(
        0,
        1,
        &one_voter,
        &BTreeMap::from([
            (worker("old"), Admission::from(Some(g0))),
            (worker("p"), Admission::from(None)),
            (worker("q"), Admission::from(None)),
        ]),
    );

    remove(&mut roster, "old", 1);

    let collapsed = generation(0, 1, 2);
    assert_eq!(
        roster.configuration(),
        &Configuration::single(Single {
            generation: collapsed,
            base: collapsed,
            voter_count: 2,
        }).expect("valid")
    );
    assert_eq!(roster.prior_admission_of(&worker("p")), None);
}

#[test]
fn a_configuration_never_shrinks_below_one_voter() {
    let mut roster = Roster::genesis(worker("creator"), 0);

    remove(&mut roster, "creator", 1);

    assert_eq!(roster.configuration(), &Configuration::genesis(0));
}

#[test]
fn a_removal_announces_a_generation_carrying_the_removing_leaders_term() {
    let mut roster = roster_of_three_and_a_joiner();

    remove(&mut roster, "b", 4);

    assert_eq!(roster.configuration().generation(), generation(0, 4, 2));
    assert_eq!(roster.configuration().base(), generation(0, 4, 2));
}

// ---- What an election founds ----

/// C0: three voters admitted at the genesis generation (0, 0, 0).
fn c0() -> Configuration {
    Configuration::single(Single {
        generation: generation(0, 0, 0),
        base: generation(0, 0, 0),
        voter_count: 3,
    }).expect("valid")
}

/// The joint configuration an election for `term` under C0 founds with
/// `respondents` respondents: new side at (0, term, 1), old side C0.
fn founded_from_c0(term: u64, respondents: usize) -> Configuration {
    let founded = generation(0, term, 1);
    Configuration::joint(Joint {
        generation: founded,
        base: founded,
        batch_generation: founded,
        old_base: generation(0, 0, 0),
        old_generation: generation(0, 0, 0),
        old_voter_count: 3,
        new_voter_count: respondents,
    }).expect("valid")
}

fn re_admitted(at: Generation, prior: Option<Generation>) -> Admission {
    Admission {
        current: Some(at),
        prior,
    }
}

#[test]
fn a_founded_configuration_counts_its_old_side_by_prior_admission() {
    let (g0, founded) = (generation(0, 0, 0), generation(0, 1, 1));
    let configuration = founded_from_c0(1, 4);

    let voters_of_c0_only = [
        ("a", re_admitted(founded, Some(g0))),
        ("b", re_admitted(founded, Some(g0))),
    ];
    let mut tally = Tally::against(&configuration);
    for (worker_label, admission) in voters_of_c0_only {
        tally.record(worker(worker_label), admission);
    }
    assert!(!tally.has_quorum(), "two of three old, but two of four new");
    tally.record(worker("joiner"), re_admitted(founded, None));
    assert!(tally.has_quorum(), "two of three old, three of four new");

    let mut missed_the_founding = Tally::against(&configuration);
    for (worker_label, admission) in [
        ("c", Admission::from(Some(g0))),
        ("a", re_admitted(founded, Some(g0))),
        ("joiner-1", re_admitted(founded, None)),
        ("joiner-2", re_admitted(founded, None)),
    ] {
        missed_the_founding.record(worker(worker_label), admission);
    }
    assert!(
        missed_the_founding.has_quorum(),
        "a C0 voter still at its old admission counts on the old side"
    );
}

#[test]
fn a_worker_a_rival_election_from_the_same_configuration_admitted_is_no_old_side_voter() {
    let (g0, rival) = (generation(0, 0, 0), generation(0, 1, 1));
    // Founded in term 2 from C0; one voter on each side, so a single worker
    // on the old side and one on the new side are a quorum.
    let later = generation(0, 2, 1);
    let configuration = Configuration::joint(Joint {
        generation: later,
        base: later,
        batch_generation: later,
        old_base: g0,
        old_generation: g0,
        old_voter_count: 1,
        new_voter_count: 1,
    }).expect("valid");
    let quorum_with = |old_side: Admission| {
        let mut tally = Tally::against(&configuration);
        tally.record(worker("new"), re_admitted(later, None));
        tally.record(worker("old"), old_side);
        tally.has_quorum()
    };

    assert!(
        !quorum_with(re_admitted(rival, None)),
        "a joiner the term-1 election admitted was never a voter of C0"
    );
    assert!(
        quorum_with(re_admitted(rival, Some(g0))),
        "a C0 voter the term-1 election re-admitted still is one"
    );
    assert!(quorum_with(Admission::from(Some(g0))));
}

#[test]
fn an_election_under_a_single_configuration_founds_a_joint_one_admitting_every_respondent() {
    let g0 = generation(0, 0, 0);
    let respondents = BTreeMap::from([
        (worker("a"), Admission::from(Some(g0))),
        (worker("b"), Admission::from(Some(g0))),
        (worker("joiner"), Admission::from(None)),
    ]);

    let roster = Roster::after_election(0, 1, &c0(), &respondents);

    let founded = generation(0, 1, 1);
    assert_eq!(roster.configuration(), &founded_from_c0(1, 3));
    for respondent in ["a", "b", "joiner"] {
        assert_eq!(roster.admission_of(&worker(respondent)), Some(founded));
    }
    assert_eq!(roster.prior_admission_of(&worker("a")), Some(g0));
    assert_eq!(roster.prior_admission_of(&worker("joiner")), None);
    assert!(roster.pending().is_empty());
}

/// A win under an uncommitted joint configuration re-stamps it at the
/// winner's term, with the same old side, and re-bases its new side there:
/// the respondents the new side counted are re-admitted at the new
/// generation, and the new side counts exactly them, and every respondent
/// keeps the prior admission it answered with (a new-side voter
/// that did not answer is no voter of the re-stamp, so counting it would
/// leave a phantom voter no one can ever supply).
#[test]
fn an_election_under_a_joint_configuration_re_stamps_it_at_the_winners_term() {
    let (g0, founded) = (generation(0, 0, 0), generation(0, 1, 1));
    let respondents = BTreeMap::from([
        (worker("a"), re_admitted(founded, Some(g0))),
        (worker("p"), re_admitted(founded, None)),
        (worker("c"), Admission::from(Some(g0))),
        (worker("joiner"), Admission::from(None)),
    ]);

    let roster = Roster::after_election(0, 3, &founded_from_c0(1, 4), &respondents);

    let restamped = generation(0, 3, 2);
    assert_eq!(
        roster.configuration(),
        &Configuration::joint(Joint {
            generation: restamped,
            base: restamped,
            batch_generation: restamped,
            old_base: g0,
            old_generation: g0,
            old_voter_count: 3,
            new_voter_count: 2,
        }).expect("valid")
    );
    assert_eq!(
        roster.counted_admission_of(&worker("a")),
        re_admitted(restamped, Some(g0))
    );
    assert_eq!(
        roster.counted_admission_of(&worker("p")),
        re_admitted(restamped, None)
    );
    assert_eq!(
        roster.counted_admission_of(&worker("c")),
        Admission::from(Some(g0)),
        "only the old side counts c, so it stays as it answered"
    );
    assert!(roster.is_pending(&worker("joiner")));
}

/// A roster founded from C0 in term 1 by a, b and the joiner.
fn founded_roster() -> Roster {
    let g0 = generation(0, 0, 0);
    Roster::after_election(
        0,
        1,
        &c0(),
        &BTreeMap::from([
            (worker("a"), Admission::from(Some(g0))),
            (worker("b"), Admission::from(Some(g0))),
            (worker("joiner"), Admission::from(None)),
        ]),
    )
}

#[test]
fn a_founding_commits_once_a_majority_of_each_side_holds_it() {
    let founded = generation(0, 1, 1);
    let mut roster = founded_roster();

    roster.record_held_generation(&worker("joiner"), founded);
    assert!(
        !roster.commit_if_confirmed(&worker("a"), 1),
        "a and the joiner: one of three on the old side"
    );
    roster.record_held_generation(&worker("b"), founded);
    assert!(roster.commit_if_confirmed(&worker("a"), 1));

    let committed = generation(0, 1, 2);
    assert_eq!(
        roster.configuration(),
        &Configuration::single(Single {
            generation: committed,
            base: committed,
            voter_count: 3,
        }).expect("valid"),
        "re-based at the commit's generation"
    );
    for member in ["a", "b", "joiner"] {
        assert_eq!(roster.admission_of(&worker(member)), Some(committed));
    }
    assert_eq!(roster.prior_admission_of(&worker("b")), None);
    assert!(
        !roster.commit_if_confirmed(&worker("a"), 1),
        "a single configuration has nothing to commit"
    );
}

/// An echo of a later generation than the joint configuration's own names
/// a configuration this leader does not lead, a rival founding say, so it
/// does not count.
#[test]
fn a_founding_commits_only_on_echoes_of_exactly_its_generation() {
    let founded = generation(0, 1, 1);
    let mut roster = founded_roster();
    roster.record_held_generation(&worker("joiner"), founded);

    roster.record_held_generation(&worker("b"), generation(0, 2, 1));
    assert!(!roster.commit_if_confirmed(&worker("a"), 1));

    roster.record_held_generation(&worker("b"), founded);
    assert!(roster.commit_if_confirmed(&worker("a"), 1));
}

/// A commit re-admits only the members its new side counted; a member
/// the old side alone counted keeps its admission and is no voter of the
/// committed configuration.
#[test]
fn a_commit_re_admits_only_the_members_its_new_side_counted() {
    let (g0, founded) = (generation(0, 0, 0), generation(0, 1, 1));
    let mut roster = Roster::after_election(
        0,
        3,
        &founded_from_c0(1, 3),
        &BTreeMap::from([
            (worker("a"), re_admitted(founded, Some(g0))),
            (worker("p"), re_admitted(founded, None)),
            (worker("c"), Admission::from(Some(g0))),
        ]),
    );
    let restamped = roster.configuration().generation();
    for member in ["p", "c"] {
        roster.record_held_generation(&worker(member), restamped);
    }

    assert!(roster.commit_if_confirmed(&worker("a"), 3));

    let committed = generation(0, 3, 3);
    assert_eq!(roster.configuration().base(), committed);
    assert_eq!(roster.admission_of(&worker("a")), Some(committed));
    assert_eq!(roster.admission_of(&worker("p")), Some(committed));
    assert_eq!(roster.admission_of(&worker("c")), Some(g0));
    assert!(!roster.configuration().is_voter(Some(g0)));
    assert!(
        !roster.configuration().is_voter(Some(restamped)),
        "a member still at the re-stamped admission, having missed the commit's ack, \
         counts only once an ack repairs it"
    );
}

#[test]
fn a_founding_does_not_commit_on_older_configurations_or_outsiders() {
    let founded = generation(0, 1, 1);
    let mut roster = founded_roster();

    roster.record_held_generation(&worker("b"), generation(0, 0, 0));
    roster.record_held_generation(&worker("stranger"), founded);

    assert!(!roster.commit_if_confirmed(&worker("a"), 1));
    assert!(roster.configuration().is_joint());
}

#[test]
fn a_leader_of_a_later_term_commits_at_a_generation_of_its_own_term() {
    let founded = generation(0, 1, 1);
    let mut roster = founded_roster();
    roster.record_held_generation(&worker("b"), founded);

    assert!(roster.commit_if_confirmed(&worker("a"), 3));

    assert_eq!(roster.configuration().generation(), generation(0, 3, 2));
}

/// A commit announces as many voters as it re-admits on the new side, whatever
/// count the joint configuration carried: a new-side voter the leader holds no
/// member for can never confirm anything.
#[test]
fn a_commit_counts_the_members_it_re_admits() {
    let (g0, founded) = (generation(0, 0, 0), generation(0, 1, 1));
    // New side: four voters counted, three held (a, b, c). Old side: C0,
    // whose voters old-1 and old-2 are two of three.
    let mut roster = Roster::new(
        founded_from_c0(1, 4),
        BTreeMap::from([
            (worker("a"), founded),
            (worker("b"), founded),
            (worker("c"), founded),
            (worker("old-1"), g0),
            (worker("old-2"), g0),
        ]),
        BTreeSet::new(),
    );
    for member in ["b", "c", "old-1", "old-2"] {
        roster.record_held_generation(&worker(member), founded);
    }

    assert!(roster.commit_if_confirmed(&worker("a"), 1));

    assert_eq!(
        roster.configuration(),
        &Configuration::single(Single {
            generation: generation(0, 1, 2),
            base: generation(0, 1, 2),
            voter_count: 3,
        }).expect("valid")
    );
}

// ---- Admission batches ----

/// A batch admits only workers its configuration does not already count:
/// pending joiners, and members that hold an admission no longer counted
/// (a respondent a re-stamp left at its old admission, say). A voter, a
/// stranger and an empty choice start nothing.
#[test]
fn a_batch_admits_only_pending_joiners_and_members_that_are_no_voters() {
    let mut roster = Roster::new(
        three_voters_at(1),
        BTreeMap::from([
            (worker("a"), generation(0, 1, 1)),
            (worker("b"), generation(0, 1, 1)),
            (worker("left-out"), generation(0, 0, 0)),
        ]),
        BTreeSet::new(),
    );
    let before = roster.configuration().clone();

    assert!(!roster.begin_batch(&BTreeSet::from([worker("a"), worker("stranger")]), 1));
    assert!(!roster.begin_batch(&BTreeSet::new(), 1));
    assert_eq!(roster.configuration(), &before);

    assert!(roster.begin_batch(&BTreeSet::from([worker("left-out")]), 1));
    let batch = generation(0, 1, 2);
    assert_eq!(
        roster.counted_admission_of(&worker("left-out")),
        re_admitted(batch, None)
    );
    let mut tally = Tally::against(roster.configuration());
    for member in ["a", "b", "left-out"] {
        tally.record(worker(member), roster.counted_admission_of(&worker(member)));
    }
    assert!(tally.has_quorum(), "old side a and b; new side all three");
}

/// A worker once taken out is never held as pending again: a heartbeat of
/// a departed worker still in flight must not bring it back into a batch.
/// Each process start is a fresh identity, so a departed one never
/// returns under the same `WorkerId`.
#[test]
fn a_removed_worker_is_never_held_as_pending_again() {
    let mut roster = roster_of_three_and_a_joiner();

    remove(&mut roster, "b", 1);
    remove(&mut roster, "joiner", 1);
    roster.add_pending(worker("b"));
    roster.add_pending(worker("joiner"));

    assert!(!roster.is_pending(&worker("b")));
    assert!(!roster.is_pending(&worker("joiner")));
}

// ---- Removals applied together (every pending SELF_REMOVE lands in the
// next generation) ----

#[test]
fn removals_applied_together_during_a_founding_shrink_each_side_once() {
    let g0 = generation(0, 0, 0);
    let mut roster = founded_roster();

    roster.remove_all(&BTreeSet::from([worker("b"), worker("joiner")]), 1);

    let shrunk = generation(0, 1, 2);
    assert_eq!(
        roster.configuration(),
        &Configuration::joint(Joint {
            generation: shrunk,
            base: shrunk,
            batch_generation: shrunk,
            old_base: g0,
            old_generation: g0,
            old_voter_count: 2,
            new_voter_count: 1,
        }).expect("valid"),
        "b counted on both sides, the joiner on the new side alone"
    );
}

/// Workers taken out together change the configuration if any of them
/// counted, whatever the others: here a voter, and a member no longer
/// counted that sorts after it.
#[test]
fn removals_applied_together_change_the_configuration_if_any_counted() {
    let mut roster = Roster::new(
        three_voters_at(1),
        BTreeMap::from([
            (worker("a"), generation(0, 1, 1)),
            (worker("b"), generation(0, 1, 1)),
            (worker("c"), generation(0, 1, 1)),
            (worker("left-out"), generation(0, 0, 0)),
        ]),
        BTreeSet::new(),
    );

    roster.remove_all(&BTreeSet::from([worker("a"), worker("left-out")]), 1);

    let shrunk = generation(0, 1, 2);
    assert_eq!(
        roster.configuration(),
        &Configuration::single(Single {
            generation: shrunk,
            base: shrunk,
            voter_count: 2,
        }).expect("valid")
    );
    assert_eq!(roster.admission_of(&worker("left-out")), None);
}

/// A worker on a joint configuration's new side that missed its commit is
/// admitted at the commit's generation; nothing else is taken for a commit.
#[test]
fn only_a_joint_configurations_own_commit_admits_a_member_of_its_new_side() {
    let joint = Configuration::joint(Joint {
        generation: generation(0, 1, 1),
        base: generation(0, 1, 1),
        batch_generation: generation(0, 1, 1),
        old_base: generation(0, 0, 0),
        old_generation: generation(0, 0, 0),
        old_voter_count: 3,
        new_voter_count: 3,
    }).expect("valid");
    let single_at = |at: Generation, base: Generation| {
        Configuration::single(Single {
            generation: at,
            base,
            voter_count: 3,
        }).expect("valid")
    };
    let commit = single_at(generation(0, 1, 2), generation(0, 1, 2));

    assert_eq!(
        joint.admission_after_commit(&commit, admitted(0, 1, 1)),
        admitted(0, 1, 2)
    );
    // Not on the new side: only on the old one, or pending.
    assert_eq!(joint.admission_after_commit(&commit, admitted(0, 0, 0)), None);
    assert_eq!(joint.admission_after_commit(&commit, None), None);
    // Not its commit: a later term's change, a change further on, one not
    // re-based there, or a single configuration's successor.
    for other in [
        single_at(generation(0, 2, 2), generation(0, 2, 2)),
        single_at(generation(0, 1, 3), generation(0, 1, 3)),
        single_at(generation(0, 1, 2), generation(0, 1, 1)),
        // A removal re-announcing the joint configuration, still joint.
        Configuration::joint(Joint {
            generation: generation(0, 1, 2),
            base: generation(0, 1, 2),
            batch_generation: generation(0, 1, 2),
            old_base: generation(0, 0, 0),
            old_generation: generation(0, 0, 0),
            old_voter_count: 3,
            new_voter_count: 2,
        }).expect("valid"),
    ] {
        assert_eq!(joint.admission_after_commit(&other, admitted(0, 1, 1)), None, "{other:?}");
    }
    assert_eq!(
        single_at(generation(0, 1, 1), generation(0, 1, 1))
            .admission_after_commit(&commit, admitted(0, 1, 1)),
        None
    );
}

#[test]
fn a_configuration_that_breaks_a_rule_is_refused_by_its_constructor() {
    let g = |counter| generation(1, 1, counter);
    let valid_joint = || Joint {
        generation: g(4),
        base: g(3),
        batch_generation: g(3),
        old_base: g(1),
        old_generation: g(2),
        old_voter_count: 3,
        new_voter_count: 4,
    };

    assert!(Configuration::joint(valid_joint()).is_ok());
    assert!(
        Configuration::single(Single {
            generation: g(4),
            base: g(3),
            voter_count: 1,
        })
        .is_ok()
    );

    let single = |generation, base, voter_count| {
        Configuration::single(Single {
            generation,
            base,
            voter_count,
        })
    };
    assert_eq!(
        single(g(3), g(4), 2),
        Err(InvalidConfiguration::BaseAfterGeneration)
    );
    assert_eq!(
        single(g(4), g(3), 0),
        Err(InvalidConfiguration::ZeroVoterCount)
    );

    let joint_rows = [
        (
            InvalidConfiguration::BaseAfterGeneration,
            Joint {
                base: g(5),
                ..valid_joint()
            },
        ),
        (
            InvalidConfiguration::BaseAfterBatchGeneration,
            Joint {
                base: g(4),
                ..valid_joint()
            },
        ),
        (
            InvalidConfiguration::BatchGenerationAfterGeneration,
            Joint {
                batch_generation: g(4),
                generation: g(3),
                ..valid_joint()
            },
        ),
        (
            InvalidConfiguration::OldBaseAfterOldGeneration,
            Joint {
                old_base: g(2),
                old_generation: g(1),
                ..valid_joint()
            },
        ),
        (
            InvalidConfiguration::OldGenerationNotBeforeBatchGeneration,
            Joint {
                old_generation: g(3),
                ..valid_joint()
            },
        ),
        (
            InvalidConfiguration::OldBaseAfterBase,
            Joint {
                base: g(0),
                ..valid_joint()
            },
        ),
        (
            InvalidConfiguration::ZeroVoterCount,
            Joint {
                old_voter_count: 0,
                ..valid_joint()
            },
        ),
        (
            InvalidConfiguration::ZeroVoterCount,
            Joint {
                new_voter_count: 0,
                ..valid_joint()
            },
        ),
    ];
    for (rule, joint) in joint_rows {
        assert_eq!(Configuration::joint(joint), Err(rule), "{rule}");
    }
}
