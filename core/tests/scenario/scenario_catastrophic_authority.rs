//! Catastrophic-authority scenarios (README §15, §26.3, ADR-0001 decisions
//! 11 and 12) on the `Cluster` harness, with every node configured with the
//! shared authority (see `Cluster::bootstrap_with_authority`): workers that
//! lose the authority orphan themselves, a leaderless remnant that still
//! reaches it recovers the shard through the authority path, and an
//! authority that restarts or loses its data is waited out or repaired.
//!
//! The TLA+ model does not cover the authority path, so these are its safety
//! evidence: every scenario checks, at every step of the way, that no two
//! nodes' schedulers ever hold a valid grant at once.
//!
//! Suspicion timeouts are 2 s against the authority's 30 s TTL, so a
//! scenario that waits out a registration runs through many heartbeats and
//! roll calls first. A node "crashed" here is stalled for good: it neither
//! renews its registration nor fences itself, as a dead process would not.

use std::collections::{BTreeMap, BTreeSet};

use crate::support::authority::authority_ttl;
use crate::support::builders::{past_any_suspicion, shard};
use crate::support::harness::Cluster;
use kabudachi_core::configuration::Admission;
use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch, Uuid7Lineages};
use kabudachi_core::election::{AuthorityRequest, Input, Output, StopReason};
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::election_message::Payload;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::time::Duration;

const SUSPECT_TIMEOUT: Duration = Duration::from_secs(2);
/// How often `run_for` stops to check the cluster.
const CHECK_EVERY: Duration = Duration::from_millis(250);

fn ttl() -> Duration {
    authority_ttl()
}

fn ticks(duration: Duration) -> u64 {
    duration.as_ticks()
}

fn ttls(n: u64) -> Duration {
    Duration::from_ticks(ticks(ttl()) * n)
}

/// A cluster of `n` nodes with the authority, settled with one leader that
/// holds a grant and every other node `Active` under it.
fn elected(n: usize) -> (Cluster, WorkerId) {
    let mut cluster = Cluster::bootstrap_with_authority(n, 0, SUSPECT_TIMEOUT);
    settle(&mut cluster);
    let leader = cluster.leader().expect("the cluster must elect a leader");
    assert!(
        cluster.holds_valid_grant(&leader),
        "a leader with a reachable authority holds the fence, so a grant"
    );
    for (id, state) in cluster.states() {
        if id != leader {
            assert_eq!(state, WorkerState::Active, "{id:?} must follow the leader");
        }
    }
    (cluster, leader)
}

/// Runs a freshly built cluster past every node's suspicion timeout, and on
/// until it has elected a leader and settled.
fn settle(cluster: &mut Cluster) {
    cluster.advance(past_any_suspicion(ticks(SUSPECT_TIMEOUT)));
    cluster.run_until_quiescent(Duration::from_millis(500), 100);
}

/// Every node but `leader`, in order.
fn followers(cluster: &Cluster, leader: &WorkerId) -> Vec<WorkerId> {
    cluster
        .node_ids()
        .into_iter()
        .filter(|id| id != leader)
        .collect()
}

/// Stalls `id` for good: a crashed worker, which renews nothing, answers
/// nothing and never fences itself.
fn crash(cluster: &mut Cluster, id: &WorkerId) {
    cluster.stall(id, Duration::from_secs(1_000_000));
}

fn set_reachable(cluster: &Cluster, ids: &BTreeSet<WorkerId>, reachable: bool) {
    for id in ids {
        cluster.node_authority(id).set_reachable(reachable);
    }
}

/// The workers the authority lists as live now.
fn live(cluster: &Cluster) -> BTreeSet<WorkerId> {
    cluster
        .authority()
        .live_registrations(&shard("shard-1"))
        .expect("the seeding handle reaches the authority")
        .addresses()
        .keys()
        .cloned()
        .collect()
}

fn authority_epoch(cluster: &Cluster) -> Option<u64> {
    cluster
        .authority()
        .read_recovery_epoch(&shard("shard-1"))
        .expect("the seeding handle reaches the authority")
        .map(|epoch| epoch.number)
}

/// The nodes among `among` that are `Leader` now.
fn leaders_among(cluster: &Cluster, among: &BTreeSet<WorkerId>) -> BTreeSet<WorkerId> {
    let states = cluster.states();
    among
        .iter()
        .filter(|id| states[*id] == WorkerState::Leader)
        .cloned()
        .collect()
}

/// Runs the cluster for `total`, stopping every `CHECK_EVERY` to check that
/// no two schedulers have held a valid grant at once, then to run `check`.
fn run_for(cluster: &mut Cluster, total: Duration, mut check: impl FnMut(&Cluster)) {
    let end = cluster.now() + total;
    while cluster.now() < end {
        let step = CHECK_EVERY.min(end - cluster.now());
        cluster.advance(step);
        assert_eq!(
            cluster.first_grant_overlap(),
            None,
            "two nodes held a valid grant at once"
        );
        check(cluster);
    }
}

#[test]
fn a_majority_without_the_authority_orphans_and_a_minority_with_it_recovers() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    let majority: BTreeSet<WorkerId> = [leader.clone(), others[0].clone(), others[1].clone()]
        .into_iter()
        .collect();
    let minority: BTreeSet<WorkerId> = others[2..].iter().cloned().collect();

    set_reachable(&cluster, &majority, false);
    cluster.partition(majority.clone(), minority.clone());

    run_for(&mut cluster, ttls(2), |cluster| {
        let live = live(cluster);
        for id in &majority {
            if !live.contains(id) {
                assert_eq!(
                    cluster.states()[id],
                    WorkerState::Fenced,
                    "{id:?} must fence itself before its registration lapses at the authority"
                );
            }
        }
        cluster.assert_at_most_one_in_leader_state();
    });

    for id in &majority {
        assert_eq!(cluster.states()[id], WorkerState::Fenced);
        assert_eq!(cluster.node(id).recovery_epoch(), 0);
    }
    let new_leader = leaders_among(&cluster, &minority)
        .pop_first()
        .expect("the minority that still reaches the authority must recover the shard");
    assert!(cluster.holds_valid_grant(&new_leader));
    assert_eq!(authority_epoch(&cluster), Some(1));
    for id in &minority {
        assert_eq!(
            cluster.node(id).recovery_epoch(),
            1,
            "{id:?} must lead or follow at the swapped epoch"
        );
    }
}

#[test]
fn the_authority_path_waits_out_the_warm_up_after_an_authority_restart() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    for id in [&leader, &others[0], &others[1]] {
        crash(&mut cluster, id);
    }
    let survivors: BTreeSet<WorkerId> = others[2..].iter().cloned().collect();
    let crashed_at = cluster.now();

    // The authority restarts half a TTL in: it keeps its data but trusts
    // no live count until a TTL after it is back, well past the instant
    // the crashed workers' registrations lapse.
    cluster.advance(Duration::from_ticks(ticks(ttl()) / 2 - 5_000));
    cluster.authority().set_available(false);
    cluster.advance(Duration::from_secs(5));
    cluster.authority().set_available(true);
    let warm_at = cluster.now() + ttl();
    assert!(warm_at > crashed_at + Duration::from_ticks(ticks(ttl()) + 2 * ticks(SUSPECT_TIMEOUT)));

    cluster.record_steps();
    let until_warm = warm_at - cluster.now();
    run_for(&mut cluster, until_warm, |cluster| {
        assert_eq!(authority_epoch(cluster), Some(0), "no swap during warm-up");
        assert!(leaders_among(cluster, &survivors).is_empty());
    });
    let tried_during_warm_up = cluster.take_steps().iter().any(|step| {
        step.at > crashed_at + ttl() && step.outputs.iter().any(|output| {
            matches!(
                output,
                Output::Authority(call) if call.request == AuthorityRequest::ReadLiveRegistrations
            )
        })
    });
    assert!(
        tried_during_warm_up,
        "a survivor must have taken the authority path once only warm-up stood in its way"
    );

    run_for(&mut cluster, ttl(), |_| {});
    let new_leader = leaders_among(&cluster, &survivors)
        .pop_first()
        .expect("once warm, the survivors are a majority of the live registrations");
    assert!(cluster.holds_valid_grant(&new_leader));
    assert_eq!(authority_epoch(&cluster), Some(1));
}

#[test]
fn a_flushed_authority_with_no_leader_left_abandons_the_shard_after_warm_up() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    for id in [&leader, &others[0], &others[1]] {
        crash(&mut cluster, id);
    }
    let survivors: BTreeSet<WorkerId> = others[2..].iter().cloned().collect();

    cluster.advance(Duration::from_ticks(ticks(ttl()) / 2));
    cluster.authority().flush();
    let warm_at = cluster.now() + ttl();

    let until_warm = warm_at - cluster.now();
    run_for(&mut cluster, until_warm, |cluster| {
        for id in &survivors {
            assert_ne!(
                cluster.states()[id],
                WorkerState::Stopped,
                "no count is trusted during warm-up, so nothing is abandoned yet"
            );
        }
    });
    cluster.record_steps();
    run_for(&mut cluster, ttl(), |cluster| {
        assert!(leaders_among(cluster, &survivors).is_empty());
    });

    for id in &survivors {
        assert_eq!(cluster.states()[id], WorkerState::Stopped);
        assert_eq!(cluster.node(id).stop_reason(), Some(StopReason::Abandoned));
    }
    let alerts = cluster
        .take_steps()
        .iter()
        .filter(|step| step.outputs.contains(&Output::ShardAbandoned))
        .count();
    assert_eq!(
        alerts,
        survivors.len(),
        "each survivor raises the alert once"
    );
    assert_eq!(
        authority_epoch(&cluster),
        None,
        "nobody founds a shard in its place"
    );
}

#[test]
fn a_whole_authority_outage_fences_every_worker_and_they_resume_at_the_same_epoch() {
    let (mut cluster, _leader) = elected(5);
    let ids = cluster.node_ids();
    let held: BTreeMap<WorkerId, _> = ids
        .iter()
        .map(|id| {
            let node = cluster.node(id);
            (
                id.clone(),
                (node.configuration().cloned(), node.admission()),
            )
        })
        .collect();

    cluster.authority().set_available(false);
    run_for(
        &mut cluster,
        Duration::from_ticks(ticks(ttl()) + 5_000),
        |_| {},
    );
    for id in &ids {
        assert_eq!(cluster.states()[id], WorkerState::Fenced);
    }
    assert!(cluster.valid_grant_holders().is_empty());

    cluster.authority().set_available(true);
    let mut resumed = BTreeSet::new();
    run_for(&mut cluster, ttl(), |cluster| {
        for (id, state) in cluster.states() {
            if state != WorkerState::Fenced && resumed.insert(id.clone()) {
                let node = cluster.node(&id);
                assert_eq!(
                    (node.configuration().cloned(), node.admission()),
                    held[&id],
                    "{id:?} resumes with the configuration and admission it held"
                );
            }
        }
    });
    assert_eq!(resumed, ids, "every worker resumes");
    for id in &ids {
        assert_eq!(cluster.node(id).recovery_epoch(), 0);
    }
    assert_eq!(authority_epoch(&cluster), Some(0));
    let leader = cluster.leader().expect("the shard elects again");
    cluster.assert_at_most_one_in_leader_state();
    assert!(cluster.holds_valid_grant(&leader));
}

/// Cuts `cut_off` off from the authority and flushes it. Once every one of
/// them has fenced itself and the authority has warmed up again with none of
/// them registered, a bootstrapper finds the shard gone and founds it afresh
/// at epoch 0, of a new lineage, as the bootstrap cascade would. Then lets
/// `cut_off` reach the authority again. Returns the new epoch.
fn refound_while_fenced(cluster: &mut Cluster, cut_off: &BTreeSet<WorkerId>) -> RecoveryEpoch {
    set_reachable(cluster, cut_off, false);
    cluster.authority().flush();
    run_for(cluster, Duration::from_ticks(ticks(ttl()) + 5_000), |_| {});
    for id in cut_off {
        assert_eq!(cluster.states()[id], WorkerState::Fenced);
    }
    assert!(
        live(cluster).is_empty(),
        "the flushed authority lists no one"
    );

    let refounded = RecoveryEpoch::founding(0, &mut Uuid7Lineages);
    let bootstrapper = WorkerId::new("bootstrapper");
    cluster
        .authority()
        .register(&shard("shard-1"), &bootstrapper, bootstrapper.as_str())
        .expect("the seeding handle reaches the authority");
    cluster
        .authority()
        .compare_and_swap_recovery_epoch(&shard("shard-1"), None, refounded)
        .expect("the flushed authority holds no epoch, so create-if-absent succeeds");
    set_reachable(cluster, cut_off, true);
    refounded
}

/// Asserts that every one of `ids` went back to `Bootstrapping` to join the
/// shard founded at `refounded`, with it as its floor, rather than resuming
/// or staying fenced.
fn assert_rejoining(cluster: &Cluster, ids: &BTreeSet<WorkerId>, refounded: RecoveryEpoch) {
    for id in ids {
        let node = cluster.node(id);
        assert_eq!(cluster.states()[id], WorkerState::Bootstrapping, "{id:?}");
        assert_eq!(
            (node.recovery_epoch(), node.recovery_lineage()),
            (refounded.number, Some(refounded.lineage)),
            "{id:?} rejoins at the epoch founded afresh"
        );
    }
}

// The same, for a shard already recovered past epoch 0: its workers find the
// authority at an epoch below their own. That is no lost data they could
// wait out, but a shard founded afresh, so they rejoin it rather than stay
// fenced for good.
#[test]
fn workers_fenced_through_a_flush_above_epoch_0_rejoin_rather_than_stay_fenced() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    let majority: BTreeSet<WorkerId> = [leader.clone(), others[0].clone(), others[1].clone()]
        .into_iter()
        .collect();
    let minority: BTreeSet<WorkerId> = others[2..].iter().cloned().collect();
    set_reachable(&cluster, &majority, false);
    cluster.partition(majority.clone(), minority.clone());
    run_for(&mut cluster, ttls(2), |_| {});
    for id in &minority {
        assert_eq!(
            cluster.node(id).recovery_epoch(),
            1,
            "the minority recovered"
        );
    }

    let ids = cluster.node_ids();
    let refounded = refound_while_fenced(&mut cluster, &ids);
    run_for(&mut cluster, ttl(), |_| {});

    assert_rejoining(&cluster, &ids, refounded);
}

#[test]
fn a_worker_with_no_authority_never_orphans_itself() {
    let mut cluster = Cluster::bootstrap(5, SUSPECT_TIMEOUT);
    settle(&mut cluster);
    let leader = cluster.leader().expect("the cluster must elect a leader");
    let others = followers(&cluster, &leader);
    let minority: BTreeSet<WorkerId> = others[..2].iter().cloned().collect();
    let majority: BTreeSet<WorkerId> = cluster.node_ids().difference(&minority).cloned().collect();
    cluster.partition(majority, minority.clone());
    cluster.record_steps();

    run_for(&mut cluster, ttls(4), |cluster| {
        assert!(
            cluster
                .states()
                .values()
                .all(|state| *state != WorkerState::Fenced),
            "a node with no authority has no registration to lose"
        );
    });

    for step in cluster.take_steps() {
        assert!(
            !step
                .outputs
                .iter()
                .any(|output| matches!(output, Output::Authority(_))),
            "a node with no authority asks nothing of one"
        );
    }
    for id in &minority {
        assert!(
            matches!(
                cluster.states()[id],
                WorkerState::NoQuorum | WorkerState::RollCall
            ),
            "{id:?} waits for its peers"
        );
    }
    assert_eq!(cluster.states()[&leader], WorkerState::Leader);
    assert!(cluster.holds_valid_grant(&leader));
}

#[test]
fn a_flush_under_a_live_quorum_is_repaired_by_its_leader() {
    let (mut cluster, leader) = elected(5);
    let ids = cluster.node_ids();

    cluster.authority().flush();
    let flushed_at = cluster.now();
    let warm_at = flushed_at + ttl();
    let mut republished_by = None;

    run_for(&mut cluster, ttls(2), |cluster| {
        assert_eq!(
            cluster.leader(),
            Some(leader.clone()),
            "the leader keeps the shard"
        );
        cluster.assert_at_most_one_in_leader_state();
        for id in &ids {
            assert_eq!(
                cluster.node(id).recovery_epoch(),
                0,
                "nobody founds another shard"
            );
        }
        assert!(
            !cluster
                .states()
                .values()
                .any(|state| matches!(state, WorkerState::Fenced | WorkerState::Bootstrapping)),
            "a flush under a live quorum fences no one: {:?}",
            cluster.states()
        );
        if republished_by.is_none() && authority_epoch(cluster) == Some(0) {
            republished_by = Some(cluster.now());
        }
        let at = cluster.now();
        if at + CHECK_EVERY >= warm_at && at < warm_at {
            assert!(
                !cluster.holds_valid_grant(&leader),
                "no fence, and so no grant, outlasts the warm-up"
            );
        }
    });

    let republished_by = republished_by.expect("the leader republishes its epoch");
    assert!(
        republished_by <= flushed_at + Duration::from_ticks(ticks(ttl()) / 3 + ticks(CHECK_EVERY))
    );
    assert!(
        cluster.holds_valid_grant(&leader),
        "the leader acts again after warm-up"
    );
    assert_eq!(authority_epoch(&cluster), Some(0));
}

#[test]
fn departed_workers_stop_blocking_a_leaderless_shard_once_their_registrations_lapse() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    crash(&mut cluster, &leader);
    crash(&mut cluster, &others[0]);
    let cut_off = others[1].clone();
    let survivors: BTreeSet<WorkerId> = others[2..].iter().cloned().collect();
    let departed: BTreeSet<WorkerId> = [leader.clone(), others[0].clone(), cut_off.clone()]
        .into_iter()
        .collect();
    cluster.node_authority(&cut_off).set_reachable(false);
    cluster.partition(departed.clone(), survivors.clone());

    let mut led_while_departed_live = false;
    run_for(&mut cluster, ttls(2), |cluster| {
        if !leaders_among(cluster, &survivors).is_empty() && !live(cluster).is_disjoint(&departed) {
            led_while_departed_live = true;
        }
    });

    assert!(!led_while_departed_live);
    assert_eq!(cluster.states()[&cut_off], WorkerState::Fenced);
    let new_leader = leaders_among(&cluster, &survivors)
        .pop_first()
        .expect("the survivors recover once the departed have lapsed");
    assert!(cluster.holds_valid_grant(&new_leader));
    assert_eq!(authority_epoch(&cluster), Some(1));
    let founded = cluster
        .node(&new_leader)
        .configuration()
        .expect("a leader holds its configuration")
        .generation();
    assert_eq!(founded.recovery_epoch(), 1);
}

#[test]
fn an_orphan_rejoins_as_pending_and_a_straggler_finds_the_recovered_leader_by_refusal() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    crash(&mut cluster, &leader);
    let orphan = others[0].clone();
    let straggler = others[1].clone();
    let recovering: BTreeSet<WorkerId> = others[2..].iter().cloned().collect();
    let away: BTreeSet<WorkerId> = [leader.clone(), orphan.clone(), straggler.clone()]
        .into_iter()
        .collect();
    cluster.node_authority(&orphan).set_reachable(false);
    cluster.partition(away, recovering.clone());

    run_for(&mut cluster, ttls(2), |_| {});
    let new_leader = leaders_among(&cluster, &recovering)
        .pop_first()
        .expect("the two that reach each other are a majority of the three live");
    assert_eq!(cluster.states()[&orphan], WorkerState::Fenced);
    assert_ne!(
        cluster.states()[&straggler],
        WorkerState::Fenced,
        "the straggler reaches the authority"
    );
    assert_eq!(cluster.node(&straggler).recovery_epoch(), 0);

    // The connection acks the recovered leader sends as the partition heals
    // are lost, so the straggler learns of it only from a refusal.
    cluster.network().set_drop_rate(1.0);
    cluster.heal();
    cluster.network().set_drop_rate(0.0);
    cluster.node_authority(&orphan).set_reachable(true);

    cluster.record_steps();
    run_for(&mut cluster, ttl(), |_| {});
    let steps = cluster.take_steps();

    assert_eq!(cluster.states()[&straggler], WorkerState::Active);
    assert_eq!(cluster.node(&straggler).recovery_epoch(), 1);
    assert_eq!(
        cluster.node(&straggler).known_leader().map(|(id, _)| id),
        Some(new_leader.clone())
    );
    let refused_naming_leader = steps.iter().any(|step| {
        step.outputs.iter().any(|output| match output {
            Output::Send { to, message } if *to == straggler => matches!(
                &message.payload,
                Some(Payload::ElectionReject(reject))
                    if reject.leader.as_ref().is_some_and(|named| named.leader_id == Some(new_leader.clone().into()))
            ),
            _ => false,
        })
    });
    assert!(
        refused_naming_leader,
        "a refusal named the recovered leader"
    );

    assert_eq!(cluster.states()[&orphan], WorkerState::Active);
    assert_eq!(cluster.node(&orphan).recovery_epoch(), 1);
    assert!(
        steps.iter().any(|step| step.node == orphan
            && step.recovery_epoch == 1
            && step.admission.is_none()
            && matches!(step.input, Some(Input::JoinAnswer(_)))
            && step.state == WorkerState::Active),
        "the orphan rejoined at the new epoch as a pending member, on a JOIN answer"
    );
    let orphan_admission = Admission {
        current: cluster.node(&orphan).admission(),
        prior: cluster.node(&orphan).prior_admission(),
    };
    assert!(
        cluster
            .node(&new_leader)
            .configuration()
            .is_some_and(|configuration| configuration.is_voter(orphan_admission)),
        "then a batch admitted it (ADR-0001 decision 9): {orphan_admission:?}"
    );
    assert_eq!(
        cluster.node(&orphan).known_leader().map(|(id, _)| id),
        Some(new_leader.clone())
    );
    assert!(cluster.holds_valid_grant(&new_leader));
}

// A leader paused past its registration wakes with its inputs held. It must
// fence itself before acting on any of them: were it still to lead, a fence
// refused because the authority was flushed meanwhile would have it
// republish its old epoch over the one the survivors recovered to, and no
// node could lead again.
#[test]
fn a_leader_paused_past_its_registration_does_not_republish_its_old_epoch() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    cluster.stall(&leader, Duration::from_secs(45));
    crash(&mut cluster, &others[0]);
    crash(&mut cluster, &others[1]);
    let survivors: BTreeSet<WorkerId> = others[2..].iter().cloned().collect();
    run_for(&mut cluster, Duration::from_secs(44), |_| {});
    assert_eq!(
        authority_epoch(&cluster),
        Some(1),
        "the survivors recovered"
    );

    cluster.authority().flush();
    run_for(&mut cluster, ttls(4), |_| {});

    assert_eq!(authority_epoch(&cluster), Some(1));
    assert_eq!(leaders_among(&cluster, &survivors).len(), 1);
    assert_eq!(
        cluster.node(&leader).recovery_epoch(),
        1,
        "the old leader rejoined"
    );
}

// A leader paused past its quorum-contact lease, though not past its
// registration, no longer leads: had it handled the inputs held during the
// pause as leader, a fence refused because the authority was flushed would
// have it republish its old epoch below the one its survivors had swapped
// to, and they could never recover the shard.
#[test]
fn a_leader_paused_past_its_quorum_lease_does_not_republish_its_old_epoch() {
    let (mut cluster, leader) = elected(5);
    let others = followers(&cluster, &leader);
    crash(&mut cluster, &others[0]);
    crash(&mut cluster, &others[1]);
    run_for(
        &mut cluster,
        Duration::from_ticks(ticks(ttl()) + ticks(SUSPECT_TIMEOUT)),
        |_| {},
    );
    assert_eq!(cluster.states()[&leader], WorkerState::Leader);

    cluster.stall(&leader, Duration::from_secs(15));
    let started = cluster.now();
    while authority_epoch(&cluster) != Some(1) {
        assert!(
            cluster.now() < started + Duration::from_secs(12),
            "the survivors swap the epoch while the leader is paused"
        );
        run_for(&mut cluster, CHECK_EVERY, |_| {});
    }
    cluster.authority().flush();

    run_for(&mut cluster, Duration::from_secs(20), |cluster| {
        assert_ne!(
            authority_epoch(cluster),
            Some(0),
            "the paused leader republished the epoch its survivors left"
        );
    });
    assert_ne!(cluster.states()[&leader], WorkerState::Leader);
}
