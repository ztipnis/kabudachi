//! The safety oracle every scenario asserts through: which sequences of
//! grant reports it calls two nodes leading at once.

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::Output;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::worker_state::WorkerState;
use kabudachi_core::scheduler::{LeadershipGrant, LeaseEnd};
use kabudachi_core::time::Instant;
use kabudachi_testkit::{StepRecord, first_grant_overlap};

fn grant(valid_until: LeaseEnd) -> Option<LeadershipGrant> {
    Some(LeadershipGrant {
        term: 1,
        recovery_epoch: RecoveryEpoch::new(0, 0),
        valid_until,
        reconnect_timeout: kabudachi_core::election::ElectionTimings::DEFAULT_RECONNECT_TIMEOUT,
    })
}

fn reports(node: &str, at: u64, grant: Option<LeadershipGrant>) -> StepRecord {
    StepRecord {
        at: Instant::at(at),
        node: WorkerId::new(node),
        input: None,
        state: WorkerState::Leader,
        term: 1,
        recovery_epoch: 0,
        recovery_lineage: 0,
        admission: None,
        prior_admission: None,
        leader: None,
        outputs: vec![Output::Grant(grant)],
    }
}

#[test]
fn grants_overlap_only_while_two_nodes_hold_them_at_once() {
    let lease_to_15 = || LeaseEnd::At(Instant::at(15));
    let cases: Vec<(&str, Vec<StepRecord>, Option<(&str, &str)>)> = vec![
        (
            "a second node granted while the first still holds",
            vec![
                reports("a", 10, grant(LeaseEnd::Unbounded)),
                reports("b", 20, grant(LeaseEnd::Unbounded)),
                reports("a", 30, None),
            ],
            Some(("a", "b")),
        ),
        (
            "the first withdrawn before the second begins",
            vec![
                reports("a", 10, grant(LeaseEnd::Unbounded)),
                reports("a", 20, None),
                reports("b", 20, grant(LeaseEnd::Unbounded)),
            ],
            None,
        ),
        (
            "a node renewing its own grant",
            vec![
                reports("a", 10, grant(LeaseEnd::At(Instant::at(30)))),
                reports("a", 20, grant(LeaseEnd::At(Instant::at(40)))),
            ],
            None,
        ),
        (
            "a second node granted at the instant the first lease ends",
            vec![
                reports("a", 10, grant(lease_to_15())),
                reports("b", 15, grant(LeaseEnd::Unbounded)),
            ],
            None,
        ),
        (
            "a second node granted before the first lease ends",
            vec![
                reports("a", 10, grant(lease_to_15())),
                reports("b", 14, grant(LeaseEnd::Unbounded)),
            ],
            Some(("a", "b")),
        ),
    ];

    for (case, records, expected) in cases {
        let found = first_grant_overlap(&records)
            .map(|(first, second)| (first.node, second.node));
        let expected = expected.map(|(first, second)| (WorkerId::new(first), WorkerId::new(second)));
        assert_eq!(found, expected, "{case}");
    }
}
