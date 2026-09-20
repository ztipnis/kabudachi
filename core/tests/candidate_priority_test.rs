use kabudachi_core::election::candidate_priority;
use kabudachi_core::hashing::HashFunction;
use kabudachi_core::protocol::ids::{ShardId, WorkerId};

fn shard() -> ShardId {
    ShardId::new("shard-1")
}

fn worker() -> WorkerId {
    WorkerId::new("worker-1")
}

/// Pins the default (SHA-256) priority encoding. The expected value was
/// computed with Python's `hashlib` over length-prefixed strings and
/// big-endian integers; if this fails, election winners have changed for every
/// build that doesn't share the change.
#[test]
fn default_priority_is_stable_across_builds() {
    let priority = candidate_priority(&HashFunction::default(), &shard(), 0, 3, &worker());
    assert_eq!(priority, 10_384_955_935_822_374_861);
}

#[test]
fn priority_follows_the_configured_hash_function() {
    let sha3 = HashFunction::new::<sha3::Sha3_256>();
    let priority = candidate_priority(&sha3, &shard(), 0, 3, &worker());
    assert_eq!(priority, 18_294_837_748_612_206_051);
}

#[test]
fn priority_depends_on_every_input() {
    let hash = HashFunction::default();
    let base = candidate_priority(&hash, &shard(), 0, 3, &worker());

    assert_ne!(
        base,
        candidate_priority(&hash, &ShardId::new("shard-2"), 0, 3, &worker())
    );
    assert_ne!(base, candidate_priority(&hash, &shard(), 1, 3, &worker()));
    assert_ne!(base, candidate_priority(&hash, &shard(), 0, 4, &worker()));
    assert_ne!(
        base,
        candidate_priority(&hash, &shard(), 0, 3, &WorkerId::new("worker-2"))
    );
}
