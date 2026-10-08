use kabudachi_core::coordination_authority::{AuthorityError, CoordinationAuthority};
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
use kabudachi_redis_authority::RedisAuthority;
use valkey_test_support::{ServerMode, ValkeyServer};

use crate::contract::config;

#[test]
fn a_paused_server_answers_unavailable_within_the_call_timeout_then_withholds_the_count() {
    let server = ValkeyServer::start(ServerMode::Standalone);
    let config = config(vec![server.url()]);
    let authority = RedisAuthority::connect(config).expect("valid config");
    let (name, shard_id, worker) = (
        ShardName::new("paused"),
        ShardId::new("paused/1"),
        WorkerId::new("w"),
    );
    authority
        .register(&name, &shard_id, &worker, "addr")
        .expect("registered");
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    authority
        .register(&name, &shard_id, &worker, "addr")
        .expect("renewed");
    assert_eq!(
        authority
            .live_registrations(&name, &shard_id)
            .expect("listed")
            .authoritative_count(),
        Some(1)
    );

    let started = std::time::Instant::now();
    server.pause_clients(std::time::Duration::from_millis(1_000));
    assert_eq!(
        authority.register(&name, &shard_id, &worker, "addr"),
        Err(AuthorityError::Unavailable)
    );
    assert!(
        started.elapsed() < std::time::Duration::from_millis(450),
        "bounded by the 200 ms call timeout, took {:?}",
        started.elapsed()
    );

    std::thread::sleep(std::time::Duration::from_millis(1_100).saturating_sub(started.elapsed()));
    authority
        .register(&name, &shard_id, &worker, "addr")
        .expect("back");
    assert_eq!(
        authority
            .live_registrations(&name, &shard_id)
            .expect("listed")
            .authoritative_count(),
        None,
        "the count is withheld for a TTL after an outage this client saw"
    );
}
