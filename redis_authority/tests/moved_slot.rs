use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch, ShardRecord};
use kabudachi_core::protocol::ids::{ShardId, ShardName};
use kabudachi_redis_authority::RedisAuthority;
use valkey_test_support::{ServerMode, ValkeyServer};

use crate::contract::config;

#[test]
fn a_shard_whose_slot_moves_is_followed_to_its_new_node() {
    let first = ValkeyServer::start(ServerMode::Cluster);
    let second = first.add_cluster_node();
    let mut config = config(vec![first.url()]);
    config.cluster = true;
    let authority = RedisAuthority::connect(config.clone()).expect("valid config");
    let name = ShardName::new("moving");
    let record = ShardRecord {
        shard_id: ShardId::new("moving/1"),
        recovery_epoch: RecoveryEpoch::new(0, 7),
    };
    authority
        .compare_and_swap_shard(&name, None, &record)
        .expect("founded on the first node");

    first.move_slot_of(&format!("{}{{moving}}:shard", config.key_prefix), &second);

    assert_eq!(
        authority.read_shard(&name),
        Ok(Some(record.clone())),
        "read from the new owner"
    );
    let next = ShardRecord {
        recovery_epoch: RecoveryEpoch::new(1, 7),
        ..record.clone()
    };
    assert_eq!(
        authority.compare_and_swap_shard(&name, Some(&record), &next),
        Ok(()),
        "swapped on the new owner"
    );
}
