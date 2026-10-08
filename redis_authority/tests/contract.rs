use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch, ShardRecord};
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
use kabudachi_core::time::Duration;
use kabudachi_redis_authority::{RedisAuthority, RedisAuthorityConfig};
use kabudachi_testkit::{AuthorityAdapter, PassTime, check_authority_contract};
use valkey_test_support::{ServerMode, ValkeyServer};

pub struct Redis {
    pub server: ValkeyServer,
    pub config: RedisAuthorityConfig,
    pub has_outages: bool,
}

pub fn config(urls: Vec<String>) -> RedisAuthorityConfig {
    let mut config = RedisAuthorityConfig::new(urls).with_ttl(Duration::from_millis(1_000));
    config.call_timeout = Duration::from_millis(200);
    config
}

impl AuthorityAdapter for Redis {
    type Authority = RedisAuthority;

    fn ttl(&self) -> Duration {
        self.config.ttl
    }

    fn fresh(&self) -> RedisAuthority {
        self.server.flushall();
        RedisAuthority::connect(self.config.clone()).expect("valid config")
    }

    fn flush(&self, _authority: &RedisAuthority) {
        self.server.flushall();
    }

    fn go_down(&self, _authority: &RedisAuthority) {
        self.server.shutdown_save();
    }

    fn come_back(&self, _authority: &RedisAuthority) {
        self.server.restart();
    }

    // A restarted cluster node refuses writes for its first seconds, longer
    // than a TTL of a second; the standalone ACL run covers outages.
    fn has_outages(&self) -> bool {
        self.has_outages
    }
}

pub struct RealTime;

impl PassTime for RealTime {
    fn pass(&self, duration: Duration) {
        std::thread::sleep(std::time::Duration::from_millis(duration.as_ticks()));
    }
}

#[test]
fn an_acl_user_limited_to_its_prefix_and_commands_keeps_the_contract_and_sees_no_other_prefix() {
    let server = ValkeyServer::start(ServerMode::StandaloneWithAcl {
        user: "kabudachi".into(),
        password: "secret".into(),
        key_pattern: "kabu:*".into(),
    });
    let mut config = config(vec![server.url()]);
    config.key_prefix = "kabu:one:".into();
    config.database = 1;
    let redis = Redis {
        server,
        config,
        has_outages: true,
    };
    check_authority_contract(&redis, &RealTime);

    let one = redis.fresh();
    let other = RedisAuthority::connect(RedisAuthorityConfig {
        key_prefix: "kabu:two:".into(),
        ..redis.config.clone()
    })
    .expect("valid config");
    let name = ShardName::new("shared-name");
    let record = ShardRecord {
        shard_id: ShardId::new("shared-name/1"),
        recovery_epoch: RecoveryEpoch::new(0, 1),
    };
    one.compare_and_swap_shard(&name, None, &record)
        .expect("created under the first prefix");
    one.register(&name, &record.shard_id, &WorkerId::new("w"), "addr")
        .expect("registered");
    assert_eq!(
        other.read_shard(&name),
        Ok(None),
        "another prefix sees no shard record"
    );
    assert!(
        other
            .live_registrations(&name, &record.shard_id)
            .expect("listed")
            .addresses()
            .is_empty(),
        "another prefix sees no registration"
    );
}

#[test]
fn a_single_node_cluster_keeps_the_coordination_authority_contract() {
    let server = ValkeyServer::start(ServerMode::Cluster);
    let mut config = config(vec![server.url()]);
    config.cluster = true;
    check_authority_contract(
        &Redis {
            server,
            config,
            has_outages: false,
        },
        &RealTime,
    );
}
