use kabudachi_core::coordination_authority::{CoordinationAuthority, RecoveryEpoch, ShardRecord};
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
use kabudachi_core::time::Duration;
use kabudachi_redis_authority::{RedisAuthority, RedisAuthorityConfig};
use kabudachi_testkit::{AuthorityAdapter, PassTime, check_authority_contract};
use valkey_test_support::{ServerMode, ValkeyServer};

pub struct Redis {
    pub server: ValkeyServer,
    pub config: RedisAuthorityConfig,
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
        let authority = RedisAuthority::connect(self.config.clone()).expect("valid config");
        touch_contract_names(&authority);
        authority
    }

    fn flush(&self, authority: &RedisAuthority) {
        self.server.flushall();
        touch_contract_names(authority);
    }

    fn go_down(&self, _authority: &RedisAuthority) {
        self.server.shutdown_save();
    }

    fn come_back(&self, _authority: &RedisAuthority) {
        self.server.restart();
    }
}

/// The contract expects an authority to start its warm-up when it starts or
/// loses its data. This one starts a name's warm-up at the first call that
/// names it, so the adapter makes that call right away for each name the
/// contract uses.
fn touch_contract_names(authority: &RedisAuthority) {
    for name in ["contract-shard", "contract-other-shard"] {
        authority
            .read_shard(&ShardName::new(name))
            .expect("the server answers");
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
    let redis = Redis { server, config };
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
    // A restarted cluster node takes writes only two seconds after it starts,
    // and the contract's outage keeps a registration alive through the
    // restart, so the TTL must be well above that.
    config.ttl = Duration::from_millis(4_000);
    check_authority_contract(&Redis { server, config }, &RealTime);
}
