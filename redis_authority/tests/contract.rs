use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::protocol::ids::ShardName;
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
fn a_standalone_server_keeps_the_coordination_authority_contract() {
    let server = ValkeyServer::start(ServerMode::Standalone);
    let config = config(vec![server.url()]);
    check_authority_contract(&Redis { server, config }, &RealTime);
}
