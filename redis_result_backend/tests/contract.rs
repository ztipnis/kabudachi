//! The valkey result backend keeps the result backend contract as an ACL
//! user limited to its commands and its key prefix, and sees no entry of
//! another prefix.

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::result_backend::{ResultBackend, ResultBounds, ResultKey, task_entries};
use kabudachi_core::time::{Duration, RealClock, WallTime};
use kabudachi_redis_authority::RedisAuthorityConfig;
use kabudachi_redis_result_backend::{COMMANDS, RedisResultBackend, RedisResultBackendError};
use kabudachi_testkit::{BackendAdapter, PassTime, check_result_backend_contract};
use valkey_test_support::{ServerMode, ValkeyServer};

struct Valkey {
    server: ValkeyServer,
    config: RedisAuthorityConfig,
}

impl BackendAdapter for Valkey {
    type Backend = RedisResultBackend;

    fn fresh(&self) -> RedisResultBackend {
        self.server.flushall();
        RedisResultBackend::connect(self.config.clone()).expect("a valid config")
    }

    fn now(&self) -> WallTime {
        WallTime::now(&RealClock::new())
    }
}

struct RealTime;

impl PassTime for RealTime {
    fn pass(&self, duration: Duration) {
        std::thread::sleep(std::time::Duration::from_millis(duration.as_ticks()));
    }
}

#[test]
fn an_acl_user_limited_to_its_prefix_and_commands_keeps_the_contract_and_sees_no_other_prefix() {
    let server = ValkeyServer::start(ServerMode::StandaloneWithAcl {
        user: "results".into(),
        password: "secret".into(),
        key_pattern: "kabu:*".into(),
        commands: COMMANDS,
    });
    let mut config = RedisAuthorityConfig::new(vec![server.url()]);
    config.key_prefix = "kabu:one:".into();
    config.database = 1;
    config.call_timeout = Duration::from_millis(500);
    let valkey = Valkey { server, config };
    check_result_backend_contract(&valkey, &RealTime);

    let one = valkey.fresh();
    let other = RedisResultBackend::connect(RedisAuthorityConfig {
        key_prefix: "kabu:two:".into(),
        ..valkey.config.clone()
    })
    .expect("a valid config");
    let task = TaskId::new("shared-id");
    let expires_at = WallTime::from_unix_millis(valkey.now().as_unix_millis() + 60_000);
    for entry in task_entries(
        &ResultBounds::DEFAULT,
        &task,
        &TaskRunId::new("r"),
        &Digest::blake3(b"v"),
        b"v",
        expires_at,
    ) {
        one.store(&entry).expect("stored under the first prefix");
    }
    assert_eq!(other.load(&ResultKey::Task(task)), Ok(None), "another prefix sees no entry");

    let cluster = RedisAuthorityConfig {
        cluster: true,
        ..valkey.config.clone()
    };
    assert!(matches!(
        RedisResultBackend::connect(cluster),
        Err(RedisResultBackendError::Cluster)
    ));
}
