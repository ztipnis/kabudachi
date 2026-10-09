use kabudachi_core::time::Duration;
use kabudachi_redis_authority::{ConfigError, RedisAuthority, RedisAuthorityConfig};

fn url() -> Vec<String> {
    vec!["redis://127.0.0.1:6379/".to_string()]
}

fn connect(config: RedisAuthorityConfig) -> Result<(), ConfigError> {
    RedisAuthority::connect(config).map(|_| ())
}

#[test]
fn connect_refuses_each_config_the_adapter_cannot_honour_and_opens_no_connection() {
    // Port 6379 has no server here: accepting this config proves no connection is made.
    assert_eq!(connect(RedisAuthorityConfig::new(url())), Ok(()));

    assert_eq!(connect(RedisAuthorityConfig::new(vec![])), Err(ConfigError::Urls));
    let two = vec![url()[0].clone(), url()[0].clone()];
    assert_eq!(connect(RedisAuthorityConfig::new(two.clone())), Err(ConfigError::Urls));
    let mut cluster = RedisAuthorityConfig::new(two);
    cluster.cluster = true;
    assert_eq!(connect(cluster.clone()), Ok(()));
    cluster.database = 1;
    assert_eq!(connect(cluster), Err(ConfigError::ClusterDatabase));

    let http = vec!["http://127.0.0.1:6379/".to_string()];
    assert_eq!(
        connect(RedisAuthorityConfig::new(http)),
        Err(ConfigError::BadUrl("http://127.0.0.1:6379/".into()))
    );
    // TLS: accepted, and still no connection is made.
    let tls = vec!["rediss://127.0.0.1:6380/".to_string()];
    assert_eq!(connect(RedisAuthorityConfig::new(tls)), Ok(()));

    let mut braces = RedisAuthorityConfig::new(url());
    braces.key_prefix = "a{b:".into();
    assert_eq!(connect(braces), Err(ConfigError::PrefixBraces));

    let mut zero = RedisAuthorityConfig::new(url());
    zero.call_timeout = Duration::from_millis(0);
    assert_eq!(connect(zero), Err(ConfigError::CallTimeout));
    let mut third = RedisAuthorityConfig::new(url()).with_ttl(Duration::from_millis(900));
    third.call_timeout = Duration::from_millis(300);
    assert_eq!(connect(third), Err(ConfigError::CallTimeout));
}
