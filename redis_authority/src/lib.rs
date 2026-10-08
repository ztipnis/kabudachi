//! A [`CoordinationAuthority`](kabudachi_core::coordination_authority::CoordinationAuthority)
//! over Redis or Valkey.

mod config;

pub use config::{ConfigError, DEFAULT_TTL, RedisAuthorityConfig};

/// Every command the adapter sends, spelled as ACL rules take them. An ACL
/// user needs exactly these and `~<key_prefix>*`.
pub const COMMANDS: &[&str] = &[
    "watch",
    "unwatch",
    "multi",
    "exec",
    "get",
    "set",
    "hgetall",
    "hset",
    "hdel",
    "time",
    "info",
    "select",
    "cluster|slots",
];

pub struct RedisAuthority {
    #[allow(dead_code)]
    config: RedisAuthorityConfig,
    #[allow(dead_code)]
    urls: Vec<String>,
}

impl RedisAuthority {
    /// Checks `config` and opens no connection: a worker that starts while
    /// the server is down gets `Unavailable` from its calls, as in any outage.
    pub fn connect(config: RedisAuthorityConfig) -> Result<Self, ConfigError> {
        let urls = config.validated_urls()?;
        Ok(Self { config, urls })
    }
}
