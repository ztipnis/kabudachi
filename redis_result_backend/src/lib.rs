//! A [`ResultBackend`] over a standalone Redis or Valkey server.
//!
//! It is configured like the coordination authority, with
//! [`RedisAuthorityConfig`]: one `redis://` or `rediss://` URL, the key
//! prefix, the database and the call timeout. Its `ttl` is not used: each
//! entry expires on its own, by the server (`SET … PX`). A cluster is
//! refused.
//!
//! # Keys
//!
//! `<prefix>result:` followed by the entry key's text (`task/<id>`,
//! `composite/<id>/page/<n>`, …), each an encoded stored result. The
//! authority's own keys start `<prefix>{`, so both can share a prefix.
//!
//! # Operating it
//!
//! - An ACL user needs exactly the commands in [`COMMANDS`] and the keys
//!   `~<key_prefix>*`.
//! - Use an eviction policy that evicts only keys with a TTL
//!   (`volatile-*`) or none (`noeviction`): every key here has one.
//! - Every call, connecting included, is bounded by the config's call
//!   timeout. Past it the call answers `BackendError::Unavailable` and the
//!   connection is dropped.

use std::sync::{Mutex, PoisonError};

use kabudachi_core::result_backend::{BackendError, ResultBackend, ResultEntry, ResultKey};
use kabudachi_core::time::{RealClock, WallTime};
use kabudachi_redis_authority::{ConfigError, RedisAuthorityConfig};

/// Every command the backend sends, spelled as ACL rules take them. An ACL
/// user needs exactly these and `~<key_prefix>*`.
pub const COMMANDS: &[&str] = &["get", "set", "select"];

#[derive(Debug, thiserror::Error)]
pub enum RedisResultBackendError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("a result backend serves a standalone server, not a cluster")]
    Cluster,
}

pub struct RedisResultBackend {
    url: String,
    key_prefix: String,
    call_timeout: std::time::Duration,
    clock: RealClock,
    /// Reused while it works; dropped after any failure.
    connection: Mutex<Option<redis::Connection>>,
}

impl RedisResultBackend {
    /// A backend for `config`'s server. Checks the config; connects at the
    /// first call, so a server that is down now is no error here.
    pub fn connect(config: RedisAuthorityConfig) -> Result<Self, RedisResultBackendError> {
        if config.cluster {
            return Err(RedisResultBackendError::Cluster);
        }
        let url = config.validated_urls()?.remove(0);
        Ok(RedisResultBackend {
            url,
            key_prefix: config.key_prefix,
            call_timeout: std::time::Duration::from_millis(config.call_timeout.as_ticks()),
            clock: RealClock::new(),
            connection: Mutex::new(None),
        })
    }

    fn key(&self, key: &ResultKey) -> String {
        format!("{}result:{}", self.key_prefix, key.as_text())
    }

    /// Runs `command` on a connection, opening one first if there is none,
    /// each round trip bounded by the call timeout. Any failure drops the
    /// connection.
    fn call<T>(
        &self,
        command: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> Result<T, BackendError> {
        let mut slot = self
            .connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            let client = redis::Client::open(self.url.as_str())
                .map_err(|error| BackendError::Refused(error.to_string()))?;
            let connection = client
                .get_connection_with_timeout(self.call_timeout)
                .map_err(|error| BackendError::Unavailable(error.to_string()))?;
            *slot = Some(connection);
        }
        let connection = slot.as_mut().expect("just opened");
        let bounded = connection
            .set_read_timeout(Some(self.call_timeout))
            .and_then(|()| connection.set_write_timeout(Some(self.call_timeout)));
        let outcome = bounded.and_then(|()| command(connection));
        outcome.map_err(|error| {
            *slot = None;
            if error.is_timeout() || error.is_io_error() || error.is_connection_dropped() {
                BackendError::Unavailable(error.to_string())
            } else {
                BackendError::Refused(error.to_string())
            }
        })
    }
}

impl ResultBackend for RedisResultBackend {
    fn store(&self, entry: &ResultEntry) -> Result<(), BackendError> {
        let now = WallTime::now(&self.clock).as_unix_millis();
        let left = entry.expires_at.as_unix_millis().saturating_sub(now);
        if left == 0 {
            return Ok(());
        }
        let key = self.key(&entry.key);
        let bytes = entry.encode();
        self.call(|connection| {
            redis::cmd("SET")
                .arg(&key)
                .arg(bytes)
                .arg("PX")
                .arg(left)
                .query(connection)
        })
    }

    fn load(&self, key: &ResultKey) -> Result<Option<ResultEntry>, BackendError> {
        let key = self.key(key);
        let bytes: Option<Vec<u8>> =
            self.call(|connection| redis::cmd("GET").arg(&key).query(connection))?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let entry = ResultEntry::decode(&bytes)?;
        // The server's clock decides; this one only guards against an entry
        // a skewed server still holds.
        Ok((!entry.has_expired(WallTime::now(&self.clock))).then_some(entry))
    }
}
