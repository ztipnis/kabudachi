use kabudachi_core::time::Duration;

/// The TTL used unless a config sets another.
pub const DEFAULT_TTL: Duration = Duration::from_secs(30);

const MAX_DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(2);

/// How to reach the Redis or Valkey server (or cluster) an authority keeps
/// its data in, and the time bounds it keeps.
#[derive(Clone, Debug)]
pub struct RedisAuthorityConfig {
    /// One `redis://` (or `rediss://`, over TLS) URL for a standalone server;
    /// seed nodes for a cluster.
    pub urls: Vec<String>,
    pub cluster: bool,
    /// Prepended to every key. May not contain braces, which delimit hash tags.
    pub key_prefix: String,
    pub database: u16,
    pub ttl: Duration,
    /// Bounds every call. Above zero and below a third of the TTL.
    pub call_timeout: Duration,
}

impl RedisAuthorityConfig {
    pub fn new(urls: Vec<String>) -> Self {
        Self {
            urls,
            cluster: false,
            key_prefix: "kabudachi:".into(),
            database: 0,
            ttl: DEFAULT_TTL,
            call_timeout: default_call_timeout(DEFAULT_TTL),
        }
    }

    /// Sets the TTL and the call timeout that goes with it (a tenth, at most 2 s).
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self.call_timeout = default_call_timeout(ttl);
        self
    }
}

fn default_call_timeout(ttl: Duration) -> Duration {
    Duration::from_ticks(ttl.as_ticks() / 10).min(MAX_DEFAULT_CALL_TIMEOUT)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("at least one server URL is needed, and exactly one without cluster mode")]
    Urls,
    #[error("server URL {0:?} is not a redis:// or rediss:// URL")]
    BadUrl(String),
    #[error("the key prefix may not contain '{{' or '}}'")]
    PrefixBraces,
    #[error("a cluster has only database 0")]
    ClusterDatabase,
    #[error("the call timeout must be above zero and below a third of the TTL")]
    CallTimeout,
}

impl RedisAuthorityConfig {
    /// Checks the config and returns each URL with its database path set.
    pub(crate) fn validated_urls(&self) -> Result<Vec<String>, ConfigError> {
        if self.urls.is_empty() || (!self.cluster && self.urls.len() != 1) {
            return Err(ConfigError::Urls);
        }
        let mut urls = Vec::with_capacity(self.urls.len());
        for text in &self.urls {
            let mut url = url::Url::parse(text)
                .ok()
                .filter(|url| matches!(url.scheme(), "redis" | "rediss") && url.has_host())
                .ok_or_else(|| ConfigError::BadUrl(text.clone()))?;
            url.set_path(&format!("/{}", self.database));
            urls.push(url.into());
        }
        if self.key_prefix.contains(['{', '}']) {
            return Err(ConfigError::PrefixBraces);
        }
        if self.cluster && self.database != 0 {
            return Err(ConfigError::ClusterDatabase);
        }
        let timeout = self.call_timeout.as_ticks();
        if timeout == 0 || timeout * 3 >= self.ttl.as_ticks() {
            return Err(ConfigError::CallTimeout);
        }
        Ok(urls)
    }
}
