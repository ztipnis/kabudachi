//! Connections to the server or cluster, each call bounded by its deadline.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use kabudachi_core::coordination_authority::AuthorityError;
use redis::{Connection, RedisError};

use crate::keys::Keys;

const RETRY_PAUSE: Duration = Duration::from_millis(20);

/// The instant by which a call must have answered.
pub(crate) struct Deadline(Instant);

impl Deadline {
    pub(crate) fn after(timeout: Duration) -> Self {
        Self(Instant::now() + timeout)
    }

    pub(crate) fn left(&self) -> Result<Duration, Failure> {
        let left = self.0.saturating_duration_since(Instant::now());
        if left.is_zero() {
            Err(Failure::TimedOut)
        } else {
            Ok(left)
        }
    }

    /// Bounds the connection's next round trip by the time left.
    pub(crate) fn apply(&self, connection: &Connection) -> Result<(), Failure> {
        let left = self.left()?;
        connection.set_read_timeout(Some(left))?;
        connection.set_write_timeout(Some(left))?;
        Ok(())
    }
}

/// Sends the pipeline and reads `replies` replies, each read bounded by what
/// is left of the deadline. The first failure returns at once: redis-rs's
/// own pipeline would go on reading, a timeout per outstanding reply, and
/// overrun the deadline. The connection is then out of step: drop it.
pub(crate) fn round_trip(
    connection: &mut Connection,
    deadline: &Deadline,
    pipe: &redis::Pipeline,
    replies: usize,
) -> Result<Vec<redis::Value>, Failure> {
    deadline.apply(connection)?;
    connection.send_packed_command(&pipe.get_packed_pipeline())?;
    let mut values = Vec::with_capacity(replies);
    for _ in 0..replies {
        deadline.apply(connection)?;
        match connection.recv_response()? {
            redis::Value::ServerError(error) => return Err(RedisError::from(error).into()),
            value => values.push(value),
        }
    }
    Ok(values)
}

#[derive(Debug)]
pub(crate) enum Failure {
    /// The slot lives on the node at this address.
    Moved(String),
    /// A cluster in transition: ask again shortly.
    Retry,
    Io,
    /// The first round trip on a pooled connection failed with an I/O
    /// error: the server may have restarted since the connection was pooled.
    Stale,
    TimedOut,
    /// Every attempt of a transaction was overtaken by a writer.
    Contended,
    /// The server refused a command; the error is logged where it arrives.
    Server,
    /// A reply that makes no sense.
    Corrupt,
}

impl From<RedisError> for Failure {
    fn from(error: RedisError) -> Self {
        match error.code() {
            Some("MOVED") => {
                if let Some((address, _slot)) = error.redirect_node() {
                    return Failure::Moved(address.to_string());
                }
            }
            Some("ASK" | "TRYAGAIN" | "CLUSTERDOWN") => return Failure::Retry,
            _ => {}
        }
        if error.is_timeout() {
            Failure::TimedOut
        } else if error.is_io_error() || error.is_connection_dropped() {
            Failure::Io
        } else {
            tracing::warn!(%error, "the server refused a command");
            Failure::Server
        }
    }
}

/// This client's own view of the server's availability: whether its last
/// call failed, and since when the calls have worked again.
#[derive(Default)]
struct Outage {
    failing: bool,
    recovered_at: Option<Instant>,
}

pub(crate) struct Connections {
    seeds: Vec<url::Url>,
    cluster: bool,
    pool: Mutex<HashMap<String, Vec<Connection>>>,
    owners: Mutex<HashMap<u16, String>>,
    outage: Mutex<Outage>,
}

impl Connections {
    pub(crate) fn new(urls: &[String], cluster: bool) -> Self {
        Self {
            seeds: urls
                .iter()
                .map(|url| url::Url::parse(url).expect("validated URL"))
                .collect(),
            cluster,
            pool: Mutex::default(),
            owners: Mutex::default(),
            outage: Mutex::default(),
        }
    }

    /// Whether this client has seen the server answer for at least `ttl`
    /// without a failure in between.
    pub(crate) fn quiet_for(&self, ttl: Duration) -> bool {
        let outage = self.outage.lock().unwrap();
        !outage.failing && outage.recovered_at.is_none_or(|at| at.elapsed() >= ttl)
    }

    fn failed(&self) {
        self.outage.lock().unwrap().failing = true;
    }

    fn succeeded(&self) {
        let mut outage = self.outage.lock().unwrap();
        if outage.failing {
            outage.failing = false;
            outage.recovered_at = Some(Instant::now());
        }
    }

    /// Runs `run` on a connection to the node that owns `keys`, following
    /// moves and pauses of a cluster, within `deadline`.
    pub(crate) fn with_connection<T>(
        &self,
        keys: &Keys,
        deadline: &Deadline,
        mut run: impl FnMut(&mut Connection) -> Result<T, Failure>,
    ) -> Result<T, AuthorityError> {
        let mut retried_stale = false;
        let mut node = self.owner(keys, deadline)?;
        loop {
            let (mut connection, pooled) = self.take(&node, deadline)?;
            match run(&mut connection) {
                Ok(value) => {
                    self.put(&node, connection);
                    self.succeeded();
                    return Ok(value);
                }
                // The connection may hold a half-run transaction: drop it.
                Err(Failure::Moved(address)) => node = self.moved(keys, &node, address),
                Err(Failure::Retry) => self.sleep_at_most(RETRY_PAUSE, deadline)?,
                Err(Failure::Stale) if pooled && !retried_stale => {
                    // A server that restarted closed every pooled connection.
                    retried_stale = true;
                    self.pool.lock().unwrap().remove(&node);
                }
                Err(Failure::Contended) => return Err(AuthorityError::Unavailable),
                Err(failure) => {
                    // A fresh connection's first failure is no stale one.
                    let failure = if matches!(failure, Failure::Stale) {
                        Failure::Io
                    } else {
                        failure
                    };
                    tracing::warn!(?failure, node, "authority call failed");
                    self.failed();
                    return Err(AuthorityError::Unavailable);
                }
            }
        }
    }

    fn sleep_at_most(&self, pause: Duration, deadline: &Deadline) -> Result<(), AuthorityError> {
        let left = deadline.left().map_err(|_| self.timed_out())?;
        std::thread::sleep(pause.min(left));
        Ok(())
    }

    fn timed_out(&self) -> AuthorityError {
        tracing::warn!("authority call timed out");
        self.failed();
        AuthorityError::Unavailable
    }

    fn take(&self, node: &str, deadline: &Deadline) -> Result<(Connection, bool), AuthorityError> {
        if let Some(connection) = self.pool.lock().unwrap().get_mut(node).and_then(Vec::pop) {
            return Ok((connection, true));
        }
        let left = deadline.left().map_err(|_| self.timed_out())?;
        let connection = self
            .connect(node, left)
            .map_err(|error| {
                tracing::warn!(%error, node, "cannot connect to the authority");
                self.failed();
                AuthorityError::Unavailable
            })?;
        Ok((connection, false))
    }

    fn put(&self, node: &str, connection: Connection) {
        self.pool
            .lock()
            .unwrap()
            .entry(node.to_string())
            .or_default()
            .push(connection);
    }

    fn connect(&self, node: &str, timeout: Duration) -> redis::RedisResult<Connection> {
        let invalid = |what: &str| {
            RedisError::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{what} in node address {node:?}"),
            ))
        };
        let (host, port) = node.rsplit_once(':').ok_or_else(|| invalid("no port"))?;
        let port: u16 = port.parse().map_err(|_| invalid("bad port"))?;
        let mut url = self.seeds[0].clone();
        // A bracketed IPv6 host is accepted as it is.
        url.set_host(Some(host)).map_err(|_| invalid("bad host"))?;
        url.set_port(Some(port)).map_err(|_| invalid("bad port"))?;
        redis::Client::open(url.as_str())?.get_connection_with_timeout(timeout)
    }

    /// The seed's own address, which a node reporting itself without a host
    /// is reached at.
    fn seed_address(&self) -> String {
        let seed = &self.seeds[0];
        format!(
            "{}:{}",
            seed.host_str().unwrap_or("127.0.0.1"),
            seed.port().unwrap_or(6379)
        )
    }

    fn owner(&self, keys: &Keys, deadline: &Deadline) -> Result<String, AuthorityError> {
        if !self.cluster {
            return Ok(self.seed_address());
        }
        let slot = keys.slot();
        if let Some(node) = self.owners.lock().unwrap().get(&slot) {
            return Ok(node.clone());
        }
        self.refresh_owners(deadline)?;
        self.owners
            .lock()
            .unwrap()
            .get(&slot)
            .cloned()
            .ok_or_else(|| {
                tracing::warn!(slot, "no cluster node owns the slot");
                self.failed();
                AuthorityError::Unavailable
            })
    }

    /// Reads the slot map from the first seed that answers.
    fn refresh_owners(&self, deadline: &Deadline) -> Result<(), AuthorityError> {
        let mut last = None;
        for index in 0..self.seeds.len() {
            let left = deadline.left().map_err(|_| self.timed_out())?;
            let seed = &self.seeds[index];
            let attempt = redis::Client::open(seed.as_str())
                .and_then(|client| client.get_connection_with_timeout(left))
                .and_then(|mut connection| {
                    deadline.apply(&connection).map_err(|_| io_timeout())?;
                    redis::cmd("CLUSTER")
                        .arg("SLOTS")
                        .query::<Vec<Vec<redis::Value>>>(&mut connection)
                });
            match attempt {
                Ok(ranges) => {
                    let owners = parse_slots(&ranges, seed.host_str().unwrap_or("127.0.0.1"));
                    self.owners.lock().unwrap().extend(owners);
                    return Ok(());
                }
                Err(error) => last = Some(error),
            }
        }
        tracing::warn!(error = ?last, "cannot read the cluster's slots");
        self.failed();
        Err(AuthorityError::Unavailable)
    }

    /// Records that the slot of `keys` now lives at `address`, which the node
    /// `from` gave; an address without a host means `from`'s own host.
    fn moved(&self, keys: &Keys, from: &str, address: String) -> String {
        let address = match address.strip_prefix(':') {
            Some(port) => {
                let host = from.rsplit_once(':').map_or(from, |(host, _)| host);
                format!("{host}:{port}")
            }
            None => address,
        };
        self.owners.lock().unwrap().insert(keys.slot(), address.clone());
        address
    }
}

fn io_timeout() -> RedisError {
    RedisError::from(std::io::Error::new(std::io::ErrorKind::TimedOut, "deadline passed"))
}

/// Each range's master, as `host:port`. A node that reports an empty host is
/// the one the client asked: `seed_host`.
fn parse_slots(ranges: &[Vec<redis::Value>], seed_host: &str) -> Vec<(u16, String)> {
    let mut owners = Vec::new();
    for range in ranges {
        let [start, end, master, ..] = range.as_slice() else {
            continue;
        };
        let (Ok(start), Ok(end), Ok(master)) = (
            redis::from_redis_value::<u16>(start.clone()),
            redis::from_redis_value::<u16>(end.clone()),
            redis::from_redis_value::<Vec<redis::Value>>(master.clone()),
        ) else {
            continue;
        };
        let [host, port, ..] = master.as_slice() else {
            continue;
        };
        let (Ok(host), Ok(port)) = (
            redis::from_redis_value::<String>(host.clone()),
            redis::from_redis_value::<u16>(port.clone()),
        ) else {
            continue;
        };
        let host = if host.is_empty() { seed_host } else { &host };
        for slot in start..=end {
            owners.push((slot, format!("{host}:{port}")));
        }
    }
    owners
}
