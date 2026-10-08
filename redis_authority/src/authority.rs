use std::collections::BTreeMap;

use kabudachi_core::coordination_authority::{
    AuthorityError, CoordinationAuthority, LeaderHint, LiveRegistrations, ShardRecord,
};
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
use kabudachi_core::time::Duration;

use crate::config::{ConfigError, RedisAuthorityConfig};
use crate::connection::{Connections, Deadline, Failure, round_trip};
use crate::keys::{Keys, Part};
use crate::view::{
    View, encode_fence, encode_hint, encode_registration, encode_shard,
};

const MAX_ATTEMPTS: usize = 8;

pub struct RedisAuthority {
    config: RedisAuthorityConfig,
    connections: Connections,
}

enum Write {
    Set(Part, String),
    HSet(Part, String, String),
    HDel(Part, Vec<String>),
}

impl Write {
    fn queue(&self, pipe: &mut redis::Pipeline, keys: &Keys) {
        match self {
            Write::Set(part, value) => pipe.set(keys.key(*part), value).ignore(),
            Write::HSet(part, field, value) => pipe.hset(keys.key(*part), field, value).ignore(),
            Write::HDel(part, fields) => pipe.hdel(keys.key(*part), fields).ignore(),
        };
    }
}

struct Step<T> {
    writes: Vec<Write>,
    answer: Result<T, AuthorityError>,
}

impl<T> Step<T> {
    fn answer(answer: Result<T, AuthorityError>) -> Self {
        Self {
            writes: Vec::new(),
            answer,
        }
    }

    fn write(writes: Vec<Write>, answer: Result<T, AuthorityError>) -> Self {
        Self { writes, answer }
    }
}

impl RedisAuthority {
    /// Checks `config` and opens no connection: a worker that starts while
    /// the server is down gets `Unavailable` from its calls, as in any outage.
    pub fn connect(config: RedisAuthorityConfig) -> Result<Self, ConfigError> {
        let urls = config.validated_urls()?;
        let connections = Connections::new(&urls, config.cluster);
        Ok(Self {
            config,
            connections,
        })
    }

    fn ttl_ms(&self) -> u64 {
        self.config.ttl.as_ticks()
    }

    /// One call as one transaction: read `parts` and the sentinel under
    /// `WATCH`, decide in `body`, then write the sentinel's repair and the
    /// body's writes in one `MULTI`/`EXEC`. An `EXEC` that a concurrent
    /// writer aborted is retried.
    fn transaction<T>(
        &self,
        name: &ShardName,
        parts: &[Part],
        body: impl Fn(&View) -> Result<Step<T>, AuthorityError>,
    ) -> Result<T, AuthorityError> {
        let deadline = Deadline::after(std::time::Duration::from_millis(
            self.config.call_timeout.as_ticks(),
        ));
        let keys = Keys::new(&self.config.key_prefix, name);
        self.connections
            .with_connection(&keys, &deadline, |connection| {
                for _ in 0..MAX_ATTEMPTS {
                    deadline.apply(connection)?;
                    let view = View::read(connection, &deadline, &keys, parts)?;
                    let step = body(&view).unwrap_or_else(|error| Step::answer(Err(error)));
                    let repair = view.repair();
                    if step.writes.is_empty() && repair.is_empty() {
                        redis::cmd("UNWATCH")
                            .exec(connection)
                            .map_err(Failure::from)?;
                        return Ok(step.answer);
                    }
                    let mut pipe = redis::pipe();
                    pipe.atomic();
                    if !repair.is_empty() {
                        let hset = pipe.cmd("HSET").arg(keys.sentinel());
                        for (field, value) in &repair {
                            hset.arg(field).arg(value);
                        }
                        hset.ignore();
                    }
                    for write in &step.writes {
                        write.queue(&mut pipe, &keys);
                    }
                    // MULTI, each command's QUEUED, then EXEC: nil if a writer got in first.
                    let count = pipe.len() + 2;
                    let replies = round_trip(connection, &deadline, &pipe, count)?;
                    match replies.last() {
                        Some(redis::Value::Nil) => {}
                        Some(redis::Value::Array(_)) => return Ok(step.answer),
                        _ => return Err(Failure::Corrupt),
                    }
                }
                Err(Failure::Contended)
            })?
    }
}

impl CoordinationAuthority for RedisAuthority {
    fn ttl(&self) -> Duration {
        self.config.ttl
    }

    fn register(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
        worker_id: &WorkerId,
        address: &str,
    ) -> Result<Duration, AuthorityError> {
        let ttl_ms = self.ttl_ms();
        self.transaction(name, &[Part::Regs], |view| {
            let now = view.now_ms;
            let mut writes = Vec::new();
            let lapsed: Vec<String> = view
                .registrations()?
                .into_iter()
                .filter(|registration| registration.expires_ms <= now)
                .map(|registration| registration.worker.as_str().to_string())
                .collect();
            if !lapsed.is_empty() {
                writes.push(Write::HDel(Part::Regs, lapsed));
            }
            writes.push(Write::HSet(
                Part::Regs,
                worker_id.as_str().to_string(),
                encode_registration(shard_id, now + ttl_ms, address),
            ));
            Ok(Step::write(writes, Ok(self.config.ttl)))
        })
    }

    fn live_registrations(
        &self,
        name: &ShardName,
        shard_id: &ShardId,
    ) -> Result<LiveRegistrations, AuthorityError> {
        let ttl_ms = self.ttl_ms();
        let quiet = std::time::Duration::from_millis(ttl_ms);
        self.transaction(name, &[Part::Regs], |view| {
            let now = view.now_ms;
            let addresses: BTreeMap<WorkerId, String> = view
                .registrations()?
                .into_iter()
                .filter(|registration| {
                    registration.shard_id == *shard_id && now < registration.expires_ms
                })
                .map(|registration| (registration.worker, registration.address))
                .collect();
            let warmed_up = now.saturating_sub(view.available_ms()) >= ttl_ms
                && self.connections.quiet_for(quiet);
            Ok(Step::answer(Ok(LiveRegistrations::new(addresses, warmed_up))))
        })
    }

    fn read_shard(&self, name: &ShardName) -> Result<Option<ShardRecord>, AuthorityError> {
        self.transaction(name, &[Part::Shard], |view| Ok(Step::answer(Ok(view.shard()?))))
    }

    fn compare_and_swap_shard(
        &self,
        name: &ShardName,
        expected: Option<&ShardRecord>,
        new: &ShardRecord,
    ) -> Result<(), AuthorityError> {
        let value = encode_shard(new);
        self.transaction(name, &[Part::Shard], |view| {
            let current = view.shard()?;
            if current.as_ref() != expected {
                return Ok(Step::answer(Err(AuthorityError::ShardConflict { current })));
            }
            Ok(Step::write(
                vec![Write::Set(Part::Shard, value.clone())],
                Ok(()),
            ))
        })
    }

    fn acquire_fence(
        &self,
        name: &ShardName,
        holder: &WorkerId,
        record: &ShardRecord,
    ) -> Result<Duration, AuthorityError> {
        let ttl_ms = self.ttl_ms();
        self.transaction(name, &[Part::Shard, Part::Fence], |view| {
            let now = view.now_ms;
            let current = view.shard()?;
            if current.as_ref() != Some(record) {
                return Ok(Step::answer(Err(AuthorityError::ShardConflict { current })));
            }
            if let Some(fence) = view.fence()?
                && fence.holder != *holder
                && now < fence.expires_ms
            {
                return Ok(Step::answer(Err(AuthorityError::FenceHeld {
                    remaining: Duration::from_millis(fence.expires_ms - now),
                })));
            }
            // Fences taken before the data was lost are unknown here; all
            // have expired one TTL later.
            let warm_up_ends = view.created_ms() + ttl_ms;
            if now < warm_up_ends {
                return Ok(Step::answer(Err(AuthorityError::FenceHeld {
                    remaining: Duration::from_millis(warm_up_ends - now),
                })));
            }
            Ok(Step::write(
                vec![Write::Set(
                    Part::Fence,
                    encode_fence(holder, record, now + ttl_ms),
                )],
                Ok(self.config.ttl),
            ))
        })
    }

    fn publish_leader_hint(
        &self,
        name: &ShardName,
        hint: &LeaderHint,
    ) -> Result<(), AuthorityError> {
        let ttl_ms = self.ttl_ms();
        self.transaction(name, &[], |view| {
            Ok(Step::write(
                vec![Write::Set(
                    Part::Hint,
                    encode_hint(hint, view.now_ms + ttl_ms),
                )],
                Ok(()),
            ))
        })
    }

    fn read_leader_hint(&self, name: &ShardName) -> Result<Option<LeaderHint>, AuthorityError> {
        self.transaction(name, &[Part::Hint], |view| {
            let live = view
                .hint()?
                .filter(|(_, expires_ms)| view.now_ms < *expires_ms)
                .map(|(hint, _)| hint);
            Ok(Step::answer(Ok(live)))
        })
    }
}
