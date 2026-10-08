//! What one transaction attempt read: the server's clock and run id, the
//! shard name's sentinel, and the values of the keys the call needs.

use std::collections::HashMap;

use kabudachi_core::coordination_authority::{AuthorityError, LeaderHint, RecoveryEpoch, ShardRecord};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use redis::Connection;

use crate::connection::Failure;
use crate::keys::{Keys, Part, join, split};

/// When the shard name's data began and last became available, and on which
/// server run. A missing sentinel means the server holds nothing for the name.
#[derive(Debug, Clone)]
pub(crate) struct Sentinel {
    created_ms: u64,
    available_ms: u64,
    run_id: String,
}

enum Raw {
    Text(Option<String>),
    Hash(HashMap<String, String>),
}

pub(crate) struct Fence {
    pub(crate) holder: WorkerId,
    pub(crate) expires_ms: u64,
}

pub(crate) struct Registration {
    pub(crate) worker: WorkerId,
    pub(crate) shard_id: ShardId,
    pub(crate) expires_ms: u64,
    pub(crate) address: String,
}

pub(crate) struct View {
    pub(crate) now_ms: u64,
    run_id: String,
    sentinel: Option<Sentinel>,
    values: Vec<(Part, Raw)>,
    keys: Keys,
}

impl View {
    /// One round trip: `WATCH` the sentinel and the parts, then read the
    /// server's clock and run id, the sentinel and the parts.
    pub(crate) fn read(
        connection: &mut Connection,
        keys: &Keys,
        parts: &[Part],
    ) -> Result<Self, Failure> {
        let mut pipe = redis::pipe();
        let mut watch = redis::cmd("WATCH");
        watch.arg(keys.sentinel());
        for part in parts {
            watch.arg(keys.key(*part));
        }
        pipe.add_command(watch).ignore();
        pipe.cmd("TIME");
        pipe.cmd("INFO").arg("server");
        pipe.cmd("HGETALL").arg(keys.sentinel());
        for part in parts {
            match part {
                Part::Regs => pipe.cmd("HGETALL").arg(keys.key(*part)),
                _ => pipe.cmd("GET").arg(keys.key(*part)),
            };
        }
        let mut replies = pipe.query::<Vec<redis::Value>>(connection)?.into_iter();
        let mut next = || replies.next().ok_or(Failure::Corrupt);
        let time: Vec<String> = redis::from_redis_value(next()?).map_err(|_| Failure::Corrupt)?;
        let info: String = redis::from_redis_value(next()?).map_err(|_| Failure::Corrupt)?;
        let sentinel: HashMap<String, String> =
            redis::from_redis_value(next()?).map_err(|_| Failure::Corrupt)?;
        let [seconds, micros] = time.as_slice() else {
            return Err(Failure::Corrupt);
        };
        let now_ms = seconds.parse::<u64>().map_err(|_| Failure::Corrupt)? * 1000
            + micros.parse::<u64>().map_err(|_| Failure::Corrupt)? / 1000;
        let run_id = info
            .lines()
            .find_map(|line| line.strip_prefix("run_id:"))
            .ok_or(Failure::Corrupt)?
            .trim()
            .to_string();
        let mut values = Vec::with_capacity(parts.len());
        for part in parts {
            let raw = match part {
                Part::Regs => Raw::Hash(
                    redis::from_redis_value(next()?).map_err(|_| Failure::Corrupt)?,
                ),
                _ => Raw::Text(redis::from_redis_value(next()?).map_err(|_| Failure::Corrupt)?),
            };
            values.push((*part, raw));
        }
        Ok(Self {
            now_ms,
            run_id,
            sentinel: decode_sentinel(&sentinel),
            values,
            keys: keys.clone(),
        })
    }

    /// The sentinel fields this call must write. A missing sentinel restarts
    /// both waits; one from another server run (a restart that kept its
    /// data) restarts only the count's.
    pub(crate) fn repair(&self) -> Vec<(&'static str, String)> {
        match &self.sentinel {
            None => vec![
                ("created_ms", self.now_ms.to_string()),
                ("available_ms", self.now_ms.to_string()),
                ("run_id", self.run_id.clone()),
            ],
            Some(sentinel) if sentinel.run_id != self.run_id => vec![
                ("available_ms", self.now_ms.to_string()),
                ("run_id", self.run_id.clone()),
            ],
            Some(_) => Vec::new(),
        }
    }

    /// When the name's data began, as the sentinel will read after the repair.
    pub(crate) fn created_ms(&self) -> u64 {
        self.sentinel
            .as_ref()
            .map_or(self.now_ms, |sentinel| sentinel.created_ms)
    }

    /// When the name last became available, as the sentinel will read after the repair.
    pub(crate) fn available_ms(&self) -> u64 {
        match &self.sentinel {
            Some(sentinel) if sentinel.run_id == self.run_id => sentinel.available_ms,
            _ => self.now_ms,
        }
    }

    pub(crate) fn shard(&self) -> Result<Option<ShardRecord>, AuthorityError> {
        self.text(Part::Shard, |fields| {
            let [shard_id, number, lineage] = fields[..] else {
                return None;
            };
            Some(ShardRecord {
                shard_id: ShardId::new(shard_id),
                recovery_epoch: RecoveryEpoch::new(number.parse().ok()?, lineage.parse().ok()?),
            })
        })
    }

    pub(crate) fn fence(&self) -> Result<Option<Fence>, AuthorityError> {
        self.text(Part::Fence, |fields| {
            let [holder, _shard_id, _number, _lineage, expires_ms] = fields[..] else {
                return None;
            };
            Some(Fence {
                holder: WorkerId::new(holder),
                expires_ms: expires_ms.parse().ok()?,
            })
        })
    }

    pub(crate) fn hint(&self) -> Result<Option<(LeaderHint, u64)>, AuthorityError> {
        self.text(Part::Hint, |fields| {
            let [shard_id, leader, address, number, lineage, term, expires_ms] = fields[..] else {
                return None;
            };
            let hint = LeaderHint {
                shard_id: ShardId::new(shard_id),
                leader: WorkerId::new(leader),
                address: address.to_string(),
                recovery_epoch: RecoveryEpoch::new(number.parse().ok()?, lineage.parse().ok()?),
                term: term.parse().ok()?,
            };
            Some((hint, expires_ms.parse().ok()?))
        })
    }

    pub(crate) fn registrations(&self) -> Result<Vec<Registration>, AuthorityError> {
        let Some((_, Raw::Hash(hash))) = self.values.iter().find(|(part, _)| *part == Part::Regs)
        else {
            return Err(AuthorityError::Unavailable);
        };
        let mut registrations = Vec::with_capacity(hash.len());
        for (worker, value) in hash {
            let decoded = split(value).and_then(|fields| {
                let [shard_id, expires_ms, address] = fields[..] else {
                    return None;
                };
                Some(Registration {
                    worker: WorkerId::new(worker.as_str()),
                    shard_id: ShardId::new(shard_id),
                    expires_ms: expires_ms.parse().ok()?,
                    address: address.to_string(),
                })
            });
            match decoded {
                Some(registration) => registrations.push(registration),
                None => return Err(self.undecodable(Part::Regs)),
            }
        }
        Ok(registrations)
    }

    fn text<T>(
        &self,
        part: Part,
        decode: impl FnOnce(Vec<&str>) -> Option<T>,
    ) -> Result<Option<T>, AuthorityError> {
        let Some((_, Raw::Text(text))) = self.values.iter().find(|(p, _)| *p == part) else {
            return Err(AuthorityError::Unavailable);
        };
        let Some(text) = text else {
            return Ok(None);
        };
        match split(text).and_then(decode) {
            Some(value) => Ok(Some(value)),
            None => Err(self.undecodable(part)),
        }
    }

    fn undecodable(&self, part: Part) -> AuthorityError {
        tracing::warn!(key = self.keys.key(part), "a stored value does not decode");
        AuthorityError::Unavailable
    }
}

fn decode_sentinel(hash: &HashMap<String, String>) -> Option<Sentinel> {
    Some(Sentinel {
        created_ms: hash.get("created_ms")?.parse().ok()?,
        available_ms: hash.get("available_ms")?.parse().ok()?,
        run_id: hash.get("run_id")?.clone(),
    })
}

pub(crate) fn encode_shard(record: &ShardRecord) -> String {
    join(&[
        record.shard_id.as_str(),
        &record.recovery_epoch.number.to_string(),
        &record.recovery_epoch.lineage.to_string(),
    ])
}

pub(crate) fn encode_fence(holder: &WorkerId, record: &ShardRecord, expires_ms: u64) -> String {
    join(&[
        holder.as_str(),
        record.shard_id.as_str(),
        &record.recovery_epoch.number.to_string(),
        &record.recovery_epoch.lineage.to_string(),
        &expires_ms.to_string(),
    ])
}

pub(crate) fn encode_registration(shard_id: &ShardId, expires_ms: u64, address: &str) -> String {
    join(&[shard_id.as_str(), &expires_ms.to_string(), address])
}

pub(crate) fn encode_hint(hint: &LeaderHint, expires_ms: u64) -> String {
    join(&[
        hint.shard_id.as_str(),
        hint.leader.as_str(),
        &hint.address,
        &hint.recovery_epoch.number.to_string(),
        &hint.recovery_epoch.lineage.to_string(),
        &hint.term.to_string(),
        &expires_ms.to_string(),
    ])
}
