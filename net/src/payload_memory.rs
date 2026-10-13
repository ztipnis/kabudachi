//! The blob payloads a process holds whole while they cross the network: an
//! input it is storing, a claimed run's input it fetched, a result it is
//! storing or fetching. They count against the process's memory limits as the
//! scheduler's pending inputs do: one that does not fit now waits or is
//! declined, and only one that alone is past the hard limit is refused.
//! Inline payloads, bounded by one message, are not counted.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use kabudachi_core::scheduler::MemoryLimits;
use tokio::sync::Notify;

/// The failure kind of a run whose input alone is past this worker's hard
/// limit.
pub const PAYLOAD_TOO_LARGE_KIND: &str = "PayloadTooLarge";

/// A payload that alone is past the hard memory limit: it can never be held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a payload of {size} bytes is past the hard memory limit of {limit}")]
pub struct TooLarge {
    pub size: u64,
    pub limit: u64,
}

/// How a process splits its memory limits between the payloads it holds in
/// flight and the blobs it stores for its shard: half of each limit for the
/// payloads in flight, and the rest of the hard limit for the store (so the
/// two shares add up to the hard limit). Without limits, neither is bounded.
pub fn payload_shares(limits: Option<MemoryLimits>) -> (Option<MemoryLimits>, Option<u64>) {
    match limits {
        Some(MemoryLimits { soft, hard }) => (
            Some(MemoryLimits { soft: soft / 2, hard: hard / 2 }),
            Some(hard - hard / 2),
        ),
        None => (None, None),
    }
}

#[derive(Default)]
struct Ledger {
    limits: Option<MemoryLimits>,
    in_use: u64,
}

/// Blob payloads held whole in this process while they cross the network,
/// counted against its limits.
pub struct PayloadMemory {
    ledger: Mutex<Ledger>,
    released: Notify,
}

/// Bytes counted in use until it is dropped.
#[must_use]
pub struct Taken {
    memory: Arc<PayloadMemory>,
    bytes: u64,
}

impl Drop for Taken {
    fn drop(&mut self) {
        self.memory.lock().in_use -= self.bytes;
        self.memory.released.notify_waiters();
    }
}

impl PayloadMemory {
    /// A ledger held to `limits`, or to none with `None`.
    pub fn new(limits: Option<MemoryLimits>) -> Arc<Self> {
        Arc::new(PayloadMemory {
            ledger: Mutex::new(Ledger { limits, in_use: 0 }),
            released: Notify::new(),
        })
    }

    /// Holds the ledger to `limits` from now on, or to none with `None`.
    /// What is counted stays counted.
    pub fn set_limits(&self, limits: Option<MemoryLimits>) {
        self.lock().limits = limits;
        self.released.notify_waiters();
    }

    /// The bytes counted in use.
    pub fn in_use(&self) -> u64 {
        self.lock().in_use
    }

    /// The hard limit: the most one payload can ever be counted at. `None`
    /// with no limits.
    pub fn hard_limit(&self) -> Option<u64> {
        self.lock().limits.map(|limits| limits.hard)
    }

    /// The soft limit: past it, a process holding these payloads asks its
    /// own bulk submission to wait. `None` with no limits.
    pub fn soft_limit(&self) -> Option<u64> {
        self.lock().limits.map(|limits| limits.soft)
    }

    /// The hard limit less what is in use; `None` with no limits.
    pub fn room(&self) -> Option<u64> {
        let ledger = self.lock();
        ledger.limits.map(|limits| limits.hard.saturating_sub(ledger.in_use))
    }

    /// Counts `bytes` in use if they fit now: `Ok(None)` if they do not.
    /// `TooLarge` if they alone are past the hard limit.
    pub fn try_take(self: &Arc<Self>, bytes: u64) -> Result<Option<Taken>, TooLarge> {
        let mut ledger = self.lock();
        if let Some(limits) = ledger.limits {
            if bytes > limits.hard {
                return Err(TooLarge { size: bytes, limit: limits.hard });
            }
            if ledger.in_use.saturating_add(bytes) > limits.hard {
                return Ok(None);
            }
        }
        ledger.in_use += bytes;
        Ok(Some(Taken { memory: Arc::clone(self), bytes }))
    }

    /// Counts `bytes` in use once they fit, waiting for others to be
    /// released until then. `TooLarge` if they alone are past the hard limit.
    pub async fn take_when_room(self: &Arc<Self>, bytes: u64) -> Result<Taken, TooLarge> {
        loop {
            // Registered before the check, so a release between the check and
            // the wait is not missed.
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if let Some(taken) = self.try_take(bytes)? {
                return Ok(taken);
            }
            released.await;
        }
    }

    /// Counts `bytes` already in memory in use, even past the hard limit.
    /// `TooLarge` if they alone are past it.
    pub fn take_anyway(self: &Arc<Self>, bytes: u64) -> Result<Taken, TooLarge> {
        let mut ledger = self.lock();
        if let Some(limits) = ledger.limits
            && bytes > limits.hard
        {
            return Err(TooLarge { size: bytes, limit: limits.hard });
        }
        ledger.in_use += bytes;
        Ok(Taken { memory: Arc::clone(self), bytes })
    }

    fn lock(&self) -> MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
