//! SlowDown on a networked worker: what the shard's leader last said about
//! it, kept until a poll hears the leader no longer says so, and this
//! process's own payloads in flight when they are past their soft share.

use std::sync::Arc;

use super::seam::Shared;
use crate::work::PyEvent;

/// Usage at or under this percent of the soft limit clears a local SlowDown
/// (the scheduler's own hysteresis).
const CLEARS_AT_PERCENT: u128 = 80;

/// What the leader last said about SlowDown, whether a poll is asking, and
/// whether this process's payloads in flight hold submitters back.
#[derive(Debug, Default)]
pub(super) struct SlowDownWatch {
    /// The leader said submitters are to wait, and no poll has heard it clear.
    raised: bool,
    /// A poll is asking the leader.
    polling: bool,
    /// This process's payloads in flight are past their soft share and have
    /// not fallen to 80% of it.
    local: bool,
}

impl SlowDownWatch {
    /// Whether submitters are to wait, given this process's payloads in
    /// flight: `in_use` bytes against `soft`, their soft share (`None`
    /// without limits).
    fn waiting(&mut self, in_use: u64, soft: Option<u64>) -> bool {
        self.local = match soft {
            None => false,
            Some(soft) if !self.local => in_use > soft,
            Some(soft) => u128::from(in_use) * 100 > u128::from(soft) * CLEARS_AT_PERCENT,
        };
        self.raised || self.local
    }
}

impl Shared {
    /// Whether the leader or this process's own payloads in flight ask
    /// submitters to wait.
    pub(super) fn slow_down(&self) -> bool {
        let memory = self.net.payload_memory();
        self.slow_down
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waiting(memory.in_use(), memory.soft_limit())
    }
}

/// Records what an accepted answer said. A raised flag raises `SLOW_DOWN` once
/// and starts the poll if none runs; a cleared one is left to the poll. Call it
/// from within the worker's Tokio runtime.
pub(super) fn heard_slow_down(shared: &Arc<Shared>, raised: bool) {
    if !raised {
        return;
    }
    let (announce, poll) = {
        let mut watch = shared
            .slow_down
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let announce = !watch.raised;
        let poll = !watch.polling;
        watch.raised = true;
        watch.polling = true;
        // Pushed under the watch lock, so the events follow the flag's order.
        // Lock order: watch, then seam; nothing takes the seam and then the watch.
        if announce {
            shared.seam().events.push_back(PyEvent::slow_down(true));
        }
        (announce, poll)
    };
    if announce {
        shared.wake();
    }
    if poll {
        tokio::spawn(poll_slow_down(Arc::clone(shared)));
    }
}

/// Asks the leader every heartbeat interval until it no longer asks
/// submitters to wait, then raises `SLOW_DOWN` cleared. A leader lost
/// meanwhile is not an answer: the next one is asked. A worker that has left
/// its shard has no leader to wait for, so that clears it too.
async fn poll_slow_down(shared: Arc<Shared>) {
    loop {
        tokio::time::sleep(shared.retry.ask_again_after).await;
        if shared.net.query_slow_down(shared.retry).await != Some(true) {
            break;
        }
    }
    {
        let mut watch = shared
            .slow_down
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        watch.raised = false;
        watch.polling = false;
        // Pushed under the watch lock: a raise that lands after this sees the
        // cleared flag and queues its event after this one.
        shared.seam().events.push_back(PyEvent::slow_down(false));
    }
    shared.wake();
}
