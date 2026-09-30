//! When a driven node re-crawls its peer routing (`Net::refresh_peer_routing`),
//! decided as a pure function of what the node shows and the time, so the
//! schedule is tested without a socket. `crate::driver::run_driver` feeds it
//! the node's view after each batch and fires the crawl it asks for.

use std::time::Duration as StdDuration;

use kabudachi_core::configuration::{Configuration, Generation};
use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::time::{Clock, Duration, Instant};

/// How many of its node's suspicion timeouts `crate::driver::run_driver`
/// waits, by default, between re-crawls of peer routing that nothing else
/// prompted (see `Net::refresh_peer_routing`). It only backs up the crawls a
/// change to the node's view of its shard starts (a new leader, a new
/// configuration, its own admission), finding a peer those missed; a crawl
/// costs a few `kad` queries, so a crawl every few suspicion timeouts is
/// cheap.
pub const DEFAULT_ROUTING_REFRESH_SUSPICIONS: u32 = 10;

/// The shortest period between routing crawls nothing else prompted (see
/// `DriverConfig::routing_refresh_period`), whatever the suspicion timeout: a
/// lone node may run with a suspicion timeout of zero.
pub const MIN_ROUTING_REFRESH_PERIOD: StdDuration = StdDuration::from_secs(1);

/// A changed view is crawled once it has held still for this fraction of a
/// suspicion timeout.
const ROUTING_SETTLE_DIVISOR: u64 = 4;

/// What the node shows that a routing crawl answers to: whom it names leader,
/// the configuration generation it holds, and whether it is admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardView {
    pub(crate) leader: Option<WorkerId>,
    pub(crate) generation: Option<Generation>,
    pub(crate) admitted: bool,
}

impl ShardView {
    pub(crate) fn of<C: Clock>(node: &WorkerNode<C>) -> Self {
        ShardView {
            leader: node.known_leader().map(|(leader, _)| leader),
            generation: node.configuration().map(Configuration::generation),
            admitted: node.admission().is_some(),
        }
    }
}

/// One decision: crawl now or not, and when the next is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Refresh {
    pub(crate) crawl: bool,
    pub(crate) next_due: Instant,
}

/// When [`run_driver`](crate::driver::run_driver) re-crawls its node's peer
/// routing, so that the workers of a shard stay connected to one another and
/// not only to their leader.
///
/// A worker's JOIN connects it to its seed and its leader alone, and `kad`'s
/// own crawl on that first connection finds only the peers its leader knew
/// by then: a burst joining through the leader would be left a star, which
/// no roll call crosses once the leader is gone. So the driver crawls again
/// once the node's view of its shard has changed and then held still for a
/// quarter of a suspicion timeout, and, failing that, every
/// [`DEFAULT_ROUTING_REFRESH_SUSPICIONS`] suspicion timeouts.
///
/// Waiting for the view to settle bounds the cost. A crawl's first run
/// connects the node to every peer it finds, a burst of connection
/// handshakes; a burst of joiners that each crawled at every change of an
/// admission in flight would spend them while the batch commits, and slow it
/// by whole seconds on one host. Settled, each node crawls once after the
/// burst, and a view that never settles still crawls within a period of its
/// first unserved change. The routing table is never read as membership (see
/// `crate::swarm`).
pub(crate) struct RoutingRefresh {
    /// The view last seen.
    seen: Option<ShardView>,
    /// When the view last changed.
    last_change: Instant,
    /// When the view first changed since the last crawl; `None` while no
    /// change waits for one.
    first_unserved: Option<Instant>,
    last_crawl: Option<Instant>,
    settle: Duration,
    period: Duration,
}

impl RoutingRefresh {
    /// `period` is `DriverConfig::routing_refresh_period`; `None` means
    /// [`DEFAULT_ROUTING_REFRESH_SUSPICIONS`] suspicion timeouts. Either way it
    /// is at least [`MIN_ROUTING_REFRESH_PERIOD`]. The settle time is a
    /// quarter suspicion timeout.
    pub(crate) fn new(
        suspect_timeout: Duration,
        period: Option<StdDuration>,
        now: Instant,
    ) -> Self {
        let suspect = suspect_timeout.as_ticks();
        let period_millis = match period {
            Some(period) => u64::try_from(period.as_millis()).unwrap_or(u64::MAX),
            // A tick is a millisecond.
            None => suspect.saturating_mul(u64::from(DEFAULT_ROUTING_REFRESH_SUSPICIONS)),
        };
        let min_millis = u64::try_from(MIN_ROUTING_REFRESH_PERIOD.as_millis()).unwrap_or(u64::MAX);
        RoutingRefresh {
            seen: None,
            last_change: now,
            first_unserved: None,
            last_crawl: None,
            settle: Duration::from_ticks(suspect / ROUTING_SETTLE_DIVISOR),
            period: Duration::from_millis(period_millis.max(min_millis)),
        }
    }

    /// Notes `view` at `now` (a change restarts the settle wait), and says
    /// whether to crawl. Pure: the caller fires the crawl.
    pub(crate) fn decide(&mut self, view: &ShardView, now: Instant) -> Refresh {
        if self.seen.as_ref() != Some(view) {
            self.seen = Some(view.clone());
            self.last_change = now;
            self.first_unserved.get_or_insert(now);
        }
        let due = match self.first_unserved {
            Some(first) => std::cmp::min(self.last_change + self.settle, first + self.period),
            None => self.last_crawl.map_or(now, |last| last + self.period),
        };
        if due > now {
            return Refresh {
                crawl: false,
                next_due: due,
            };
        }
        self.last_crawl = Some(now);
        self.first_unserved = None;
        Refresh {
            crawl: true,
            next_due: now + self.period,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(leader: &str) -> ShardView {
        ShardView {
            leader: Some(WorkerId::new(leader)),
            generation: None,
            admitted: true,
        }
    }

    fn t0() -> Instant {
        Instant::at(1_000)
    }

    fn at(millis: u64) -> Instant {
        t0() + Duration::from_millis(millis)
    }

    fn schedule(suspect_millis: u64, period: Option<StdDuration>) -> RoutingRefresh {
        RoutingRefresh::new(Duration::from_millis(suspect_millis), period, t0())
    }

    /// Runs `rows` of `(at, view, crawl, next_due)` through `schedule`.
    fn assert_rows(schedule: &mut RoutingRefresh, rows: &[(u64, &ShardView, bool, u64)]) {
        for &(when, view, crawl, next_due) in rows {
            let want = Refresh {
                crawl,
                next_due: at(next_due),
            };
            assert_eq!(schedule.decide(view, at(when)), want, "at {when} ms");
        }
    }

    #[test]
    fn a_changed_view_is_crawled_once_it_has_held_still_for_a_quarter_suspicion() {
        let (a, b) = (view("a"), view("b"));
        assert_rows(
            &mut schedule(200, None),
            &[
                (0, &a, false, 50),
                (30, &b, false, 80),
                (79, &b, false, 80),
                (80, &b, true, 2080),
            ],
        );
    }

    #[test]
    fn a_view_that_never_settles_is_crawled_within_a_period_of_its_first_change() {
        let (a, b) = (view("a"), view("b"));
        let mut schedule = schedule(200, None);
        for step in 0..50 {
            let shown = if step % 2 == 0 { &a } else { &b };
            let refresh = schedule.decide(shown, at(step * 40));
            assert!(!refresh.crawl, "crawled early at {} ms", step * 40);
        }
        assert_rows(&mut schedule, &[(2000, &a, true, 4000)]);
    }

    #[test]
    fn with_nothing_changing_it_crawls_once_a_period() {
        let a = view("a");
        assert_rows(
            &mut schedule(200, None),
            &[
                (0, &a, false, 50),
                (50, &a, true, 2050),
                (1000, &a, false, 2050),
                (2050, &a, true, 4050),
            ],
        );
    }

    #[test]
    fn the_period_defaults_to_ten_suspicions_and_never_drops_under_a_second() {
        let a = view("a");
        assert_rows(
            &mut schedule(200, None),
            &[(0, &a, false, 50), (50, &a, true, 2050)],
        );
        assert_rows(
            &mut schedule(200, Some(StdDuration::from_millis(10))),
            &[(0, &a, false, 50), (50, &a, true, 1050)],
        );
        // A lone node may run with no suspicion timeout: crawl at once, then
        // once a second, never a spin.
        assert_rows(&mut schedule(0, None), &[(0, &a, true, 1000)]);
    }
}
