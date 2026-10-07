//! A new leader's asking as the driver runs it: the questions to its shard
//! about what each worker holds, and the lookups of the records it lacks.
//! What the answers decide is core's `OfficeReconciliation`.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::{ElectionCertificate, TaskRecord};
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::ReconcileRequest;
use kabudachi_core::reconcile::{Cursor, DriftWatch, ReconcileTerm, ReportPage, wire};
use kabudachi_core::task_record::OfficeReconciliation;
use kabudachi_core::time::{Clock, Duration, Instant};
use libp2p::futures::StreamExt;
use libp2p::futures::future::BoxFuture;
use libp2p::futures::stream::FuturesUnordered;

use crate::messenger::Net;
use crate::reconcile::report::page_of;
use crate::reconcile::request_after;

/// How many record lookups a reconciling leader runs at once.
const FETCHES_IN_FLIGHT: usize = 32;

/// How long a record is left alone after it was looked up, before it is
/// looked up again if the lookup did not find it.
const FETCH_COOLDOWN: Duration = Duration::from_millis(250);

/// One office's reconciliation as the driver runs it: what it decides, and
/// the questions and lookups in flight.
pub(crate) struct LeaderReconciliation<'n> {
    /// What this office's reconciliation decides.
    pub(crate) reconciling: OfficeReconciliation,
    net: &'n Net,
    /// The certificate of this office, which every worker asked checks.
    proof: ElectionCertificate,
    me: WorkerId,
    grace: Duration,
    /// Each question or page in flight, as the worker asked, when its first
    /// question was asked, whether it asks for runs only (a re-report after a
    /// drift), and the page that came back, if any.
    asks: FuturesUnordered<BoxFuture<'n, (WorkerId, Instant, bool, Option<ReportPage>)>>,
    asking: BTreeSet<WorkerId>,
    /// Every worker asked so far.
    known: BTreeSet<WorkerId>,
    /// The roster's reconcilees as last seen, sorted.
    roster: Vec<WorkerId>,
    asked_again_at: Instant,
    fetches: FuturesUnordered<BoxFuture<'n, (TaskId, Option<TaskRecord>)>>,
    fetching: BTreeSet<TaskId>,
    /// When each record was last looked up over the network, and how many
    /// times it has been.
    looked_up: BTreeMap<TaskId, (Instant, u32)>,
    /// Whether the records missing may have changed since the last scan.
    scan: bool,
    scan_again_at: Option<Instant>,
    /// Missing records not yet looked up once.
    unlooked: usize,
    /// Which answered workers' heartbeats keep disagreeing with this leader.
    drift: DriftWatch,
}

impl<'n> LeaderReconciliation<'n> {
    /// Starts reconciling `office`: asks every one of `reconcilees` for its
    /// first page. This worker answers itself, from its own runs and records,
    /// with no message. `grace` is one suspicion timeout.
    pub(crate) fn start(
        net: &'n Net,
        office: ReconcileTerm,
        proof: ElectionCertificate,
        reconcilees: Vec<WorkerId>,
        now: Instant,
        grace: Duration,
        heartbeat_interval: Duration,
    ) -> Self {
        let mut roster = reconcilees.clone();
        roster.sort();
        let mut reconciliation = LeaderReconciliation {
            reconciling: OfficeReconciliation::new(office, reconcilees.iter().cloned(), now, grace),
            net,
            proof,
            me: net.local_worker_id(),
            grace,
            asks: FuturesUnordered::new(),
            asking: BTreeSet::new(),
            known: reconcilees.iter().cloned().collect(),
            roster,
            asked_again_at: now + grace,
            fetches: FuturesUnordered::new(),
            fetching: BTreeSet::new(),
            looked_up: BTreeMap::new(),
            scan: true,
            scan_again_at: None,
            unlooked: 0,
            drift: DriftWatch::new(heartbeat_interval, grace),
        };
        for worker in reconcilees {
            reconciliation.ask(worker, now);
        }
        reconciliation
    }

    pub(crate) fn office(&self) -> ReconcileTerm {
        self.reconciling.office()
    }

    /// Asks `worker` for its first page; this worker reads its own.
    fn ask(&mut self, worker: WorkerId, now: Instant) {
        if worker == self.me {
            self.answer_locally(now);
        } else if !self.asking.contains(&worker) {
            self.send_ask(worker, now, None, false);
        }
    }

    fn send_ask(
        &mut self,
        worker: WorkerId,
        asked_at: Instant,
        after: Option<Cursor>,
        runs_only: bool,
    ) {
        let (net, office, proof) = (self.net, self.office(), self.proof.clone());
        self.asking.insert(worker.clone());
        self.asks.push(Box::pin(async move {
            let page = net
                .ask_reconcile(worker.clone(), office, proof, after, runs_only)
                .await;
            (worker, asked_at, runs_only, page)
        }));
    }

    /// A heartbeat of `worker` said it holds runs of digest `heard`, and this
    /// leader's scheduler believes `believed`. A difference that lasts asks the
    /// worker for its runs alone, and what it answers goes through the same
    /// table as a late answer. A worker still to answer in full is asked for
    /// everything anyway, and one already being asked is left to answer.
    pub(crate) fn runs_heard(
        &mut self,
        worker: &WorkerId,
        heard: &[u8],
        believed: &Digest,
        now: Instant,
    ) {
        if *worker == self.me || !self.reconciling.leads() {
            return;
        }
        if self.drift.heard(worker, heard, believed, now)
            && self.reconciling.round().answered().contains(worker)
            && !self.asking.contains(worker)
        {
            self.drift.asked(worker, now);
            tracing::debug!(
                worker = worker.as_str(),
                "a worker's heartbeats disagree about the runs it holds: asking for them"
            );
            self.send_ask(worker.clone(), now, None, true);
        }
    }

    /// Reads this worker's own page and takes it into the round, page by page.
    fn answer_locally(&mut self, now: Instant) {
        let (runs, held) = (self.net.claimed_runs(), self.net.held_records());
        let mut after = None;
        loop {
            let request = ReconcileRequest {
                after: after.map(request_after),
                ..ReconcileRequest::default()
            };
            let report = page_of(&request, &runs, &held);
            let page = match wire::page(&report) {
                Ok(page) => page,
                Err(malformed) => {
                    tracing::warn!(%malformed, "this worker's own reconciliation page could not be read");
                    return;
                }
            };
            self.reconciling.heard();
            self.scan = true;
            match self.reconciling.round_mut().page(&self.me, page, now) {
                Some(cursor) => after = Some(cursor),
                None => return,
            }
        }
    }

    /// Waits for the next answer or lookup and takes it in. Cancel-safe; for
    /// ever when nothing is in flight.
    pub(crate) async fn next(&mut self) {
        if self.asks.is_empty() && self.fetches.is_empty() {
            return std::future::pending().await;
        }
        tokio::select! {
            Some((worker, asked_at, runs_only, page)) = self.asks.next() => {
                self.asked(worker, asked_at, runs_only, page);
            }
            Some((task, record)) = self.fetches.next() => {
                self.fetching.remove(&task);
                if let Some(record) = record {
                    self.reconciling.round_mut().fetched(record);
                }
                self.reconciling.heard();
                self.scan = true;
            }
        }
    }

    fn asked(
        &mut self,
        worker: WorkerId,
        asked_at: Instant,
        runs_only: bool,
        page: Option<ReportPage>,
    ) {
        self.asking.remove(&worker);
        self.reconciling.heard();
        self.scan = true;
        if let Some(page) = page
            && let Some(cursor) = self.reconciling.round_mut().page(&worker, page, asked_at)
        {
            self.send_ask(worker, asked_at, Some(cursor), runs_only);
        }
    }

    /// Asks a worker that joined the roster since, every member that has not
    /// answered once per suspicion timeout, and looks up the records the round
    /// lacks.
    pub(crate) fn ask_and_fetch<C: Clock>(&mut self, node: &WorkerNode<C>, now: Instant) {
        self.ask_who_is_due(node, now);
        self.fetch_what_is_missing(node, now);
    }

    /// Whether no lookup is in flight and none is owed.
    pub(crate) fn lookups_done(&self) -> bool {
        self.fetching.is_empty() && self.unlooked == 0
    }

    /// Asks a worker that joined the roster since, and, once per suspicion
    /// timeout, every member that has not answered in full.
    fn ask_who_is_due<C: Clock>(&mut self, node: &WorkerNode<C>, now: Instant) {
        let mut roster = node.reconcilees();
        roster.sort();
        if roster != self.roster {
            for worker in &roster {
                if self.known.insert(worker.clone()) {
                    self.reconciling.round_mut().ask_also(worker.clone());
                    self.ask(worker.clone(), now);
                }
            }
            for gone in self.roster.iter().filter(|worker| !roster.contains(worker)) {
                self.drift.forget(gone);
            }
            self.roster = roster;
            // A holder that left may have made a record certain.
            self.reconciling.heard();
        }
        if now >= self.asked_again_at {
            self.asked_again_at = now + self.grace;
            for worker in self.reconciling.round().unanswered() {
                if node.is_member(&worker) {
                    self.ask(worker, now);
                }
            }
        }
    }

    /// Reads every record the round lacks from this worker's own store, and
    /// looks up those that are not there, a bounded number at once.
    fn fetch_what_is_missing<C: Clock>(&mut self, node: &WorkerNode<C>, now: Instant) {
        let cooled_down = self.scan_again_at.is_some_and(|at| at <= now);
        if !(self.scan || cooled_down) {
            return;
        }
        self.scan = false;
        self.scan_again_at = None;
        self.unlooked = 0;
        let held = self.net.held_records();
        for task in self.reconciling.round().missing_records(|worker| node.is_member(worker)) {
            if let Some(record) = held.get(&task) {
                self.reconciling.round_mut().fetched(record);
            }
        }
        for task in self.reconciling.round().missing_records(|worker| node.is_member(worker)) {
            if self.fetching.contains(&task) {
                continue;
            }
            if let Some((at, times)) = self.looked_up.get(&task)
                && now < *at + self.cooldown(*times)
            {
                let again = *at + self.cooldown(*times);
                self.scan_again_at = Some(self.scan_again_at.map_or(again, |at| at.min(again)));
                continue;
            }
            if self.fetching.len() >= FETCHES_IN_FLIGHT {
                // Scanned again when a lookup ends.
                if !self.looked_up.contains_key(&task) {
                    self.unlooked += 1;
                }
                continue;
            }
            let times = self.looked_up.get(&task).map_or(0, |(_, times)| times + 1);
            self.looked_up.insert(task.clone(), (now, times));
            self.fetching.insert(task.clone());
            let net = self.net;
            self.fetches.push(Box::pin(async move {
                let record = net.get_record(task.clone()).await;
                (task, record)
            }));
        }
    }

    /// How long a record looked up `times` before is left alone: it doubles
    /// each time, up to one suspicion timeout, so a record no one can supply
    /// is not looked up every few hundred milliseconds for the whole office.
    fn cooldown(&self, times: u32) -> Duration {
        let ticks = FETCH_COOLDOWN.as_ticks().saturating_mul(1u64 << times.min(16));
        Duration::from_ticks(ticks).min(self.grace.max(FETCH_COOLDOWN))
    }

    /// When the driver must wake for this reconciliation: what core's
    /// `OfficeReconciliation` falls due at, the next time to ask again, or a
    /// record to look up again. What fell due since `progress` last ran is due
    /// now, so no wake is lost to the time `progress` took; `progress` moves
    /// its own time on, so this does not spin.
    pub(crate) fn wake_at<C: Clock>(&self, node: &WorkerNode<C>) -> Option<Instant> {
        let after = self.reconciling.progressed_at();
        let ask_again = (!self.reconciling.round().is_complete(|worker| node.is_member(worker)))
            .then_some(self.asked_again_at);
        [
            self.reconciling.wake_at(),
            ask_again.filter(|at| *at > after),
            self.scan_again_at.filter(|at| *at > after),
        ]
        .into_iter()
        .flatten()
        .min()
    }
}
