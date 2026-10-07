//! A new leader's reconciliation as the driver runs it: asking its shard what
//! it holds, fetching the records it lacks, deciding when to stop waiting,
//! writing the rebuilt records again at its term, and, once it leads, taking
//! the answers that come late.

use std::collections::{BTreeMap, BTreeSet};

use kabudachi_core::election::WorkerNode;
use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::generated::TaskRecord;
use kabudachi_core::protocol::ids::{TaskId, WorkerId};
use kabudachi_core::protocol::messages::ReconcileRequest;
use kabudachi_core::reconcile::{
    Cursor, DriftWatch, Rebuild, ReconcileRound, ReconcileTerm, Republish, ReportPage, wire,
};
use kabudachi_core::time::{Clock, Duration, Instant};
use libp2p::futures::StreamExt;
use libp2p::futures::future::BoxFuture;
use libp2p::futures::stream::FuturesUnordered;

use crate::messenger::{Net, PlacedWrite, WriteOutcome};
use crate::reconcile::report::page_of;
use crate::reconcile::request_after;

/// How many record lookups a reconciling leader runs at once.
const FETCHES_IN_FLIGHT: usize = 32;

/// How long a record is left alone after it was looked up, before it is
/// looked up again if the lookup did not find it.
const FETCH_COOLDOWN: Duration = Duration::from_millis(250);

/// What the driver is to do next for a reconciliation.
pub(crate) enum Progress {
    /// Nothing yet.
    Waiting,
    /// The round may stop: rebuild the scheduler from this.
    Rebuild(Rebuild),
    /// The rebuilt scheduler's records are to be placed on the voters and
    /// written again: an earlier attempt could not place them all.
    Place(Vec<TaskRecord>),
    /// The voters changed while records are still to be stored: place them
    /// on the new voters (see [`LeaderReconciliation::re_place`]).
    RePlace,
    /// Every republished record is stored: the node may lead.
    Republished(ReconcileTerm),
    /// What was learnt since, for the leading scheduler to adopt.
    Learnt(Rebuild),
}

/// What a reconciliation could not finish, to be done again.
pub(crate) enum Stuck {
    /// The scheduler did not take the rebuild.
    Rebuild(Rebuild),
    /// These records could not all be placed on the voters.
    Place(Vec<TaskRecord>),
}

/// Work that failed and is tried again when the voters change and, failing
/// that, once per `every`.
struct Retry<T> {
    work: Option<T>,
    voters: Vec<WorkerId>,
    at: Instant,
    every: Duration,
}

impl<T> Retry<T> {
    fn new(every: Duration) -> Self {
        Retry {
            work: None,
            voters: Vec::new(),
            at: Instant::at(0),
            every,
        }
    }

    /// Keeps `work` to try again, given the voters it failed with.
    fn hold(&mut self, work: T, mut voters: Vec<WorkerId>, now: Instant) {
        voters.sort();
        self.work = Some(work);
        self.voters = voters;
        self.at = now + self.every;
    }

    /// The work to try again, if the voters changed or the time has come.
    fn take_if_due(&mut self, voters: &[WorkerId], now: Instant) -> Option<T> {
        let mut voters = voters.to_vec();
        voters.sort();
        if self.work.is_some() && (voters != self.voters || now >= self.at) {
            self.work.take()
        } else {
            None
        }
    }

    fn wake_at(&self) -> Option<Instant> {
        self.work.as_ref().map(|_| self.at)
    }
}

/// One office's reconciliation: the round, the questions and lookups in
/// flight, the republish, and, once the node leads, the answers still
/// awaited.
pub(crate) struct LeaderReconciliation<'n> {
    round: ReconcileRound,
    net: &'n Net,
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
    republish: Option<Republish>,
    /// The voters, sorted, the republish was last placed on.
    republish_voters: Vec<WorkerId>,
    rebuilt: bool,
    /// The rebuild or placement that failed, tried again.
    stuck: Retry<Stuck>,
    /// When `progress` last ran: what falls due after it is woken for.
    progressed_at: Instant,
    led: bool,
    /// Whether something arrived that the round has not yet handed over.
    news: bool,
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
        reconcilees: Vec<WorkerId>,
        now: Instant,
        grace: Duration,
        heartbeat_interval: Duration,
    ) -> Self {
        let mut roster = reconcilees.clone();
        roster.sort();
        let mut reconciliation = LeaderReconciliation {
            round: ReconcileRound::new(office, reconcilees.iter().cloned(), now, grace),
            net,
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
            republish: None,
            republish_voters: Vec::new(),
            rebuilt: false,
            stuck: Retry::new(grace),
            progressed_at: now,
            led: false,
            news: false,
            drift: DriftWatch::new(heartbeat_interval, grace),
        };
        for worker in reconcilees {
            reconciliation.ask(worker, now);
        }
        reconciliation
    }

    pub(crate) fn office(&self) -> ReconcileTerm {
        self.round.term()
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
        let (net, office) = (self.net, self.round.term());
        self.asking.insert(worker.clone());
        self.asks.push(Box::pin(async move {
            let page = net
                .ask_reconcile(worker.clone(), office, after, runs_only)
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
        if *worker == self.me || !self.led {
            return;
        }
        if self.drift.heard(worker, heard, believed, now)
            && self.round.answered().contains(worker)
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
            self.news = true;
            self.scan = true;
            match self.round.page(&self.me, page, now) {
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
                    self.round.fetched(record);
                }
                self.news = true;
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
        self.news = true;
        self.scan = true;
        if let Some(page) = page
            && let Some(cursor) = self.round.page(&worker, page, asked_at)
        {
            self.send_ask(worker, asked_at, Some(cursor), runs_only);
        }
    }

    /// What the driver should do now, given its node and whether its
    /// scheduler leads. What late answers teach is handed over only while it
    /// does, and is kept until then.
    pub(crate) fn progress<C: Clock>(
        &mut self,
        node: &WorkerNode<C>,
        leading: bool,
        now: Instant,
    ) -> Progress {
        self.progressed_at = now;
        self.ask_who_is_due(node, now);
        self.fetch_what_is_missing(node, now);

        // A republish that cannot reach a quorum is not given up: this leader
        // keeps office, writes each record again, and places the writes anew
        // whenever the voters change. That is accepted because a node holds
        // office only while its lease holds, and holding the lease means a
        // quorum of voters has been heard from lately. When the lease ends the
        // node leaves office (`NoQuorum`), the driver drops this
        // reconciliation. Writes that fell due before the node stepped may
        // still go out; a holder refuses them against a newer term.
        if let Some(republish) = self.republish.as_mut() {
            let due = republish.due(now);
            if !due.is_empty() {
                self.net.write_records(due);
            }
            if republish.is_done() && !self.led {
                self.led = true;
                return Progress::Republished(self.round.term());
            }
            let mut voters = node.voters();
            voters.sort();
            if !republish.is_done() && voters != self.republish_voters {
                self.republish_voters = voters;
                return Progress::RePlace;
            }
        }
        if let Some(stuck) = self.stuck.take_if_due(&node.voters(), now) {
            return match stuck {
                Stuck::Rebuild(rebuild) => Progress::Rebuild(rebuild),
                Stuck::Place(records) => Progress::Place(records),
            };
        }
        if !self.rebuilt {
            let answered = node.voters_answered(&self.round.answered());
            if self.round.may_finish(answered, now) && self.fetching.is_empty() && self.unlooked == 0
            {
                self.rebuilt = true;
                self.news = false;
                return Progress::Rebuild(self.round.take_settled(|worker| node.is_member(worker)));
            }
            return Progress::Waiting;
        }
        if self.led && self.news && leading {
            self.news = false;
            let learnt = self.round.take_settled(|worker| node.is_member(worker));
            if !learnt.records.is_empty() || !learnt.reports.is_empty() {
                return Progress::Learnt(learnt);
            }
        }
        Progress::Waiting
    }

    /// Takes back what the scheduler could not adopt because it did not lead:
    /// it is offered again, with what is learnt meanwhile, once it does.
    pub(crate) fn give_back(&mut self, learnt: Rebuild) {
        self.round.give_back(learnt);
        self.news = true;
    }

    /// Asks a worker that joined the roster since, and, once per suspicion
    /// timeout, every member that has not answered in full.
    fn ask_who_is_due<C: Clock>(&mut self, node: &WorkerNode<C>, now: Instant) {
        let mut roster = node.reconcilees();
        roster.sort();
        if roster != self.roster {
            for worker in &roster {
                if self.known.insert(worker.clone()) {
                    self.round.ask_also(worker.clone());
                    self.ask(worker.clone(), now);
                }
            }
            for gone in self.roster.iter().filter(|worker| !roster.contains(worker)) {
                self.drift.forget(gone);
            }
            self.roster = roster;
            // A holder that left may have made a record certain.
            self.news = true;
        }
        if now >= self.asked_again_at {
            self.asked_again_at = now + self.grace;
            for worker in self.round.unanswered() {
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
        for task in self.round.missing_records(|worker| node.is_member(worker)) {
            if let Some(record) = held.get(&task) {
                self.round.fetched(record);
            }
        }
        for task in self.round.missing_records(|worker| node.is_member(worker)) {
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

    /// Keeps what could not be finished, to be done again when the voters
    /// change or after a suspicion timeout.
    pub(crate) fn stuck(&mut self, work: Stuck, voters: Vec<WorkerId>, now: Instant) {
        self.stuck.hold(work, voters, now);
    }

    /// The rebuild ran: write these republished records, each again after
    /// `retry_after` if it was not stored.
    pub(crate) fn republishing(
        &mut self,
        writes: Vec<PlacedWrite>,
        retry_after: Duration,
        mut voters: Vec<WorkerId>,
    ) {
        voters.sort();
        self.republish_voters = voters;
        self.republish = Some(Republish::new(writes, retry_after));
    }

    /// The voters changed: places every republished write not yet stored on
    /// them, and writes each refused one again now.
    pub(crate) fn re_place(&mut self, replace: impl FnMut(&mut PlacedWrite), now: Instant) {
        if let Some(republish) = self.republish.as_mut() {
            republish.re_place(replace, now);
        }
    }

    /// Takes a write outcome that belongs to the republish; whether it did.
    pub(crate) fn settle(&mut self, outcome: &WriteOutcome, now: Instant) -> bool {
        self.republish
            .as_mut()
            .is_some_and(|republish| republish.settled(outcome, now))
    }

    /// When the driver must wake for this reconciliation: the end of the
    /// grace, the next time to ask again, a record to look up again, a write
    /// or a stuck step to try again. What fell due since `progress` last ran
    /// is due now, so no wake is lost to the time `progress` took; `progress`
    /// moves its own time on, so this does not spin.
    pub(crate) fn wake_at<C: Clock>(&self, node: &WorkerNode<C>) -> Option<Instant> {
        let grace = (!self.rebuilt).then(|| self.round.grace_ends_at());
        let ask_again = (!self.round.is_complete(|worker| node.is_member(worker)))
            .then_some(self.asked_again_at);
        [
            grace,
            ask_again,
            self.scan_again_at,
            self.republish.as_ref().and_then(Republish::wake_at),
            self.stuck.wake_at(),
        ]
        .into_iter()
        .flatten()
        .filter(|at| *at > self.progressed_at)
        .min()
    }
}
