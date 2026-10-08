//! The seam between a worker's driver and the executor that runs its
//! TaskRuns: two channels, made by [`executor_channel`]. The driver holds the
//! [`ExecutorEndpoint`] (see `crate::worker::WorkerConfig::with_executor`); the
//! executor holds the [`ExecutorHandle`].
//!
//! ## What the executor sends: [`Report`]
//!
//! `Capacity(n)` offers `n` more places. The driver claims work while places
//! it was offered are not taken, one per run it hands over, and the executor
//! offers a place again once a run has ended. An offer is a credit, never a
//! count of free places, so an offer and a run crossing on the two channels
//! never make the driver claim more than the executor can hold.
//!
//! Per run, in order: `Started` once the body runs (a compaction run sends
//! none), then exactly one of `Completed` (with the digest of its result),
//! `Failed` (with the error's type name), `Lost` (the process running it died
//! or was killed at an abort deadline, so no outcome is known), or, for a
//! compaction run, `Compacted` (the fold). The driver takes each report to
//! the leader in that order, asking again as leaders change, until one takes
//! it.
//!
//! ## What the driver sends: [`Work`]
//!
//! `Run(claim)` hands over a run the leader granted: run the task's body.
//! `Compact(claim)` hands over a compaction run: fold the claim's chain,
//! oldest first, with the task's merge. `Cancel(run)`: the run is no longer
//! this worker's to finish; stop its body cooperatively and report however it
//! ends.
//!
//! `Abort { run, deadline }`: the worker cannot show that its leader still
//! hears it, so another leader may replay the run once `deadline` has passed,
//! on this host's monotonic clock. The executor cancels the body
//! cooperatively at `deadline` less its cancel grace (at once if that has
//! passed), and, if the body still runs at `deadline`, kills the process
//! running it. Runs sharing that process die with it, which is safe: they
//! share the lost contact, and so the deadline. A run killed this way is
//! reported `Lost`. A later `Abort` for the same run replaces its deadline.
//! `AbortWithdrawn(run)`: the leader hears the worker again before the
//! deadline; drop the pending abort and let the run go on.
//!
//! The driver claims nothing until some leader has vouched for hearing this
//! worker (see `WorkerNode::has_contact_floor`), so every run it hands over
//! has an abort deadline it could name, and nothing while an abort deadline
//! stands. While this worker leads, it claims from its own scheduler and
//! decides its own reports there, each held, like any other decision, until a
//! quorum of the task's placement has stored it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    Claim, ClaimResponse, ReportCompacted, ReportCompleted, ReportFailed, ReportLost, ReportStarted, TaskRejectReason,
    TaskResponse, claim_response, task_request, task_response,
};
use kabudachi_core::task_record::Settled;
use kabudachi_core::time::WallTime;
use libp2p::futures::{FutureExt, StreamExt};
use libp2p::futures::future::BoxFuture;
use libp2p::futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tokio::time::Instant as TokioInstant;

use crate::discovery::{Found, IdleBackoff};
use crate::messenger::Net;
use crate::task_exchange::{self, TaskFailure, reported_run};

/// What the driver hands the executor (see the module doc).
#[derive(Debug, Clone, PartialEq)]
pub enum Work {
    Run(Claim),
    Compact(Claim),
    Cancel(TaskRunId),
    Abort { run: TaskRunId, deadline: Instant },
    AbortWithdrawn(TaskRunId),
}

/// What the executor tells the driver (see the module doc).
#[derive(Debug, Clone, PartialEq)]
pub enum Report {
    Capacity(u32),
    Started(TaskRunId),
    Completed { run: TaskRunId, digest: Digest },
    Failed { run: TaskRunId, reason: String },
    Lost(TaskRunId),
    Compacted { run: TaskRunId, payload: Vec<u8> },
}

/// The driver's end of the seam, with what the driver knows of its executor
/// between runs of the driver.
pub struct ExecutorEndpoint {
    pub(crate) work: mpsc::UnboundedSender<Work>,
    pub(crate) reports: mpsc::UnboundedReceiver<Report>,
    pub(crate) held: Held,
}

/// The executor's end of the seam.
pub struct ExecutorHandle {
    work: mpsc::UnboundedReceiver<Work>,
    reports: mpsc::UnboundedSender<Report>,
}

/// The driver holding the other end has stopped, and dropped it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverGone;

/// A new seam: the endpoint for the worker's driver, the handle for its
/// executor. Neither channel is bounded: the driver hands over no more runs
/// than the executor offered places for, and the executor reports on no more.
pub fn executor_channel() -> (ExecutorEndpoint, ExecutorHandle) {
    let (work_sender, work) = mpsc::unbounded_channel();
    let (reports_sender, reports) = mpsc::unbounded_channel();
    (
        ExecutorEndpoint {
            work: work_sender,
            reports,
            held: Held::default(),
        },
        ExecutorHandle {
            work,
            reports: reports_sender,
        },
    )
}

impl ExecutorHandle {
    /// The next work the driver hands over, in the order it was handed;
    /// `None` once the driver has stopped and nothing is left.
    pub async fn next_work(&mut self) -> Option<Work> {
        self.work.recv().await
    }

    /// Tells the driver `report`.
    pub fn report(&self, report: Report) -> Result<(), DriverGone> {
        self.reports.send(report).map_err(|_| DriverGone)
    }
}

/// A report on one of this worker's runs, as the leader is asked to take it.
pub(crate) type RunReport = task_request::Request;

/// What the driver keeps of its executor across runs of the driver.
#[derive(Default)]
pub(crate) struct Held {
    /// Places the executor offered that no run has taken.
    pub(crate) credits: u32,
    /// The runs handed over whose end the executor has not reported.
    pub(crate) handed: BTreeSet<TaskRunId>,
    /// The runs of `handed` the executor was already told to stop.
    pub(crate) cancelled: BTreeSet<TaskRunId>,
    /// Per run, the reports no leader has taken yet, oldest first.
    pub(crate) waiting: BTreeMap<TaskRunId, VecDeque<RunReport>>,
    /// The executor dropped its handle: nothing more is claimed for it.
    pub(crate) gone: bool,
    /// The abort deadline every run in `handed` was given, while one stands.
    pub(crate) abort_by: Option<Instant>,
}

/// A claim or report of this worker's own, decided by its own scheduler
/// while it leads, and held like a remote worker's until the writes the
/// decision made are stored.
pub(crate) enum OwnAnswer {
    Claim { response: ClaimResponse, reserved: u32 },
    Report { request: RunReport, response: TaskResponse },
}

/// A report sent, with the answer it got or why it got none.
type Reply = (RunReport, Result<TaskResponse, TaskFailure>);

/// The driver's side of the seam for one run of the driver: what the
/// executor reported and the driver has yet to act on, and the claims and
/// reports under way. What must outlive the run is kept in the endpoint's
/// [`Held`].
pub(crate) struct Executing<'n> {
    endpoint: &'n mut ExecutorEndpoint,
    net: &'n Net,
    /// How long a report waits after one was refused, or got no answer,
    /// before reports are sent again.
    retry_after: Duration,
    /// A discovery under way at a remote leader, with the places it reserved.
    discovering: Option<(u32, BoxFuture<'n, Found>)>,
    /// Places this leader's own claim reserved, until its answer settles.
    own_claim: Option<u32>,
    idle: IdleBackoff,
    /// No claim before this, after one that found nothing.
    next_claim_at: Option<TokioInstant>,
    /// The runs whose first waiting report is with a leader now.
    sending: BTreeSet<TaskRunId>,
    in_flight: FuturesUnordered<BoxFuture<'n, Reply>>,
    /// No report is sent before this, after one was refused or unanswered.
    retry_at: Option<TokioInstant>,
    arrived: Vec<Report>,
    found: Option<(u32, Found)>,
    replies: Vec<Reply>,
}

impl<'n> Executing<'n> {
    /// The driver's side for one run of the driver. A claim the last run
    /// was granted and never handed over (it stopped mid-discovery) is
    /// reported lost, so the leader replays it.
    pub(crate) fn new(endpoint: &'n mut ExecutorEndpoint, net: &'n Net, retry_after: Duration) -> Self {
        let mut executing = Executing {
            endpoint,
            net,
            retry_after,
            discovering: None,
            own_claim: None,
            idle: IdleBackoff::default(),
            next_claim_at: None,
            sending: BTreeSet::new(),
            in_flight: FuturesUnordered::new(),
            retry_at: None,
            arrived: Vec::new(),
            found: None,
            replies: Vec::new(),
        };
        for run in net.claimed_runs().active_ids() {
            let held = &executing.endpoint.held;
            if !held.handed.contains(&run) && !held.waiting.contains_key(&run) {
                executing.queue_lost(run);
            }
        }
        executing
    }

    /// Waits for the executor's next report, a discovery's end, the answer to
    /// a report, or the time to claim or report again. Cancel-safe: what
    /// arrives is kept for [`Self::take_arrived`].
    pub(crate) async fn wake(&mut self) {
        let due = match (self.next_claim_at, self.retry_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let gone = self.endpoint.held.gone;
        tokio::select! {
            report = self.endpoint.reports.recv(), if !gone => match report {
                Some(report) => self.arrived.push(report),
                None => self.endpoint.held.gone = true,
            },
            found = next_found(&mut self.discovering) => self.found = Some(found),
            Some(reply) = self.in_flight.next(), if !self.in_flight.is_empty() => self.replies.push(reply),
            () = sleep_until(due) => {}
        }
    }

    /// Forgets the waits that have passed by `now`, so a wait that cannot
    /// be acted on yet (no leader known) never wakes the driver again.
    pub(crate) fn expire(&mut self, now: TokioInstant) {
        if self.next_claim_at.is_some_and(|at| at <= now) {
            self.next_claim_at = None;
        }
        if self.retry_at.is_some_and(|at| at <= now) {
            self.retry_at = None;
        }
    }

    /// Acts on what arrived since the last batch, and on what is ready now,
    /// so a burst costs one batch: the executor's reports, a discovery's
    /// claims, and the answers reports got. Once the executor is gone, every
    /// run it was handed and has not ended is reported lost.
    pub(crate) fn take_arrived(&mut self) {
        self.drain_reports();
        while let Some(Some(reply)) = self.in_flight.next().now_or_never() {
            self.replies.push(reply);
        }
        for report in std::mem::take(&mut self.arrived) {
            self.on_report(report);
        }
        if self.endpoint.held.gone {
            self.lose_handed();
        }
        if let Some((reserved, found)) = self.found.take() {
            let claims = found.claims.into_iter().map(|(_, claim)| claim).collect();
            self.claimed(reserved, claims);
        }
        for (request, reply) in std::mem::take(&mut self.replies) {
            self.on_reply(request, reply);
        }
    }

    /// The first waiting report of every run that has none with a leader
    /// now, marked as sent; none while a retry is pending.
    pub(crate) fn reports_to_send(&mut self) -> Vec<RunReport> {
        if self.retry_at.is_some() {
            return Vec::new();
        }
        let mut due = Vec::new();
        for (run, waiting) in &self.endpoint.held.waiting {
            if let Some(first) = waiting.front()
                && self.sending.insert(run.clone())
            {
                due.push(first.clone());
            }
        }
        due
    }

    /// Sends `request` to `leader`, a remote leader.
    pub(crate) fn send_report(&mut self, leader: WorkerId, request: RunReport) {
        let net = self.net;
        self.in_flight.push(Box::pin(async move {
            let reply = net.report(leader, request.clone()).await;
            (request, reply)
        }));
    }

    /// How many places to claim now, reserved until the claim is answered:
    /// all the places offered and not taken, unless the executor is gone, no
    /// place is offered, a claim is under way, one found nothing too
    /// recently, or an abort deadline stands. The caller claims them at once.
    pub(crate) fn places_to_claim(&mut self) -> Option<u32> {
        let held = &mut self.endpoint.held;
        let busy = self.discovering.is_some() || self.found.is_some() || self.own_claim.is_some();
        if held.gone || held.credits == 0 || busy || self.next_claim_at.is_some() || held.abort_by.is_some() {
            return None;
        }
        Some(std::mem::take(&mut held.credits))
    }

    /// Claims `places` from `leader`, a remote leader, through discovery
    /// (see `Net::discover`); `now` judges which of this worker's own
    /// records are due.
    pub(crate) fn discover(&mut self, leader: WorkerId, places: u32, now: WallTime) {
        let limit = usize::try_from(places).unwrap_or(usize::MAX);
        let discovery = self.net.discover(leader, limit, now, true);
        self.discovering = Some((places, Box::pin(discovery)));
    }

    /// This worker leads: the `places` just reserved are claimed from its own
    /// scheduler, whose answer arrives through [`Self::own_settled`].
    pub(crate) fn claiming_own(&mut self, places: u32) {
        self.own_claim = Some(places);
    }

    /// Acts on an answer of this worker's own once its writes settled:
    /// claims granted are entered in the ledger and handed over; a report
    /// taken updates the ledger, as a remote leader's answer would.
    pub(crate) fn own_settled(&mut self, settled: Settled<OwnAnswer>) {
        match settled {
            Settled::Released(OwnAnswer::Claim { response, reserved }) => {
                let claims = granted(response);
                for claim in &claims {
                    self.net.claimed_runs().claimed(claim.clone());
                }
                self.own_claim = None;
                self.claimed(reserved, claims);
            }
            Settled::NotLeader(OwnAnswer::Claim { response, reserved }) => {
                // Its own scheduler granted these, but the grant was never
                // stored: none is handed over, and each is reported lost so
                // whichever leader decides it replays it.
                for claim in granted(response) {
                    if let Some(run) = claim.task_run_id {
                        self.queue_lost(run.into());
                    }
                }
                self.own_claim = None;
                self.claimed(reserved, Vec::new());
            }
            Settled::Released(OwnAnswer::Report { request, response }) => {
                self.net.note_answer(&request, &response);
                self.on_reply(request, Ok(response));
            }
            Settled::NotLeader(OwnAnswer::Report { request, .. }) => {
                self.on_reply(request, Ok(task_exchange::not_leader()));
            }
        }
    }

    /// Tells the executor to stop `run`'s body, once, if it was handed over
    /// and has not ended. A leader lists a cancelled run in every ack until
    /// it sees the run gone, so the same cancel arrives more than once.
    pub(crate) fn cancel(&mut self, run: &TaskRunId) {
        let held = &mut self.endpoint.held;
        if held.handed.contains(run) && held.cancelled.insert(run.clone()) {
            let _ = self.endpoint.work.send(Work::Cancel(run.clone()));
        }
    }

    /// The node reported a new abort deadline, on this host's clock, or
    /// lifted the one it had: every run handed over is told, a lifted
    /// deadline withdrawing each pending abort.
    pub(crate) fn follow_abort_deadline(&mut self, deadline: Option<Instant>) {
        let held = &mut self.endpoint.held;
        let withdrawn = deadline.is_none() && held.abort_by.is_some();
        held.abort_by = deadline;
        for run in &held.handed {
            let work = match deadline {
                Some(deadline) => Work::Abort { run: run.clone(), deadline },
                None if withdrawn => Work::AbortWithdrawn(run.clone()),
                None => continue,
            };
            let _ = self.endpoint.work.send(work);
        }
    }

    /// Keeps every report the executor has sent, until none is left or the
    /// executor is gone.
    fn drain_reports(&mut self) {
        while !self.endpoint.held.gone {
            match self.endpoint.reports.try_recv() {
                Ok(report) => self.arrived.push(report),
                Err(mpsc::error::TryRecvError::Disconnected) => self.endpoint.held.gone = true,
                Err(mpsc::error::TryRecvError::Empty) => break,
            }
        }
    }

    fn on_report(&mut self, report: Report) {
        let request = match report {
            Report::Capacity(places) => {
                let held = &mut self.endpoint.held;
                held.credits = held.credits.saturating_add(places);
                return;
            }
            Report::Started(run) => task_request::Request::Started(ReportStarted {
                task_run_id: Some(run.into()),
            }),
            Report::Completed { run, digest } => {
                self.ended(&run);
                task_request::Request::Completed(ReportCompleted {
                    task_run_id: Some(run.into()),
                    result_digest: Some(digest.into()),
                })
            }
            Report::Failed { run, reason } => {
                self.ended(&run);
                task_request::Request::Failed(ReportFailed {
                    task_run_id: Some(run.into()),
                    failure_kind: reason,
                })
            }
            Report::Lost(run) => {
                self.ended(&run);
                task_request::Request::Lost(ReportLost {
                    task_run_id: Some(run.into()),
                })
            }
            Report::Compacted { run, payload } => {
                self.ended(&run);
                task_request::Request::Compacted(ReportCompacted {
                    task_run_id: Some(run.into()),
                    folded_payload: payload,
                })
            }
        };
        self.queue(request);
    }

    /// A report got `reply`. One the leader may take later (it did not
    /// lead, did not know the run yet, or no answer came) waits to be sent
    /// again; any other answer ends it, and the run's next report may go. A
    /// start refused because the run is not this worker's stops its body.
    fn on_reply(&mut self, request: RunReport, reply: Result<TaskResponse, TaskFailure>) {
        let run = reported_run(&request).expect("every report the driver sends names its run");
        self.sending.remove(&run);
        let retry = match &reply {
            Err(_) => true,
            Ok(response) => match rejection(response) {
                Some(TaskRejectReason::TaskRejectNotLeader | TaskRejectReason::TaskRejectNotReady) => true,
                Some(TaskRejectReason::TaskRejectUnknownRun | TaskRejectReason::TaskRejectNotAuthoritative) => {
                    if matches!(request, task_request::Request::Started(_)) {
                        self.cancel(&run);
                    }
                    false
                }
                _ => false,
            },
        };
        if retry {
            self.retry_at.get_or_insert(TokioInstant::now() + self.retry_after);
            return;
        }
        let waiting = &mut self.endpoint.held.waiting;
        if let Some(reports) = waiting.get_mut(&run) {
            reports.pop_front();
            if reports.is_empty() {
                waiting.remove(&run);
            }
        }
    }

    /// A claim that reserved `reserved` places was granted `claims`: the
    /// places left over are offered again, the claims are handed over, and a
    /// claim that found nothing waits out the idle backoff.
    fn claimed(&mut self, reserved: u32, claims: Vec<Claim>) {
        let taken = u32::try_from(claims.len()).unwrap_or(u32::MAX);
        let held = &mut self.endpoint.held;
        held.credits = held.credits.saturating_add(reserved.saturating_sub(taken));
        let wait = self.idle.after(claims.len());
        self.next_claim_at = (!wait.is_zero()).then(|| TokioInstant::now() + wait);
        for claim in claims {
            self.hand_over(claim);
        }
    }

    fn hand_over(&mut self, claim: Claim) {
        let Some(run) = claim.task_run_id.clone().map(TaskRunId::from) else {
            // Not a claim a leader grants: its place is offered again.
            self.endpoint.held.credits += 1;
            return;
        };
        let compacts = claim.task.as_ref().is_some_and(|task| task.compacts.is_some());
        let work = if compacts { Work::Compact(claim) } else { Work::Run(claim) };
        if self.endpoint.work.send(work).is_err() {
            // No executor is left to run it: the leader is told at once, so it
            // is replayed rather than held by a worker that will never run it,
            // and so is every run handed over before it, this batch or earlier.
            // What it reported before it went is acted on first, so a run it
            // finished is not replayed.
            self.drain_reports();
            self.endpoint.held.gone = true;
            for report in std::mem::take(&mut self.arrived) {
                self.on_report(report);
            }
            self.queue_lost(run);
            self.lose_handed();
            return;
        }
        if let Some(deadline) = self.endpoint.held.abort_by {
            let _ = self.endpoint.work.send(Work::Abort { run: run.clone(), deadline });
        }
        self.endpoint.held.handed.insert(run);
    }

    /// The executor is gone: no run it was handed and has not ended will
    /// report again, so each is reported lost.
    fn lose_handed(&mut self) {
        for run in std::mem::take(&mut self.endpoint.held.handed) {
            self.endpoint.held.cancelled.remove(&run);
            self.queue_lost(run);
        }
    }

    fn queue_lost(&mut self, run: TaskRunId) {
        self.queue(task_request::Request::Lost(ReportLost {
            task_run_id: Some(run.into()),
        }));
    }

    fn ended(&mut self, run: &TaskRunId) {
        self.endpoint.held.handed.remove(run);
        self.endpoint.held.cancelled.remove(run);
    }

    fn queue(&mut self, request: RunReport) {
        let run = reported_run(&request).expect("every report the driver makes names its run");
        self.endpoint.held.waiting.entry(run).or_default().push_back(request);
    }
}

impl Drop for Executing<'_> {
    /// Acts on what already arrived, and offers again the places a claim
    /// still under way reserved, so the next run of the driver counts them.
    fn drop(&mut self) {
        self.take_arrived();
        let reserved = self.discovering.take().map_or(0, |(reserved, _)| reserved);
        let reserved = reserved + self.own_claim.take().unwrap_or(0);
        let held = &mut self.endpoint.held;
        held.credits = held.credits.saturating_add(reserved);
    }
}

/// The claims `response` grants.
fn granted(response: ClaimResponse) -> Vec<Claim> {
    match response.result {
        Some(claim_response::Result::Batch(batch)) => batch.claims,
        _ => Vec::new(),
    }
}

/// The reason `response` refuses its report, if it does.
fn rejection(response: &TaskResponse) -> Option<TaskRejectReason> {
    match &response.result {
        Some(task_response::Result::Reject(reject)) => TaskRejectReason::try_from(reject.reason).ok(),
        _ => None,
    }
}

/// The discovery's claims once it ends, with the places it reserved; for
/// ever without one. Cancel-safe: the discovery is polled where it lies.
async fn next_found<'n>(discovering: &mut Option<(u32, BoxFuture<'n, Found>)>) -> (u32, Found) {
    let Some((_, discovery)) = discovering.as_mut() else {
        return std::future::pending().await;
    };
    let found = discovery.await;
    let (reserved, _) = discovering.take().expect("the discovery just ended");
    (reserved, found)
}

async fn sleep_until(at: Option<TokioInstant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
