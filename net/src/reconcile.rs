//! The reconcile exchange: how a new leader asks a worker what it holds, and
//! how every worker answers.
//!
//! [`codec`] frames the `/kabudachi/reconcile/1` messages. The asking side is
//! [`Net::ask_reconcile`], one page per call, to the worker the caller names.
//! The answering side is [`report::page_of`], which the driver applies to
//! every inbound request: a worker keeps no view of who leads, answers
//! whichever leader asks, and its answer decides nothing by itself.
//!
//! An answer is paged: a worker's runs first, in run id order, then a summary
//! of each record it holds, in task id order, each page within the message
//! limit and each carrying the cursor the next request continues from. What a
//! report must drop to fit is logged, never silently cut. `leader` runs a
//! new leader's side of the exchange for the driver: it asks, fetches the
//! records it lacks, decides when to stop waiting and takes late answers.
//! `republish` writes the rebuilt records again at the leader's term, a
//! bounded number at a time and in the order a supersession needs, and writes
//! again any that was refused.

use std::str::FromStr;

use kabudachi_core::protocol::ids::WorkerId;
use kabudachi_core::protocol::messages::{ReconcileReport, ReconcileRequest, reconcile_request};
use kabudachi_core::reconcile::{Cursor, ReconcileTerm, ReportPage, wire};
use libp2p::PeerId;

use crate::exchange::Asked;
use crate::messenger::Net;
use crate::peers::worker_id_of;
use crate::reconcile::codec::ReconcileCodec;

pub mod codec;
pub(crate) mod leader;
pub(crate) mod report;
pub(crate) mod republish;

/// The request's cursor for `cursor`.
pub(crate) fn request_after(cursor: Cursor) -> reconcile_request::After {
    match cursor {
        Cursor::AfterRun(run) => reconcile_request::After::AfterRun(run.into()),
        Cursor::AfterKey(task) => reconcile_request::After::AfterKey(task.into()),
    }
}

/// An unanswered inbound `/kabudachi/reconcile/1` request, returned by
/// [`Net::poll_reconcile_requests`]. Answer it with
/// [`Net::respond_reconcile`]; dropping it unanswered just lets the asker's
/// substream eventually fail on their side, the same contract as
/// `ClaimRequestHandle`.
pub struct ReconcileRequestHandle(Asked<ReconcileCodec>);

impl ReconcileRequestHandle {
    /// The `WorkerId` of whoever sent this request.
    pub fn from(&self) -> WorkerId {
        worker_id_of(&self.0.from)
    }

    /// What this request asks for.
    pub fn request(&self) -> &ReconcileRequest {
        &self.0.request
    }
}

impl Net {
    /// Asks `worker` for one page of what it holds, for the office `term`,
    /// starting after `cursor` (the first page for `None`). With
    /// `runs_only`, the page holds runs and no records. `None` if no usable
    /// answer came: the worker could not be reached, did not answer, or sent
    /// a page that could not be read. Also `None` for the local worker: the
    /// leader reads its own store directly instead of asking itself.
    pub async fn ask_reconcile(
        &self,
        worker: WorkerId,
        term: ReconcileTerm,
        cursor: Option<Cursor>,
        runs_only: bool,
    ) -> Option<ReportPage> {
        if worker == self.local_worker_id() {
            return None;
        }
        let to = PeerId::from_str(worker.as_str()).ok()?;
        let after = cursor.map(request_after);
        let request = ReconcileRequest {
            recovery_epoch: term.recovery_epoch.number,
            recovery_lineage: term.recovery_epoch.lineage,
            term: term.term,
            after,
            runs_only,
        };
        let report = self.ask::<ReconcileCodec>(to, request).await?;
        match wire::page(&report) {
            Ok(page) => Some(page),
            Err(malformed) => {
                tracing::warn!(worker = worker.as_str(), %malformed, "a reconciliation page could not be read");
                None
            }
        }
    }

    /// Drains every inbound `/kabudachi/reconcile/1` request not yet
    /// answered. Answer each with [`Self::respond_reconcile`].
    pub fn poll_reconcile_requests(&self) -> Vec<ReconcileRequestHandle> {
        self.take_asked::<ReconcileCodec>()
            .into_iter()
            .map(ReconcileRequestHandle)
            .collect()
    }

    /// Answers a request obtained from [`Self::poll_reconcile_requests`].
    /// Fire-and-forget like `respond_claim`.
    pub fn respond_reconcile(&self, handle: ReconcileRequestHandle, page: ReconcileReport) {
        self.answer::<ReconcileCodec>(handle.0.channel, page);
    }
}
