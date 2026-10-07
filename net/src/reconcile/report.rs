//! The answering side of a reconciliation: one page of what this worker
//! holds.

use kabudachi_core::protocol::generated;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
use kabudachi_core::protocol::messages::{ReconcileReport, ReconcileRequest, reconcile_request};
use prost::encoding::message;

use crate::claimed_runs::ClaimedRuns;
use crate::framing::MAX_MESSAGE_BYTES;
use crate::task_store::HeldRecords;

const RUNS_TAG: u32 = 1; // ReconcileReport.runs
const KEYS_TAG: u32 = 2; // ReconcileReport.keys
/// The encoded length of `last = true`: its key and the value.
const LAST_BYTES: usize = 2;

/// One page of this worker's answer to `request`: its runs (from `runs`)
/// after the cursor, then the records it holds (from `held`) after it, as
/// many as fit in one message. Whether `request` may be answered is not decided
/// here (see `WorkerNode::may_answer_reconcile`); the answer decides nothing
/// by itself.
pub(crate) fn page_of(
    request: &ReconcileRequest,
    runs: &ClaimedRuns,
    held: &HeldRecords,
) -> ReconcileReport {
    let mut page = Page::default();
    let mut exhausted = true;

    // A cursor that names a key says every run was already sent.
    let runs_after = match &request.after {
        Some(reconcile_request::After::AfterRun(run)) => Some(TaskRunId::from(run.clone())),
        _ => None,
    };
    let runs_sent = matches!(&request.after, Some(reconcile_request::After::AfterKey(_)));
    if !runs_sent {
        runs.reported_runs_after(runs_after.as_ref(), |run| {
            let fits = page.add_run(generated::ReportedRun::from(&run));
            exhausted &= fits;
            fits
        });
    }

    if exhausted && !request.runs_only {
        let keys_after = match &request.after {
            Some(reconcile_request::After::AfterKey(task)) => Some(TaskId::from(task.clone())),
            _ => None,
        };
        exhausted = held.keys_after(keys_after.as_ref(), |key| {
            page.add_key(generated::HeldKey::from(&key))
        });
    }

    page.report.last = exhausted;
    page.report
}

/// A page as items are added to it, so that it never outgrows one message.
#[derive(Default)]
struct Page {
    report: ReconcileReport,
    /// The encoded length of the report so far, without `last`.
    encoded_len: usize,
}

impl Page {
    /// Adds a run if the page still fits one message, and says whether it
    /// did. A page holds at least one item whatever its size: a run is a
    /// claim without its chain plus a few bytes, and a claim is sized to
    /// leave room for that, so one item alone always fits.
    fn add_run(&mut self, run: generated::ReportedRun) -> bool {
        let encoded_len = self.encoded_len + message::encoded_len(RUNS_TAG, &run);
        let fits = self.fits(encoded_len);
        if fits {
            self.report.runs.push(run);
            self.encoded_len = encoded_len;
        }
        fits
    }

    fn add_key(&mut self, key: generated::HeldKey) -> bool {
        let encoded_len = self.encoded_len + message::encoded_len(KEYS_TAG, &key);
        let fits = self.fits(encoded_len);
        if fits {
            self.report.keys.push(key);
            self.encoded_len = encoded_len;
        }
        fits
    }

    fn fits(&self, encoded_len: usize) -> bool {
        let empty = self.report.runs.is_empty() && self.report.keys.is_empty();
        empty || encoded_len + LAST_BYTES <= MAX_MESSAGE_BYTES as usize
    }
}
