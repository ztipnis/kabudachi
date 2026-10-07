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
/// many as fit in one message. A worker answers whichever leader asks: it
/// keeps no view of who leads, and its answer decides nothing by itself.
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

#[cfg(test)]
mod tests {
    use kabudachi_core::coordination_authority::RecoveryEpoch;
    use kabudachi_core::protocol::generated::{Claim, Task, TaskRecord};
    use kabudachi_core::protocol::ids::{TaskDefinitionId, WorkerId};
    use kabudachi_core::task_record::RecordVersion;
    use libp2p::kad::store::RecordStore;
    use libp2p::kad::Record;
    use prost::Message as _;

    use crate::task_store::{TaskRecordStore, record_key};

    use super::*;

    #[test]
    fn an_empty_worker_reports_one_empty_last_page() {
        let report = page_of(
            &ReconcileRequest::default(),
            &ClaimedRuns::default(),
            &HeldRecords::new(None),
        );

        assert!(report.last);
        assert!(report.runs.is_empty() && report.keys.is_empty());
    }

    fn held_task(held: &HeldRecords, id: &str) {
        let record = TaskRecord {
            version: Some(
                RecordVersion {
                    recovery_epoch: RecoveryEpoch::new(0, 0),
                    leader_term: 1,
                    revision: 1,
                }
                .into(),
            ),
            task: Some(Task {
                task_id: Some(TaskId::new(id).into()),
                task_definition_id: Some(TaskDefinitionId::new("definition").into()),
                queue: "q".to_owned(),
                ..Task::default()
            }),
            // A summary of the record carries its placement, which is what
            // makes the summaries add up to more than a page.
            placement: (0..8)
                .map(|n| WorkerId::new(format!("12D3KooW{n:0>40}")).into())
                .collect(),
            ..TaskRecord::default()
        };
        TaskRecordStore::new(held.clone())
            .put(Record::new(record_key(&TaskId::new(id)), record.encode_to_vec()))
            .expect("a well-formed record is stored");
    }

    /// Pages through a worker holding `run_count` runs, each carrying
    /// `padding` bytes, and `key_count` records, following the cursors a
    /// leader would. Returns every page.
    fn paged(run_count: usize, padding: usize, key_count: usize) -> Vec<ReconcileReport> {
        let runs = ClaimedRuns::default();
        for n in 0..run_count {
            runs.claimed(Claim {
                task: Some(Task {
                    queue: "x".repeat(padding),
                    ..Task::default()
                }),
                task_run_id: Some(TaskRunId::new(format!("run-{n:06}")).into()),
                ..Claim::default()
            });
        }
        let held = HeldRecords::new(None);
        for n in 0..key_count {
            held_task(&held, &format!("task-{n:06}"));
        }

        let mut pages = Vec::new();
        let mut after = None;
        loop {
            let request = ReconcileRequest {
                after,
                ..ReconcileRequest::default()
            };
            let page = page_of(&request, &runs, &held);
            assert!(
                page.encoded_len() + LAST_BYTES <= MAX_MESSAGE_BYTES as usize,
                "page {} encodes to {} bytes",
                pages.len(),
                page.encoded_len()
            );
            after = match (page.keys.last(), page.runs.last()) {
                (Some(key), _) => Some(reconcile_request::After::AfterKey(
                    key.task_id.clone().expect("a held key names its task"),
                )),
                (None, Some(run)) => Some(reconcile_request::After::AfterRun(
                    run.claim
                        .as_ref()
                        .and_then(|claim| claim.task_run_id.clone())
                        .expect("a reported run names itself"),
                )),
                (None, None) => None,
            };
            let last = page.last;
            pages.push(page);
            assert!(pages.len() < 1000, "paging does not end");
            if last {
                return pages;
            }
        }
    }

    fn assert_each_once_in_order(pages: &[ReconcileReport], run_count: usize, key_count: usize) {
        let runs: Vec<String> = pages
            .iter()
            .flat_map(|page| &page.runs)
            .map(|run| run.claim.as_ref().unwrap().task_run_id.as_ref().unwrap().value.clone())
            .collect();
        let keys: Vec<String> = pages
            .iter()
            .flat_map(|page| &page.keys)
            .map(|key| key.task_id.as_ref().unwrap().value.clone())
            .collect();
        let expected_runs: Vec<String> = (0..run_count).map(|n| format!("run-{n:06}")).collect();
        let expected_keys: Vec<String> = (0..key_count).map(|n| format!("task-{n:06}")).collect();
        assert_eq!(runs, expected_runs);
        assert_eq!(keys, expected_keys);
    }

    #[test]
    fn ten_thousand_runs_and_records_are_paged_once_each_in_order_within_one_message() {
        let pages = paged(10_000, 200, 10_000);

        assert_each_once_in_order(&pages, 10_000, 10_000);
        assert!(pages.iter().any(|page| !page.keys.is_empty() && !page.last), "an AfterKey cursor was followed");
    }

    #[test]
    fn a_page_the_runs_fill_leaves_the_records_to_the_next() {
        // Runs sized so that the last run fills a page and no key then fits:
        // found by sweeping the padding until a runs-only page precedes keys.
        let found = (0..64).find_map(|extra| {
            let pages = paged(8, 130_000 + extra * 997, 3);
            let at = pages.iter().position(|page| !page.keys.is_empty())?;
            (at > 0 && pages[at - 1].keys.is_empty() && !pages[at - 1].last).then_some(pages)
        });
        let pages = found.expect("some padding fills a page with runs alone");

        assert_each_once_in_order(&pages, 8, 3);
    }
}
