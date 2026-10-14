//! What Python hands the runtimes as one object rather than a list of
//! arguments: a submission, read from the attributes of a
//! `kabudachi.native_protocol.SubmitArguments`, and the checks every
//! runtime makes of its own arguments.

use kabudachi_core::protocol::ids::TaskDefinitionId;
use kabudachi_core::scheduler::{MemoryLimits, Submission};
use kabudachi_core::time::Duration as CoreDuration;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::outcomes::coalescing;

/// One task to submit. `kind` is the task's `TaskKind` value; a
/// coalescing task always has a `key`, and no other kind has one.
#[derive(FromPyObject)]
pub(crate) struct SubmitArguments {
    definition_id: String,
    source_version: u32,
    serialized_input: Py<PyBytes>,
    kind: String,
    key: Option<String>,
    retries: u32,
    drop_oldest: bool,
    non_retriable: bool,
    options: RequestedTiming,
    settings: ResolvedSettings,
    /// How long the finished task is kept, in milliseconds: its own choice, or
    /// `None` for the shard's. Zero keeps no result.
    #[pyo3(default)]
    result_ttl_ms: Option<u64>,
}

/// What the caller asked of this one submission, read from a
/// `kabudachi.options.SubmissionOptions`. Its `key` is not read here: the
/// key the submission carries is `SubmitArguments.key`, resolved for the
/// task's kind.
#[derive(FromPyObject)]
struct RequestedTiming {
    delay_ms: Option<u64>,
    expires_in_ms: Option<u64>,
}

/// What the submitting process resolved for this submission, read from a
/// `kabudachi.options.SubmissionSettings`.
#[derive(FromPyObject)]
struct ResolvedSettings {
    queue: String,
    reconnect_timeout_ms: Option<u64>,
}

impl SubmitArguments {
    /// The core submission these arguments describe.
    ///
    /// Raises `ValueError` if `kind`, `key` and `drop_oldest` disagree, or
    /// the reconnect timeout is zero.
    pub(crate) fn into_submission(self, py: Python<'_>) -> PyResult<Submission> {
        let coalescing =
            coalescing(&self.kind, self.key, self.drop_oldest).map_err(PyValueError::new_err)?;
        let mut submission = Submission::new(
            TaskDefinitionId::new(self.definition_id),
            self.source_version,
            self.serialized_input.as_bytes(py).to_vec(),
            self.settings.queue,
        )
        .with_retries(self.retries);
        if self.kind == "ephemeral" {
            submission = submission.ephemeral();
        }
        if self.non_retriable {
            submission = submission.non_retriable();
        }
        if let Some(ttl_ms) = self.result_ttl_ms {
            submission = submission.with_result_ttl(CoreDuration::from_millis(ttl_ms));
        }
        let submission = timed(
            submission,
            self.options.delay_ms,
            self.options.expires_in_ms,
            self.settings.reconnect_timeout_ms,
        )?;
        Ok(coalesced(submission, coalescing))
    }
}

/// `submission` with its delay, expiry and reconnect timeout, each when
/// given. Raises `ValueError` for a reconnect timeout of zero.
fn timed(
    mut submission: Submission,
    delay_ms: Option<u64>,
    expires_in_ms: Option<u64>,
    reconnect_timeout_ms: Option<u64>,
) -> PyResult<Submission> {
    if let Some(delay_ms) = delay_ms {
        submission = submission.with_delay(CoreDuration::from_millis(delay_ms));
    }
    if let Some(expires_in_ms) = expires_in_ms {
        submission = submission.with_expiry(CoreDuration::from_millis(expires_in_ms));
    }
    match reconnect_timeout_ms {
        Some(0) => Err(PyValueError::new_err("reconnect_timeout_ms must be positive")),
        Some(timeout_ms) => {
            Ok(submission.with_reconnect_timeout(CoreDuration::from_millis(timeout_ms)))
        }
        None => Ok(submission),
    }
}

/// `submission` with the coalescing key and drop-oldest opt-in
/// `coalescing` names, if any.
fn coalesced(submission: Submission, coalescing: Option<(String, bool)>) -> Submission {
    match coalescing {
        Some((key, true)) => submission.with_coalescing_key(key).with_drop_oldest(),
        Some((key, false)) => submission.with_coalescing_key(key),
        None => submission,
    }
}

/// How long, in milliseconds, a task may wait blocked by workers that cannot
/// run its input version before it fails: the one given, or `None` for the
/// scheduler's default. Raises `ValueError` for zero.
pub(crate) fn version_block_ttl_ms(ttl_ms: Option<u64>) -> PyResult<Option<u64>> {
    match ttl_ms {
        Some(0) => Err(PyValueError::new_err("version_block_ttl_ms must be positive")),
        other => Ok(other),
    }
}

/// Both memory limits, or neither. Raises `ValueError` for only one.
pub(crate) fn memory_limits(soft: Option<u64>, hard: Option<u64>) -> PyResult<Option<MemoryLimits>> {
    match (soft, hard) {
        (None, None) => Ok(None),
        (Some(soft), Some(hard)) => Ok(Some(MemoryLimits { soft, hard })),
        _ => Err(PyValueError::new_err("give both memory limits or neither")),
    }
}
