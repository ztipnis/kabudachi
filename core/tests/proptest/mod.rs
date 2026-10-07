//! Property tests over configuration, flow and leadership invariants.

mod proptest_compaction_invariants;
mod proptest_configuration_invariants;
mod proptest_decode;
mod proptest_flow_invariants;
mod proptest_leadership_invariants;

use std::cell::Cell;
use std::time::{Duration, Instant};

use proptest::test_runner::Config;

/// The wall-clock time one property test may spend on cases. It bounds a run
/// on a slow host by truncating it, never by hanging it, and does not apply
/// when `PROPTEST_CASES` asks for a deep run.
const BUDGET: Duration = Duration::from_secs(60);

/// The configuration of a property test that checks `default_cases` cases
/// unless `PROPTEST_CASES` names another count.
pub(crate) fn config(default_cases: u32) -> Config {
    if std::env::var_os("PROPTEST_CASES").is_some() {
        return Config::default();
    }
    Config::with_cases(default_cases)
}

thread_local! {
    /// The start of the calling test's budget, the cases it has begun, and
    /// whether any case was skipped for lack of budget.
    static STATE: Cell<Option<(Instant, u32, bool)>> = const { Cell::new(None) };
}

/// Whether the budget for the calling test is spent. Call it first in every
/// case body and return early when it is true: the remaining cases then
/// pass at once. Under libtest's default multithreaded mode each test runs on
/// its own thread, and the budget starts at that thread's first case. The
/// first call after the budget runs out prints how many cases ran.
///
/// Shrinking after the budget is spent replays each candidate as a passing
/// case, so a failure found right at the budget is reported unshrunk.
pub(crate) fn budget_spent() -> bool {
    if std::env::var_os("PROPTEST_CASES").is_some() {
        return false;
    }
    STATE.with(|state| {
        let (start, ran, skipped) = state.get().unwrap_or((Instant::now(), 0, false));
        if start.elapsed() < BUDGET {
            state.set(Some((start, ran + 1, skipped)));
            return false;
        }
        if !skipped {
            eprintln!(
                "proptest budget of {} s spent after {ran} cases; the remaining cases were skipped",
                BUDGET.as_secs()
            );
        }
        state.set(Some((start, ran, true)));
        true
    })
}

/// Whether the calling test skipped any case because its budget was spent.
/// A run that did must not assert on what its cases covered, since fewer
/// cases ran than the assertion assumes.
pub(crate) fn budget_was_spent() -> bool {
    STATE.with(|state| state.get().is_some_and(|(_, _, skipped)| skipped))
}
