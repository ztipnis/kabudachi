//! A bound on how long a whole test may take, so that a hang names the test
//! long before Bazel's guard on the whole binary ends every test at once.

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::Thread;
use std::time::{Duration, Instant};

/// Generous for a two-vCPU CI host: the slowest test takes about ten seconds
/// there, and the guard on the whole binary is 300 s.
const DEADLINE: Duration = Duration::from_secs(60);

/// Runs `body`, the whole of a test, and aborts the process naming the test if
/// it has not finished once [`DEADLINE`] of real time has passed. The test is
/// named as libtest names the thread it runs on.
///
/// A thread of its own measures the deadline against the operating system's
/// clock, not tokio's, and it aborts rather than failing the test through the
/// test's task: a test that pauses tokio's time never lets it advance, and a
/// test that blocks its runtime thread never polls anything, yet in both the
/// watchdog still runs and names the test.
///
/// The deadline also covers the test's runtime shutting down, after `body`
/// has returned or panicked: the shutdown waits for every blocking thread
/// the test started, a node's authority calls among them. So the watchdog is
/// kept until the test's thread ends, which libtest's thread does only once
/// the runtime it built for the test is gone.
pub async fn within_deadline<T>(body: impl Future<Output = T>) -> T {
    let test = std::thread::current().name().unwrap_or("a test").to_owned();
    UNTIL_THREAD_ENDS.with(|watchdog| *watchdog.borrow_mut() = Some(Watchdog::start(test)));
    body.await
}

thread_local! {
    /// The test thread's watchdog, dropped when the thread ends.
    static UNTIL_THREAD_ENDS: std::cell::RefCell<Option<Watchdog>> =
        const { std::cell::RefCell::new(None) };
}

/// A thread that names the test and aborts the process once [`DEADLINE`] has
/// passed, unless dropped first.
struct Watchdog {
    done: Arc<AtomicBool>,
    thread: Thread,
}

impl Watchdog {
    fn start(test: String) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let watching = done.clone();
        let thread = std::thread::spawn(move || {
            let end = Instant::now() + DEADLINE;
            while !watching.load(Ordering::SeqCst) {
                let left = end.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    // Not `eprintln!`: libtest captures that from threads a test spawns.
                    let _ = writeln!(
                        std::io::stderr(),
                        "{test} did not finish within {DEADLINE:?}"
                    );
                    std::process::abort();
                }
                std::thread::park_timeout(left);
            }
        })
        .thread()
        .clone();
        Watchdog { done, thread }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
        self.thread.unpark();
    }
}
