//! Native log records into Python's `logging`, under `kabudachi.native.*`.
//!
//! `tracing` events become `log` records (tracing's `log` feature, with no
//! `tracing` subscriber installed). The process-wide `log` logger here only
//! formats each record and queues it, so a native thread never waits for the
//! GIL, even one holding a lock a Python thread is waiting on. One thread
//! drains the queue into `pyo3-log`, which names the Python logger from the
//! record's target (`kabudachi.native.<crate>.<module>`) and caches loggers
//! and their levels. A full queue drops records, and the number dropped is
//! logged once it drains.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, OnceLock};

use log::{Level, LevelFilter, Log, Metadata, Record};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_log::{Caching, Logger, ResetHandle};

const QUEUE_LENGTH: usize = 4096;
const PREFIX: &str = "kabudachi.native";
const TARGET: &str = "kabudachi_bindings::native_log";

struct Line {
    level: Level,
    target: String,
    message: String,
}

/// The process-wide `log` logger. Its `log` runs on whatever thread emitted
/// the record, so it must never take the GIL or wait for anything: it formats
/// the record and `try_send`s it, and counts it as dropped if the queue is full.
struct Forwarding {
    lines: SyncSender<Line>,
    dropped: Arc<AtomicU64>,
}

impl Log for Forwarding {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let line = Line {
            level: record.level(),
            target: record.target().to_owned(),
            message: record.args().to_string(),
        };
        // Never `send`: that would wait for room, and the thread that waits
        // may hold a lock the thread holding the GIL is waiting for.
        if let Err(TrySendError::Full(_)) = self.lines.try_send(line) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn flush(&self) {}
}

static CACHE: OnceLock<ResetHandle> = OnceLock::new();

/// Set by an `atexit` callback, which runs before the interpreter starts to
/// finalize: from then on records are dropped instead of forwarded.
static STOPPED: AtomicBool = AtomicBool::new(false);

#[pyfunction]
fn stop_forwarding() {
    STOPPED.store(true, Ordering::SeqCst);
}

/// Installs the forwarding logger and starts the thread that hands its
/// records to Python, at the level Python's `logging` sets for
/// `kabudachi.native` now. A second call, or a logger installed already by
/// someone else, changes nothing. Records emitted once the interpreter begins
/// to exit are dropped.
pub(crate) fn install(py: Python<'_>) -> PyResult<()> {
    let python = Logger::new(py, Caching::LoggersAndLevels)?
        .set_prefix(PREFIX)
        .filter(LevelFilter::Trace);
    let cache = python.reset_handle();
    let level = effective_level(py)?;
    let (lines, queue) = sync_channel(QUEUE_LENGTH);
    let dropped = Arc::new(AtomicU64::new(0));
    let forwarding = Forwarding {
        lines,
        dropped: Arc::clone(&dropped),
    };
    // Spawned before the logger is installed, so nothing that can fail runs
    // after it. If installing fails, `forwarding` drops with its sender and
    // the thread ends at once.
    std::thread::Builder::new()
        .name("kabudachi-log".into())
        .spawn(move || forward(queue, python, dropped))
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    if log::set_boxed_logger(Box::new(forwarding)).is_err() {
        return Ok(());
    }
    log::set_max_level(level);
    let _ = CACHE.set(cache);
    py.import("atexit")?
        .call_method1("register", (wrap_pyfunction!(stop_forwarding, py)?,))?;
    Ok(())
}

/// The only code that takes the GIL for a log record: it runs on one thread
/// that holds no lock of this crate's and that no other thread waits for.
///
/// Attaching to a finalizing interpreter is undefined on some builds, so no
/// record is forwarded once `STOPPED` is set. A record already waiting for the
/// GIL when finalization starts can still race with it; that window is
/// accepted. Each record is logged inside `Python::try_attach` so that
/// `pyo3-log`'s own attach finds the thread attached and never has to
/// initialize the interpreter.
fn forward(queue: Receiver<Line>, python: Logger, dropped: Arc<AtomicU64>) {
    while let Ok(line) = queue.recv() {
        if STOPPED.load(Ordering::SeqCst) {
            continue;
        }
        let lost = dropped.swap(0, Ordering::Relaxed);
        Python::try_attach(|_| {
            python.log(
                &Record::builder()
                    .level(line.level)
                    .target(&line.target)
                    .args(format_args!("{}", line.message))
                    .build(),
            );
            if lost > 0 {
                python.log(
                    &Record::builder()
                        .level(Level::Warn)
                        .target(TARGET)
                        .args(format_args!(
                            "{lost} native log records were dropped: the queue to Python was full"
                        ))
                        .build(),
                );
            }
        });
    }
}

/// Reads again the level Python's `logging` sets for `kabudachi.native` and
/// forgets the levels cached for it and its children: what `logging` was
/// configured with by now applies from here on. A record below
/// `kabudachi.native`'s own effective level is never forwarded, whatever level
/// a child logger is set to.
#[pyfunction]
pub(crate) fn refresh_logging(py: Python<'_>) -> PyResult<()> {
    if let Some(cache) = CACHE.get() {
        cache.reset();
    }
    log::set_max_level(effective_level(py)?);
    Ok(())
}

fn effective_level(py: Python<'_>) -> PyResult<LevelFilter> {
    let level: i64 = py
        .import("logging")?
        .call_method1("getLogger", (PREFIX,))?
        .call_method0("getEffectiveLevel")?
        .extract()?;
    Ok(match level {
        i64::MIN..=5 =>LevelFilter::Trace,
        6..=10 => LevelFilter::Debug,
        11..=20 => LevelFilter::Info,
        21..=30 => LevelFilter::Warn,
        _ => LevelFilter::Error,
    })
}
