//! The worker's health endpoints, served on its own Tokio runtime when the
//! caller asks for them: `/healthz` answers 200 while the worker's native
//! loops run, and `/readyz` 200 while it is also ready to take work. Each
//! answers 503 otherwise.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::net::TcpListener;
use tokio::runtime::Handle;

/// How a runtime tells whether it is alive and ready; read on each request.
pub(crate) struct Probe {
    pub(crate) alive: Box<dyn Fn() -> bool + Send + Sync>,
    pub(crate) ready: Box<dyn Fn() -> bool + Send + Sync>,
}

/// Binds `listen` (`ip:port`; port 0 picks one), to be served by `serve`.
///
/// Raises `ValueError` for an address that is not `ip:port`, and
/// `RuntimeError` if it cannot be bound.
pub(crate) fn bind(listen: &str) -> PyResult<std::net::TcpListener> {
    let address: SocketAddr = listen.parse().map_err(|error| {
        PyValueError::new_err(format!("{listen:?} is not an ip:port address: {error}"))
    })?;
    let cannot = |error: std::io::Error| {
        PyRuntimeError::new_err(format!("cannot serve health on {listen:?}: {error}"))
    };
    let listener = std::net::TcpListener::bind(address).map_err(cannot)?;
    listener.set_nonblocking(true).map_err(cannot)?;
    Ok(listener)
}

/// Serves the endpoints on a listener `bind` made, on `handle`, until the
/// runtime shuts down. Returns the address bound.
///
/// Raises `RuntimeError` if the listener cannot be used.
pub(crate) fn serve(
    handle: &Handle,
    listener: std::net::TcpListener,
    probe: Probe,
) -> PyResult<SocketAddr> {
    let bound = listener
        .local_addr()
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let listener = {
        let _entered = handle.enter();
        TcpListener::from_std(listener)
    }
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(Arc::new(probe));
    handle.spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::warn!(target: "kabudachi_bindings::health", "the health server stopped: {error}");
        }
    });
    Ok(bound)
}

async fn healthz(State(probe): State<Arc<Probe>>) -> (StatusCode, &'static str) {
    if (probe.alive)() {
        (StatusCode::OK, "alive\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "stopped\n")
    }
}

async fn readyz(State(probe): State<Arc<Probe>>) -> (StatusCode, &'static str) {
    if (probe.alive)() && (probe.ready)() {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}
