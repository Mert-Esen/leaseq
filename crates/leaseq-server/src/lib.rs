//! HTTP front end: a JSON API, a live event stream and an embedded dashboard.
//!
//! # Security model
//!
//! There is **no authentication**. By default the server only listens on a
//! loopback address, and additionally defends against hostile web pages open in
//! the same browser:
//!
//! * the `Host` header must name the loopback interface (DNS rebinding);
//! * mutating requests must be JSON, which browsers cannot send cross-origin
//!   without a preflight that this server never approves, and any `Origin` header
//!   must match the host (cross-site request forgery).

mod api;
mod error;
mod guard;

use std::future::Future;
use std::io;
use std::net::SocketAddr;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::{Router, middleware};
use leaseq_engine::Engine;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Largest request body: the payload limit plus room for the envelope.
const BODY_LIMIT: usize = 320 * 1024;

/// Server options.
#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// Permit non-loopback listeners and any `Host`. Only safe behind an
    /// authenticating proxy or on a fully trusted network.
    pub allow_remote: bool,
}

impl Options {
    /// Rejects a listen address that other machines could reach, unless
    /// [`Options::allow_remote`] is set.
    ///
    /// [`serve`] enforces this itself; call it earlier to fail before opening a
    /// socket or a data directory.
    pub fn check_address(self, address: SocketAddr) -> io::Result<()> {
        if self.allow_remote || address.ip().is_loopback() {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "{address} is not a loopback address; there is no authentication, so remote access must be allowed explicitly"
            ),
        ))
    }
}

#[derive(Debug, Clone)]
struct AppState {
    engine: Engine,
    options: Options,
    /// Cancelled when the server begins shutting down, so that long polls and
    /// event streams end instead of holding the shutdown open.
    closing: CancellationToken,
}

fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/queues/{queue}/jobs", post(api::enqueue))
        .route("/queues/{queue}/lease", post(api::lease))
        .route("/jobs", get(api::jobs))
        .route("/jobs/{id}/ack", post(api::ack))
        .route("/jobs/{id}/nack", post(api::nack))
        .route("/jobs/{id}/heartbeat", post(api::heartbeat))
        .route("/jobs/{id}/retry", post(api::retry))
        .route("/stats", get(api::stats))
        .route("/events", get(api::events))
        .fallback(api::not_found);

    Router::new()
        .nest("/api", api)
        .route("/", get(api::index))
        .route("/app.js", get(api::script))
        .route("/style.css", get(api::style))
        .route("/healthz", get(api::health))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .layer(middleware::from_fn_with_state(state.clone(), guard::guard))
        .with_state(state)
}

/// Serves `engine` on `listener` until `shutdown` completes.
///
/// Fails immediately if the listener is reachable from other machines and
/// [`Options::allow_remote`] is not set. The engine is left running.
pub async fn serve(
    listener: TcpListener,
    engine: Engine,
    options: Options,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    options.check_address(listener.local_addr()?)?;
    let closing = CancellationToken::new();
    let app = router(AppState {
        engine,
        options,
        closing: closing.clone(),
    });
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.await;
            closing.cancel();
        })
        .await
}
