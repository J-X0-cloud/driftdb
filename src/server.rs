//! HTTP API.
//!
//! | Method      | Path               | Description                                  |
//! |-------------|--------------------|----------------------------------------------|
//! | `GET`       | `/health`          | Liveness probe.                              |
//! | `POST`      | `/write`           | Line protocol body; `?precision=ns/us/ms/s`. |
//! | `GET`/`POST`| `/query`           | `?q=...` (or the POST body); `?format=table`.|
//! | `GET`       | `/stats`           | Engine statistics.                           |
//! | `POST`      | `/admin/flush`     | Flush the memtable.                          |
//! | `POST`      | `/admin/compact`   | Tiered compaction (`?full=true` for major).  |
//! | `POST`      | `/admin/retention` | Apply retention now.                         |
//!
//! Storage calls block on disk I/O, so every handler runs them on Tokio's
//! blocking pool. A background task periodically flushes, compacts, applies
//! retention and runs downsampling rules.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::db::Db;
use crate::downsample::{run_rule, DownsampleRule};
use crate::error::Error;
use crate::model::Precision;

/// Largest accepted request body.
pub const MAX_BODY_BYTES: usize = 64 << 20;

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to listen on.
    pub addr: SocketAddr,
    /// How often background maintenance runs; `None` disables it.
    pub maintenance_interval: Option<Duration>,
    /// Downsampling rules run by background maintenance.
    pub rules: Vec<DownsampleRule>,
}

/// Shared handler state.
#[derive(Debug, Clone)]
pub struct AppState {
    /// The database.
    pub db: Arc<Db>,
    /// Downsampling rules.
    pub rules: Arc<Vec<DownsampleRule>>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// Error type returned by handlers.
#[derive(Debug)]
pub struct ApiError(Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = if self.0.is_client_error() {
            StatusCode::BAD_REQUEST
        } else {
            tracing::error!(error = %self.0, "request failed");
            StatusCode::INTERNAL_SERVER_ERROR
        };
        (
            status,
            Json(ErrorBody {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

/// Run blocking engine work off the async executor.
async fn blocking<T, F>(f: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> crate::Result<T> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(ApiError),
        Err(join) => Err(ApiError(Error::Io(std::io::Error::other(format!(
            "worker task failed: {join}"
        ))))),
    }
}

/// Build the router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/write", post(write))
        .route("/query", get(query).post(query))
        .route("/stats", get(stats))
        .route("/admin/flush", post(flush))
        .route("/admin/compact", post(compact))
        .route("/admin/retention", post(retention))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

#[derive(Debug, Deserialize)]
struct WriteParams {
    precision: Option<String>,
}

async fn write(
    State(state): State<AppState>,
    Query(params): Query<WriteParams>,
    body: Bytes,
) -> ApiResult<Response> {
    let precision = match params.precision.as_deref() {
        Some(p) => p.parse::<Precision>()?,
        None => Precision::Nanoseconds,
    };
    let body = String::from_utf8(body.to_vec())
        .map_err(|_| Error::InvalidArgument("request body is not valid UTF-8".into()))?;
    let db = state.db.clone();
    let summary = blocking(move || db.write_lines(&body, precision)).await?;
    if summary.expired > 0 {
        return Ok((StatusCode::OK, Json(summary)).into_response());
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    q: Option<String>,
    format: Option<String>,
}

async fn query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
    body: Bytes,
) -> ApiResult<Response> {
    let q = match params.q {
        Some(q) => q,
        None => String::from_utf8(body.to_vec())
            .map_err(|_| Error::InvalidArgument("query body is not valid UTF-8".into()))?,
    };
    if q.trim().is_empty() {
        return Err(Error::InvalidArgument("missing query: pass ?q= or a request body".into()).into());
    }
    let db = state.db.clone();
    let result = blocking(move || db.query(&q)).await?;
    match params.format.as_deref() {
        None | Some("json") => Ok(Json(result).into_response()),
        Some("table") => Ok((
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            result.to_table(true),
        )
            .into_response()),
        Some(other) => Err(Error::InvalidArgument(format!(
            "unknown format {other:?} (expected json or table)"
        ))
        .into()),
    }
}

async fn stats(State(state): State<AppState>) -> ApiResult<Response> {
    let db = state.db.clone();
    Ok(Json(blocking(move || db.stats()).await?).into_response())
}

async fn flush(State(state): State<AppState>) -> ApiResult<Response> {
    let db = state.db.clone();
    let meta = blocking(move || db.flush()).await?;
    Ok(Json(serde_json::json!({ "segment": meta })).into_response())
}

#[derive(Debug, Deserialize)]
struct CompactParams {
    full: Option<bool>,
}

async fn compact(
    State(state): State<AppState>,
    Query(params): Query<CompactParams>,
) -> ApiResult<Response> {
    let db = state.db.clone();
    let reports = blocking(move || {
        if params.full.unwrap_or(false) {
            db.compact_all().map(|r| r.into_iter().collect::<Vec<_>>())
        } else {
            db.compact()
        }
    })
    .await?;
    Ok(Json(serde_json::json!({ "compactions": reports })).into_response())
}

async fn retention(State(state): State<AppState>) -> ApiResult<Response> {
    let db = state.db.clone();
    let removed = blocking(move || db.enforce_retention()).await?;
    Ok(Json(serde_json::json!({ "segments_removed": removed })).into_response())
}

/// One pass of background maintenance.
pub fn maintenance_pass(db: &Db, rules: &[DownsampleRule]) -> crate::Result<()> {
    for rule in rules {
        let report = run_rule(db, rule)?;
        if report.points > 0 {
            tracing::info!(target_measurement = %report.target, points = report.points, "downsampled");
        }
    }
    db.flush()?;
    db.compact()?;
    db.enforce_retention()?;
    Ok(())
}

/// Serve until Ctrl-C or SIGTERM, then flush and return.
pub async fn serve(db: Arc<Db>, config: ServerConfig) -> crate::Result<()> {
    let state = AppState {
        db: db.clone(),
        rules: Arc::new(config.rules.clone()),
    };
    let maintenance = config.maintenance_interval.map(|every| {
        let state = state.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let db = state.db.clone();
                let rules = state.rules.clone();
                match tokio::task::spawn_blocking(move || maintenance_pass(&db, &rules)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::error!(error = %e, "maintenance failed"),
                    Err(e) => tracing::error!(error = %e, "maintenance task panicked"),
                }
            }
        })
    });

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(addr = %listener.local_addr()?, dir = %db.dir().display(), "driftdb listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    if let Some(task) = maintenance {
        task.abort();
    }
    tracing::info!("shutting down; flushing memtable");
    let flush_db = db.clone();
    tokio::task::spawn_blocking(move || flush_db.flush())
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))??;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "failed to listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Options;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> (tempfile::TempDir, Router) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options {
                sync_wal: false,
                ..Options::default()
            },
        )
        .unwrap();
        let state = AppState {
            db: Arc::new(db),
            rules: Arc::new(Vec::new()),
        };
        (dir, router(state))
    }

    async fn send(app: &Router, req: Request<Body>) -> (StatusCode, String) {
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    fn post(uri: &str, body: &str) -> Request<Body> {
        Request::post(uri).body(Body::from(body.to_string())).unwrap()
    }

    fn get(uri: &str) -> Request<Body> {
        Request::get(uri).body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn write_then_query_roundtrip() {
        let (_dir, app) = app();
        let (status, _) = send(&app, post("/write?precision=s", "cpu,host=a usage=1.5 1\ncpu,host=a usage=2.5 2")).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, body) = send(&app, get("/query?q=SELECT%20mean(usage)%20FROM%20cpu")).await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["series"][0]["values"][0][1], 2.0);

        let (status, body) = send(&app, post("/query?format=table", "SELECT usage FROM cpu")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("1970-01-01T00:00:01Z  1.5"), "{body}");
    }

    #[tokio::test]
    async fn client_errors_are_400_with_json() {
        let (_dir, app) = app();
        for req in [
            post("/write", "cpu usage=oops"),
            post("/write?precision=fortnights", "cpu usage=1"),
            get("/query?q=SELEKT%201"),
            get("/query"),
            get("/query?q=SHOW%20MEASUREMENTS&format=xml"),
        ] {
            let (status, body) = send(&app, req).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            let json: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert!(json["error"].as_str().is_some_and(|e| !e.is_empty()));
        }
        let (status, _) = send(
            &app,
            Request::post("/write").body(Body::from(vec![0xffu8, 0xfe])).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn admin_endpoints_and_stats() {
        let (_dir, app) = app();
        send(&app, post("/write", "m v=1 1\nm v=2 2")).await;
        let (status, body) = send(&app, post("/admin/flush", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("\"points\":2"), "{body}");
        let (_, body) = send(&app, post("/admin/compact?full=true", "")).await;
        assert!(body.contains("compactions"), "{body}");
        let (_, body) = send(&app, post("/admin/retention", "")).await;
        assert_eq!(body, r#"{"segments_removed":0}"#);
        let (status, body) = send(&app, get("/stats")).await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["series"], 1);
        assert_eq!(json["segments"].as_array().unwrap().len(), 1);
        let (status, body) = send(&app, get("/health")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("ok"));
    }

    #[tokio::test]
    async fn maintenance_pass_flushes_and_downsamples() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(crate::ManualClock::new(10 * 60 * 1_000_000_000));
        let db = Db::open(
            dir.path(),
            Options {
                sync_wal: false,
                clock,
                ..Options::default()
            },
        )
        .unwrap();
        db.write_lines("m v=1 0\nm v=3 1000", Precision::Nanoseconds).unwrap();
        let rules = vec!["m:1m:mean".parse().unwrap()];
        maintenance_pass(&db, &rules).unwrap();
        let stats = db.stats().unwrap();
        assert_eq!(stats.memtable_points, 0);
        let r = db.query("SELECT v_mean FROM m_1m").unwrap();
        assert_eq!(r.series[0].values[0][1].as_f64(), Some(2.0));
    }
}
