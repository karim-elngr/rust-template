use axum::{Router, extract::State, http::StatusCode, routing::get};
use commerce::Store;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Health {
    pub store: Store,
    pub cancel: CancellationToken,
    pub dependency_ready: Arc<AtomicBool>,
}

impl Health {
    pub fn new(store: Store, cancel: CancellationToken, ready: bool) -> Self {
        Self {
            store,
            cancel,
            dependency_ready: Arc::new(AtomicBool::new(ready)),
        }
    }

    pub async fn ready(&self) -> bool {
        if self.cancel.is_cancelled() || !self.dependency_ready.load(Ordering::Relaxed) {
            return false;
        }
        matches!(
            tokio::time::timeout(Duration::from_secs(1), self.store.check_ready()).await,
            Ok(Ok(()))
        )
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/health/live", get(|| async { StatusCode::OK }))
            .route("/health/ready", get(readiness))
            .with_state(self)
    }
}

async fn readiness(State(health): State<Health>) -> StatusCode {
    if health.ready().await {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn liveness_does_not_depend_on_database_and_draining_is_not_ready() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let health = Health::new(Store::new(pool), cancel, true);
        let response = health
            .clone()
            .router()
            .oneshot(Request::get("/health/live").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = health
            .router()
            .oneshot(Request::get("/health/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
