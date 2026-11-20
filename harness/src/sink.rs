use anyhow::Result;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
struct Counts {
    presend: AtomicU64,
    postsend: AtomicU64,
}

fn router(counts: Arc<Counts>) -> Router {
    Router::new()
        .route(
            "/presend",
            post(|State(c): State<Arc<Counts>>| async move {
                c.presend.fetch_add(1, Ordering::Relaxed);
                Json(json!({"allow": true})) as Json<Value>
            }),
        )
        .route(
            "/postsend",
            post(|State(c): State<Arc<Counts>>| async move {
                c.postsend.fetch_add(1, Ordering::Relaxed);
                StatusCode::NO_CONTENT
            }),
        )
        .with_state(counts)
}

pub async fn run(listen: &str) -> Result<()> {
    let counts = Arc::new(Counts::default());
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, router(counts.clone()))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    println!("presend={} postsend={}", counts.presend.load(Ordering::Relaxed), counts.postsend.load(Ordering::Relaxed));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    #[tokio::test]
    async fn presend_allows() {
        let counts = Arc::new(Counts::default());
        let resp = router(counts.clone())
            .oneshot(Request::post("/presend").body(Body::from("{}")).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(&bytes[..], br#"{"allow":true}"#);
        assert_eq!(counts.presend.load(Ordering::Relaxed), 1);
    }
}
