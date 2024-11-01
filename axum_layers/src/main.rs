use axum::{
    extract::{MatchedPath, Path, RawPathParams, Request},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// mod layers;

#[derive(Serialize, Deserialize)]
struct Namespace {
    id: String,
    name: String,
}

async fn get_namespace(Path(id): Path<String>) -> impl IntoResponse {
    let name = format!("Namespace {}", id);
    let user = Namespace { id, name };
    axum::Json(user)
}

async fn get_namespace_key(Path((id, key)): Path<(String, String)>) -> impl IntoResponse {
    axum::Json(format!("Namespace {} key {}", id, key))
}

async fn health_check() -> StatusCode {
    StatusCode::OK
}

async fn namespace_middleware(params: RawPathParams, request: Request, next: Next) -> Response {
    let namespace_param = params.iter().find(|(key, _)| *key == "namespace");
    if let Some((_, namespace)) = namespace_param {
        let path = if let Some(path) = request.extensions().get::<MatchedPath>() {
            path.as_str()
        } else {
            request.uri().path()
        };
        tracing::info!("Matched namespace path: {} = {}", path, namespace);
        if namespace == "invalid" {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("Namespace not found".into())
                .unwrap();
        }
    } else {
        tracing::info!("Not matching {:?}", namespace_param);
        for (key, value) in &params {
            tracing::info!("{key:?} = {value:?}");
        }
    }

    next.run(request).await
}

pub fn create_app() -> Router {
    Router::new()
        .route("/namespaces/:namespace", get(get_namespace))
        .route("/namespaces/:namespace/keys/:key", get(get_namespace_key))
        .route("/health", get(health_check))
        .layer(
            TraceLayer::new_for_http().make_span_with(|req: &Request<_>| {
                let path = if let Some(path) = req.extensions().get::<MatchedPath>() {
                    path.as_str()
                } else {
                    req.uri().path()
                };
                tracing::info_span!("http-request", %path)
            }),
        )
        .layer(middleware::from_fn(namespace_middleware))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "example_app=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = create_app();

    // Run it
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn test_get_namespace() {
        let app = create_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/namespaces/123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let namespace: Namespace = serde_json::from_slice(&body).unwrap();

        assert_eq!(namespace.id, "123");
        assert_eq!(namespace.name, "Namespace 123");
    }

    #[tokio::test]
    async fn test_get_invalid_namespace() {
        let app = create_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/namespaces/invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_health_check() {
        let app = create_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_not_found() {
        let app = create_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
