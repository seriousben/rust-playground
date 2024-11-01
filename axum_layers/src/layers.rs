use async_trait::async_trait;
use axum::{
    extract::{Path, Request},
    http::StatusCode,
    middleware::Next,
    response::Response,
    routing::Router,
};
use serde::Deserialize;
use std::future::Future;
use std::pin::Pin;
use tower_layer::Layer;
use tower_service::Service;

// Define the namespace path parameter structure
#[derive(Deserialize, Debug)]
struct NamespacePath {
    namespace: String,
}

// Trait for namespace validation
#[async_trait::async_trait]
pub trait NamespaceValidator: Clone + Send + Sync + 'static {
    async fn namespace_exists(&self, namespace: &str) -> bool;
}

// Example validator implementation
#[derive(Clone)]
pub struct ExampleValidator;

#[async_trait::async_trait]
impl NamespaceValidator for ExampleValidator {
    async fn namespace_exists(&self, namespace: &str) -> bool {
        // Replace this with your actual validation logic
        // e.g., database lookup, API call, etc.
        true
    }
}

// Layer struct that holds the validator
#[derive(Clone)]
pub struct ValidateNamespaceLayer<V> {
    validator: V,
}

impl<V: NamespaceValidator> ValidateNamespaceLayer<V> {
    pub fn new(validator: V) -> Self {
        Self { validator }
    }
}

impl<S, V> Layer<S> for ValidateNamespaceLayer<V>
where
    V: NamespaceValidator,
{
    type Service = ValidateNamespaceMiddleware<S, V>;

    fn layer(&self, inner: S) -> Self::Service {
        ValidateNamespaceMiddleware {
            inner,
            validator: self.validator.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ValidateNamespaceMiddleware<S, V> {
    inner: S,
    validator: V,
}

impl<S, V> Service<Request> for ValidateNamespaceMiddleware<S, V>
where
    S: Service<Request, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
    V: NamespaceValidator,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let validator = self.validator.clone();
        let inner_future = self.inner.call(req);

        Box::pin(async move {
            // Check if the path starts with /namespaces/
            let path = req.uri().path();
            if !path.starts_with("/namespaces/") {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(axum::body::Body::empty())
                    .unwrap());
            }

            let matched_path = req
                .extensions()
                .get::<MatchedPath>()
                .map(|matched_path| matched_path.as_str());

            // Extract and validate the namespace
            if let Ok(Path(NamespacePath { namespace })) =
                Path::<NamespacePath>::try_from(req.uri())
            {
                if validator.namespace_exists(&namespace).await {
                    return inner_future.await;
                }
            }

            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(axum::body::Body::empty())
                .unwrap())
        })
    }
}
