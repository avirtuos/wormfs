//! Tower middleware layers for gRPC services.
//!
//! Provides tower::Service implementations that wrap auth, rate limiting, and metrics.

use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::Status;
use tower::{Layer, Service};

use super::{AuthInterceptor, MetricsMiddleware, RateLimiter};
use crate::metric_service::MetricService;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ============================================================================
// Auth Middleware
// ============================================================================

/// Layer that adds authentication checking.
#[derive(Clone)]
pub struct AuthLayer {
    auth: Arc<AuthInterceptor>,
}

impl AuthLayer {
    pub fn new(auth: AuthInterceptor) -> Self {
        Self {
            auth: Arc::new(auth),
        }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthMiddleware {
            inner: service,
            auth: self.auth.clone(),
        }
    }
}

/// Service that performs authentication checking.
#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    auth: Arc<AuthInterceptor>,
}

impl<S> Service<http::Request<tonic::transport::Body>> for AuthMiddleware<S>
where
    S: Service<http::Request<tonic::transport::Body>, Response = http::Response<BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<tonic::transport::Body>) -> Self::Future {
        let auth = self.auth.clone();
        let mut inner = self.inner.clone();
        // Important: swap self with the clone to avoid issues with &mut self
        std::mem::swap(&mut self.inner, &mut inner);

        Box::pin(async move {
            // Extract metadata from HTTP headers
            let identity = req
                .headers()
                .get("x-wormfs-identity")
                .and_then(|v| v.to_str().ok());

            let psk = req.headers().get("x-wormfs-psk").map(|v| v.as_bytes());

            // Validate auth if enabled
            if auth.is_enabled() {
                if let (Some(identity), Some(psk)) = (identity, psk) {
                    if let Err(e) = auth.validate_credentials(identity, psk).await {
                        // Return error response
                        let status = e;
                        let response = status_to_http_response(status);
                        return Ok(response);
                    }
                } else {
                    // Missing auth headers
                    let status = Status::unauthenticated("Missing authentication headers");
                    let response = status_to_http_response(status);
                    return Ok(response);
                }
            }

            // Auth passed, forward to inner service
            inner.call(req).await.map_err(Into::into)
        })
    }
}

// ============================================================================
// Rate Limit Middleware
// ============================================================================

/// Layer that adds rate limiting.
#[derive(Clone)]
pub struct RateLimitLayer {
    limiter: Arc<RateLimiter>,
}

impl RateLimitLayer {
    pub fn new(limiter: RateLimiter) -> Self {
        Self {
            limiter: Arc::new(limiter),
        }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        RateLimitMiddleware {
            inner: service,
            limiter: self.limiter.clone(),
        }
    }
}

/// Service that performs rate limiting.
#[derive(Clone)]
pub struct RateLimitMiddleware<S> {
    inner: S,
    limiter: Arc<RateLimiter>,
}

impl<S> Service<http::Request<tonic::transport::Body>> for RateLimitMiddleware<S>
where
    S: Service<http::Request<tonic::transport::Body>, Response = http::Response<BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<tonic::transport::Body>) -> Self::Future {
        let limiter = self.limiter.clone();
        let mut inner = self.inner.clone();
        std::mem::swap(&mut self.inner, &mut inner);

        Box::pin(async move {
            // Extract client identity
            let identity = req
                .headers()
                .get("x-wormfs-identity")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("anonymous");

            // Check rate limit
            if let Err(status) = limiter.check_limit(identity).await {
                let response = status_to_http_response(status);
                return Ok(response);
            }

            // Rate limit passed, forward to inner service
            inner.call(req).await.map_err(Into::into)
        })
    }
}

// ============================================================================
// Metrics Middleware
// ============================================================================

/// Layer that adds metrics collection.
#[derive(Clone)]
pub struct MetricsLayer<M: MetricService> {
    metrics: Arc<MetricsMiddleware<M>>,
}

impl<M: MetricService + Clone> MetricsLayer<M> {
    pub fn new(metrics: MetricsMiddleware<M>) -> Self {
        Self {
            metrics: Arc::new(metrics),
        }
    }
}

impl<S, M> Layer<S> for MetricsLayer<M>
where
    M: MetricService + Clone,
{
    type Service = MetricsMiddlewareService<S, M>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsMiddlewareService {
            inner: service,
            metrics: self.metrics.clone(),
        }
    }
}

/// Service that collects metrics.
#[derive(Clone)]
pub struct MetricsMiddlewareService<S, M: MetricService> {
    inner: S,
    metrics: Arc<MetricsMiddleware<M>>,
}

impl<S, M> Service<http::Request<tonic::transport::Body>> for MetricsMiddlewareService<S, M>
where
    S: Service<http::Request<tonic::transport::Body>, Response = http::Response<BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    M: MetricService + Clone + 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<tonic::transport::Body>) -> Self::Future {
        let metrics = self.metrics.clone();
        let mut inner = self.inner.clone();
        std::mem::swap(&mut self.inner, &mut inner);

        Box::pin(async move {
            // Extract service and method from URI path
            // Format: /package.ServiceName/MethodName
            let path = req.uri().path();
            let parts: Vec<&str> = path.split('/').collect();
            let (service, method) = if parts.len() >= 3 {
                // parts[0] is empty, parts[1] is package.ServiceName, parts[2] is MethodName
                let service_parts: Vec<&str> = parts[1].split('.').collect();
                let service_name = service_parts.last().unwrap_or(&"unknown");
                (service_name.to_string(), parts[2].to_string())
            } else {
                ("unknown".to_string(), "unknown".to_string())
            };

            // Record request start
            let start = metrics.request_start(&service, &method);

            // Call inner service
            let result = inner.call(req).await;

            // Record request end
            let success = result.is_ok();
            metrics.request_end(&service, &method, start, success);

            result.map_err(Into::into)
        })
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert a tonic Status to an HTTP response.
fn status_to_http_response(status: Status) -> http::Response<BoxBody> {
    let mut response = http::Response::new(BoxBody::default());
    *response.status_mut() = status_code_from_tonic_code(status.code());

    // Add grpc-status and grpc-message headers
    response.headers_mut().insert(
        "grpc-status",
        http::HeaderValue::from_str(&(status.code() as i32).to_string()).unwrap(),
    );
    response.headers_mut().insert(
        "grpc-message",
        http::HeaderValue::from_str(status.message())
            .unwrap_or_else(|_| http::HeaderValue::from_static("error")),
    );

    response
}

/// Convert tonic status code to HTTP status code.
fn status_code_from_tonic_code(code: tonic::Code) -> http::StatusCode {
    match code {
        tonic::Code::Ok => http::StatusCode::OK,
        tonic::Code::Cancelled => http::StatusCode::from_u16(499).unwrap(),
        tonic::Code::Unknown => http::StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::InvalidArgument => http::StatusCode::BAD_REQUEST,
        tonic::Code::DeadlineExceeded => http::StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::NotFound => http::StatusCode::NOT_FOUND,
        tonic::Code::AlreadyExists => http::StatusCode::CONFLICT,
        tonic::Code::PermissionDenied => http::StatusCode::FORBIDDEN,
        tonic::Code::ResourceExhausted => http::StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::FailedPrecondition => http::StatusCode::BAD_REQUEST,
        tonic::Code::Aborted => http::StatusCode::CONFLICT,
        tonic::Code::OutOfRange => http::StatusCode::BAD_REQUEST,
        tonic::Code::Unimplemented => http::StatusCode::NOT_IMPLEMENTED,
        tonic::Code::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::DataLoss => http::StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::Unauthenticated => http::StatusCode::UNAUTHORIZED,
    }
}
