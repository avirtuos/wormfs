//! Middleware components for StorageEndpoint gRPC server.
//!
//! This module provides middleware layers for authentication, rate limiting,
//! and metrics collection.

#[cfg(feature = "tonic")]
pub mod auth;
#[cfg(feature = "tonic")]
pub mod metrics;
#[cfg(feature = "tonic")]
pub mod rate_limiter;

#[cfg(feature = "tonic")]
pub use auth::AuthInterceptor;
#[cfg(feature = "tonic")]
pub use metrics::MetricsMiddleware;
#[cfg(feature = "tonic")]
pub use rate_limiter::RateLimiter;
