//! Two-level rate limiting middleware for gRPC requests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::Status;
use tracing::{debug, warn};

/// Token bucket for rate limiting.
///
/// Implements the token bucket algorithm where tokens are refilled at a constant rate
/// and requests consume tokens. When tokens are depleted, requests are rejected.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    rate: f64,       // tokens per second
    max_tokens: f64, // burst capacity
}

impl TokenBucket {
    /// Create a new token bucket.
    ///
    /// # Arguments
    ///
    /// * `rate` - Tokens per second (requests per second)
    /// * `burst` - Maximum burst capacity
    fn new(rate: f64, burst: f64) -> Self {
        Self {
            tokens: burst,
            last_refill: Instant::now(),
            rate,
            max_tokens: burst,
        }
    }

    /// Try to acquire a token for a request.
    ///
    /// # Returns
    ///
    /// `true` if a token was acquired, `false` if the bucket is empty.
    fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();

        // Refill tokens based on elapsed time
        self.tokens = (self.tokens + elapsed * self.rate).min(self.max_tokens);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Refund a token (used when a request is rejected after token acquisition).
    ///
    /// This adds one token back to the bucket, capped at max_tokens.
    fn refund(&mut self) {
        self.tokens = (self.tokens + 1.0).min(self.max_tokens);
    }

    /// Check if this bucket has been idle for longer than the given duration.
    ///
    /// A bucket is considered idle if it hasn't been accessed (no try_acquire calls)
    /// for longer than the specified duration.
    fn is_idle(&self, idle_duration: Duration) -> bool {
        Instant::now().duration_since(self.last_refill) > idle_duration
    }

    /// Get the current number of available tokens.
    fn available_tokens(&self) -> f64 {
        self.tokens
    }
}

/// Two-level rate limiter: per-client and overall.
///
/// Provides both per-client rate limiting (keyed by identity) and overall
/// node-level rate limiting to prevent resource exhaustion.
#[derive(Clone)]
pub struct RateLimiter {
    /// Per-client rate limiters (keyed by identity)
    per_client: Arc<RwLock<HashMap<String, TokenBucket>>>,
    /// Overall rate limiter
    overall: Arc<RwLock<TokenBucket>>,
    /// Configuration
    per_client_rate: f64,
    overall_rate: f64,
    burst_size: f64,
    /// Last cleanup time
    last_cleanup: Arc<RwLock<Instant>>,
    /// Cleanup interval
    cleanup_interval: Duration,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// # Arguments
    ///
    /// * `per_client_rate` - Requests per second per client identity
    /// * `overall_rate` - Total requests per second for the node
    /// * `burst_size` - Burst capacity
    ///
    /// # Returns
    ///
    /// A new RateLimiter instance.
    pub fn new(
        per_client_rate: Option<usize>,
        overall_rate: Option<usize>,
        burst_size: usize,
    ) -> Self {
        let per_client_rate_f64 = per_client_rate.unwrap_or(100) as f64;
        let overall_rate_f64 = overall_rate.unwrap_or(1000) as f64;
        let burst_size_f64 = burst_size as f64;

        Self {
            per_client: Arc::new(RwLock::new(HashMap::new())),
            overall: Arc::new(RwLock::new(TokenBucket::new(
                overall_rate_f64,
                burst_size_f64,
            ))),
            per_client_rate: per_client_rate_f64,
            overall_rate: overall_rate_f64,
            burst_size: burst_size_f64,
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
            cleanup_interval: Duration::from_secs(60),
        }
    }

    /// Check rate limits for a request.
    ///
    /// This method checks both the overall rate limit and the per-client rate limit.
    ///
    /// # Arguments
    ///
    /// * `client_identity` - The client's identity
    ///
    /// # Returns
    ///
    /// `Ok(())` if the request is within rate limits, or a `Status::resource_exhausted` error.
    pub async fn check_limit(&self, client_identity: &str) -> Result<(), Status> {
        // Check overall rate limit first to avoid penalizing clients for system-wide overload
        {
            let mut overall = self.overall.write().await;
            if !overall.try_acquire() {
                warn!("Overall rate limit exceeded");
                return Err(Status::resource_exhausted("Overall rate limit exceeded"));
            }
        }

        // Only consume per-client token if overall check passed
        {
            let mut per_client = self.per_client.write().await;
            let bucket = per_client
                .entry(client_identity.to_string())
                .or_insert_with(|| {
                    // Use the smaller of burst_size and per_client_rate for per-client buckets
                    // This ensures per-client limits are more strict than overall limits
                    let client_burst = self.burst_size.min(self.per_client_rate);
                    TokenBucket::new(self.per_client_rate, client_burst)
                });

            if !bucket.try_acquire() {
                warn!("Rate limit exceeded for client: {}", client_identity);
                // Need to refund the overall token since we're rejecting the request
                let mut overall = self.overall.write().await;
                overall.refund();
                return Err(Status::resource_exhausted(format!(
                    "Rate limit exceeded for client {}",
                    client_identity
                )));
            }
        }

        // Periodic cleanup of idle clients
        self.cleanup_idle_clients().await;

        debug!("Rate limit check passed for client: {}", client_identity);
        Ok(())
    }

    /// Clean up idle client buckets to prevent unbounded memory growth.
    ///
    /// This is called periodically (every 60 seconds) to remove client buckets
    /// that haven't been accessed in the last 5 minutes.
    async fn cleanup_idle_clients(&self) {
        let now = Instant::now();
        let mut last_cleanup = self.last_cleanup.write().await;

        if now.duration_since(*last_cleanup) < self.cleanup_interval {
            return;
        }

        *last_cleanup = now;
        drop(last_cleanup); // Release the lock early

        // Remove clients that haven't been accessed in the last 5 minutes
        const IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
        let mut per_client = self.per_client.write().await;
        let initial_count = per_client.len();

        per_client.retain(|_, bucket| {
            // Keep buckets that have been accessed recently (not idle)
            !bucket.is_idle(IDLE_TIMEOUT)
        });

        let removed = initial_count - per_client.len();
        if removed > 0 {
            debug!("Cleaned up {} idle client rate limiters", removed);
        }
    }

    /// Get statistics about the rate limiter.
    pub async fn stats(&self) -> RateLimiterStats {
        let overall = self.overall.read().await;
        let per_client = self.per_client.read().await;

        RateLimiterStats {
            tracked_clients: per_client.len(),
            overall_available_tokens: overall.available_tokens(),
            overall_rate: self.overall_rate,
            per_client_rate: self.per_client_rate,
        }
    }
}

/// Statistics about the rate limiter state.
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    /// Number of client identities being tracked
    pub tracked_clients: usize,
    /// Available tokens in the overall bucket
    pub overall_available_tokens: f64,
    /// Overall request rate (requests/second)
    pub overall_rate: f64,
    /// Per-client request rate (requests/second)
    pub per_client_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10.0, 10.0);

        // Should be able to acquire tokens
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
    }

    #[test]
    fn test_token_bucket_depletion() {
        let mut bucket = TokenBucket::new(10.0, 2.0);

        // Deplete the bucket
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire()); // Should fail
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10.0, 2.0);

        // Deplete the bucket
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire());
        assert!(!bucket.try_acquire());

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Should have tokens again
        assert!(bucket.try_acquire());
    }

    #[tokio::test]
    async fn test_rate_limiter_overall_limit() {
        let limiter = RateLimiter::new(Some(100), Some(2), 2);

        // First two should succeed
        assert!(limiter.check_limit("client1").await.is_ok());
        assert!(limiter.check_limit("client2").await.is_ok());

        // Third should fail (overall limit exceeded)
        assert!(limiter.check_limit("client3").await.is_err());
    }

    #[tokio::test]
    async fn test_rate_limiter_per_client_limit() {
        // Use larger burst size for overall bucket to not interfere with per-client testing
        let limiter = RateLimiter::new(Some(2), Some(100), 10);

        // First two from same client should succeed
        assert!(limiter.check_limit("client1").await.is_ok());
        assert!(limiter.check_limit("client1").await.is_ok());

        // Third from same client should fail (per-client limit of 2)
        assert!(limiter.check_limit("client1").await.is_err());

        // Different client should still succeed (has its own quota)
        assert!(limiter.check_limit("client2").await.is_ok());
    }

    #[tokio::test]
    async fn test_rate_limiter_stats() {
        let limiter = RateLimiter::new(Some(100), Some(1000), 100);

        limiter.check_limit("client1").await.unwrap();
        limiter.check_limit("client2").await.unwrap();

        let stats = limiter.stats().await;
        assert_eq!(stats.tracked_clients, 2);
        assert!(stats.overall_available_tokens < 100.0);
    }
}
