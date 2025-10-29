//! Lock recovery utilities for handling lock poisoning scenarios.
//!
//! Rust's `RwLock` and `Mutex` types mark themselves as "poisoned" when a thread
//! panics while holding the lock. This prevents data corruption by making subsequent
//! lock attempts return a `PoisonError` instead of granting access to potentially
//! inconsistent data.
//!
//! However, in some cases (especially with transactional storage like redb), the
//! underlying data may still be consistent even if the lock is poisoned, because the
//! database layer ensures transactional integrity. In these cases, it can be safe to
//! recover from a poisoned lock using `.into_inner()` to extract the guarded data.
//!
//! # When to Use Recovery
//!
//! - **Safe to recover**: Cache data, timestamps, optional state that can be rebuilt
//! - **Use with caution**: Transactional databases with their own consistency guarantees
//! - **Never recover**: Application state that doesn't have external consistency guarantees
//!
//! # Usage
//!
//! ```ignore
//! use wormfs_v2::utils::lock_helpers::recover_write_lock;
//!
//! let my_lock = RwLock::new(some_data);
//! let guard = recover_write_lock(&my_lock, "my_operation", |e| {
//!     error!("Lock poisoned during my_operation: {}", e);
//! })?;
//! ```

use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{error, warn};

/// Attempts to acquire a write lock, recovering from poison if necessary.
///
/// This function will attempt to acquire a write lock on the provided `RwLock`.
/// If the lock is poisoned, it will:
/// 1. Log an error with the operation context
/// 2. Recover the poisoned lock using `.into_inner()`
/// 3. Return the lock guard successfully
///
/// # Safety
///
/// Only use this when the guarded data has external consistency guarantees
/// (e.g., transactional database) or is non-critical and can be recovered.
///
/// # Arguments
///
/// * `lock` - The RwLock to acquire
/// * `operation` - Description of the operation for logging (e.g., "append", "compact")
///
/// # Returns
///
/// A write guard on success, or an error string describing why recovery failed.
///
/// # Example
///
/// ```ignore
/// let db_lock = RwLock::new(database);
/// let guard = recover_write_lock(&db_lock, "database write")?;
/// ```
pub fn recover_write_lock<'a, T>(
    lock: &'a RwLock<T>,
    operation: &str,
) -> Result<RwLockWriteGuard<'a, T>, String> {
    match lock.write() {
        Ok(guard) => Ok(guard),
        Err(poison_err) => {
            error!(
                "Lock poisoned during {} operation - recovering from panic. \
                 This indicates a thread panicked while holding the lock. \
                 Data consistency depends on external guarantees (e.g., database transactions).",
                operation
            );
            warn!(
                "Recovering poisoned lock for {} - extracting data despite panic",
                operation
            );

            // Extract the guarded data from the poisoned lock
            // This is safe when the data has external consistency guarantees
            Ok(poison_err.into_inner())
        }
    }
}

/// Attempts to acquire a read lock, recovering from poison if necessary.
///
/// This function will attempt to acquire a read lock on the provided `RwLock`.
/// If the lock is poisoned, it will log an error and recover by extracting the
/// guarded data using `.into_inner()`.
///
/// # Safety
///
/// Only use this when the guarded data has external consistency guarantees
/// (e.g., transactional database) or is non-critical cache data.
///
/// # Arguments
///
/// * `lock` - The RwLock to acquire
/// * `operation` - Description of the operation for logging (e.g., "read entry", "get stats")
///
/// # Returns
///
/// A read guard on success, or an error string describing why recovery failed.
///
/// # Example
///
/// ```ignore
/// let db_lock = RwLock::new(database);
/// let guard = recover_read_lock(&db_lock, "database read")?;
/// ```
pub fn recover_read_lock<'a, T>(
    lock: &'a RwLock<T>,
    operation: &str,
) -> Result<RwLockReadGuard<'a, T>, String> {
    match lock.read() {
        Ok(guard) => Ok(guard),
        Err(poison_err) => {
            error!(
                "Lock poisoned during {} operation - recovering from panic. \
                 This indicates a thread panicked while holding the lock.",
                operation
            );
            warn!(
                "Recovering poisoned lock for {} - extracting data despite panic",
                operation
            );

            // Extract the guarded data from the poisoned lock
            Ok(poison_err.into_inner())
        }
    }
}

/// Immediately recovers a write lock without logging or error handling.
///
/// This is intended for non-critical data like caches or timestamps where
/// recovery is always safe and desired. Unlike `recover_write_lock`, this
/// function logs at a lower severity (DEBUG for poison, WARN for recovery).
///
/// # Use Cases
///
/// - Cache indices that can be rebuilt from database
/// - Timestamps that are informational only
/// - Metrics accumulators
/// - Any state that is derived or non-authoritative
///
/// # Arguments
///
/// * `lock` - The RwLock to acquire
/// * `context` - Brief context for debug logging (e.g., "cache update", "timestamp")
///
/// # Returns
///
/// Always returns a write guard by recovering from poison if necessary.
///
/// # Example
///
/// ```ignore
/// let cache_lock = RwLock::new(Some(cached_value));
/// let mut guard = recover_cache_write_lock(&cache_lock, "cache update");
/// *guard = Some(new_value);
/// ```
pub fn recover_cache_write_lock<'a, T>(
    lock: &'a RwLock<T>,
    context: &str,
) -> RwLockWriteGuard<'a, T> {
    lock.write().unwrap_or_else(|poison_err| {
        tracing::debug!(
            "Cache lock poisoned for {} - recovering (this is safe for cache data)",
            context
        );
        poison_err.into_inner()
    })
}

/// Immediately recovers a read lock for cache/non-critical data.
///
/// This is the read-only equivalent of `recover_cache_write_lock`, intended
/// for reading non-critical data where recovery is always safe.
///
/// # Arguments
///
/// * `lock` - The RwLock to acquire
/// * `context` - Brief context for debug logging
///
/// # Returns
///
/// Always returns a read guard by recovering from poison if necessary.
pub fn recover_cache_read_lock<'a, T>(
    lock: &'a RwLock<T>,
    context: &str,
) -> RwLockReadGuard<'a, T> {
    lock.read().unwrap_or_else(|poison_err| {
        tracing::debug!(
            "Cache lock poisoned for {} - recovering (this is safe for cache data)",
            context
        );
        poison_err.into_inner()
    })
}

/// Extension trait for `Result<T, PoisonError<T>>` to simplify recovery.
///
/// This trait provides convenient methods to handle poison errors inline
/// without needing separate recovery functions.
///
/// # Example
///
/// ```ignore
/// use wormfs_v2::utils::lock_helpers::PoisonRecovery;
///
/// let guard = my_lock.write()
///     .recover_or_log("my_operation")?;
/// ```
pub trait PoisonRecovery<T> {
    /// Recover from poison, logging the error, or propagate as a String error.
    fn recover_or_log(self, operation: &str) -> Result<T, String>;

    /// Recover from poison without any error propagation (always succeeds).
    fn recover_always(self, context: &str) -> T;
}

impl<T> PoisonRecovery<T> for Result<T, PoisonError<T>> {
    fn recover_or_log(self, operation: &str) -> Result<T, String> {
        match self {
            Ok(guard) => Ok(guard),
            Err(poison_err) => {
                error!("Lock poisoned during {} - recovering from panic", operation);
                warn!("Extracting data from poisoned lock for {}", operation);
                Ok(poison_err.into_inner())
            }
        }
    }

    fn recover_always(self, context: &str) -> T {
        self.unwrap_or_else(|poison_err| {
            tracing::debug!("Recovering poisoned lock for {} (always safe)", context);
            poison_err.into_inner()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_recover_write_lock_normal() {
        let lock = RwLock::new(42);
        let guard = recover_write_lock(&lock, "test").unwrap();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_recover_read_lock_normal() {
        let lock = RwLock::new(42);
        let guard = recover_read_lock(&lock, "test").unwrap();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_recover_cache_write_lock_normal() {
        let lock = RwLock::new(42);
        let guard = recover_cache_write_lock(&lock, "test");
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_recover_cache_read_lock_normal() {
        let lock = RwLock::new(42);
        let guard = recover_cache_read_lock(&lock, "test");
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_poison_recovery_write() {
        let lock = Arc::new(RwLock::new(vec![1, 2, 3]));
        let lock_clone = Arc::clone(&lock);

        // Poison the lock by panicking while holding it
        let _ = std::thread::spawn(move || {
            let mut _guard = lock_clone.write().unwrap();
            panic!("Intentional panic to poison lock");
        })
        .join();

        // Lock should be poisoned, but we can recover
        let guard = recover_write_lock(&lock, "test_operation").unwrap();
        assert_eq!(*guard, vec![1, 2, 3]);
    }

    #[test]
    fn test_poison_recovery_read() {
        let lock = Arc::new(RwLock::new(vec![1, 2, 3]));
        let lock_clone = Arc::clone(&lock);

        // Poison the lock
        let _ = std::thread::spawn(move || {
            let mut _guard = lock_clone.write().unwrap();
            panic!("Intentional panic to poison lock");
        })
        .join();

        // Lock should be poisoned, but we can recover
        let guard = recover_read_lock(&lock, "test_operation").unwrap();
        assert_eq!(*guard, vec![1, 2, 3]);
    }

    #[test]
    fn test_poison_recovery_cache_write() {
        let lock = Arc::new(RwLock::new(Some(42)));
        let lock_clone = Arc::clone(&lock);

        // Poison the lock
        let _ = std::thread::spawn(move || {
            let mut _guard = lock_clone.write().unwrap();
            panic!("Intentional panic to poison lock");
        })
        .join();

        // Should recover without error
        let guard = recover_cache_write_lock(&lock, "cache");
        assert_eq!(*guard, Some(42));
    }

    #[test]
    fn test_poison_recovery_trait() {
        let lock = Arc::new(RwLock::new(100));
        let lock_clone = Arc::clone(&lock);

        // Poison the lock
        let _ = std::thread::spawn(move || {
            let mut _guard = lock_clone.write().unwrap();
            panic!("Intentional panic");
        })
        .join();

        // Use the trait method
        let guard = lock.write().recover_or_log("test").unwrap();
        assert_eq!(*guard, 100);
    }

    #[test]
    fn test_poison_recovery_always() {
        let lock = Arc::new(RwLock::new(200));
        let lock_clone = Arc::clone(&lock);

        // Poison the lock
        let _ = std::thread::spawn(move || {
            let mut _guard = lock_clone.write().unwrap();
            panic!("Intentional panic");
        })
        .join();

        // Use recover_always - should never fail
        let guard = lock.write().recover_always("test");
        assert_eq!(*guard, 200);
    }
}
