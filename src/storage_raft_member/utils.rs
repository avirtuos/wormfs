///! Utility functions for storage_raft_member module
use std::time::SystemTime;

/// Converts the current system time to milliseconds since UNIX_EPOCH.
///
/// ## Safety
///
/// This function uses `expect()` internally because the operation is infallible in practice:
/// - `SystemTime::now()` always returns the current system time
/// - `UNIX_EPOCH` is a constant (January 1, 1970 UTC)
/// - The only way this could fail is if the system clock is set before 1970,
///   which is impossible on properly configured modern systems
///
/// If the system clock were somehow before 1970, this represents a catastrophic
/// system misconfiguration that would break many other parts of the system,
/// so panicking is appropriate.
///
/// # Returns
///
/// Current time as milliseconds since UNIX_EPOCH
#[inline]
pub(crate) fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System clock is before UNIX_EPOCH (1970-01-01) - system misconfiguration")
        .as_millis() as u64
}

/// Converts the current system time to seconds since UNIX_EPOCH.
///
/// ## Safety
///
/// This function uses `expect()` internally for the same reasons as `current_time_ms()`.
/// See that function's documentation for details on why this is safe.
///
/// # Returns
///
/// Current time as seconds since UNIX_EPOCH
#[inline]
pub(crate) fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System clock is before UNIX_EPOCH (1970-01-01) - system misconfiguration")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_time_ms() {
        let time = current_time_ms();
        // Should be a reasonable timestamp (after 2020)
        assert!(time > 1577836800000); // Jan 1, 2020 in ms
    }

    #[test]
    fn test_current_time_secs() {
        let time = current_time_secs();
        // Should be a reasonable timestamp (after 2020)
        assert!(time > 1577836800); // Jan 1, 2020 in seconds
    }

    #[test]
    fn test_time_consistency() {
        let ms = current_time_ms();
        let secs = current_time_secs();
        // ms / 1000 should be approximately equal to secs
        assert!((ms / 1000).abs_diff(secs) < 2); // Within 2 seconds
    }
}
