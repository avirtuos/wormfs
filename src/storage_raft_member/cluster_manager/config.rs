/// Configuration for the Cluster Manager
///
/// This module defines configuration options for automatic failure detection
/// and recovery in Raft clusters.
use std::time::Duration;

/// Configuration for the Cluster Manager
///
/// Controls how aggressively the cluster manager detects failures and manages
/// membership changes. Different deployment scenarios may require different
/// tuning (e.g., stable networks vs. high-churn environments).
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterManagerConfig {
    /// Maximum time to wait for a heartbeat before considering a node failed
    ///
    /// **Default:** 30 seconds
    /// **Tuning:** Lower values detect failures faster but increase false positives
    /// from temporary network issues. Higher values are more conservative.
    pub heartbeat_timeout: Duration,

    /// Number of consecutive failed health checks before marking node as failed
    ///
    /// **Default:** 3
    /// **Tuning:** Higher values reduce false positives but slow failure detection.
    pub max_consecutive_failures: u32,

    /// Replication lag threshold for warning (log entries behind leader)
    ///
    /// **Default:** 500 entries
    /// **Tuning:** Set based on your write rate and acceptable lag.
    pub warning_lag_threshold: u64,

    /// Replication lag threshold for critical status/demotion
    ///
    /// **Default:** 1000 entries
    /// **Tuning:** Must be >= warning_lag_threshold. Consider demotion impact.
    pub critical_lag_threshold: u64,

    /// Automatically promote learners to voters after they sync
    ///
    /// **Default:** true
    /// **Safety:** Disable if you want manual control over promotions.
    pub auto_promote_after_sync: bool,

    /// Maximum time to wait for a learner to sync before giving up
    ///
    /// **Default:** 60 seconds
    /// **Tuning:** Increase for slow networks or large log gaps.
    pub sync_wait_timeout: Duration,

    /// How often to check node health
    ///
    /// **Default:** 5 seconds
    /// **Performance:** Lower values detect issues faster but increase CPU usage.
    /// Must be less than heartbeat_timeout.
    pub health_check_interval: Duration,

    /// How often to collect and emit metrics
    ///
    /// **Default:** 10 seconds
    /// **Performance:** Lower values provide better observability but increase overhead.
    pub metrics_collection_interval: Duration,

    /// Minimum time between membership changes (rate limiting)
    ///
    /// **Default:** 60 seconds
    /// **Safety:** Prevents membership thrashing during cascading failures.
    pub min_membership_change_interval: Duration,

    /// Whether the cluster manager is enabled
    ///
    /// **Default:** true
    /// **Use case:** Disable for manual membership management or testing.
    pub enabled: bool,
}

impl Default for ClusterManagerConfig {
    fn default() -> Self {
        Self::moderate()
    }
}

impl ClusterManagerConfig {
    /// Conservative configuration (reduces false positives, slower failover)
    ///
    /// Best for: Stable networks, critical systems where false positives are costly
    pub fn conservative() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(30),
            max_consecutive_failures: 5,
            warning_lag_threshold: 500,
            critical_lag_threshold: 1000,
            auto_promote_after_sync: true,
            sync_wait_timeout: Duration::from_secs(120),
            health_check_interval: Duration::from_secs(10),
            metrics_collection_interval: Duration::from_secs(15),
            min_membership_change_interval: Duration::from_secs(120),
            enabled: true,
        }
    }

    /// Moderate configuration (balanced approach)
    ///
    /// Best for: Most production deployments
    pub fn moderate() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(15),
            max_consecutive_failures: 3,
            warning_lag_threshold: 300,
            critical_lag_threshold: 600,
            auto_promote_after_sync: true,
            sync_wait_timeout: Duration::from_secs(60),
            health_check_interval: Duration::from_secs(5),
            metrics_collection_interval: Duration::from_secs(10),
            min_membership_change_interval: Duration::from_secs(60),
            enabled: true,
        }
    }

    /// Aggressive configuration (fast failover, higher false positive risk)
    ///
    /// Best for: Development and testing only
    pub fn aggressive() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(5),
            max_consecutive_failures: 2,
            warning_lag_threshold: 100,
            critical_lag_threshold: 200,
            auto_promote_after_sync: true,
            sync_wait_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(2),
            metrics_collection_interval: Duration::from_secs(5),
            min_membership_change_interval: Duration::from_secs(30),
            enabled: true,
        }
    }

    /// Disabled configuration (cluster manager turned off)
    ///
    /// Best for: Manual cluster management, testing, single-node clusters
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::moderate()
        }
    }

    /// Validate the configuration
    ///
    /// Returns an error if the configuration has invalid or dangerous settings.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Health check interval must be less than heartbeat timeout
        if self.health_check_interval >= self.heartbeat_timeout {
            return Err(ConfigError::InvalidInterval(
                "health_check_interval must be < heartbeat_timeout".to_string(),
            ));
        }

        // Heartbeat timeout must be reasonable
        if self.heartbeat_timeout < Duration::from_secs(1) {
            return Err(ConfigError::TooAggressive(
                "heartbeat_timeout < 1s is too aggressive and will cause false positives"
                    .to_string(),
            ));
        }

        // Critical lag must be >= warning lag
        if self.critical_lag_threshold < self.warning_lag_threshold {
            return Err(ConfigError::InvalidThreshold(
                "critical_lag_threshold must be >= warning_lag_threshold".to_string(),
            ));
        }

        // Max consecutive failures must be at least 1
        if self.max_consecutive_failures == 0 {
            return Err(ConfigError::InvalidThreshold(
                "max_consecutive_failures must be >= 1".to_string(),
            ));
        }

        // Warn on very aggressive settings
        if self.heartbeat_timeout < Duration::from_secs(5) {
            eprintln!(
                "WARN: heartbeat_timeout < 5s may cause false positives in networks with jitter"
            );
        }

        if self.health_check_interval < Duration::from_secs(1) {
            eprintln!("WARN: health_check_interval < 1s may cause excessive CPU usage");
        }

        Ok(())
    }
}

/// Configuration validation errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// Invalid interval settings
    InvalidInterval(String),

    /// Configuration is too aggressive
    TooAggressive(String),

    /// Invalid threshold settings
    InvalidThreshold(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidInterval(msg) => write!(f, "Invalid interval: {}", msg),
            ConfigError::TooAggressive(msg) => write!(f, "Too aggressive: {}", msg),
            ConfigError::InvalidThreshold(msg) => write!(f, "Invalid threshold: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClusterManagerConfig::default();
        assert!(config.enabled);
        assert_eq!(config, ClusterManagerConfig::moderate());
    }

    #[test]
    fn test_moderate_config_valid() {
        let config = ClusterManagerConfig::moderate();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_conservative_config_valid() {
        let config = ClusterManagerConfig::conservative();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_aggressive_config_valid() {
        let config = ClusterManagerConfig::aggressive();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_disabled_config() {
        let config = ClusterManagerConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_validation_health_check_interval() {
        let mut config = ClusterManagerConfig::moderate();
        config.health_check_interval = config.heartbeat_timeout; // Equal, should fail
        assert!(config.validate().is_err());

        config.health_check_interval = config.heartbeat_timeout + Duration::from_secs(1); // Greater, should fail
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_heartbeat_timeout_too_low() {
        let mut config = ClusterManagerConfig::moderate();
        config.heartbeat_timeout = Duration::from_millis(500); // Less than 1s
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_critical_lag_less_than_warning() {
        let mut config = ClusterManagerConfig::moderate();
        config.critical_lag_threshold = config.warning_lag_threshold - 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_zero_consecutive_failures() {
        let mut config = ClusterManagerConfig::moderate();
        config.max_consecutive_failures = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_valid_config() {
        let config = ClusterManagerConfig {
            heartbeat_timeout: Duration::from_secs(10),
            max_consecutive_failures: 3,
            warning_lag_threshold: 100,
            critical_lag_threshold: 200,
            auto_promote_after_sync: true,
            sync_wait_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(5),
            metrics_collection_interval: Duration::from_secs(10),
            min_membership_change_interval: Duration::from_secs(60),
            enabled: true,
        };
        assert!(config.validate().is_ok());
    }
}
