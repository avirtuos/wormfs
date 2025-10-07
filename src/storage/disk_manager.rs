//! Disk Manager Module
//!
//! This module provides disk discovery, monitoring, and management capabilities
//! for WormFS storage nodes. It handles multiple storage devices, monitors
//! disk health, and provides real-time space utilization information.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::{debug, error, info, warn};

/// Configuration for individual disk
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskConfig {
    /// Unique identifier for this disk
    pub disk_id: String,
    /// Mount path for this disk
    pub path: PathBuf,
    /// Minimum free space required on this disk (in bytes)
    pub min_free_space: u64,
}

impl DiskConfig {
    /// Create a new disk configuration
    pub fn new<P: Into<PathBuf>>(disk_id: String, path: P, min_free_space: u64) -> Self {
        Self {
            disk_id,
            path: path.into(),
            min_free_space,
        }
    }
}

/// Configuration for disk monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMonitoringConfig {
    /// Interval between disk health checks (in seconds)
    pub check_interval_seconds: u64,
    /// Number of consecutive failures before marking disk offline
    pub failure_threshold_consecutive: u32,
    /// Percentage of disk usage that triggers space alerts
    pub space_alert_threshold_percent: u8,
}

impl Default for DiskMonitoringConfig {
    fn default() -> Self {
        Self {
            check_interval_seconds: 30,
            failure_threshold_consecutive: 3,
            space_alert_threshold_percent: 90,
        }
    }
}

/// Configuration for the disk manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskManagerConfig {
    /// List of configured disks
    pub disks: Vec<DiskConfig>,
    /// Monitoring configuration
    pub monitoring: DiskMonitoringConfig,
}

impl DiskManagerConfig {
    /// Create a new disk manager configuration
    pub fn new(disks: Vec<DiskConfig>) -> Self {
        Self {
            disks,
            monitoring: DiskMonitoringConfig::default(),
        }
    }

    /// Add a disk to the configuration
    pub fn add_disk(&mut self, disk: DiskConfig) {
        self.disks.push(disk);
    }

    /// Get disk configuration by ID
    pub fn get_disk_config(&self, disk_id: &str) -> Option<&DiskConfig> {
        self.disks.iter().find(|d| d.disk_id == disk_id)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), DiskManagerError> {
        if self.disks.is_empty() {
            return Err(DiskManagerError::InvalidConfiguration {
                reason: "At least one disk must be configured".to_string(),
            });
        }

        // Check for duplicate disk IDs
        let mut disk_ids = std::collections::HashSet::new();
        for disk in &self.disks {
            if !disk_ids.insert(&disk.disk_id) {
                return Err(DiskManagerError::InvalidConfiguration {
                    reason: format!("Duplicate disk ID: {}", disk.disk_id),
                });
            }
        }

        // Check for duplicate paths
        let mut paths = std::collections::HashSet::new();
        for disk in &self.disks {
            if !paths.insert(&disk.path) {
                return Err(DiskManagerError::InvalidConfiguration {
                    reason: format!("Duplicate disk path: {:?}", disk.path),
                });
            }
        }

        Ok(())
    }
}

/// Information about a single disk
#[derive(Debug, Clone)]
pub struct DiskInfo {
    /// Disk identifier
    pub disk_id: String,
    /// Mount path
    pub path: PathBuf,
    /// Total space on disk (in bytes)
    pub total_space: u64,
    /// Available space on disk (in bytes)
    pub available_space: u64,
    /// Used space on disk (in bytes)
    pub used_space: u64,
    /// Whether the disk is currently online and accessible
    pub is_online: bool,
    /// Minimum free space required on this disk
    pub min_free_space: u64,
    /// Number of consecutive health check failures
    pub consecutive_failures: u32,
    /// Last time this disk was checked
    pub last_checked: SystemTime,
    /// Last error encountered (if any)
    pub last_error: Option<String>,
}

impl DiskInfo {
    /// Create a new disk info from configuration
    pub fn from_config(config: &DiskConfig) -> Self {
        Self {
            disk_id: config.disk_id.clone(),
            path: config.path.clone(),
            total_space: 0,
            available_space: 0,
            used_space: 0,
            is_online: false,
            min_free_space: config.min_free_space,
            consecutive_failures: 0,
            last_checked: SystemTime::now(),
            last_error: None,
        }
    }

    /// Check if this disk has sufficient free space
    pub fn has_sufficient_space(&self) -> bool {
        self.is_online && self.available_space >= self.min_free_space
    }

    /// Get the percentage of disk space used
    pub fn usage_percentage(&self) -> f64 {
        if self.total_space == 0 {
            return 0.0;
        }
        (self.used_space as f64 / self.total_space as f64) * 100.0
    }

    /// Check if disk usage is above the alert threshold
    pub fn is_above_alert_threshold(&self, threshold_percent: u8) -> bool {
        self.usage_percentage() > threshold_percent as f64
    }

    /// Update disk space information
    pub fn update_space_info(&mut self, total: u64, available: u64) {
        self.total_space = total;
        self.available_space = available;
        self.used_space = total.saturating_sub(available);
        self.last_checked = SystemTime::now();
    }

    /// Mark disk as online after successful check
    pub fn mark_online(&mut self) {
        self.is_online = true;
        self.consecutive_failures = 0;
        self.last_error = None;
        self.last_checked = SystemTime::now();
    }

    /// Mark disk as failed with error information
    pub fn mark_failed(&mut self, error: String) {
        self.consecutive_failures += 1;
        self.last_error = Some(error);
        self.last_checked = SystemTime::now();
    }

    /// Check if disk should be marked offline based on failure threshold
    pub fn should_mark_offline(&self, threshold: u32) -> bool {
        self.consecutive_failures >= threshold
    }
}

/// Statistics about all disks
#[derive(Debug, Clone)]
pub struct DiskStats {
    /// Total number of configured disks
    pub total_disks: usize,
    /// Number of online disks
    pub online_disks: usize,
    /// Number of offline disks
    pub offline_disks: usize,
    /// Total space across all disks
    pub total_space: u64,
    /// Total available space across all disks
    pub total_available_space: u64,
    /// Total used space across all disks
    pub total_used_space: u64,
    /// Number of disks above alert threshold
    pub disks_above_threshold: usize,
}

impl DiskStats {
    /// Calculate overall usage percentage
    pub fn overall_usage_percentage(&self) -> f64 {
        if self.total_space == 0 {
            return 0.0;
        }
        (self.total_used_space as f64 / self.total_space as f64) * 100.0
    }
}

/// Errors that can occur during disk manager operations
#[derive(Error, Debug)]
pub enum DiskManagerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid configuration: {reason}")]
    InvalidConfiguration { reason: String },

    #[error("Disk not found: {disk_id}")]
    DiskNotFound { disk_id: String },

    #[error("All disks offline")]
    AllDisksOffline,

    #[error("Insufficient space on all disks")]
    InsufficientSpaceOnAllDisks,

    #[error("Disk access error for {disk_id}: {error}")]
    DiskAccessError { disk_id: String, error: String },
}

/// Result type for disk manager operations
pub type DiskManagerResult<T> = Result<T, DiskManagerError>;

/// Main disk manager implementation
pub struct DiskManager {
    config: DiskManagerConfig,
    disks: HashMap<String, DiskInfo>,
    last_global_check: SystemTime,
}

impl DiskManager {
    /// Create a new disk manager with the given configuration
    pub fn new(config: DiskManagerConfig) -> DiskManagerResult<Self> {
        // Validate configuration
        config.validate()?;

        info!(
            "Initializing disk manager with {} disks",
            config.disks.len()
        );

        let mut disks = HashMap::new();
        for disk_config in &config.disks {
            let disk_info = DiskInfo::from_config(disk_config);
            disks.insert(disk_config.disk_id.clone(), disk_info);
            debug!(
                "Added disk: {} at {:?}",
                disk_config.disk_id, disk_config.path
            );
        }

        let mut manager = Self {
            config,
            disks,
            last_global_check: SystemTime::now(),
        };

        // Perform initial health check
        manager.check_all_disks()?;

        info!("Disk manager initialized successfully");
        Ok(manager)
    }

    /// Get information about a specific disk
    pub fn get_disk_info(&self, disk_id: &str) -> Option<&DiskInfo> {
        self.disks.get(disk_id)
    }

    /// Get information about all disks
    pub fn get_all_disk_info(&self) -> &HashMap<String, DiskInfo> {
        &self.disks
    }

    /// Get a list of online disks with sufficient space
    pub fn get_available_disks(&self) -> Vec<&DiskInfo> {
        self.disks
            .values()
            .filter(|disk| disk.has_sufficient_space())
            .collect()
    }

    /// Get a list of online disk IDs with sufficient space
    pub fn get_available_disk_ids(&self) -> Vec<String> {
        self.get_available_disks()
            .into_iter()
            .map(|disk| disk.disk_id.clone())
            .collect()
    }

    /// Check if any disks are available for storage
    pub fn has_available_disks(&self) -> bool {
        !self.get_available_disks().is_empty()
    }

    /// Perform health check on all disks
    pub fn check_all_disks(&mut self) -> DiskManagerResult<()> {
        debug!("Performing health check on all disks");

        // Collect disk info to avoid borrowing conflicts
        let mut disk_checks = Vec::new();
        for (disk_id, disk_info) in &self.disks {
            disk_checks.push((disk_id.clone(), disk_info.path.clone()));
        }

        // Perform checks and update disk info
        for (disk_id, disk_path) in disk_checks {
            match self.get_disk_space_info(&disk_path) {
                Ok((total, available)) => {
                    if let Some(disk_info) = self.disks.get_mut(&disk_id) {
                        disk_info.update_space_info(total, available);
                        disk_info.mark_online();
                        debug!(
                            "Disk {} online: {:.2}% used ({} / {} bytes)",
                            disk_id,
                            disk_info.usage_percentage(),
                            disk_info.used_space,
                            disk_info.total_space
                        );

                        // Check for space alerts
                        if disk_info.is_above_alert_threshold(
                            self.config.monitoring.space_alert_threshold_percent,
                        ) {
                            warn!(
                                "Disk {} above space alert threshold: {:.2}% used",
                                disk_id,
                                disk_info.usage_percentage()
                            );
                        }
                    }
                }
                Err(e) => {
                    if let Some(disk_info) = self.disks.get_mut(&disk_id) {
                        let error_msg = format!("Failed to check disk space: {}", e);
                        disk_info.mark_failed(error_msg.clone());
                        debug!("Disk {} check failed: {}", disk_id, error_msg);

                        // Mark disk offline if threshold reached
                        if disk_info.should_mark_offline(
                            self.config.monitoring.failure_threshold_consecutive,
                        ) && disk_info.is_online
                        {
                            warn!(
                                "Marking disk {} offline after {} consecutive failures",
                                disk_id, disk_info.consecutive_failures
                            );
                            disk_info.is_online = false;
                        }
                    }
                }
            }
        }

        self.last_global_check = SystemTime::now();
        Ok(())
    }

    /// Check if it's time for a health check based on configuration
    pub fn should_check_health(&self) -> bool {
        let check_interval = Duration::from_secs(self.config.monitoring.check_interval_seconds);
        self.last_global_check.elapsed().unwrap_or_default() >= check_interval
    }

    /// Perform health check if needed
    pub fn check_health_if_needed(&mut self) -> DiskManagerResult<()> {
        if self.should_check_health() {
            self.check_all_disks()?;
        }
        Ok(())
    }

    /// Get disk space information using platform-specific methods
    fn get_disk_space_info(&self, path: &Path) -> std::io::Result<(u64, u64)> {
        // Ensure the path exists
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        // Use statvfs on Unix-like systems, GetDiskFreeSpaceEx on Windows
        #[cfg(unix)]
        {
            use std::ffi::CString;
            use std::mem::MaybeUninit;

            let c_path = CString::new(path.to_string_lossy().as_bytes()).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid path")
            })?;

            let mut statvfs = MaybeUninit::uninit();
            let result = unsafe { libc::statvfs(c_path.as_ptr(), statvfs.as_mut_ptr()) };

            if result == 0 {
                let statvfs = unsafe { statvfs.assume_init() };
                let block_size = statvfs.f_frsize;
                let total_blocks = statvfs.f_blocks;
                let available_blocks = statvfs.f_bavail;

                let total_space = total_blocks * block_size;
                let available_space = available_blocks * block_size;

                Ok((total_space, available_space))
            } else {
                Err(std::io::Error::last_os_error())
            }
        }

        #[cfg(windows)]
        {
            use std::ffi::OsStr;
            use std::iter::once;
            use std::os::windows::ffi::OsStrExt;
            use winapi::um::fileapi::GetDiskFreeSpaceExW;

            let wide_path: Vec<u16> = OsStr::new(path).encode_wide().chain(once(0)).collect();

            let mut available_space = 0u64;
            let mut total_space = 0u64;

            let result = unsafe {
                GetDiskFreeSpaceExW(
                    wide_path.as_ptr(),
                    &mut available_space,
                    &mut total_space,
                    std::ptr::null_mut(),
                )
            };

            if result != 0 {
                Ok((total_space, available_space))
            } else {
                Err(std::io::Error::last_os_error())
            }
        }

        #[cfg(not(any(unix, windows)))]
        {
            // Fallback for other platforms - return placeholder values
            warn!("Disk space checking not implemented for this platform");
            Ok((u64::MAX / 2, u64::MAX / 4)) // Assume plenty of space
        }
    }

    /// Get comprehensive statistics about all disks
    pub fn get_stats(&self) -> DiskStats {
        let total_disks = self.disks.len();
        let online_disks = self.disks.values().filter(|d| d.is_online).count();
        let offline_disks = total_disks - online_disks;

        let (total_space, total_available_space, total_used_space, disks_above_threshold) =
            self.disks.values().fold(
                (0u64, 0u64, 0u64, 0usize),
                |(total, available, used, above_threshold), disk| {
                    let new_above_threshold = if disk.is_above_alert_threshold(
                        self.config.monitoring.space_alert_threshold_percent,
                    ) {
                        above_threshold + 1
                    } else {
                        above_threshold
                    };

                    (
                        total + disk.total_space,
                        available + disk.available_space,
                        used + disk.used_space,
                        new_above_threshold,
                    )
                },
            );

        DiskStats {
            total_disks,
            online_disks,
            offline_disks,
            total_space,
            total_available_space,
            total_used_space,
            disks_above_threshold,
        }
    }

    /// Ensure a disk directory exists and is accessible
    pub fn ensure_disk_directory(
        &self,
        disk_id: &str,
        subpath: &Path,
    ) -> DiskManagerResult<PathBuf> {
        let disk_info = self
            .disks
            .get(disk_id)
            .ok_or_else(|| DiskManagerError::DiskNotFound {
                disk_id: disk_id.to_string(),
            })?;

        if !disk_info.is_online {
            return Err(DiskManagerError::DiskAccessError {
                disk_id: disk_id.to_string(),
                error: "Disk is offline".to_string(),
            });
        }

        let full_path = disk_info.path.join(subpath);

        if !full_path.exists() {
            fs::create_dir_all(&full_path)?;
        }

        Ok(full_path)
    }

    /// Get the configuration
    pub fn config(&self) -> &DiskManagerConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_disk_config(temp_dir: &TempDir, disk_id: &str) -> DiskConfig {
        let path = temp_dir.path().join(disk_id);
        fs::create_dir_all(&path).unwrap();
        DiskConfig::new(disk_id.to_string(), path, 1024)
    }

    #[test]
    fn test_disk_config_creation() {
        let config = DiskConfig::new("disk1".to_string(), "/test/path", 1024);
        assert_eq!(config.disk_id, "disk1");
        assert_eq!(config.path, PathBuf::from("/test/path"));
        assert_eq!(config.min_free_space, 1024);
    }

    #[test]
    fn test_disk_manager_config_validation() {
        // Valid configuration
        let temp_dir = TempDir::new().unwrap();
        let disk1 = create_test_disk_config(&temp_dir, "disk1");
        let disk2 = create_test_disk_config(&temp_dir, "disk2");
        let config = DiskManagerConfig::new(vec![disk1, disk2]);
        assert!(config.validate().is_ok());

        // Empty configuration should fail
        let empty_config = DiskManagerConfig::new(vec![]);
        assert!(empty_config.validate().is_err());

        // Duplicate disk IDs should fail
        let disk1 = create_test_disk_config(&temp_dir, "disk1");
        let disk1_dup = create_test_disk_config(&temp_dir, "disk1");
        let dup_config = DiskManagerConfig::new(vec![disk1, disk1_dup]);
        assert!(dup_config.validate().is_err());
    }

    #[test]
    fn test_disk_info_operations() {
        let temp_dir = TempDir::new().unwrap();
        let disk_config = create_test_disk_config(&temp_dir, "test_disk");
        let mut disk_info = DiskInfo::from_config(&disk_config);

        // Initial state
        assert!(!disk_info.is_online);
        assert_eq!(disk_info.consecutive_failures, 0);

        // Mark as failed
        disk_info.mark_failed("Test error".to_string());
        assert_eq!(disk_info.consecutive_failures, 1);
        assert_eq!(disk_info.last_error, Some("Test error".to_string()));

        // Mark as online
        disk_info.mark_online();
        assert!(disk_info.is_online);
        assert_eq!(disk_info.consecutive_failures, 0);
        assert_eq!(disk_info.last_error, None);

        // Test space information
        disk_info.update_space_info(1000, 200);
        assert_eq!(disk_info.total_space, 1000);
        assert_eq!(disk_info.available_space, 200);
        assert_eq!(disk_info.used_space, 800);
        assert_eq!(disk_info.usage_percentage(), 80.0);
    }

    #[test]
    fn test_disk_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let disk1 = create_test_disk_config(&temp_dir, "disk1");
        let disk2 = create_test_disk_config(&temp_dir, "disk2");
        let config = DiskManagerConfig::new(vec![disk1, disk2]);

        let manager = DiskManager::new(config).unwrap();
        assert_eq!(manager.disks.len(), 2);
        assert!(manager.disks.contains_key("disk1"));
        assert!(manager.disks.contains_key("disk2"));
    }

    #[test]
    fn test_disk_manager_available_disks() {
        let temp_dir = TempDir::new().unwrap();
        let disk1 = create_test_disk_config(&temp_dir, "disk1");
        let config = DiskManagerConfig::new(vec![disk1]);

        let manager = DiskManager::new(config).unwrap();

        // Should have available disks after initialization
        assert!(manager.has_available_disks());
        let available = manager.get_available_disks();
        assert!(!available.is_empty());

        let available_ids = manager.get_available_disk_ids();
        assert!(available_ids.contains(&"disk1".to_string()));
    }

    #[test]
    fn test_disk_stats() {
        let temp_dir = TempDir::new().unwrap();
        let disk1 = create_test_disk_config(&temp_dir, "disk1");
        let disk2 = create_test_disk_config(&temp_dir, "disk2");
        let config = DiskManagerConfig::new(vec![disk1, disk2]);

        let manager = DiskManager::new(config).unwrap();
        let stats = manager.get_stats();

        assert_eq!(stats.total_disks, 2);
        assert!(stats.online_disks > 0);
        assert!(stats.total_space > 0);
    }

    #[test]
    fn test_ensure_disk_directory() {
        let temp_dir = TempDir::new().unwrap();
        let disk1 = create_test_disk_config(&temp_dir, "disk1");
        let config = DiskManagerConfig::new(vec![disk1]);

        let manager = DiskManager::new(config).unwrap();

        let subpath = Path::new("test/subdirectory");
        let full_path = manager.ensure_disk_directory("disk1", subpath).unwrap();

        assert!(full_path.exists());
        assert!(full_path.is_dir());
    }

    #[test]
    fn test_disk_not_found_error() {
        let temp_dir = TempDir::new().unwrap();
        let disk1 = create_test_disk_config(&temp_dir, "disk1");
        let config = DiskManagerConfig::new(vec![disk1]);

        let manager = DiskManager::new(config).unwrap();

        let result = manager.ensure_disk_directory("nonexistent_disk", Path::new("test"));
        assert!(matches!(result, Err(DiskManagerError::DiskNotFound { .. })));
    }

    #[test]
    fn test_monitoring_config_defaults() {
        let config = DiskMonitoringConfig::default();
        assert_eq!(config.check_interval_seconds, 30);
        assert_eq!(config.failure_threshold_consecutive, 3);
        assert_eq!(config.space_alert_threshold_percent, 90);
    }
}
