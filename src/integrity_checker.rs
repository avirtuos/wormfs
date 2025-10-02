//! Integrity Checker Module
//!
//! This module provides comprehensive data integrity validation for WormFS,
//! including shallow and deep integrity checks, corruption detection, and
//! repair recommendations.

use crate::chunk_format::{read_chunk, validate_chunk, ChunkError};
use crate::erasure_coding::{decode_stripe_with_size, ErasureError};
use crate::metadata_store::{FileMetadata, MetadataError, MetadataStore, StripeId};
use crate::storage_node::{StorageNodeConfig, StorageNodeError};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Errors that can occur during integrity checking operations
#[derive(Error, Debug)]
pub enum IntegrityError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Metadata error: {0}")]
    Metadata(#[from] MetadataError),

    #[error("Chunk format error: {0}")]
    ChunkFormat(#[from] ChunkError),

    #[error("Erasure coding error: {0}")]
    ErasureCoding(#[from] ErasureError),

    #[error("Storage node error: {0}")]
    StorageNode(#[from] StorageNodeError),

    #[error("Integrity check cancelled")]
    Cancelled,
}

/// Result type for integrity operations
pub type IntegrityResult<T> = Result<T, IntegrityError>;

/// Severity levels for integrity issues
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IssueSeverity {
    /// Minor issue that doesn't affect data availability
    Warning,
    /// Issue that may affect performance but data is still accessible
    Minor,
    /// Issue that affects data availability but may be recoverable
    Major,
    /// Critical issue that indicates data loss
    Critical,
}

/// Types of integrity issues that can be detected
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueType {
    /// Chunk file is missing from disk
    MissingChunk,
    /// Chunk file exists but has invalid checksum
    CorruptedChunk,
    /// Chunk metadata exists but file is missing
    OrphanedMetadata,
    /// Chunk file exists but no metadata
    OrphanedChunk,
    /// Stripe cannot be reconstructed due to too many missing chunks
    UnrecoverableStripe,
    /// Stripe metadata is inconsistent
    InconsistentStripeMetadata,
    /// File metadata is inconsistent with chunks
    InconsistentFileMetadata,
    /// Chunk placement violates configuration rules
    InvalidChunkPlacement,
}

/// Information about a detected integrity issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityIssue {
    /// Unique identifier for this issue
    pub issue_id: Uuid,
    /// Severity of the issue
    pub severity: IssueSeverity,
    /// Type of integrity issue
    pub issue_type: IssueType,
    /// Affected file ID (if applicable)
    pub file_id: Option<Uuid>,
    /// Affected stripe index (if applicable)
    pub stripe_index: Option<u64>,
    /// Affected chunk index (if applicable)
    pub chunk_index: Option<u8>,
    /// Human-readable description of the issue
    pub description: String,
    /// Path to affected file or chunk (if applicable)
    pub affected_path: Option<PathBuf>,
    /// When the issue was detected
    pub detected_at: SystemTime,
    /// Whether this issue can potentially be repaired
    pub can_repair: bool,
    /// Recommended repair action
    pub repair_recommendation: String,
}

impl IntegrityIssue {
    /// Create a new integrity issue
    pub fn new(
        severity: IssueSeverity,
        issue_type: IssueType,
        description: String,
        repair_recommendation: String,
    ) -> Self {
        Self {
            issue_id: Uuid::new_v4(),
            severity,
            issue_type,
            file_id: None,
            stripe_index: None,
            chunk_index: None,
            description,
            affected_path: None,
            detected_at: SystemTime::now(),
            can_repair: false,
            repair_recommendation,
        }
    }

    /// Set the affected file ID
    pub fn with_file_id(mut self, file_id: Uuid) -> Self {
        self.file_id = Some(file_id);
        self
    }

    /// Set the affected stripe index
    pub fn with_stripe_index(mut self, stripe_index: u64) -> Self {
        self.stripe_index = Some(stripe_index);
        self
    }

    /// Set the affected chunk index
    pub fn with_chunk_index(mut self, chunk_index: u8) -> Self {
        self.chunk_index = Some(chunk_index);
        self
    }

    /// Set the affected path
    pub fn with_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.affected_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Mark this issue as repairable
    pub fn with_repair_capability(mut self, can_repair: bool) -> Self {
        self.can_repair = can_repair;
        self
    }
}

/// Statistics about an integrity check run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityCheckStats {
    /// When the check started
    pub started_at: SystemTime,
    /// When the check completed
    pub completed_at: Option<SystemTime>,
    /// Duration of the check
    pub duration: Option<Duration>,
    /// Total number of files checked
    pub files_checked: u64,
    /// Total number of stripes checked
    pub stripes_checked: u64,
    /// Total number of chunks checked
    pub chunks_checked: u64,
    /// Number of issues found by severity
    pub issues_by_severity: HashMap<IssueSeverity, u64>,
    /// Total bytes checked
    pub bytes_checked: u64,
    /// Whether the check was cancelled
    pub cancelled: bool,
}

impl IntegrityCheckStats {
    /// Create new integrity check statistics
    pub fn new() -> Self {
        let mut issues_by_severity = HashMap::new();
        issues_by_severity.insert(IssueSeverity::Warning, 0);
        issues_by_severity.insert(IssueSeverity::Minor, 0);
        issues_by_severity.insert(IssueSeverity::Major, 0);
        issues_by_severity.insert(IssueSeverity::Critical, 0);

        Self {
            started_at: SystemTime::now(),
            completed_at: None,
            duration: None,
            files_checked: 0,
            stripes_checked: 0,
            chunks_checked: 0,
            issues_by_severity,
            bytes_checked: 0,
            cancelled: false,
        }
    }

    /// Mark the check as completed
    pub fn complete(&mut self) {
        let now = SystemTime::now();
        self.completed_at = Some(now);
        self.duration = now.duration_since(self.started_at).ok();
    }

    /// Record an issue
    pub fn record_issue(&mut self, severity: IssueSeverity) {
        *self.issues_by_severity.entry(severity).or_insert(0) += 1;
    }

    /// Get total number of issues
    pub fn total_issues(&self) -> u64 {
        self.issues_by_severity.values().sum()
    }

    /// Check if any critical issues were found
    pub fn has_critical_issues(&self) -> bool {
        self.issues_by_severity
            .get(&IssueSeverity::Critical)
            .unwrap_or(&0)
            > &0
    }
}

impl Default for IntegrityCheckStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for integrity checking operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityCheckConfig {
    /// Whether to perform deep integrity checks (reconstruct stripes)
    pub deep_check: bool,
    /// Whether to check chunk checksums
    pub verify_checksums: bool,
    /// Whether to verify stripe reconstructability
    pub verify_reconstruction: bool,
    /// Maximum number of issues to collect before stopping
    pub max_issues: Option<usize>,
    /// Whether to continue checking after finding critical issues
    pub continue_after_critical: bool,
    /// Timeout for the entire check operation
    pub timeout: Option<Duration>,
}

impl IntegrityCheckConfig {
    /// Create a basic (shallow) integrity check configuration
    pub fn shallow() -> Self {
        Self {
            deep_check: false,
            verify_checksums: true,
            verify_reconstruction: false,
            max_issues: None,
            continue_after_critical: true,
            timeout: None,
        }
    }

    /// Create a comprehensive (deep) integrity check configuration
    pub fn deep() -> Self {
        Self {
            deep_check: true,
            verify_checksums: true,
            verify_reconstruction: true,
            max_issues: None,
            continue_after_critical: true,
            timeout: Some(Duration::from_secs(3600)), // 1 hour timeout
        }
    }

    /// Create a quick integrity check configuration
    pub fn quick() -> Self {
        Self {
            deep_check: false,
            verify_checksums: false,
            verify_reconstruction: false,
            max_issues: Some(100),
            continue_after_critical: false,
            timeout: Some(Duration::from_secs(300)), // 5 minute timeout
        }
    }
}

impl Default for IntegrityCheckConfig {
    fn default() -> Self {
        Self::shallow()
    }
}

/// Results of an integrity check operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityCheckResult {
    /// Statistics about the check
    pub stats: IntegrityCheckStats,
    /// Issues found during the check
    pub issues: Vec<IntegrityIssue>,
    /// Configuration used for the check
    pub config: IntegrityCheckConfig,
}

impl IntegrityCheckResult {
    /// Create a new integrity check result
    pub fn new(config: IntegrityCheckConfig) -> Self {
        Self {
            stats: IntegrityCheckStats::new(),
            issues: Vec::new(),
            config,
        }
    }

    /// Add an issue to the result
    pub fn add_issue(&mut self, issue: IntegrityIssue) {
        self.stats.record_issue(issue.severity);
        self.issues.push(issue);
    }

    /// Check if the integrity check should stop based on configuration
    pub fn should_stop(&self) -> bool {
        if let Some(max_issues) = self.config.max_issues {
            if self.issues.len() >= max_issues {
                return true;
            }
        }

        if !self.config.continue_after_critical && self.stats.has_critical_issues() {
            return true;
        }

        false
    }

    /// Get issues grouped by severity
    pub fn issues_by_severity(&self) -> HashMap<IssueSeverity, Vec<&IntegrityIssue>> {
        let mut grouped = HashMap::new();
        for issue in &self.issues {
            grouped
                .entry(issue.severity)
                .or_insert_with(Vec::new)
                .push(issue);
        }
        grouped
    }

    /// Get repairable issues
    pub fn repairable_issues(&self) -> Vec<&IntegrityIssue> {
        self.issues
            .iter()
            .filter(|issue| issue.can_repair)
            .collect()
    }
}

/// Main integrity checker implementation
pub struct IntegrityChecker<'a> {
    metadata_store: &'a MetadataStore,
    _config: &'a StorageNodeConfig,
}

impl<'a> IntegrityChecker<'a> {
    /// Create a new integrity checker
    pub fn new(metadata_store: &'a MetadataStore, config: &'a StorageNodeConfig) -> Self {
        Self {
            metadata_store,
            _config: config,
        }
    }

    /// Perform a comprehensive integrity check on all stored data
    pub fn check_integrity(
        &self,
        check_config: IntegrityCheckConfig,
    ) -> IntegrityResult<IntegrityCheckResult> {
        info!("Starting integrity check with config: {:?}", check_config);
        let mut result = IntegrityCheckResult::new(check_config.clone());

        // Get all files from metadata store
        let files = self.metadata_store.list_files()?;
        info!("Checking integrity of {} files", files.len());

        for file in files {
            if result.should_stop() {
                warn!("Stopping integrity check due to configuration limits");
                result.stats.cancelled = true;
                break;
            }

            self.check_file_integrity(&file, &check_config, &mut result)?;
            result.stats.files_checked += 1;
        }

        result.stats.complete();
        info!(
            "Integrity check completed: {} issues found in {:?}",
            result.stats.total_issues(),
            result.stats.duration.unwrap_or_default()
        );

        Ok(result)
    }

    /// Check integrity of a specific file
    pub fn check_file_integrity(
        &self,
        file: &FileMetadata,
        config: &IntegrityCheckConfig,
        result: &mut IntegrityCheckResult,
    ) -> IntegrityResult<()> {
        debug!(
            "Checking file integrity: {} ({})",
            file.file_id,
            file.path.display()
        );

        result.stats.bytes_checked += file.size;

        // Check each stripe
        for stripe_index in 0..file.stripe_count {
            if result.should_stop() {
                break;
            }

            let stripe_id = StripeId::new(file.file_id, stripe_index);
            self.check_stripe_integrity(stripe_id, config, result)?;
            result.stats.stripes_checked += 1;
        }

        Ok(())
    }

    /// Check integrity of a specific stripe
    pub fn check_stripe_integrity(
        &self,
        stripe_id: StripeId,
        config: &IntegrityCheckConfig,
        result: &mut IntegrityCheckResult,
    ) -> IntegrityResult<()> {
        debug!("Checking stripe integrity: {:?}", stripe_id);

        // Get stripe metadata
        let stripe_metadata = match self.metadata_store.get_stripe(stripe_id) {
            Ok(metadata) => metadata,
            Err(MetadataError::StripeNotFound { .. }) => {
                let issue = IntegrityIssue::new(
                    IssueSeverity::Critical,
                    IssueType::InconsistentStripeMetadata,
                    format!("Stripe metadata missing for stripe {:?}", stripe_id),
                    "Recreate stripe metadata or remove orphaned chunks".to_string(),
                )
                .with_file_id(stripe_id.file_id)
                .with_stripe_index(stripe_id.stripe_index);

                result.add_issue(issue);
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        // Get chunks for this stripe
        let chunk_metadatas = self.metadata_store.get_chunks_for_stripe(stripe_id)?;
        debug!("Stripe has {} chunks", chunk_metadatas.len());

        // Check each chunk
        let mut available_chunks = 0;
        let mut chunk_data = vec![None; stripe_metadata.erasure_config.total_shards()];

        for chunk_metadata in &chunk_metadatas {
            if result.should_stop() {
                break;
            }

            let chunk_ok = self.check_chunk_integrity(chunk_metadata, config, result)?;
            if chunk_ok {
                available_chunks += 1;

                // If deep check is enabled, read chunk data for reconstruction test
                if config.deep_check && config.verify_reconstruction {
                    if let Ok(data) = self.read_chunk_data(&chunk_metadata.storage_location.path) {
                        chunk_data[chunk_metadata.chunk_index as usize] = Some(data);
                    }
                }
            }

            result.stats.chunks_checked += 1;
        }

        // Check if stripe is recoverable
        let required_chunks = stripe_metadata.erasure_config.data_shards;
        if available_chunks < required_chunks {
            let issue = IntegrityIssue::new(
                IssueSeverity::Critical,
                IssueType::UnrecoverableStripe,
                format!(
                    "Stripe {:?} has only {} available chunks, need {} for recovery",
                    stripe_id, available_chunks, required_chunks
                ),
                "Data loss detected - stripe cannot be reconstructed".to_string(),
            )
            .with_file_id(stripe_id.file_id)
            .with_stripe_index(stripe_id.stripe_index);

            result.add_issue(issue);
        } else if config.deep_check && config.verify_reconstruction {
            // Test stripe reconstruction
            match decode_stripe_with_size(
                &chunk_data,
                &stripe_metadata.erasure_config,
                stripe_metadata.original_size,
            ) {
                Ok(_) => {
                    debug!("Stripe {:?} reconstruction test passed", stripe_id);
                }
                Err(e) => {
                    let issue = IntegrityIssue::new(
                        IssueSeverity::Major,
                        IssueType::UnrecoverableStripe,
                        format!("Stripe {:?} reconstruction failed: {}", stripe_id, e),
                        "Check chunk integrity and repair corrupted chunks".to_string(),
                    )
                    .with_file_id(stripe_id.file_id)
                    .with_stripe_index(stripe_id.stripe_index);

                    result.add_issue(issue);
                }
            }
        }

        Ok(())
    }

    /// Check integrity of a specific chunk
    fn check_chunk_integrity(
        &self,
        chunk_metadata: &crate::metadata_store::ChunkMetadata,
        config: &IntegrityCheckConfig,
        result: &mut IntegrityCheckResult,
    ) -> IntegrityResult<bool> {
        let chunk_path = &chunk_metadata.storage_location.path;

        // Check if chunk file exists
        if !chunk_path.exists() {
            let issue = IntegrityIssue::new(
                IssueSeverity::Major,
                IssueType::MissingChunk,
                format!(
                    "Chunk file missing: stripe {}, chunk {}",
                    chunk_metadata.stripe_index, chunk_metadata.chunk_index
                ),
                "Recreate chunk from other chunks in stripe if possible".to_string(),
            )
            .with_file_id(chunk_metadata.file_id)
            .with_stripe_index(chunk_metadata.stripe_index)
            .with_chunk_index(chunk_metadata.chunk_index)
            .with_path(chunk_path)
            .with_repair_capability(true);

            result.add_issue(issue);
            return Ok(false);
        }

        // If checksum verification is enabled, read and validate chunk
        if config.verify_checksums {
            match self.read_and_validate_chunk(chunk_path) {
                Ok(_) => {
                    debug!("Chunk integrity OK: {:?}", chunk_path);
                    return Ok(true);
                }
                Err(e) => {
                    let issue = IntegrityIssue::new(
                        IssueSeverity::Major,
                        IssueType::CorruptedChunk,
                        format!(
                            "Chunk validation failed for stripe {}, chunk {}: {}",
                            chunk_metadata.stripe_index, chunk_metadata.chunk_index, e
                        ),
                        "Recreate chunk from other chunks in stripe".to_string(),
                    )
                    .with_file_id(chunk_metadata.file_id)
                    .with_stripe_index(chunk_metadata.stripe_index)
                    .with_chunk_index(chunk_metadata.chunk_index)
                    .with_path(chunk_path)
                    .with_repair_capability(true);

                    result.add_issue(issue);
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Read and validate a chunk file
    fn read_and_validate_chunk(&self, chunk_path: &Path) -> IntegrityResult<Vec<u8>> {
        let mut chunk_file = BufReader::new(File::open(chunk_path)?);
        let (chunk_header, chunk_data) = read_chunk(&mut chunk_file)?;
        validate_chunk(&chunk_header, &chunk_data)?;
        Ok(chunk_data)
    }

    /// Read chunk data without validation (for reconstruction tests)
    fn read_chunk_data(&self, chunk_path: &Path) -> IntegrityResult<Vec<u8>> {
        match self.read_and_validate_chunk(chunk_path) {
            Ok(data) => Ok(data),
            Err(_) => {
                // If validation fails, try to read just the data portion
                // This allows reconstruction testing even with corrupted checksums
                let mut chunk_file = BufReader::new(File::open(chunk_path)?);
                let (_, chunk_data) = read_chunk(&mut chunk_file)?;
                Ok(chunk_data)
            }
        }
    }

    /// Check integrity of a specific file by ID
    pub fn check_file_by_id(
        &self,
        file_id: Uuid,
        config: IntegrityCheckConfig,
    ) -> IntegrityResult<IntegrityCheckResult> {
        let file = self.metadata_store.get_file(file_id)?;
        let mut result = IntegrityCheckResult::new(config.clone());

        self.check_file_integrity(&file, &config, &mut result)?;
        result.stats.files_checked = 1;
        result.stats.complete();

        Ok(result)
    }

    /// Perform a quick health check on the storage system
    pub fn quick_health_check(&self) -> IntegrityResult<IntegrityCheckResult> {
        self.check_integrity(IntegrityCheckConfig::quick())
    }

    /// Get recommended repair actions for a set of issues
    pub fn get_repair_recommendations(&self, issues: &[IntegrityIssue]) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Group issues by type
        let mut missing_chunks = 0;
        let mut corrupted_chunks = 0;
        let mut unrecoverable_stripes = 0;

        for issue in issues {
            match issue.issue_type {
                IssueType::MissingChunk => missing_chunks += 1,
                IssueType::CorruptedChunk => corrupted_chunks += 1,
                IssueType::UnrecoverableStripe => unrecoverable_stripes += 1,
                _ => {}
            }
        }

        if missing_chunks > 0 {
            recommendations.push(format!(
                "Recreate {} missing chunks from erasure coding redundancy",
                missing_chunks
            ));
        }

        if corrupted_chunks > 0 {
            recommendations.push(format!(
                "Repair {} corrupted chunks using stripe reconstruction",
                corrupted_chunks
            ));
        }

        if unrecoverable_stripes > 0 {
            recommendations.push(format!(
                "Data loss detected in {} stripes - restore from backup if available",
                unrecoverable_stripes
            ));
        }

        if recommendations.is_empty() {
            recommendations.push("No repair actions needed".to_string());
        }

        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_node::StorageNode;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_setup() -> (StorageNode, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let mut config = crate::storage_node::StorageNodeConfig::new().unwrap();
        config.metadata_db_path = root.join("metadata.db");
        config.storage_root = root.join("chunks");
        config.min_free_space = 1024;
        config.max_file_size = 10 * 1024 * 1024; // 10MB for tests

        let storage_node = StorageNode::new(config).unwrap();
        (storage_node, temp_dir)
    }

    fn create_test_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let file_path = dir.join(name);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(content).unwrap();
        file_path
    }

    #[test]
    fn test_integrity_checker_creation() {
        let (storage_node, _temp_dir) = create_test_setup();

        let checker = IntegrityChecker::new(storage_node.metadata_store(), storage_node.config());

        // Basic functionality check
        let result = checker.quick_health_check().unwrap();
        assert_eq!(result.stats.files_checked, 0);
        assert_eq!(result.stats.total_issues(), 0);
    }

    #[test]
    fn test_integrity_check_healthy_file() {
        let (storage_node, temp_dir) = create_test_setup();

        // Store a test file
        let test_data = b"Hello, integrity checker! This is a test file.";
        let source_path = create_test_file(temp_dir.path(), "test.txt", test_data);
        let file_id = storage_node
            .store_file(&source_path, "/test/file.txt")
            .unwrap();

        // Create integrity checker
        let checker = IntegrityChecker::new(storage_node.metadata_store(), storage_node.config());

        // Check file integrity
        let result = checker
            .check_file_by_id(file_id, IntegrityCheckConfig::deep())
            .unwrap();

        assert_eq!(result.stats.files_checked, 1);
        assert_eq!(result.stats.total_issues(), 0);
        assert!(!result.stats.has_critical_issues());
    }

    #[test]
    fn test_shallow_vs_deep_check() {
        let (storage_node, temp_dir) = create_test_setup();

        // Store a test file
        let test_data = b"Test data for shallow vs deep check comparison";
        let source_path = create_test_file(temp_dir.path(), "test.txt", test_data);
        let file_id = storage_node
            .store_file(&source_path, "/test/file.txt")
            .unwrap();

        let checker = IntegrityChecker::new(storage_node.metadata_store(), storage_node.config());

        // Shallow check
        let shallow_result = checker
            .check_file_by_id(file_id, IntegrityCheckConfig::shallow())
            .unwrap();

        // Deep check
        let deep_result = checker
            .check_file_by_id(file_id, IntegrityCheckConfig::deep())
            .unwrap();

        // Both should pass for healthy data
        assert_eq!(shallow_result.stats.total_issues(), 0);
        assert_eq!(deep_result.stats.total_issues(), 0);

        // Deep check should take longer (though hard to test reliably)
        assert!(deep_result.config.deep_check);
        assert!(!shallow_result.config.deep_check);
    }

    #[test]
    fn test_integrity_issue_creation() {
        let issue = IntegrityIssue::new(
            IssueSeverity::Major,
            IssueType::MissingChunk,
            "Test chunk missing".to_string(),
            "Recreate from stripe".to_string(),
        )
        .with_file_id(Uuid::new_v4())
        .with_stripe_index(0)
        .with_chunk_index(1)
        .with_repair_capability(true);

        assert_eq!(issue.severity, IssueSeverity::Major);
        assert_eq!(issue.issue_type, IssueType::MissingChunk);
        assert!(issue.file_id.is_some());
        assert_eq!(issue.stripe_index, Some(0));
        assert_eq!(issue.chunk_index, Some(1));
        assert!(issue.can_repair);
    }

    #[test]
    fn test_check_config_presets() {
        let shallow = IntegrityCheckConfig::shallow();
        assert!(!shallow.deep_check);
        assert!(shallow.verify_checksums);
        assert!(!shallow.verify_reconstruction);

        let deep = IntegrityCheckConfig::deep();
        assert!(deep.deep_check);
        assert!(deep.verify_checksums);
        assert!(deep.verify_reconstruction);

        let quick = IntegrityCheckConfig::quick();
        assert!(!quick.deep_check);
        assert!(!quick.verify_checksums);
        assert_eq!(quick.max_issues, Some(100));
    }

    #[test]
    fn test_integrity_check_stats() {
        let mut stats = IntegrityCheckStats::new();

        assert_eq!(stats.total_issues(), 0);
        assert!(!stats.has_critical_issues());

        stats.record_issue(IssueSeverity::Warning);
        stats.record_issue(IssueSeverity::Critical);

        assert_eq!(stats.total_issues(), 2);
        assert!(stats.has_critical_issues());

        stats.complete();
        assert!(stats.completed_at.is_some());
        assert!(stats.duration.is_some());
    }
}
