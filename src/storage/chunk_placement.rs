//! Chunk Placement Strategy Module
//!
//! This module implements intelligent chunk placement algorithms for WormFS
//! that ensure blast radius protection (max 1 chunk per stripe per disk),
//! load balancing across available disks, and fault tolerance.

use crate::disk_manager::{DiskInfo, DiskManager};
use crate::metadata_store::{ChunkId, StripeId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use tracing::{debug, warn};

/// Configuration for chunk placement strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkPlacementConfig {
    /// Minimum number of disks required for placement
    pub min_disks_required: usize,
    /// Whether to prefer disks with more available space
    pub prefer_more_space: bool,
    /// Maximum attempts to find placement before giving up
    pub max_placement_attempts: u32,
    /// Whether to allow degraded placement (fewer disks than chunks)
    pub allow_degraded_placement: bool,
}

impl Default for ChunkPlacementConfig {
    fn default() -> Self {
        Self {
            min_disks_required: 1,
            prefer_more_space: true,
            max_placement_attempts: 10,
            allow_degraded_placement: false,
        }
    }
}

/// Result of a chunk placement operation
#[derive(Debug, Clone)]
pub struct PlacementResult {
    /// Disk assignments for each chunk index
    pub chunk_to_disk: HashMap<u8, String>,
    /// Disks that will be used for this placement
    pub disks_used: HashSet<String>,
    /// Whether this is a degraded placement (some chunks on same disk)
    pub is_degraded: bool,
    /// Placement quality score (0.0 = worst, 1.0 = perfect)
    pub quality_score: f64,
}

impl PlacementResult {
    /// Create a new placement result
    pub fn new(chunk_to_disk: HashMap<u8, String>) -> Self {
        let disks_used: HashSet<String> = chunk_to_disk.values().cloned().collect();
        let is_degraded = disks_used.len() < chunk_to_disk.len();

        // Calculate quality score based on distribution
        let quality_score = if is_degraded {
            // Degraded placement gets lower score
            (disks_used.len() as f64) / (chunk_to_disk.len() as f64) * 0.8
        } else {
            // Perfect distribution gets full score
            1.0
        };

        Self {
            chunk_to_disk,
            disks_used,
            is_degraded,
            quality_score,
        }
    }

    /// Get the disk ID for a specific chunk index
    pub fn get_disk_for_chunk(&self, chunk_index: u8) -> Option<&String> {
        self.chunk_to_disk.get(&chunk_index)
    }

    /// Get the number of disks used
    pub fn disk_count(&self) -> usize {
        self.disks_used.len()
    }

    /// Check if placement meets minimum quality requirements
    pub fn meets_quality_threshold(&self, threshold: f64) -> bool {
        self.quality_score >= threshold
    }
}

/// Information about existing chunk placements for blast radius checking
#[derive(Debug, Clone)]
pub struct ExistingPlacements {
    /// Map of stripe_id to (chunk_index -> disk_id)
    stripe_placements: HashMap<StripeId, HashMap<u8, String>>,
}

impl ExistingPlacements {
    /// Create new empty placement tracker
    pub fn new() -> Self {
        Self {
            stripe_placements: HashMap::new(),
        }
    }

    /// Add an existing chunk placement
    pub fn add_chunk_placement(&mut self, chunk_id: ChunkId, disk_id: String) {
        let stripe_id = chunk_id.stripe_id();
        self.stripe_placements
            .entry(stripe_id)
            .or_default()
            .insert(chunk_id.chunk_index, disk_id);
    }

    /// Get existing placements for a stripe
    pub fn get_stripe_placements(&self, stripe_id: StripeId) -> Option<&HashMap<u8, String>> {
        self.stripe_placements.get(&stripe_id)
    }

    /// Check if a disk already has a chunk from this stripe
    pub fn disk_has_chunk_from_stripe(&self, stripe_id: StripeId, disk_id: &str) -> bool {
        if let Some(placements) = self.stripe_placements.get(&stripe_id) {
            placements
                .values()
                .any(|existing_disk| existing_disk == disk_id)
        } else {
            false
        }
    }

    /// Remove all placements for a stripe (for cleanup)
    pub fn remove_stripe(&mut self, stripe_id: StripeId) {
        self.stripe_placements.remove(&stripe_id);
    }
}

impl Default for ExistingPlacements {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during chunk placement
#[derive(Error, Debug)]
pub enum ChunkPlacementError {
    #[error("No disks available for chunk placement")]
    NoDisksAvailable,

    #[error("Insufficient disks available: need {required}, have {available}")]
    InsufficientDisks { required: usize, available: usize },

    #[error("Blast radius violation: disk {disk_id} already has chunk from stripe {stripe_id}")]
    BlastRadiusViolation {
        disk_id: String,
        stripe_id: StripeId,
    },

    #[error("Placement quality too low: {quality:.2} < {threshold:.2}")]
    QualityTooLow { quality: f64, threshold: f64 },

    #[error("Max placement attempts exceeded: {attempts}")]
    MaxAttemptsExceeded { attempts: u32 },

    #[error("Configuration error: {reason}")]
    Configuration { reason: String },
}

/// Result type for chunk placement operations
pub type ChunkPlacementResult<T> = Result<T, ChunkPlacementError>;

/// Main chunk placement strategy implementation
#[derive(Default)]
pub struct ChunkPlacementStrategy {
    config: ChunkPlacementConfig,
}

impl ChunkPlacementStrategy {
    /// Create a new chunk placement strategy
    pub fn new(config: ChunkPlacementConfig) -> ChunkPlacementResult<Self> {
        // Validate configuration
        if config.min_disks_required == 0 {
            return Err(ChunkPlacementError::Configuration {
                reason: "min_disks_required must be at least 1".to_string(),
            });
        }

        Ok(Self { config })
    }

    /// Place chunks for a new stripe across available disks
    pub fn place_stripe_chunks(
        &self,
        stripe_id: StripeId,
        chunk_count: usize,
        disk_manager: &DiskManager,
        existing_placements: &ExistingPlacements,
    ) -> ChunkPlacementResult<PlacementResult> {
        debug!(
            "Placing {} chunks for stripe {} across available disks",
            chunk_count, stripe_id
        );

        let available_disks = disk_manager.get_available_disks();

        if available_disks.is_empty() {
            return Err(ChunkPlacementError::NoDisksAvailable);
        }

        if available_disks.len() < self.config.min_disks_required {
            return Err(ChunkPlacementError::InsufficientDisks {
                required: self.config.min_disks_required,
                available: available_disks.len(),
            });
        }

        // Filter out disks that already have chunks from this stripe
        let eligible_disks: Vec<&DiskInfo> = available_disks
            .into_iter()
            .filter(|disk| {
                !existing_placements.disk_has_chunk_from_stripe(stripe_id, &disk.disk_id)
            })
            .collect();

        debug!(
            "Found {} eligible disks for placement",
            eligible_disks.len()
        );

        if eligible_disks.is_empty() {
            return Err(ChunkPlacementError::NoDisksAvailable);
        }

        // Check if we can achieve perfect placement (one chunk per disk)
        let perfect_placement_possible = eligible_disks.len() >= chunk_count;

        if !perfect_placement_possible && !self.config.allow_degraded_placement {
            return Err(ChunkPlacementError::InsufficientDisks {
                required: chunk_count,
                available: eligible_disks.len(),
            });
        }

        // Attempt placement with multiple strategies
        for attempt in 0..self.config.max_placement_attempts {
            match self.attempt_placement(chunk_count, &eligible_disks, attempt) {
                Ok(placement) => {
                    debug!(
                        "Successful placement on attempt {}: {} disks used, quality {:.2}",
                        attempt + 1,
                        placement.disk_count(),
                        placement.quality_score
                    );
                    return Ok(placement);
                }
                Err(e) => {
                    debug!("Placement attempt {} failed: {}", attempt + 1, e);
                    continue;
                }
            }
        }

        Err(ChunkPlacementError::MaxAttemptsExceeded {
            attempts: self.config.max_placement_attempts,
        })
    }

    /// Attempt to place chunks using a specific strategy
    fn attempt_placement(
        &self,
        chunk_count: usize,
        eligible_disks: &[&DiskInfo],
        attempt: u32,
    ) -> ChunkPlacementResult<PlacementResult> {
        let mut chunk_to_disk = HashMap::new();

        // Sort disks by preferred criteria
        let mut sorted_disks = eligible_disks.to_vec();
        self.sort_disks_by_preference(&mut sorted_disks, attempt);

        // Assign chunks to disks
        for chunk_index in 0..chunk_count {
            let disk_index = if sorted_disks.len() >= chunk_count {
                // Perfect placement: one chunk per disk
                chunk_index % sorted_disks.len()
            } else {
                // Degraded placement: distribute as evenly as possible
                self.calculate_degraded_disk_index(chunk_index, sorted_disks.len(), chunk_count)
            };

            let selected_disk = &sorted_disks[disk_index];
            chunk_to_disk.insert(chunk_index as u8, selected_disk.disk_id.clone());
        }

        let placement = PlacementResult::new(chunk_to_disk);

        // Validate placement quality if this is not a degraded placement attempt
        if !self.config.allow_degraded_placement && placement.is_degraded {
            return Err(ChunkPlacementError::QualityTooLow {
                quality: placement.quality_score,
                threshold: 1.0,
            });
        }

        Ok(placement)
    }

    /// Sort disks according to placement preferences and attempt number
    fn sort_disks_by_preference(&self, disks: &mut Vec<&DiskInfo>, attempt: u32) {
        match attempt {
            0 => {
                // First attempt: prefer disks with more available space
                if self.config.prefer_more_space {
                    disks.sort_by(|a, b| b.available_space.cmp(&a.available_space));
                }
            }
            1 => {
                // Second attempt: prefer disks with less usage percentage
                disks.sort_by(|a, b| {
                    a.usage_percentage()
                        .partial_cmp(&b.usage_percentage())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
            2 => {
                // Third attempt: randomize order (using disk_id as pseudo-random)
                disks.sort_by(|a, b| a.disk_id.cmp(&b.disk_id));
            }
            _ => {
                // Subsequent attempts: reverse previous order
                disks.reverse();
            }
        }
    }

    /// Calculate disk index for degraded placement to distribute chunks evenly
    fn calculate_degraded_disk_index(
        &self,
        chunk_index: usize,
        disk_count: usize,
        chunk_count: usize,
    ) -> usize {
        // Use round-robin distribution to spread chunks as evenly as possible
        (chunk_index * disk_count) / chunk_count
    }

    /// Validate an existing placement against blast radius rules
    pub fn validate_placement(
        &self,
        stripe_id: StripeId,
        placement: &PlacementResult,
        existing_placements: &ExistingPlacements,
    ) -> ChunkPlacementResult<()> {
        // Check blast radius: no disk should have more than one chunk from same stripe
        let mut disk_chunk_count = HashMap::new();

        for disk_id in placement.chunk_to_disk.values() {
            *disk_chunk_count.entry(disk_id).or_insert(0) += 1;

            // Check against existing placements
            if existing_placements.disk_has_chunk_from_stripe(stripe_id, disk_id) {
                return Err(ChunkPlacementError::BlastRadiusViolation {
                    disk_id: disk_id.clone(),
                    stripe_id,
                });
            }
        }

        // Verify no disk gets more than one chunk in this placement
        for (disk_id, count) in disk_chunk_count {
            if count > 1 {
                warn!(
                    "Blast radius violation in placement: disk {} assigned {} chunks from stripe {}",
                    disk_id, count, stripe_id
                );
                return Err(ChunkPlacementError::BlastRadiusViolation {
                    disk_id: disk_id.clone(),
                    stripe_id,
                });
            }
        }

        Ok(())
    }

    /// Get placement statistics for monitoring
    pub fn get_placement_stats(&self, placements: &[PlacementResult]) -> PlacementStats {
        if placements.is_empty() {
            return PlacementStats::default();
        }

        let total_placements = placements.len();
        let degraded_placements = placements.iter().filter(|p| p.is_degraded).count();
        let perfect_placements = total_placements - degraded_placements;

        let average_quality =
            placements.iter().map(|p| p.quality_score).sum::<f64>() / total_placements as f64;

        let average_disks_used = placements.iter().map(|p| p.disk_count()).sum::<usize>() as f64
            / total_placements as f64;

        PlacementStats {
            total_placements,
            perfect_placements,
            degraded_placements,
            average_quality,
            average_disks_used,
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &ChunkPlacementConfig {
        &self.config
    }
}

/// Statistics about chunk placements
#[derive(Debug, Clone)]
pub struct PlacementStats {
    /// Total number of placements analyzed
    pub total_placements: usize,
    /// Number of perfect placements (one chunk per disk)
    pub perfect_placements: usize,
    /// Number of degraded placements (multiple chunks per disk)
    pub degraded_placements: usize,
    /// Average quality score across all placements
    pub average_quality: f64,
    /// Average number of disks used per placement
    pub average_disks_used: f64,
}

impl Default for PlacementStats {
    fn default() -> Self {
        Self {
            total_placements: 0,
            perfect_placements: 0,
            degraded_placements: 0,
            average_quality: 0.0,
            average_disks_used: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_manager::{DiskConfig, DiskManager, DiskManagerConfig};
    use tempfile::TempDir;
    use uuid::Uuid;

    fn create_test_disk_manager(disk_count: usize) -> (DiskManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut disk_configs = Vec::new();

        for i in 0..disk_count {
            let disk_id = format!("disk{}", i + 1);
            let path = temp_dir.path().join(&disk_id);
            std::fs::create_dir_all(&path).unwrap();
            disk_configs.push(DiskConfig::new(disk_id, path, 1024));
        }

        let config = DiskManagerConfig::new(disk_configs);
        let manager = DiskManager::new(config).unwrap();
        (manager, temp_dir)
    }

    #[test]
    fn test_chunk_placement_strategy_creation() {
        let config = ChunkPlacementConfig::default();
        let strategy = ChunkPlacementStrategy::new(config).unwrap();
        assert!(strategy.config().min_disks_required >= 1);
    }

    #[test]
    fn test_invalid_configuration() {
        let config = ChunkPlacementConfig {
            min_disks_required: 0,
            ..Default::default()
        };

        let result = ChunkPlacementStrategy::new(config);
        assert!(matches!(
            result,
            Err(ChunkPlacementError::Configuration { .. })
        ));
    }

    #[test]
    fn test_perfect_placement() {
        let (disk_manager, _temp_dir) = create_test_disk_manager(6);
        let strategy = ChunkPlacementStrategy::default();
        let existing_placements = ExistingPlacements::new();

        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);

        // Place 4 chunks across 6 disks - should achieve perfect placement
        let result = strategy
            .place_stripe_chunks(stripe_id, 4, &disk_manager, &existing_placements)
            .unwrap();

        assert!(!result.is_degraded);
        assert_eq!(result.chunk_to_disk.len(), 4);
        assert_eq!(result.disks_used.len(), 4);
        assert_eq!(result.quality_score, 1.0);
    }

    #[test]
    fn test_degraded_placement() {
        let (disk_manager, _temp_dir) = create_test_disk_manager(2);
        let config = ChunkPlacementConfig {
            allow_degraded_placement: true,
            ..Default::default()
        };
        let strategy = ChunkPlacementStrategy::new(config).unwrap();
        let existing_placements = ExistingPlacements::new();

        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);

        // Place 4 chunks across 2 disks - should result in degraded placement
        let result = strategy
            .place_stripe_chunks(stripe_id, 4, &disk_manager, &existing_placements)
            .unwrap();

        assert!(result.is_degraded);
        assert_eq!(result.chunk_to_disk.len(), 4);
        assert_eq!(result.disks_used.len(), 2);
        assert!(result.quality_score < 1.0);
    }

    #[test]
    fn test_insufficient_disks_error() {
        let (disk_manager, _temp_dir) = create_test_disk_manager(1);
        let config = ChunkPlacementConfig {
            min_disks_required: 3, // Require more disks than available
            ..Default::default()
        };

        let strategy = ChunkPlacementStrategy::new(config).unwrap();
        let existing_placements = ExistingPlacements::new();

        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);

        let result =
            strategy.place_stripe_chunks(stripe_id, 4, &disk_manager, &existing_placements);

        assert!(matches!(
            result,
            Err(ChunkPlacementError::InsufficientDisks { .. })
        ));
    }

    #[test]
    fn test_blast_radius_protection() {
        let (disk_manager, _temp_dir) = create_test_disk_manager(4);
        let strategy = ChunkPlacementStrategy::default();
        let mut existing_placements = ExistingPlacements::new();

        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);

        // Add existing chunk placement
        let existing_chunk_id = ChunkId::new(file_id, 0, 0);
        existing_placements.add_chunk_placement(existing_chunk_id, "disk1".to_string());

        // Try to place 2 more chunks - should avoid disk1 and use remaining disks
        let result = strategy
            .place_stripe_chunks(stripe_id, 2, &disk_manager, &existing_placements)
            .unwrap();

        // Verify disk1 is not used in new placement
        assert!(!result.disks_used.contains("disk1"));
        assert!(
            result.disks_used.contains("disk2")
                || result.disks_used.contains("disk3")
                || result.disks_used.contains("disk4")
        );
    }

    #[test]
    fn test_existing_placements_tracking() {
        let mut placements = ExistingPlacements::new();
        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);
        let chunk_id = ChunkId::new(file_id, 0, 0);

        // Initially no placements
        assert!(!placements.disk_has_chunk_from_stripe(stripe_id, "disk1"));

        // Add placement
        placements.add_chunk_placement(chunk_id, "disk1".to_string());
        assert!(placements.disk_has_chunk_from_stripe(stripe_id, "disk1"));
        assert!(!placements.disk_has_chunk_from_stripe(stripe_id, "disk2"));

        // Remove stripe
        placements.remove_stripe(stripe_id);
        assert!(!placements.disk_has_chunk_from_stripe(stripe_id, "disk1"));
    }

    #[test]
    fn test_placement_validation() {
        let strategy = ChunkPlacementStrategy::default();
        let existing_placements = ExistingPlacements::new();

        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);

        // Create valid placement
        let mut chunk_to_disk = HashMap::new();
        chunk_to_disk.insert(0, "disk1".to_string());
        chunk_to_disk.insert(1, "disk2".to_string());
        let placement = PlacementResult::new(chunk_to_disk);

        // Should validate successfully
        assert!(strategy
            .validate_placement(stripe_id, &placement, &existing_placements)
            .is_ok());
    }

    #[test]
    fn test_placement_validation_blast_radius_violation() {
        let strategy = ChunkPlacementStrategy::default();
        let mut existing_placements = ExistingPlacements::new();

        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);

        // Add existing placement
        let existing_chunk_id = ChunkId::new(file_id, 0, 0);
        existing_placements.add_chunk_placement(existing_chunk_id, "disk1".to_string());

        // Create placement that violates blast radius
        let mut chunk_to_disk = HashMap::new();
        chunk_to_disk.insert(1, "disk1".to_string()); // Same disk as existing chunk
        let placement = PlacementResult::new(chunk_to_disk);

        // Should fail validation
        let result = strategy.validate_placement(stripe_id, &placement, &existing_placements);
        assert!(matches!(
            result,
            Err(ChunkPlacementError::BlastRadiusViolation { .. })
        ));
    }

    #[test]
    fn test_placement_stats() {
        let strategy = ChunkPlacementStrategy::default();

        // Create test placements
        let mut placements = Vec::new();

        // Perfect placement
        let mut chunk_to_disk1 = HashMap::new();
        chunk_to_disk1.insert(0, "disk1".to_string());
        chunk_to_disk1.insert(1, "disk2".to_string());
        placements.push(PlacementResult::new(chunk_to_disk1));

        // Degraded placement
        let mut chunk_to_disk2 = HashMap::new();
        chunk_to_disk2.insert(0, "disk1".to_string());
        chunk_to_disk2.insert(1, "disk1".to_string()); // Same disk
        placements.push(PlacementResult::new(chunk_to_disk2));

        let stats = strategy.get_placement_stats(&placements);

        assert_eq!(stats.total_placements, 2);
        assert_eq!(stats.perfect_placements, 1);
        assert_eq!(stats.degraded_placements, 1);
        assert!(stats.average_quality > 0.0 && stats.average_quality < 1.0);
    }

    #[test]
    fn test_placement_result_operations() {
        let mut chunk_to_disk = HashMap::new();
        chunk_to_disk.insert(0, "disk1".to_string());
        chunk_to_disk.insert(1, "disk2".to_string());
        chunk_to_disk.insert(2, "disk1".to_string()); // Degraded: same disk

        let placement = PlacementResult::new(chunk_to_disk);

        assert_eq!(placement.disk_count(), 2);
        assert!(placement.is_degraded);
        assert!(placement.quality_score < 1.0);
        assert_eq!(placement.get_disk_for_chunk(0), Some(&"disk1".to_string()));
        assert_eq!(placement.get_disk_for_chunk(1), Some(&"disk2".to_string()));
        assert_eq!(placement.get_disk_for_chunk(3), None);

        assert!(placement.meets_quality_threshold(0.5));
        assert!(!placement.meets_quality_threshold(0.9));
    }
}
