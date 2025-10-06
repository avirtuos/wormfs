//! Sequence Tracker Module
//!
//! This module provides sequence number management for metadata events in WormFS.
//! It implements Phase 2B.4: Sequence Number Management.
//!
//! The tracker handles:
//! - Per-node sequence number generation and tracking
//! - Tracking last seen sequences from all peers
//! - Gap detection in received event sequences
//! - Persistence of sequence numbers across restarts
//! - Handling u64 rollover scenarios

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Represents a gap in the sequence of events from a peer
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceGap {
    /// The node ID that originated the events
    pub node_id: Uuid,
    /// Start of the gap (inclusive)
    pub start_sequence: u64,
    /// End of the gap (inclusive)
    pub end_sequence: u64,
}

impl SequenceGap {
    /// Create a new sequence gap
    pub fn new(node_id: Uuid, start: u64, end: u64) -> Self {
        Self {
            node_id,
            start_sequence: start,
            end_sequence: end,
        }
    }

    /// Number of missing events in this gap
    pub fn count(&self) -> u64 {
        self.end_sequence.saturating_sub(self.start_sequence) + 1
    }
}

/// Event emitted when a sequence gap is detected
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceEvent {
    /// A gap was detected in the sequence from a peer
    GapDetected(SequenceGap),
    /// A previously detected gap was filled
    GapFilled {
        node_id: Uuid,
        start_sequence: u64,
        end_sequence: u64,
    },
}

/// Tracks sequence numbers for local node and all peers
pub struct SequenceTracker {
    /// Our local node ID
    local_node_id: Uuid,
    /// Our local sequence number (next to be assigned)
    local_sequence: u64,
    /// Last seen sequence from each peer
    /// Map: node_id -> last_sequence
    peer_sequences: HashMap<Uuid, u64>,
    /// Detected gaps from peers
    /// Map: node_id -> Vec<SequenceGap>
    detected_gaps: HashMap<Uuid, Vec<SequenceGap>>,
}

impl SequenceTracker {
    /// Create a new SequenceTracker
    pub fn new(local_node_id: Uuid) -> Self {
        info!("SequenceTracker initialized for node {}", local_node_id);

        Self {
            local_node_id,
            local_sequence: 0,
            peer_sequences: HashMap::new(),
            detected_gaps: HashMap::new(),
        }
    }

    /// Create a SequenceTracker with a specific starting sequence number
    /// Used when restoring from persistence
    pub fn with_sequence(local_node_id: Uuid, starting_sequence: u64) -> Self {
        info!(
            "SequenceTracker initialized for node {} starting at sequence {}",
            local_node_id, starting_sequence
        );

        Self {
            local_node_id,
            local_sequence: starting_sequence,
            peer_sequences: HashMap::new(),
            detected_gaps: HashMap::new(),
        }
    }

    /// Get the next sequence number for a local event
    ///
    /// This increments the local sequence counter and returns the new value.
    /// The caller should persist this value for crash recovery.
    pub fn next_sequence(&mut self) -> u64 {
        // Handle u64::MAX rollover by wrapping to 1 (0 is reserved for "uninitialized")
        if self.local_sequence == u64::MAX {
            warn!(
                "Sequence number rollover detected for node {}",
                self.local_node_id
            );
            self.local_sequence = 1;
        } else {
            self.local_sequence += 1;
        }

        debug!(
            "Generated sequence {} for node {}",
            self.local_sequence, self.local_node_id
        );

        self.local_sequence
    }

    /// Get the current local sequence number without incrementing
    pub fn current_sequence(&self) -> u64 {
        self.local_sequence
    }

    /// Record a received event and check for gaps
    ///
    /// Returns Ok(Some(SequenceEvent)) if a gap was detected
    /// Returns Ok(None) if sequence is in order
    /// Returns Err if the event is invalid (e.g., from local node)
    pub fn record_event(&mut self, node_id: Uuid, sequence: u64) -> Result<Option<SequenceEvent>> {
        // Don't track our own events
        if node_id == self.local_node_id {
            return Err(anyhow!("Cannot record events from local node"));
        }

        // Get the last seen sequence for this peer
        let last_seen = self.peer_sequences.get(&node_id).copied().unwrap_or(0);

        debug!(
            "Recording event from node {}: sequence {} (last seen: {})",
            node_id, sequence, last_seen
        );

        // Check for gap
        if sequence > last_seen + 1 {
            // Gap detected!
            let gap = SequenceGap::new(node_id, last_seen + 1, sequence - 1);

            warn!(
                "Gap detected from node {}: missing sequences {}-{} ({} events)",
                node_id,
                gap.start_sequence,
                gap.end_sequence,
                gap.count()
            );

            // Record the gap
            self.detected_gaps
                .entry(node_id)
                .or_default()
                .push(gap.clone());

            // Update the last seen sequence
            self.peer_sequences.insert(node_id, sequence);

            return Ok(Some(SequenceEvent::GapDetected(gap)));
        }

        // Check if this fills an existing gap
        if sequence <= last_seen {
            // This might fill a gap
            if let Some(gaps) = self.detected_gaps.get_mut(&node_id) {
                // Find and remove any gaps that contain this sequence
                let mut filled_gaps = Vec::new();
                gaps.retain(|gap| {
                    if sequence >= gap.start_sequence && sequence <= gap.end_sequence {
                        filled_gaps.push(gap.clone());
                        false // Remove this gap
                    } else {
                        true // Keep this gap
                    }
                });

                if !filled_gaps.is_empty() {
                    for gap in filled_gaps {
                        info!(
                            "Filled gap from node {}: sequence {} was in range {}-{}",
                            node_id, sequence, gap.start_sequence, gap.end_sequence
                        );
                    }
                }
            }

            // Don't update last_seen for out-of-order events
            return Ok(None);
        }

        // Normal case: sequence == last_seen + 1
        self.peer_sequences.insert(node_id, sequence);

        Ok(None)
    }

    /// Get the last seen sequence for a specific peer
    pub fn last_seen_sequence(&self, node_id: &Uuid) -> Option<u64> {
        self.peer_sequences.get(node_id).copied()
    }

    /// Get all detected gaps for a specific peer
    pub fn get_gaps(&self, node_id: &Uuid) -> Vec<SequenceGap> {
        self.detected_gaps.get(node_id).cloned().unwrap_or_default()
    }

    /// Get all detected gaps across all peers
    pub fn get_all_gaps(&self) -> Vec<SequenceGap> {
        self.detected_gaps.values().flatten().cloned().collect()
    }

    /// Clear gaps for a specific peer (called after successful replay)
    pub fn clear_gaps(&mut self, node_id: &Uuid) {
        if let Some(gaps) = self.detected_gaps.remove(node_id) {
            info!("Cleared {} gaps for node {}", gaps.len(), node_id);
        }
    }

    /// Get statistics about tracked sequences
    pub fn stats(&self) -> SequenceStats {
        SequenceStats {
            local_sequence: self.local_sequence,
            tracked_peers: self.peer_sequences.len(),
            total_gaps: self.detected_gaps.values().map(|v| v.len()).sum(),
            peers_with_gaps: self.detected_gaps.len(),
        }
    }

    /// Restore peer sequences from persistence
    pub fn restore_peer_sequences(&mut self, peer_sequences: HashMap<Uuid, u64>) {
        info!(
            "Restoring {} peer sequences from persistence",
            peer_sequences.len()
        );
        self.peer_sequences = peer_sequences;
    }

    /// Get all peer sequences for persistence
    pub fn get_peer_sequences(&self) -> &HashMap<Uuid, u64> {
        &self.peer_sequences
    }
}

/// Statistics about sequence tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceStats {
    /// Current local sequence number
    pub local_sequence: u64,
    /// Number of peers being tracked
    pub tracked_peers: usize,
    /// Total number of gaps detected
    pub total_gaps: usize,
    /// Number of peers with gaps
    pub peers_with_gaps: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_generation() {
        let node_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::new(node_id);

        // First sequence should be 1
        assert_eq!(tracker.next_sequence(), 1);
        assert_eq!(tracker.current_sequence(), 1);

        // Should increment
        assert_eq!(tracker.next_sequence(), 2);
        assert_eq!(tracker.next_sequence(), 3);
        assert_eq!(tracker.current_sequence(), 3);
    }

    #[test]
    fn test_with_sequence() {
        let node_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::with_sequence(node_id, 100);

        assert_eq!(tracker.current_sequence(), 100);
        assert_eq!(tracker.next_sequence(), 101);
        assert_eq!(tracker.current_sequence(), 101);
    }

    #[test]
    fn test_sequence_rollover() {
        let node_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::with_sequence(node_id, u64::MAX);

        // Should wrap to 1
        assert_eq!(tracker.next_sequence(), 1);
        assert_eq!(tracker.current_sequence(), 1);
    }

    #[test]
    fn test_record_sequential_events() {
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::new(local_id);

        // Record sequential events
        let result = tracker.record_event(peer_id, 1).unwrap();
        assert!(result.is_none()); // No gap

        let result = tracker.record_event(peer_id, 2).unwrap();
        assert!(result.is_none()); // No gap

        assert_eq!(tracker.last_seen_sequence(&peer_id), Some(2));
    }

    #[test]
    fn test_gap_detection() {
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::new(local_id);

        // Record sequence 1
        tracker.record_event(peer_id, 1).unwrap();

        // Skip to sequence 5 - should detect gap
        let result = tracker.record_event(peer_id, 5).unwrap();
        assert!(result.is_some());

        if let Some(SequenceEvent::GapDetected(gap)) = result {
            assert_eq!(gap.node_id, peer_id);
            assert_eq!(gap.start_sequence, 2);
            assert_eq!(gap.end_sequence, 4);
            assert_eq!(gap.count(), 3);
        } else {
            panic!("Expected GapDetected event");
        }

        // Verify gap is tracked
        let gaps = tracker.get_gaps(&peer_id);
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].start_sequence, 2);
        assert_eq!(gaps[0].end_sequence, 4);
    }

    #[test]
    fn test_cannot_record_own_events() {
        let node_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::new(node_id);

        // Should fail to record event from self
        let result = tracker.record_event(node_id, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_gaps() {
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::new(local_id);

        // Create first gap
        tracker.record_event(peer_id, 1).unwrap();
        tracker.record_event(peer_id, 5).unwrap();

        // Create second gap
        tracker.record_event(peer_id, 10).unwrap();

        let gaps = tracker.get_gaps(&peer_id);
        assert_eq!(gaps.len(), 2);

        assert_eq!(gaps[0].start_sequence, 2);
        assert_eq!(gaps[0].end_sequence, 4);

        assert_eq!(gaps[1].start_sequence, 6);
        assert_eq!(gaps[1].end_sequence, 9);
    }

    #[test]
    fn test_clear_gaps() {
        let local_id = Uuid::new_v4();
        let peer_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::new(local_id);

        // Create gap
        tracker.record_event(peer_id, 1).unwrap();
        tracker.record_event(peer_id, 5).unwrap();

        assert_eq!(tracker.get_gaps(&peer_id).len(), 1);

        // Clear gaps
        tracker.clear_gaps(&peer_id);
        assert_eq!(tracker.get_gaps(&peer_id).len(), 0);
    }

    #[test]
    fn test_stats() {
        let local_id = Uuid::new_v4();
        let peer1 = Uuid::new_v4();
        let peer2 = Uuid::new_v4();
        let mut tracker = SequenceTracker::new(local_id);

        tracker.next_sequence();
        tracker.next_sequence();

        tracker.record_event(peer1, 1).unwrap();
        tracker.record_event(peer1, 5).unwrap(); // Creates gap

        tracker.record_event(peer2, 1).unwrap();

        let stats = tracker.stats();
        assert_eq!(stats.local_sequence, 2);
        assert_eq!(stats.tracked_peers, 2);
        assert_eq!(stats.total_gaps, 1);
        assert_eq!(stats.peers_with_gaps, 1);
    }

    #[test]
    fn test_restore_peer_sequences() {
        let node_id = Uuid::new_v4();
        let mut tracker = SequenceTracker::with_sequence(node_id, 100);

        let peer1 = Uuid::new_v4();
        let peer2 = Uuid::new_v4();

        let mut sequences = HashMap::new();
        sequences.insert(peer1, 50);
        sequences.insert(peer2, 75);

        tracker.restore_peer_sequences(sequences);

        assert_eq!(tracker.last_seen_sequence(&peer1), Some(50));
        assert_eq!(tracker.last_seen_sequence(&peer2), Some(75));
    }
}
