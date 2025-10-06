//! Metadata Replicator Module
//!
//! This module provides the MetadataReplicator component for distributing metadata
//! events across the WormFS cluster. It implements Phase 2B.3: Event Broadcasting Foundation
//! and Phase 2B.4: Sequence Number Management.
//!
//! The replicator handles:
//! - Broadcasting metadata events to all connected peers
//! - Preventing self-echo (sender doesn't receive own events)
//! - Fire-and-forget delivery (reliability added in later phases)
//! - Dynamic peer set handling (peers joining/leaving)
//! - Sequence number generation and gap detection
//! - Persistence of sequence state

use crate::metadata_protocol::MetadataEvent;
use crate::metadata_protocol_handler::MetadataMessage;
use crate::metadata_store::MetadataStore;
use crate::networking::NetworkServiceHandle;
use crate::sequence_tracker::{SequenceEvent, SequenceStats, SequenceTracker};
use anyhow::Result;
use libp2p::PeerId;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// The MetadataReplicator component manages metadata event distribution
pub struct MetadataReplicator {
    /// Handle to the networking layer for sending messages
    network_handle: NetworkServiceHandle,
    /// Local peer ID to prevent self-echo
    local_peer_id: PeerId,
    /// Local node ID for event origination
    local_node_id: Uuid,
    /// Sequence tracker for event ordering and gap detection
    sequence_tracker: Arc<RwLock<SequenceTracker>>,
    /// Metadata store for persistence
    metadata_store: Arc<MetadataStore>,
    /// Counter for events since last persistence
    events_since_persist: Arc<RwLock<u64>>,
    /// Last time we persisted sequences
    last_persist_time: Arc<RwLock<Instant>>,
}

impl MetadataReplicator {
    /// Create a new MetadataReplicator
    pub async fn new(
        network_handle: NetworkServiceHandle,
        local_peer_id: PeerId,
        local_node_id: Uuid,
        metadata_store: Arc<MetadataStore>,
    ) -> Result<Self> {
        info!(
            "MetadataReplicator initializing for peer {} (node {})",
            local_peer_id, local_node_id
        );

        // Load persisted sequence state
        let local_sequence = metadata_store.load_sequence_number(local_node_id)?;
        let peer_sequences = metadata_store.load_peer_sequences(local_node_id)?;

        // Create sequence tracker with restored state
        let mut tracker = if let Some(seq) = local_sequence {
            info!("Restored local sequence: {}", seq);
            SequenceTracker::with_sequence(local_node_id, seq)
        } else {
            SequenceTracker::new(local_node_id)
        };

        if !peer_sequences.is_empty() {
            tracker.restore_peer_sequences(peer_sequences);
        }

        info!(
            "MetadataReplicator initialized for peer {} (node {})",
            local_peer_id, local_node_id
        );

        Ok(Self {
            network_handle,
            local_peer_id,
            local_node_id,
            sequence_tracker: Arc::new(RwLock::new(tracker)),
            metadata_store,
            events_since_persist: Arc::new(RwLock::new(0)),
            last_persist_time: Arc::new(RwLock::new(Instant::now())),
        })
    }

    /// Broadcast a metadata event to all connected peers
    ///
    /// This is a fire-and-forget operation in Phase 2B.3. Reliability will be
    /// added in later phases (acknowledgments, retries, etc.)
    pub async fn broadcast_event(&self, event: MetadataEvent) -> Result<()> {
        // Get list of connected peers
        let peers = self.network_handle.list_connected_peers().await?;

        if peers.is_empty() {
            debug!("No peers connected, skipping broadcast");
            return Ok(());
        }

        info!(
            "Broadcasting metadata event (seq: {}) to {} peers",
            event.sequence_number,
            peers.len()
        );

        // Track broadcast success/failure
        let mut success_count = 0;
        let mut failure_count = 0;

        // Broadcast to all peers except self
        for peer_id in peers {
            // Skip self to prevent echo
            if peer_id == self.local_peer_id {
                debug!("Skipping self-echo for peer {}", peer_id);
                continue;
            }

            // Send the event to this peer
            match self.send_event_to_peer(peer_id, &event).await {
                Ok(()) => {
                    debug!("Successfully sent event to peer {}", peer_id);
                    success_count += 1;
                }
                Err(e) => {
                    warn!("Failed to send event to peer {}: {}", peer_id, e);
                    failure_count += 1;
                }
            }
        }

        info!(
            "Broadcast complete: {} successful, {} failed",
            success_count, failure_count
        );

        Ok(())
    }

    /// Send a metadata event to a specific peer
    ///
    /// This uses the metadata protocol handler to send the event via libp2p
    async fn send_event_to_peer(&self, peer_id: PeerId, event: &MetadataEvent) -> Result<()> {
        // Wrap the event in a MetadataMessage
        let message = MetadataMessage::Event(event.clone());

        debug!("Sending metadata event to peer {}", peer_id);

        // Send the message via the network service
        self.network_handle
            .send_metadata_message(peer_id, message)
            .await?;

        Ok(())
    }

    /// Get the next sequence number for event ordering
    ///
    /// Uses the sequence tracker for proper per-node sequence management
    pub async fn next_sequence_number(&self) -> u64 {
        let mut tracker = self.sequence_tracker.write().await;
        let sequence = tracker.next_sequence();

        // Save to metadata store
        if let Err(e) = self
            .metadata_store
            .save_sequence_number(self.local_node_id, sequence)
        {
            warn!("Failed to persist sequence number: {}", e);
        }

        sequence
    }

    /// Handle a received metadata event
    ///
    /// Records the event sequence and detects gaps
    pub async fn handle_received_event(&self, node_id: Uuid, sequence: u64) -> Result<()> {
        let mut tracker = self.sequence_tracker.write().await;

        match tracker.record_event(node_id, sequence)? {
            Some(SequenceEvent::GapDetected(gap)) => {
                warn!(
                    "Gap detected from node {}: missing sequences {}-{} ({} events)",
                    gap.node_id,
                    gap.start_sequence,
                    gap.end_sequence,
                    gap.count()
                );
                // TODO: Phase 2B.8 will implement gap recovery/replay
            }
            Some(SequenceEvent::GapFilled {
                node_id,
                start_sequence,
                end_sequence,
            }) => {
                info!(
                    "Gap filled from node {}: sequences {}-{}",
                    node_id, start_sequence, end_sequence
                );
            }
            None => {
                debug!(
                    "Received in-order event from node {} (seq: {})",
                    node_id, sequence
                );
            }
        }

        // Periodically persist peer sequences
        self.persist_sequences_if_needed().await?;

        Ok(())
    }

    /// Persist sequences if enough events have occurred or enough time has passed
    async fn persist_sequences_if_needed(&self) -> Result<()> {
        const PERSIST_EVENT_THRESHOLD: u64 = 100;
        const PERSIST_TIME_THRESHOLD: Duration = Duration::from_secs(10);

        let mut events_count = self.events_since_persist.write().await;
        *events_count += 1;

        let mut last_time = self.last_persist_time.write().await;
        let now = Instant::now();
        let time_elapsed = now.duration_since(*last_time);

        let should_persist =
            *events_count >= PERSIST_EVENT_THRESHOLD || time_elapsed >= PERSIST_TIME_THRESHOLD;

        if should_persist {
            let tracker = self.sequence_tracker.read().await;
            let peer_sequences = tracker.get_peer_sequences();

            self.metadata_store
                .save_peer_sequences(self.local_node_id, peer_sequences)
                .map_err(|e| anyhow::anyhow!("Failed to save peer sequences: {}", e))?;

            debug!(
                "Persisted {} peer sequences (events: {}, time: {:?})",
                peer_sequences.len(),
                *events_count,
                time_elapsed
            );

            *events_count = 0;
            *last_time = now;
        }

        Ok(())
    }

    /// Get sequence tracker statistics
    pub async fn get_sequence_stats(&self) -> SequenceStats {
        let tracker = self.sequence_tracker.read().await;
        tracker.stats()
    }

    /// Get the local peer ID
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    /// Get the local node ID
    pub fn local_node_id(&self) -> Uuid {
        self.local_node_id
    }

    /// Create a peer info structure for event origination
    pub fn create_peer_info(&self) -> crate::metadata_protocol::MetadataPeerInfo {
        crate::metadata_protocol::MetadataPeerInfo {
            peer_id: self.local_peer_id.to_string(),
            node_id: self.local_node_id.to_string(),
            timestamp: crate::metadata_protocol::system_time_to_timestamp(
                std::time::SystemTime::now(),
            ),
        }
    }
}

/// Helper functions for creating common metadata events
impl MetadataReplicator {
    /// Create and broadcast a FileCreated event
    pub async fn broadcast_file_created(
        &self,
        file_metadata: &crate::metadata_store::FileMetadata,
    ) -> Result<()> {
        let sequence = self.next_sequence_number().await;

        let event = crate::metadata_protocol::create_file_created_event(
            sequence,
            self.local_peer_id.to_string(),
            self.local_node_id,
            file_metadata,
        );

        self.broadcast_event(event).await
    }

    /// Create and broadcast a FileDeleted event
    pub async fn broadcast_file_deleted(
        &self,
        file_id: Uuid,
        path: std::path::PathBuf,
    ) -> Result<()> {
        let sequence = self.next_sequence_number().await;

        let event = crate::metadata_protocol::create_file_deleted_event(
            sequence,
            self.local_peer_id.to_string(),
            self.local_node_id,
            file_id,
            path,
        );

        self.broadcast_event(event).await
    }

    /// Create and broadcast a ChunkCreated event
    pub async fn broadcast_chunk_created(
        &self,
        chunk_metadata: &crate::metadata_store::ChunkMetadata,
    ) -> Result<()> {
        let sequence = self.next_sequence_number().await;

        let event = crate::metadata_protocol::create_chunk_placed_event(
            sequence,
            self.local_peer_id.to_string(),
            self.local_node_id,
            chunk_metadata,
        );

        self.broadcast_event(event).await
    }

    /// Create and broadcast a ChunkRemoved event
    pub async fn broadcast_chunk_removed(
        &self,
        chunk_id: crate::metadata_store::ChunkId,
        node_id: Uuid,
    ) -> Result<()> {
        let sequence = self.next_sequence_number().await;

        let event = crate::metadata_protocol::create_chunk_removed_event(
            sequence,
            self.local_peer_id.to_string(),
            self.local_node_id,
            chunk_id,
            node_id,
        );

        self.broadcast_event(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full integration tests will be in tests/metadata_broadcast_tests.rs
    // These are just unit tests for the replicator logic

    #[test]
    fn test_sequence_number_increment() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // This test would need a mock NetworkServiceHandle
            // For now, just verify the basic structure compiles
            let _node_id = Uuid::new_v4();
            let _peer_id = libp2p::PeerId::random();

            // We can't easily create a NetworkServiceHandle for testing without
            // the full networking stack, so we'll defer full tests to integration tests
        });
    }

    #[test]
    fn test_peer_info_creation() {
        let _node_id = Uuid::new_v4();
        let _peer_id = libp2p::PeerId::random();

        // Test that we can create peer info structures
        // This would be part of a full MetadataReplicator test
    }
}
