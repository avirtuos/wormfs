//! OpenRaft type configuration and integration types for WormFS.
//!
//! This module defines the WormFsTypeConfig that implements OpenRaft's RaftTypeConfig trait,
//! mapping WormFS-specific types to OpenRaft's generic type parameters.

use openraft::RaftTypeConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::types::{NodeId, WormFsOperation};

/// WormFS type configuration for OpenRaft.
///
/// This type implements RaftTypeConfig to specify how WormFS integrates with OpenRaft.
/// It defines all the necessary types for the Raft protocol including node IDs, log entries,
/// responses, and snapshot metadata.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct WormFsTypeConfig {}

impl RaftTypeConfig for WormFsTypeConfig {
    /// Node identifier type - maps to our NodeId
    type NodeId = NodeId;

    /// Node network address type - using socket addresses via libp2p
    type Node = WormFsNode;

    /// Entry payload type - our WormFsOperation enum
    type Entry = openraft::Entry<Self>;

    /// Type for client request data
    type D = WormFsOperation;

    /// Type for client response data
    type R = WormFsResponse;

    /// Responder type for sending responses
    type Responder = openraft::impls::OneshotResponder<Self>;

    /// Snapshot data type - using tokio::fs::File for streaming large snapshots
    type SnapshotData = tokio::io::BufReader<tokio::fs::File>;

    /// Async runtime - using Tokio
    type AsyncRuntime = openraft::TokioRuntime;
}

/// Node information for WormFS cluster members.
///
/// Contains network address and other metadata about a node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct WormFsNode {
    /// Network address of the node (from libp2p PeerId)
    pub peer_id: String,

    /// Additional metadata about the node
    pub metadata: Option<NodeMetadata>,
}

/// Additional metadata about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeMetadata {
    /// Human-readable name for the node
    pub name: Option<String>,

    /// Node version information
    pub version: Option<String>,
}

/// Response type for WormFS Raft operations.
///
/// Contains the result of applying a WormFsOperation to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WormFsResponse {
    /// Empty response for entries that don't produce application-level responses (Blank, Membership)
    Empty,

    /// Transaction prepare phase completed successfully
    TransactionPrepared {
        tx_id: crate::storage_raft_member::types::TxId,
        vote: PrepareVote,
    },

    /// Transaction committed successfully
    TransactionCommitted {
        tx_id: crate::storage_raft_member::types::TxId,
    },

    /// Transaction aborted
    TransactionAborted {
        tx_id: crate::storage_raft_member::types::TxId,
        reason: Option<String>,
    },
}

/// Vote cast by a node during the prepare phase of 2PC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrepareVote {
    /// Node is prepared to commit the transaction
    Prepared,

    /// Node cannot commit the transaction
    Abort,
}

/// Snapshot data for WormFS.
///
/// This represents a point-in-time snapshot of the metadata store that can be
/// used for log compaction and node catch-up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WormFsSnapshotData {
    /// The log index that this snapshot covers up to
    pub last_included_index: u64,

    /// The term of the last included log entry
    pub last_included_term: u64,

    /// Cluster membership at the time of snapshot
    pub membership: BTreeSet<NodeId>,

    /// Snapshot file path (relative to snapshot directory)
    /// The actual metadata is stored in this file, not in memory
    pub snapshot_file: String,

    /// Size of the snapshot file in bytes
    pub file_size: u64,

    /// CRC32 checksum of the snapshot file
    pub checksum: u32,

    /// Whether the snapshot is compressed with zstd
    pub compressed: bool,
}

impl WormFsSnapshotData {
    /// Create a new snapshot data descriptor.
    pub fn new(
        last_included_index: u64,
        last_included_term: u64,
        membership: BTreeSet<NodeId>,
        snapshot_file: String,
        file_size: u64,
        checksum: u32,
        compressed: bool,
    ) -> Self {
        Self {
            last_included_index,
            last_included_term,
            membership,
            snapshot_file,
            file_size,
            checksum,
            compressed,
        }
    }

    /// Get the snapshot ID (log ID) for this snapshot.
    pub fn snapshot_id(&self) -> openraft::LogId<NodeId> {
        openraft::LogId::new(
            openraft::CommittedLeaderId::new(self.last_included_term, NodeId(0)),
            self.last_included_index,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wormfs_node_serialization() {
        let node = WormFsNode {
            peer_id: "test-peer-123".to_string(),
            metadata: Some(NodeMetadata {
                name: Some("node1".to_string()),
                version: Some("0.1.0".to_string()),
            }),
        };

        let serialized = bincode::serialize(&node).unwrap();
        let deserialized: WormFsNode = bincode::deserialize(&serialized).unwrap();
        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_snapshot_data_serialization() {
        let mut membership = BTreeSet::new();
        membership.insert(NodeId(1));
        membership.insert(NodeId(2));

        let snapshot = WormFsSnapshotData::new(
            100,
            5,
            membership,
            "snapshot-100-5.db".to_string(),
            1024,
            0x12345678,
            true,
        );

        let serialized = bincode::serialize(&snapshot).unwrap();
        let deserialized: WormFsSnapshotData = bincode::deserialize(&serialized).unwrap();
        assert_eq!(
            snapshot.last_included_index,
            deserialized.last_included_index
        );
        assert_eq!(snapshot.snapshot_file, deserialized.snapshot_file);
    }

    #[test]
    fn test_prepare_vote() {
        assert_eq!(PrepareVote::Prepared, PrepareVote::Prepared);
        assert_ne!(PrepareVote::Prepared, PrepareVote::Abort);
    }
}
