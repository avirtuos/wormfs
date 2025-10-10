//! Raft consensus implementation using OpenRaft.
//!
//! This module provides the core components for Raft-based consensus:
//! - Type definitions and configuration
//! - Persistent log storage using redb
//! - State machine implementation using SQLite
//! - Network adapter for libp2p integration
//! - Raft node wrapper and request handling

pub mod config;
pub mod log_store;
pub mod network;
pub mod node;
pub mod proto_types;
pub mod request_handler;
pub mod snapshot_store;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use config::RaftConfig;
pub use log_store::LogStore;
pub use network::{WormFSRaftNetwork, WormFSRaftNetworkFactory};
pub use node::{RaftNode, RaftNodeMetrics};
pub use proto_types::{deserialize_metadata_op, serialize_metadata_op};
pub use request_handler::RaftRequestHandler;
pub use snapshot_store::SnapshotStore;
pub use state_machine::StateMachine;
pub use types::WormFSTypeConfig;
