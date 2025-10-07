//! Raft consensus implementation using OpenRaft.
//!
//! This module provides the core components for Raft-based consensus:
//! - Type definitions and configuration
//! - Persistent log storage using redb
//! - State machine implementation using SQLite
//!
//! Network transport and node management will be implemented in Phase 2B.

pub mod config;
pub mod log_store;
pub mod snapshot_store;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use config::RaftConfig;
pub use log_store::LogStore;
pub use snapshot_store::SnapshotStore;
pub use state_machine::StateMachine;
pub use types::WormFSTypeConfig;
