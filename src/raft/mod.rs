// Raft consensus module for WormFS metadata operations
//
// This module implements Raft consensus using OpenRaft for strong consistency
// of metadata operations across the storage cluster.
//
// Module Structure:
// - types: Core Raft types and metadata operations
// - storage: RaftLogStorage implementation using redb
// - state_machine: State machine that applies committed ops to SQLite
// - local_network: In-memory network for testing (Phase 2A)
// - libp2p_network: Production network over libp2p (Phase 2B)
// - peer_manager: Peer discovery and health monitoring
// - client: Client API for proposals and queries
// - config: Raft configuration structs
// - snapshot: Snapshot management

pub mod types;
// pub mod storage;
// pub mod state_machine;
// pub mod local_network;
// pub mod libp2p_network;
// pub mod peer_manager;
// pub mod client;
// pub mod config;
// pub mod snapshot;

// Re-export commonly used types
pub use types::*;
