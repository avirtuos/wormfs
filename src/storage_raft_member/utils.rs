///! Utility functions for storage_raft_member module
use sha2::{Digest, Sha256};
use std::time::SystemTime;

/// Derive a deterministic Ed25519 keypair from a node ID.
///
/// Uses SHA-256 to hash the full u64 node_id into a 32-byte seed,
/// ensuring unique keypairs for all possible node IDs. The domain separator
/// prevents collision with other uses of SHA-256 in the system.
///
/// This function is used by both production code (ClusterManager) and test code
/// (StubStorageNetwork) to ensure consistent peer ID generation across the system.
///
/// # Arguments
/// * `node_id` - The u64 node identifier
///
/// # Returns
/// * `Ok(Keypair)` - The derived Ed25519 keypair
/// * `Err(String)` - Error message if keypair creation fails
///
/// # Examples
/// ```
/// use wormfs::storage_raft_member::utils::derive_keypair_from_node_id;
///
/// let keypair = derive_keypair_from_node_id(42).expect("Failed to create keypair");
/// let peer_id = libp2p::PeerId::from(keypair.public());
/// ```
pub fn derive_keypair_from_node_id(node_id: u64) -> Result<libp2p::identity::Keypair, String> {
    let mut hasher = Sha256::new();
    hasher.update(b"wormfs-node-keypair-v1:"); // Domain separator
    hasher.update(node_id.to_le_bytes());
    let hash = hasher.finalize();

    // hash is 32 bytes, exactly what ed25519_from_bytes needs
    let seed: [u8; 32] = hash.into();
    libp2p::identity::Keypair::ed25519_from_bytes(seed)
        .map_err(|e| format!("Failed to create keypair: {}", e))
}

/// Converts the current system time to milliseconds since UNIX_EPOCH.
///
/// ## Safety
///
/// This function uses `expect()` internally because the operation is infallible in practice:
/// - `SystemTime::now()` always returns the current system time
/// - `UNIX_EPOCH` is a constant (January 1, 1970 UTC)
/// - The only way this could fail is if the system clock is set before 1970,
///   which is impossible on properly configured modern systems
///
/// If the system clock were somehow before 1970, this represents a catastrophic
/// system misconfiguration that would break many other parts of the system,
/// so panicking is appropriate.
///
/// # Returns
///
/// Current time as milliseconds since UNIX_EPOCH
#[inline]
pub(crate) fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System clock is before UNIX_EPOCH (1970-01-01) - system misconfiguration")
        .as_millis() as u64
}

/// Converts the current system time to seconds since UNIX_EPOCH.
///
/// ## Safety
///
/// This function uses `expect()` internally for the same reasons as `current_time_ms()`.
/// See that function's documentation for details on why this is safe.
///
/// # Returns
///
/// Current time as seconds since UNIX_EPOCH
#[inline]
pub(crate) fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System clock is before UNIX_EPOCH (1970-01-01) - system misconfiguration")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_time_ms() {
        let time = current_time_ms();
        // Should be a reasonable timestamp (after 2020)
        assert!(time > 1577836800000); // Jan 1, 2020 in ms
    }

    #[test]
    fn test_current_time_secs() {
        let time = current_time_secs();
        // Should be a reasonable timestamp (after 2020)
        assert!(time > 1577836800); // Jan 1, 2020 in seconds
    }

    #[test]
    fn test_time_consistency() {
        let ms = current_time_ms();
        let secs = current_time_secs();
        // ms / 1000 should be approximately equal to secs
        assert!((ms / 1000).abs_diff(secs) < 2); // Within 2 seconds
    }

    #[test]
    fn test_derive_keypair_deterministic() {
        // Same node_id should always produce same keypair
        let keypair1 = derive_keypair_from_node_id(42).expect("Failed to create keypair");
        let keypair2 = derive_keypair_from_node_id(42).expect("Failed to create keypair");

        let peer_id1 = libp2p::PeerId::from(keypair1.public());
        let peer_id2 = libp2p::PeerId::from(keypair2.public());

        assert_eq!(
            peer_id1, peer_id2,
            "Same node_id should produce same peer_id"
        );
    }

    #[test]
    fn test_derive_keypair_unique() {
        // Different node_ids should produce different keypairs
        let keypair1 = derive_keypair_from_node_id(0).expect("Failed to create keypair");
        let keypair2 = derive_keypair_from_node_id(1).expect("Failed to create keypair");
        let keypair256 = derive_keypair_from_node_id(256).expect("Failed to create keypair");

        let peer_id1 = libp2p::PeerId::from(keypair1.public());
        let peer_id2 = libp2p::PeerId::from(keypair2.public());
        let peer_id256 = libp2p::PeerId::from(keypair256.public());

        assert_ne!(
            peer_id1, peer_id2,
            "Different node_ids should produce different peer_ids"
        );
        assert_ne!(
            peer_id1, peer_id256,
            "Node 0 and node 256 should have different peer_ids"
        );
        assert_ne!(
            peer_id2, peer_id256,
            "Node 1 and node 256 should have different peer_ids"
        );
    }

    #[test]
    fn test_derive_keypair_large_node_ids() {
        // Test with large node_ids to ensure we use the full u64 range
        let keypair_max = derive_keypair_from_node_id(u64::MAX).expect("Failed to create keypair");
        let keypair_large =
            derive_keypair_from_node_id(1_000_000_000).expect("Failed to create keypair");

        let peer_id_max = libp2p::PeerId::from(keypair_max.public());
        let peer_id_large = libp2p::PeerId::from(keypair_large.public());

        assert_ne!(
            peer_id_max, peer_id_large,
            "Large node_ids should produce unique peer_ids"
        );
    }
}
