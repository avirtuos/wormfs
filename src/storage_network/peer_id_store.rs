//! Persistent storage for learned peer IDs in auto-discovery mode.
//!
//! The PeerIdStore maintains a durable mapping of IP addresses to peer IDs
//! for peers configured in AutoId mode. Once a peer ID is learned for an IP,
//! it cannot be changed, ensuring consistent peer identity validation.

use super::types::{Error, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// Persistent store for learned peer IDs.
///
/// This store maintains IP -> PeerID mappings that are learned during
/// the first connection from a peer in AutoId mode. The mappings are
/// persisted to disk to ensure consistency across restarts.
#[derive(Debug)]
pub struct PeerIdStore {
    /// Path to the JSON file storing peer ID mappings
    store_path: PathBuf,

    /// In-memory cache of IP -> PeerID mappings
    mappings: RwLock<HashMap<IpAddr, PeerId>>,
}

/// Serializable format for the peer ID store file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeerIdStoreData {
    /// Map of IP address (as string) to peer ID bytes
    mappings: HashMap<String, Vec<u8>>,
}

impl PeerIdStore {
    /// Create a new PeerIdStore, loading existing mappings from disk if available.
    ///
    /// # Arguments
    ///
    /// * `store_path` - Path to the JSON file for persistent storage
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn new<P: AsRef<Path>>(store_path: P) -> Result<Self, Error> {
        let store_path = store_path.as_ref().to_path_buf();
        let mappings = if store_path.exists() {
            Self::load_from_disk(&store_path)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            store_path,
            mappings: RwLock::new(mappings),
        })
    }

    /// Load peer ID mappings from disk.
    fn load_from_disk(path: &Path) -> Result<HashMap<IpAddr, PeerId>, Error> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            Error::ConfigError(format!(
                "Failed to read peer ID store from {:?}: {}",
                path, e
            ))
        })?;

        let data: PeerIdStoreData = serde_json::from_str(&contents)
            .map_err(|e| Error::ConfigError(format!("Failed to parse peer ID store: {}", e)))?;

        let mut mappings = HashMap::new();
        for (ip_str, peer_id_bytes) in data.mappings {
            let ip: IpAddr = ip_str.parse().map_err(|e| {
                Error::ConfigError(format!("Invalid IP address in store '{}': {}", ip_str, e))
            })?;
            mappings.insert(ip, PeerId::new(peer_id_bytes));
        }

        Ok(mappings)
    }

    /// Save current mappings to disk.
    fn save_to_disk(&self) -> Result<(), Error> {
        let mappings = self.mappings.read().map_err(|e| {
            Error::ConfigError(format!("Lock poisoned while reading peer ID store: {}", e))
        })?;

        let data = PeerIdStoreData {
            mappings: mappings
                .iter()
                .map(|(ip, peer_id)| (ip.to_string(), peer_id.as_bytes().to_vec()))
                .collect(),
        };

        let json = serde_json::to_string_pretty(&data)
            .map_err(|e| Error::ConfigError(format!("Failed to serialize peer ID store: {}", e)))?;

        // Ensure parent directory exists
        if let Some(parent) = self.store_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::ConfigError(format!(
                    "Failed to create peer ID store directory {:?}: {}",
                    parent, e
                ))
            })?;
        }

        std::fs::write(&self.store_path, json).map_err(|e| {
            Error::ConfigError(format!(
                "Failed to write peer ID store to {:?}: {}",
                self.store_path, e
            ))
        })?;

        Ok(())
    }

    /// Get the stored peer ID for an IP address, if it exists.
    ///
    /// # Arguments
    ///
    /// * `ip` - IP address to look up
    ///
    /// # Returns
    ///
    /// The stored peer ID if found, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal lock is poisoned.
    pub fn get(&self, ip: &IpAddr) -> Result<Option<PeerId>, Error> {
        let mappings = self.mappings.read().map_err(|e| {
            Error::ConfigError(format!("Lock poisoned while reading peer ID store: {}", e))
        })?;
        Ok(mappings.get(ip).cloned())
    }

    /// Check if a peer ID has been previously seen/stored.
    ///
    /// This method is used for peer-ID-based validation where we only care
    /// about the peer ID, not the IP address.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Peer ID to look up
    ///
    /// # Returns
    ///
    /// The peer ID if it exists in the store, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal lock is poisoned.
    pub fn get_by_peer_id(&self, peer_id: &PeerId) -> Result<Option<PeerId>, Error> {
        let mappings = self.mappings.read().map_err(|e| {
            Error::ConfigError(format!("Lock poisoned while reading peer ID store: {}", e))
        })?;

        // Check if this peer ID exists in any of the stored mappings
        for stored_peer_id in mappings.values() {
            if stored_peer_id == peer_id {
                return Ok(Some(peer_id.clone()));
            }
        }
        Ok(None)
    }

    /// Store a peer ID for peer-ID-based validation.
    ///
    /// Since we're validating by peer ID only (not IP), we use a placeholder
    /// IP address (0.0.0.0) as the key. This allows reusing the existing
    /// IP-based storage infrastructure.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Peer ID to store
    ///
    /// # Returns
    ///
    /// `Ok(())` if successful, error otherwise.
    pub fn store_by_peer_id(&self, peer_id: PeerId) -> Result<(), Error> {
        let mappings_guard = self.mappings.read().map_err(|e| {
            Error::ConfigError(format!("Lock poisoned while reading peer ID store: {}", e))
        })?;

        // Check if this peer ID already exists
        for stored_peer_id in mappings_guard.values() {
            if stored_peer_id == &peer_id {
                // Already stored, nothing to do
                return Ok(());
            }
        }
        drop(mappings_guard);

        // Use a unique placeholder IP for each peer ID
        // We hash the peer ID bytes to create a deterministic "IP"
        let peer_bytes = peer_id.as_bytes();
        let hash = peer_bytes
            .iter()
            .fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
        let placeholder_ip = IpAddr::V4(std::net::Ipv4Addr::new(
            ((hash >> 24) & 0xFF) as u8,
            ((hash >> 16) & 0xFF) as u8,
            ((hash >> 8) & 0xFF) as u8,
            (hash & 0xFF) as u8,
        ));

        // Store using the regular store method
        self.store(placeholder_ip, peer_id)
    }

    /// Store a new IP -> PeerID mapping.
    ///
    /// This method enforces two invariants:
    /// 1. An IP can only have one peer ID (no overwrites)
    /// 2. A peer ID can only be associated with one IP
    ///
    /// # Arguments
    ///
    /// * `ip` - IP address of the peer
    /// * `peer_id` - Peer ID to associate with this IP
    ///
    /// # Returns
    ///
    /// `Ok(())` if the mapping was stored successfully, or if it already exists
    /// with the same peer ID.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The IP already has a different peer ID
    /// - The peer ID is already associated with a different IP
    /// - The mapping cannot be persisted to disk
    /// - The internal lock is poisoned
    pub fn store(&self, ip: IpAddr, peer_id: PeerId) -> Result<(), Error> {
        let mut mappings = self.mappings.write().map_err(|e| {
            Error::ConfigError(format!("Lock poisoned while writing peer ID store: {}", e))
        })?;

        // Check if IP already has a peer ID
        if let Some(existing_peer_id) = mappings.get(&ip) {
            if existing_peer_id != &peer_id {
                return Err(Error::ValidationFailed(format!(
                    "IP {} already has a different peer ID stored",
                    ip
                )));
            }
            // Same peer ID already stored, nothing to do
            return Ok(());
        }

        // Check if peer ID is already associated with a different IP
        for (existing_ip, existing_peer_id) in mappings.iter() {
            if existing_peer_id == &peer_id && existing_ip != &ip {
                return Err(Error::ValidationFailed(format!(
                    "Peer ID is already associated with IP {}",
                    existing_ip
                )));
            }
        }

        // Store the new mapping
        mappings.insert(ip, peer_id);

        // Release the lock before disk I/O
        drop(mappings);

        // Persist to disk
        self.save_to_disk()?;

        Ok(())
    }

    /// Get all stored IP -> PeerID mappings.
    ///
    /// # Returns
    ///
    /// A clone of all currently stored mappings.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal lock is poisoned.
    pub fn get_all(&self) -> Result<HashMap<IpAddr, PeerId>, Error> {
        let mappings = self.mappings.read().map_err(|e| {
            Error::ConfigError(format!("Lock poisoned while reading peer ID store: {}", e))
        })?;
        Ok(mappings.clone())
    }

    /// Remove a mapping for an IP address.
    ///
    /// This is primarily intended for testing and administrative purposes.
    /// In normal operation, mappings should not be removed.
    ///
    /// # Arguments
    ///
    /// * `ip` - IP address to remove
    ///
    /// # Returns
    ///
    /// The removed peer ID if it existed, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal lock is poisoned or disk I/O fails.
    #[allow(dead_code)]
    pub fn remove(&self, ip: &IpAddr) -> Result<Option<PeerId>, Error> {
        let removed = {
            let mut mappings = self.mappings.write().map_err(|e| {
                Error::ConfigError(format!("Lock poisoned while writing peer ID store: {}", e))
            })?;
            mappings.remove(ip)
        };

        if removed.is_some() {
            self.save_to_disk()?;
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_peer_id_store_new_empty() {
        let temp_path = std::env::temp_dir().join("test_peer_store_empty.json");
        let _ = std::fs::remove_file(&temp_path); // Clean up from previous test

        let store = PeerIdStore::new(&temp_path).expect("Failed to create store");
        assert_eq!(
            store.get_all().expect("Failed to get all mappings").len(),
            0
        );
    }

    #[test]
    fn test_peer_id_store_store_and_get() {
        let temp_path = std::env::temp_dir().join("test_peer_store_store_get.json");
        let _ = std::fs::remove_file(&temp_path);

        let store = PeerIdStore::new(&temp_path).expect("Failed to create store");
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        // Store mapping
        store
            .store(ip, peer_id.clone())
            .expect("Failed to store mapping");

        // Retrieve mapping
        let retrieved = store
            .get(&ip)
            .expect("Failed to get mapping")
            .expect("Mapping not found");
        assert_eq!(retrieved, peer_id);
    }

    #[test]
    fn test_peer_id_store_no_overwrite() {
        let temp_path = std::env::temp_dir().join("test_peer_store_no_overwrite.json");
        let _ = std::fs::remove_file(&temp_path);

        let store = PeerIdStore::new(&temp_path).expect("Failed to create store");
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 101));
        let peer_id1 = PeerId::new(vec![1, 2, 3, 4, 5]);
        let peer_id2 = PeerId::new(vec![6, 7, 8, 9, 10]);

        // Store first mapping
        store
            .store(ip, peer_id1.clone())
            .expect("Failed to store first mapping");

        // Try to overwrite with different peer ID - should fail
        let result = store.store(ip, peer_id2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already has a different peer ID"));

        // Original mapping should still be intact
        assert_eq!(store.get(&ip).unwrap().unwrap(), peer_id1);
    }

    #[test]
    fn test_peer_id_store_no_duplicate_peer_ids() {
        let temp_path = std::env::temp_dir().join("test_peer_store_no_dup_peer_ids.json");
        let _ = std::fs::remove_file(&temp_path);

        let store = PeerIdStore::new(&temp_path).expect("Failed to create store");
        let ip1 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 102));
        let ip2 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 103));
        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        // Store mapping for first IP
        store
            .store(ip1, peer_id.clone())
            .expect("Failed to store first mapping");

        // Try to store same peer ID for different IP - should fail
        let result = store.store(ip2, peer_id);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already associated with IP"));
    }

    #[test]
    fn test_peer_id_store_persistence() {
        let temp_path = std::env::temp_dir().join("test_peer_store_persistence.json");
        let _ = std::fs::remove_file(&temp_path);

        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 104));
        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        // Create store and add mapping
        {
            let store = PeerIdStore::new(&temp_path).expect("Failed to create store");
            store
                .store(ip, peer_id.clone())
                .expect("Failed to store mapping");
        }

        // Create new store instance - should load from disk
        let store2 = PeerIdStore::new(&temp_path).expect("Failed to load store");
        let retrieved = store2
            .get(&ip)
            .expect("Failed to get mapping")
            .expect("Mapping not found after reload");
        assert_eq!(retrieved, peer_id);
    }

    #[test]
    fn test_peer_id_store_idempotent_store() {
        let temp_path = std::env::temp_dir().join("test_peer_store_idempotent.json");
        let _ = std::fs::remove_file(&temp_path);

        let store = PeerIdStore::new(&temp_path).expect("Failed to create store");
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 105));
        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        // Store same mapping twice - should succeed both times
        store
            .store(ip, peer_id.clone())
            .expect("Failed to store first time");
        store
            .store(ip, peer_id.clone())
            .expect("Failed to store second time");

        assert_eq!(store.get(&ip).unwrap().unwrap(), peer_id);
    }
}
