//! PSK-based authentication interceptor for gRPC requests.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Status};
use tracing::{debug, error, info, warn};

use crate::storage_endpoint::EndpointError;

/// PSK-based authentication interceptor.
///
/// Validates client identity using Pre-Shared Keys stored in the identities directory.
/// Each file in the directory represents an identity with the filename as the identity name.
#[derive(Clone)]
pub struct AuthInterceptor {
    enabled: bool,
    /// Map of identity name -> PSK bytes
    identities: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    /// This node's identity
    node_identity: String,
}

impl AuthInterceptor {
    /// Create a new auth interceptor.
    ///
    /// # Arguments
    ///
    /// * `enabled` - Whether authentication is enabled
    /// * `identities_dir` - Directory containing PSK identity files
    /// * `node_identity` - This node's identity name
    ///
    /// # Returns
    ///
    /// A new AuthInterceptor instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the identities directory cannot be read or PSK files are invalid.
    pub async fn new(
        enabled: bool,
        identities_dir: Option<PathBuf>,
        node_identity: Option<String>,
    ) -> Result<Self, EndpointError> {
        let mut identities = HashMap::new();

        if enabled {
            if let Some(dir) = identities_dir.as_ref() {
                info!("Loading PSK identities from {}", dir.display());

                // Load all PSK files from the identities directory
                let mut entries = tokio::fs::read_dir(&dir).await.map_err(|e| {
                    EndpointError::InvalidPskConfig(format!(
                        "Cannot read identities directory: {}",
                        e
                    ))
                })?;

                while let Some(entry) = entries.next_entry().await.map_err(|e| {
                    EndpointError::InvalidPskConfig(format!("Cannot read identity file: {}", e))
                })? {
                    let path = entry.path();
                    if path.is_file() {
                        let identity_name = path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("")
                            .to_string();
                        let psk = tokio::fs::read(&path).await.map_err(|e| {
                            EndpointError::InvalidPskConfig(format!(
                                "Cannot read PSK for {}: {}",
                                identity_name, e
                            ))
                        })?;
                        debug!("Loaded PSK for identity: {}", identity_name);
                        identities.insert(identity_name, psk);
                    }
                }

                if identities.is_empty() {
                    warn!("No PSK identities loaded from {}", dir.display());
                }
            } else {
                return Err(EndpointError::InvalidPskConfig(
                    "Authentication is enabled but no identities_dir provided".to_string(),
                ));
            }
        }

        Ok(Self {
            enabled,
            identities: Arc::new(RwLock::new(identities)),
            node_identity: node_identity.unwrap_or_else(|| "storage_node".to_string()),
        })
    }

    /// Intercept and validate request authentication.
    ///
    /// # Arguments
    ///
    /// * `request` - The incoming gRPC request
    ///
    /// # Returns
    ///
    /// The request if authentication succeeds, or a Status error if it fails.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - `Status::unauthenticated` if identity or PSK headers are missing or identity is unknown
    /// - `Status::permission_denied` if the PSK is invalid
    pub async fn intercept<T>(&self, request: Request<T>) -> Result<Request<T>, Status> {
        if !self.enabled {
            return Ok(request);
        }

        // Extract identity from metadata
        let identity = request
            .metadata()
            .get("x-wormfs-identity")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("Missing identity header"))?;

        // Extract PSK from metadata
        let psk = request
            .metadata()
            .get("x-wormfs-psk")
            .map(|v| v.as_bytes())
            .ok_or_else(|| Status::unauthenticated("Missing PSK header"))?;

        // Validate PSK
        let identities = self.identities.read().await;
        match identities.get(identity) {
            Some(expected_psk) if expected_psk.as_slice() == psk => {
                // Valid authentication
                debug!("Authenticated request from identity: {}", identity);
                Ok(request)
            }
            Some(_) => {
                error!("Invalid PSK for identity: {}", identity);
                Err(Status::permission_denied("Invalid PSK"))
            }
            None => {
                error!("Unknown identity: {}", identity);
                Err(Status::unauthenticated("Unknown identity"))
            }
        }
    }

    /// Get client identity from request metadata (after authentication).
    ///
    /// # Arguments
    ///
    /// * `request` - The gRPC request
    ///
    /// # Returns
    ///
    /// The client identity if present, None otherwise.
    pub fn get_identity<T>(&self, request: &Request<T>) -> Option<String> {
        request
            .metadata()
            .get("x-wormfs-identity")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }

    /// Get this node's identity.
    pub fn node_identity(&self) -> &str {
        &self.node_identity
    }

    /// Check if authentication is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Validate credentials without consuming a request.
    ///
    /// # Arguments
    ///
    /// * `identity` - Client identity string
    /// * `psk` - Pre-shared key bytes
    ///
    /// # Returns
    ///
    /// Ok(()) if credentials are valid, Status error otherwise.
    pub async fn validate_credentials(&self, identity: &str, psk: &[u8]) -> Result<(), Status> {
        if !self.enabled {
            return Ok(());
        }

        let identities = self.identities.read().await;
        match identities.get(identity) {
            Some(expected_psk) if expected_psk.as_slice() == psk => {
                debug!("Authenticated request from identity: {}", identity);
                Ok(())
            }
            Some(_) => {
                error!("Invalid PSK for identity: {}", identity);
                Err(Status::permission_denied("Invalid PSK"))
            }
            None => {
                error!("Unknown identity: {}", identity);
                Err(Status::unauthenticated("Unknown identity"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tonic::metadata::MetadataValue;

    async fn setup_test_dir() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Create test identity files
        tokio::fs::write(path.join("client1"), b"secret1")
            .await
            .unwrap();
        tokio::fs::write(path.join("client2"), b"secret2")
            .await
            .unwrap();

        (dir, path)
    }

    #[tokio::test]
    async fn test_auth_disabled() {
        let auth = AuthInterceptor::new(false, None, None).await.unwrap();

        let request = Request::new(());
        let result = auth.intercept(request).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_valid_credentials() {
        let (_dir, path) = setup_test_dir().await;
        let auth = AuthInterceptor::new(true, Some(path), Some("node1".to_string()))
            .await
            .unwrap();

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-wormfs-identity", MetadataValue::from_static("client1"));
        request
            .metadata_mut()
            .insert("x-wormfs-psk", MetadataValue::from_static("secret1"));

        let result = auth.intercept(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_invalid_psk() {
        let (_dir, path) = setup_test_dir().await;
        let auth = AuthInterceptor::new(true, Some(path), Some("node1".to_string()))
            .await
            .unwrap();

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-wormfs-identity", MetadataValue::from_static("client1"));
        request
            .metadata_mut()
            .insert("x-wormfs-psk", MetadataValue::from_static("wrong_secret"));

        let result = auth.intercept(request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn test_auth_unknown_identity() {
        let (_dir, path) = setup_test_dir().await;
        let auth = AuthInterceptor::new(true, Some(path), Some("node1".to_string()))
            .await
            .unwrap();

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-wormfs-identity", MetadataValue::from_static("unknown"));
        request
            .metadata_mut()
            .insert("x-wormfs-psk", MetadataValue::from_static("secret"));

        let result = auth.intercept(request).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn test_auth_missing_identity() {
        let (_dir, path) = setup_test_dir().await;
        let auth = AuthInterceptor::new(true, Some(path), Some("node1".to_string()))
            .await
            .unwrap();

        let request = Request::new(());
        let result = auth.intercept(request).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn test_get_identity() {
        let (_dir, path) = setup_test_dir().await;
        let auth = AuthInterceptor::new(true, Some(path), Some("node1".to_string()))
            .await
            .unwrap();

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("x-wormfs-identity", MetadataValue::from_static("client1"));

        let identity = auth.get_identity(&request);
        assert_eq!(identity, Some("client1".to_string()));
    }
}
