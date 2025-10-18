//! Configuration handler for admin API endpoints.
//!
//! Provides handlers for configuration viewing and management.

use axum::{http::StatusCode, response::IntoResponse, Json};

/// Handler for `/api/config` endpoint.
///
/// Returns the current admin configuration in JSON format.
pub async fn config_handler() -> impl IntoResponse {
    // TODO: Load actual configuration from the admin server state
    // For now, return a placeholder response
    let config = serde_json::json!({
        "admin": {
            "enabled": true,
            "port": 9090,
            "bind_address": "127.0.0.1"
        },
        "metrics": {
            "enabled": true,
            "aggregation_window_secs": 60,
            "buffer_size": 1000
        },
        "filesystem": {
            "mount_point": "/tmp/wormfs-test-mount",
            "metadata_db": "/tmp/wormfs-test-data/metadata.db",
            "data_dir": "/tmp/wormfs-test-data/chunks"
        }
    });

    (StatusCode::OK, Json(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_handler() {
        let response = config_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
