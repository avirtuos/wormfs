//! Health and status handler for admin API endpoints.
//!
//! Provides handlers for system health monitoring and status checks.

use axum::{http::StatusCode, response::IntoResponse, Json};

/// Handler for `/api/status` endpoint.
///
/// Returns comprehensive system status information including
/// service health, uptime, and resource usage.
pub async fn status_handler() -> impl IntoResponse {
    // TODO: Gather actual system status from various components
    // For now, return a placeholder with example data
    let status = serde_json::json!({
        "system": {
            "status": "healthy",
            "uptime_seconds": 3600,
            "version": env!("CARGO_PKG_VERSION")
        },
        "services": {
            "filesystem": {
                "status": "running",
                "mounted": true,
                "mount_point": "/tmp/wormfs-test-mount"
            },
            "metrics": {
                "status": "running",
                "aggregation_active": true
            },
            "admin": {
                "status": "running",
                "websocket_connections": 0
            }
        },
        "resources": {
            "memory_usage_mb": 45,
            "cpu_usage_percent": 2.5,
            "disk_usage": {
                "total_gb": 100,
                "used_gb": 25,
                "available_gb": 75
            }
        }
    });

    (StatusCode::OK, Json(status))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_status_handler() {
        let response = status_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
