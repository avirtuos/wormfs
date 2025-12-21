//! Standard gRPC health checking service implementation.

use tonic::{Request, Response, Status};
use tracing::debug;

use crate::storage_endpoint::proto::wormfs::health::health_server::Health;
use crate::storage_endpoint::proto::wormfs::health::*;

/// Standard gRPC health check service implementation.
///
/// Implements the standard grpc.health.v1.Health service protocol.
pub struct HealthServiceImpl {
    // Component health checkers can be added here in the future
}

impl HealthServiceImpl {
    /// Create a new health service.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HealthServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl Health for HealthServiceImpl {
    /// Check the health of a specific service or overall system.
    ///
    /// # Arguments
    ///
    /// * `request` - Health check request with optional service name
    ///
    /// # Returns
    ///
    /// The serving status of the requested service.
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let service = request.into_inner().service;

        debug!("Health check requested for service: {:?}", service);

        // Check specific service health or overall health
        let status = if service.is_empty() {
            // Overall health check - all services are serving
            health_check_response::ServingStatus::Serving
        } else {
            // Service-specific health check
            match service.as_str() {
                "wormfs.filesystem.FilesystemService" => {
                    health_check_response::ServingStatus::Serving
                }
                "wormfs.chunk.ChunkService" => health_check_response::ServingStatus::Serving,
                "wormfs.snapshot.SnapshotService" => health_check_response::ServingStatus::Serving,
                "wormfs.transaction_log.TransactionLogService" => {
                    health_check_response::ServingStatus::Serving
                }
                _ => health_check_response::ServingStatus::ServiceUnknown,
            }
        };

        Ok(Response::new(HealthCheckResponse {
            status: status as i32,
        }))
    }

    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<HealthCheckResponse, Status>>;

    /// Watch the health of a service with streaming updates.
    ///
    /// # Arguments
    ///
    /// * `request` - Health check request with optional service name
    ///
    /// # Returns
    ///
    /// A stream of health status updates.
    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let service = request.into_inner().service;
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        debug!("Health watch requested for service: {:?}", service);

        // Spawn task to send periodic health updates
        tokio::spawn(async move {
            // Send initial health status
            let _ = tx
                .send(Ok(HealthCheckResponse {
                    status: health_check_response::ServingStatus::Serving as i32,
                }))
                .await;

            // In the future, we could send periodic updates based on component health
            // For now, just keep the stream open
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check_overall() {
        let health = HealthServiceImpl::new();
        let request = Request::new(HealthCheckRequest {
            service: String::new(),
        });

        let response = health.check(request).await.unwrap();
        assert_eq!(
            response.into_inner().status,
            health_check_response::ServingStatus::Serving as i32
        );
    }

    #[tokio::test]
    async fn test_health_check_specific_service() {
        let health = HealthServiceImpl::new();
        let request = Request::new(HealthCheckRequest {
            service: "wormfs.filesystem.FilesystemService".to_string(),
        });

        let response = health.check(request).await.unwrap();
        assert_eq!(
            response.into_inner().status,
            health_check_response::ServingStatus::Serving as i32
        );
    }

    #[tokio::test]
    async fn test_health_check_unknown_service() {
        let health = HealthServiceImpl::new();
        let request = Request::new(HealthCheckRequest {
            service: "unknown.Service".to_string(),
        });

        let response = health.check(request).await.unwrap();
        assert_eq!(
            response.into_inner().status,
            health_check_response::ServingStatus::ServiceUnknown as i32
        );
    }

    #[tokio::test]
    async fn test_health_watch() {
        let health = HealthServiceImpl::new();
        let request = Request::new(HealthCheckRequest {
            service: String::new(),
        });

        let response = health.watch(request).await.unwrap();
        // Just verify it returns a stream without error
        let _stream = response.into_inner();
    }
}
