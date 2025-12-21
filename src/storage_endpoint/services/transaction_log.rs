//! TransactionLogService gRPC implementation.
//!
//! Provides access to Raft log entries for replication and debugging,
//! delegating to the TransactionLogStore component.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use super::conversions::transaction_log_error_to_status;
use crate::storage_endpoint::proto::wormfs::transaction_log::transaction_log_service_server::TransactionLogService;
use crate::storage_endpoint::proto::wormfs::transaction_log::*;
use crate::transaction_log_store::TransactionLogStore;

/// TransactionLogService gRPC implementation.
///
/// Delegates transaction log operations to the TransactionLogStore component.
pub struct TransactionLogServiceImpl<T: TransactionLogStore> {
    transaction_log_store: Arc<T>,
}

impl<T: TransactionLogStore> TransactionLogServiceImpl<T> {
    /// Create a new TransactionLogService.
    ///
    /// # Arguments
    ///
    /// * `transaction_log_store` - TransactionLogStore instance for log operations
    pub fn new(transaction_log_store: Arc<T>) -> Self {
        Self {
            transaction_log_store,
        }
    }
}

#[tonic::async_trait]
impl<T: TransactionLogStore + 'static> TransactionLogService for TransactionLogServiceImpl<T> {
    type GetLogEntriesStream = tokio_stream::wrappers::ReceiverStream<Result<LogEntry, Status>>;

    async fn get_log_entries(
        &self,
        request: Request<GetLogEntriesRequest>,
    ) -> Result<Response<Self::GetLogEntriesStream>, Status> {
        let req = request.into_inner();
        info!(
            "GetLogEntries request: start={}, end={}",
            req.start_index, req.end_index
        );

        let transaction_log_store = self.transaction_log_store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        // Spawn task to stream log entries
        tokio::spawn(async move {
            match transaction_log_store
                .get_entries(req.start_index, req.end_index)
                .await
            {
                Ok(entries) => {
                    for entry in entries {
                        let log_entry = LogEntry {
                            index: entry.index,
                            term: entry.term,
                            data: entry.operations,
                            timestamp: entry
                                .timestamp
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs() as i64,
                        };

                        if tx.send(Ok(log_entry)).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(transaction_log_error_to_status(e))).await;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn get_log_state(
        &self,
        _request: Request<GetLogStateRequest>,
    ) -> Result<Response<LogStateResponse>, Status> {
        debug!("GetLogState request");

        let first_index = self.transaction_log_store.get_first_index();
        let last_index = self.transaction_log_store.get_last_index();

        // TODO: Get actual commit index from Raft member
        let commit_index = last_index;

        Ok(Response::new(LogStateResponse {
            first_index,
            last_index,
            commit_index,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_log_store::MockTransactionLogStore;

    #[tokio::test]
    async fn test_get_log_state() {
        let mut mock_store = MockTransactionLogStore::new();
        mock_store.expect_get_first_index().returning(|| 1);
        mock_store.expect_get_last_index().returning(|| 100);

        let service = TransactionLogServiceImpl::new(Arc::new(mock_store));

        let request = Request::new(GetLogStateRequest {});
        let response = service.get_log_state(request).await;

        assert!(response.is_ok());
        let state = response.unwrap().into_inner();
        assert_eq!(state.first_index, 1);
        assert_eq!(state.last_index, 100);
    }
}
