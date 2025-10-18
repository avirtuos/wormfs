//! Logs handler for admin API endpoints.
//!
//! Provides handlers for viewing recent system logs using a ring buffer.

use axum::{http::StatusCode, response::IntoResponse, Json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Maximum number of log entries to keep in the ring buffer
const MAX_LOG_ENTRIES: usize = 1000;

/// A single log entry
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub timestamp: u64,
    pub level: String,
    pub target: String,
    pub message: String,
}

/// Ring buffer for storing recent log entries
#[derive(Debug, Clone)]
pub struct LogBuffer {
    entries: Arc<Mutex<VecDeque<LogEntry>>>,
}

impl LogBuffer {
    /// Create a new log buffer
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_LOG_ENTRIES))),
        }
    }

    /// Add a log entry to the buffer
    pub fn push(&self, entry: LogEntry) {
        let mut entries = self.entries.lock().unwrap();

        // Remove oldest entry if at capacity
        if entries.len() >= MAX_LOG_ENTRIES {
            entries.pop_front();
        }

        entries.push_back(entry);
    }

    /// Get all log entries
    pub fn get_all(&self) -> Vec<LogEntry> {
        let entries = self.entries.lock().unwrap();
        entries.iter().cloned().collect()
    }

    /// Get the most recent N log entries
    pub fn get_recent(&self, count: usize) -> Vec<LogEntry> {
        let entries = self.entries.lock().unwrap();
        let start_idx = entries.len().saturating_sub(count);
        entries.iter().skip(start_idx).cloned().collect()
    }

    /// Clear all log entries
    pub fn clear(&self) {
        let mut entries = self.entries.lock().unwrap();
        entries.clear();
    }
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Handler for `/api/logs` endpoint.
///
/// Returns recent log entries from the ring buffer.
pub async fn logs_handler() -> impl IntoResponse {
    // TODO: Use actual log buffer from admin server state
    // For now, return placeholder log entries
    let logs = vec![
        serde_json::json!({
            "timestamp": 1234567890,
            "level": "INFO",
            "target": "wormfs::filesystem",
            "message": "Filesystem mounted successfully"
        }),
        serde_json::json!({
            "timestamp": 1234567891,
            "level": "DEBUG",
            "target": "wormfs::metrics",
            "message": "Metrics aggregation started"
        }),
        serde_json::json!({
            "timestamp": 1234567892,
            "level": "INFO",
            "target": "wormfs::admin",
            "message": "Admin server started on 127.0.0.1:9090"
        }),
    ];

    let response = serde_json::json!({
        "logs": logs,
        "total_count": logs.len(),
        "buffer_size": MAX_LOG_ENTRIES
    });

    (StatusCode::OK, Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_buffer_basic() {
        let buffer = LogBuffer::new();

        let entry = LogEntry {
            timestamp: 123456,
            level: "INFO".to_string(),
            target: "test".to_string(),
            message: "Test message".to_string(),
        };

        buffer.push(entry.clone());

        let entries = buffer.get_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "Test message");
    }

    #[test]
    fn test_log_buffer_capacity() {
        let buffer = LogBuffer::new();

        // Add more than MAX_LOG_ENTRIES
        for i in 0..(MAX_LOG_ENTRIES + 100) {
            let entry = LogEntry {
                timestamp: i as u64,
                level: "INFO".to_string(),
                target: "test".to_string(),
                message: format!("Message {}", i),
            };
            buffer.push(entry);
        }

        let entries = buffer.get_all();

        // Should only keep MAX_LOG_ENTRIES
        assert_eq!(entries.len(), MAX_LOG_ENTRIES);

        // Should have the most recent entries
        assert_eq!(entries[0].timestamp, 100); // First entry should be from iteration 100
        assert_eq!(
            entries[MAX_LOG_ENTRIES - 1].timestamp,
            (MAX_LOG_ENTRIES + 99) as u64
        );
    }

    #[test]
    fn test_log_buffer_get_recent() {
        let buffer = LogBuffer::new();

        for i in 0..10 {
            let entry = LogEntry {
                timestamp: i as u64,
                level: "INFO".to_string(),
                target: "test".to_string(),
                message: format!("Message {}", i),
            };
            buffer.push(entry);
        }

        let recent = buffer.get_recent(5);
        assert_eq!(recent.len(), 5);
        assert_eq!(recent[0].timestamp, 5);
        assert_eq!(recent[4].timestamp, 9);
    }

    #[tokio::test]
    async fn test_logs_handler() {
        let response = logs_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
