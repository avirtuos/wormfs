//! Raft status handler for admin API endpoints.
//!
//! Provides handlers for viewing Raft cluster status, member information, and consensus metrics.

use crate::storage_raft_member::{StorageRaftMember, StorageRaftMemberImpl};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tracing::error;

/// Convert Instant to seconds elapsed since given instant.
///
/// Returns the elapsed seconds as a floating point number.
/// Returns None if the instant is in the future (shouldn't happen).
fn instant_to_seconds_ago(instant: &Instant, now: &Instant) -> Option<f64> {
    now.checked_duration_since(*instant)
        .map(|d| d.as_secs_f64())
}

/// Handler for `/api/raft/metrics` endpoint.
///
/// Returns comprehensive Raft metrics including cluster status,
/// log state, and heartbeat information.
///
/// # Returns
///
/// JSON response with Raft metrics:
/// - `current_term`: Current Raft term
/// - `role`: Current role (Leader, Follower, Candidate)
/// - `leader_id`: ID of current leader (if known)
/// - `commit_index`: Index of highest committed log entry
/// - `last_applied`: Index of highest applied log entry
/// - `last_log_index`: Index of last log entry
/// - `snapshot_index`: Index of last snapshot
/// - `cluster_size`: Number of nodes in cluster
/// - `replication_lag`: Map of follower ID to replication lag (leader only)
/// - `heartbeat_status`: Map of follower ID to heartbeat timing info (leader only)
pub async fn raft_metrics_handler(
    State(raft_member): State<Arc<StorageRaftMemberImpl>>,
) -> impl IntoResponse {
    // Get Raft metrics
    let metrics = raft_member.get_metrics();

    let now = Instant::now();

    // Convert replication lag to JSON
    let replication_lag: serde_json::Map<String, serde_json::Value> = metrics
        .replication_lag
        .iter()
        .map(|(node_id, lag)| (node_id.as_u64().to_string(), json!(lag)))
        .collect();

    // Convert heartbeat timing info to JSON
    let heartbeat_status: Vec<serde_json::Value> = metrics
        .heartbeat_sent
        .iter()
        .map(|(node_id, sent_instant)| {
            let sent_ago = instant_to_seconds_ago(sent_instant, &now).unwrap_or(0.0);
            let acked_ago = metrics
                .heartbeat_acked
                .get(node_id)
                .and_then(|acked_instant| instant_to_seconds_ago(acked_instant, &now))
                .unwrap_or(f64::MAX); // Very large number if never acked

            let replication_lag = metrics
                .replication_lag
                .get(node_id)
                .copied()
                .unwrap_or(0);

            json!({
                "node_id": node_id.as_u64(),
                "last_heartbeat_sent_secs_ago": sent_ago,
                "last_heartbeat_acked_secs_ago": if acked_ago == f64::MAX { serde_json::Value::Null } else { json!(acked_ago) },
                "replication_lag": replication_lag,
                "is_responsive": acked_ago < 5.0, // Consider responsive if acked within 5 seconds
            })
        })
        .collect();

    // Convert cluster members to JSON
    let cluster_members_json: Vec<serde_json::Value> = metrics
        .cluster_members
        .iter()
        .map(|member| {
            json!({
                "node_id": member.node_id.as_u64(),
                "is_voter": member.is_voter,
                "role": if member.is_voter { "Voter" } else { "Learner" },
            })
        })
        .collect();

    let response = json!({
        "current_term": metrics.current_term,
        "role": format!("{:?}", metrics.role),
        "leader_id": metrics.leader_id.map(|id| id.as_u64()),
        "commit_index": metrics.commit_index,
        "last_applied": metrics.last_applied,
        "last_log_index": metrics.last_log_index,
        "snapshot_index": metrics.snapshot_index,
        "cluster_size": metrics.cluster_size,
        "cluster_members": cluster_members_json,
        "replication_lag": replication_lag,
        "heartbeat_status": heartbeat_status,
    });

    (StatusCode::OK, Json(response))
}

/// Handler for `/api/raft/status` endpoint.
///
/// Returns simplified Raft status information suitable for quick health checks.
///
/// # Returns
///
/// JSON response with basic Raft status:
/// - `is_leader`: Whether this node is the leader
/// - `role`: Current role as string
/// - `leader_id`: ID of current leader (if known)
/// - `cluster_size`: Number of nodes in cluster
/// - `is_healthy`: Whether the cluster is healthy (has leader, quorum, etc.)
pub async fn raft_status_handler(
    State(raft_member): State<Arc<StorageRaftMemberImpl>>,
) -> impl IntoResponse {
    // Get Raft metrics
    let metrics = raft_member.get_metrics();

    // Determine if cluster is healthy
    let is_healthy = metrics.leader_id.is_some() && metrics.cluster_size > 0;

    let is_leader = matches!(
        metrics.role,
        crate::storage_raft_member::types::RaftRole::Leader
    );

    let response = json!({
        "is_leader": is_leader,
        "role": format!("{:?}", metrics.role),
        "leader_id": metrics.leader_id.map(|id| id.as_u64()),
        "cluster_size": metrics.cluster_size,
        "is_healthy": is_healthy,
        "current_term": metrics.current_term,
    });

    (StatusCode::OK, Json(response))
}

/// Handler for `/api/raft/proposals` endpoint.
///
/// Returns persisted proposal history from the MetadataStore.
/// This shows ALL proposals applied to the state machine on this node,
/// including proposals received as a follower. Proposals are persisted
/// to SQLite and survive restarts.
///
/// # Returns
///
/// JSON response with proposal history:
/// - Array of proposal records containing:
///   - `log_index`: Raft log index (for click-through to details)
///   - `log_term`: Raft term when committed
///   - `leader_node_id`: Node that was leader when proposed
///   - `applied_at`: When this proposal was applied locally
///   - `operation_type`: Type of operation (e.g., "AtomicTransaction")
///   - `tx_id`: Transaction ID (if applicable)
///   - `operation_count`: Number of operations in the proposal
///   - `success`: Whether the operation succeeded
///   - `error_message`: Error message if failed
pub async fn raft_proposals_handler(
    State(raft_member): State<Arc<StorageRaftMemberImpl>>,
) -> impl IntoResponse {
    // Get persisted proposal history from MetadataStore (default limit: 50)
    let proposals = raft_member.get_persisted_proposal_history(50).await;

    // Convert SystemTime to RFC3339 strings for JSON serialization
    let proposals_json: Vec<serde_json::Value> = proposals
        .iter()
        .map(|p| {
            let timestamp_str = p
                .applied_at
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| {
                    chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
                })
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "unknown".to_string());

            json!({
                "log_index": p.log_index,
                "log_term": p.log_term,
                "leader_node_id": p.leader_node_id,
                "applied_at": timestamp_str,
                "operation_type": p.operation_type,
                "tx_id": p.tx_id,
                "operation_count": p.operation_count,
                "success": p.success,
                "error_message": p.error_message,
            })
        })
        .collect();

    (StatusCode::OK, Json(proposals_json))
}

/// Handler for `/api/raft/proposals/:log_index` endpoint.
///
/// Returns detailed information for a specific proposal, including
/// the full JSON-serialized operation details for display in the UI.
///
/// # Arguments
///
/// * `log_index` - The Raft log index of the proposal to retrieve
///
/// # Returns
///
/// JSON response with full proposal details:
/// - `log_index`: Raft log index
/// - `log_term`: Raft term when committed
/// - `leader_node_id`: Node that was leader when proposed
/// - `applied_at`: When this proposal was applied locally
/// - `operation_type`: Type of operation
/// - `tx_id`: Transaction ID (if applicable)
/// - `operation_count`: Number of operations
/// - `success`: Whether the operation succeeded
/// - `error_message`: Error message if failed
/// - `operation_details`: Full JSON operation details
///
/// Returns 404 if proposal not found.
pub async fn raft_proposal_details_handler(
    State(raft_member): State<Arc<StorageRaftMemberImpl>>,
    Path(log_index): Path<u64>,
) -> impl IntoResponse {
    // Get proposal details from MetadataStore
    match raft_member.get_persisted_proposal_details(log_index).await {
        Some(proposal) => {
            let timestamp_str = proposal
                .applied_at
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| {
                    chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
                })
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "unknown".to_string());

            let response = json!({
                "log_index": proposal.log_index,
                "log_term": proposal.log_term,
                "leader_node_id": proposal.leader_node_id,
                "applied_at": timestamp_str,
                "operation_type": proposal.operation_type,
                "tx_id": proposal.tx_id,
                "operation_count": proposal.operation_count,
                "success": proposal.success,
                "error_message": proposal.error_message,
                "operation_details": proposal.operation_details,
            });

            (StatusCode::OK, Json(response))
        }
        None => {
            let error_response = json!({
                "error": "Proposal not found",
                "log_index": log_index,
            });

            (StatusCode::NOT_FOUND, Json(error_response))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_instant_to_seconds_ago() {
        let now = Instant::now();
        let five_secs_ago = now - Duration::from_secs(5);

        let elapsed = instant_to_seconds_ago(&five_secs_ago, &now);
        assert!(elapsed.is_some());
        let elapsed = elapsed.unwrap();
        assert!(elapsed >= 4.9 && elapsed <= 5.1); // Allow small tolerance
    }

    #[test]
    fn test_instant_to_seconds_ago_zero() {
        let now = Instant::now();

        let elapsed = instant_to_seconds_ago(&now, &now);
        assert!(elapsed.is_some());
        let elapsed = elapsed.unwrap();
        assert!(elapsed < 0.001); // Should be very close to zero
    }

    #[test]
    fn test_instant_to_seconds_ago_future() {
        // Test with "future" instant (shouldn't happen in practice, but test anyway)
        let now = Instant::now();
        let future = now + Duration::from_secs(10);

        let elapsed = instant_to_seconds_ago(&future, &now);
        assert!(elapsed.is_none());
    }
}
