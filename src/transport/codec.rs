//! Protobuf encoding/decoding utilities for Raft messages
//!
//! This module provides helper functions to serialize and deserialize
//! Raft RPC messages using protobuf.

use crate::raft::proto_types::proto::{RaftRequest, RaftResponse};
use crate::transport::{Result, TransportError};
use prost::Message;

/// Encode a RaftRequest to protobuf bytes
pub fn encode_raft_request(req: &RaftRequest) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    req.encode(&mut buf)
        .map_err(|e| TransportError::Serialization(format!("Failed to encode request: {}", e)))?;
    Ok(buf)
}

/// Decode a RaftRequest from protobuf bytes
pub fn decode_raft_request(bytes: &[u8]) -> Result<RaftRequest> {
    RaftRequest::decode(bytes)
        .map_err(|e| TransportError::Serialization(format!("Failed to decode request: {}", e)))
}

/// Encode a RaftResponse to protobuf bytes
pub fn encode_raft_response(resp: &RaftResponse) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    resp.encode(&mut buf)
        .map_err(|e| TransportError::Serialization(format!("Failed to encode response: {}", e)))?;
    Ok(buf)
}

/// Decode a RaftResponse from protobuf bytes
pub fn decode_raft_response(bytes: &[u8]) -> Result<RaftResponse> {
    RaftResponse::decode(bytes)
        .map_err(|e| TransportError::Serialization(format!("Failed to decode response: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::proto_types::proto::{
        raft_request, raft_response, AppendEntriesRequest, AppendEntriesResponse,
    };

    #[test]
    fn test_request_encode_decode_roundtrip() {
        let request = RaftRequest {
            request: Some(raft_request::Request::AppendEntries(AppendEntriesRequest {
                term: 1,
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            })),
        };

        let encoded = encode_raft_request(&request).unwrap();
        let decoded = decode_raft_request(&encoded).unwrap();

        assert!(decoded.request.is_some());
        match decoded.request.unwrap() {
            raft_request::Request::AppendEntries(req) => {
                assert_eq!(req.term, 1);
                assert_eq!(req.leader_id, 1);
            }
            _ => panic!("Wrong request type"),
        }
    }

    #[test]
    fn test_response_encode_decode_roundtrip() {
        let response = RaftResponse {
            response: Some(raft_response::Response::AppendEntries(
                AppendEntriesResponse {
                    term: 1,
                    success: true,
                    conflict: None,
                },
            )),
        };

        let encoded = encode_raft_response(&response).unwrap();
        let decoded = decode_raft_response(&encoded).unwrap();

        assert!(decoded.response.is_some());
        match decoded.response.unwrap() {
            raft_response::Response::AppendEntries(resp) => {
                assert_eq!(resp.term, 1);
                assert!(resp.success);
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_decode_invalid_data() {
        let invalid_data = vec![0xFF, 0xFF, 0xFF];
        assert!(decode_raft_request(&invalid_data).is_err());
        assert!(decode_raft_response(&invalid_data).is_err());
    }
}
