//! libp2p request-response protocol definition for Raft RPCs
//!
//! This module defines the protocol and codec for Raft message exchange
//! over libp2p's request-response protocol.

use crate::raft::proto_types::proto::{RaftRequest, RaftResponse};
use crate::transport::codec::{
    decode_raft_request, decode_raft_response, encode_raft_request, encode_raft_response,
};
use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::Codec;
use std::io;

/// The Raft protocol name for libp2p
#[derive(Debug, Clone)]
pub struct RaftProtocol;

impl AsRef<str> for RaftProtocol {
    fn as_ref(&self) -> &str {
        "/wormfs/raft/1.0.0"
    }
}

/// Codec for encoding/decoding Raft messages over libp2p
#[derive(Debug, Clone)]
pub struct RaftCodec;

impl Default for RaftCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftCodec {
    /// Create a new RaftCodec
    pub fn new() -> Self {
        RaftCodec
    }

    /// Maximum message size (10MB)
    const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;
}

#[async_trait]
impl Codec for RaftCodec {
    type Protocol = RaftProtocol;
    type Request = RaftRequest;
    type Response = RaftResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Read message length (4 bytes, big-endian)
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // Check message size limit
        if len > Self::MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Message too large: {} bytes (max: {})",
                    len,
                    Self::MAX_MESSAGE_SIZE
                ),
            ));
        }

        // Read message data
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;

        // Decode protobuf
        decode_raft_request(&buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to decode request: {}", e),
            )
        })
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Read message length (4 bytes, big-endian)
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // Check message size limit
        if len > Self::MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Message too large: {} bytes (max: {})",
                    len,
                    Self::MAX_MESSAGE_SIZE
                ),
            ));
        }

        // Read message data
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;

        // Decode protobuf
        decode_raft_response(&buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to decode response: {}", e),
            )
        })
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Encode protobuf
        let data = encode_raft_request(&req).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to encode request: {}", e),
            )
        })?;

        // Check message size limit
        if data.len() > Self::MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Message too large: {} bytes (max: {})",
                    data.len(),
                    Self::MAX_MESSAGE_SIZE
                ),
            ));
        }

        // Write message length (4 bytes, big-endian)
        let len = data.len() as u32;
        io.write_all(&len.to_be_bytes()).await?;

        // Write message data
        io.write_all(&data).await?;
        io.flush().await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Encode protobuf
        let data = encode_raft_response(&resp).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to encode response: {}", e),
            )
        })?;

        // Check message size limit
        if data.len() > Self::MAX_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Message too large: {} bytes (max: {})",
                    data.len(),
                    Self::MAX_MESSAGE_SIZE
                ),
            ));
        }

        // Write message length (4 bytes, big-endian)
        let len = data.len() as u32;
        io.write_all(&len.to_be_bytes()).await?;

        // Write message data
        io.write_all(&data).await?;
        io.flush().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::proto_types::proto::{raft_request, AppendEntriesRequest};
    use futures::io::Cursor;

    #[tokio::test]
    async fn test_request_write_read_roundtrip() {
        let mut codec = RaftCodec::new();
        let protocol = RaftProtocol;

        let request = RaftRequest {
            request: Some(raft_request::Request::AppendEntries(AppendEntriesRequest {
                term: 42,
                leader_id: 1,
                prev_log_index: 10,
                prev_log_term: 5,
                entries: vec![],
                leader_commit: 8,
            })),
        };

        // Write request to buffer
        let mut buffer = Vec::new();
        codec
            .write_request(&protocol, &mut buffer, request.clone())
            .await
            .unwrap();

        // Read request back
        let mut cursor = Cursor::new(buffer);
        let decoded = codec.read_request(&protocol, &mut cursor).await.unwrap();

        // Verify
        assert!(decoded.request.is_some());
        match decoded.request.unwrap() {
            raft_request::Request::AppendEntries(req) => {
                assert_eq!(req.term, 42);
                assert_eq!(req.leader_id, 1);
                assert_eq!(req.prev_log_index, 10);
            }
            _ => panic!("Wrong request type"),
        }
    }

    #[tokio::test]
    async fn test_message_size_limit() {
        let mut codec = RaftCodec::new();
        let protocol = RaftProtocol;

        // Create a message that's too large
        let large_data = vec![0u8; RaftCodec::MAX_MESSAGE_SIZE + 1];

        // Try to read a message with size > MAX_MESSAGE_SIZE
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&((RaftCodec::MAX_MESSAGE_SIZE + 1) as u32).to_be_bytes());
        buffer.extend_from_slice(&large_data);

        let mut cursor = Cursor::new(buffer);
        let result = codec.read_request(&protocol, &mut cursor).await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too large"));
    }
}
