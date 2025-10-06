//! Metadata Protocol Handler for libp2p
//!
//! This module implements a custom libp2p request-response protocol for
//! metadata synchronization in WormFS. It handles encoding/decoding of
//! protobuf messages and manages protocol streams.

use anyhow::{anyhow, Result};
use futures::{AsyncRead, AsyncWrite};
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::StreamProtocol;
use prost::Message as ProstMessage;
use std::io;

use crate::metadata_protocol::{
    MasterAnnouncement, MasterHeartbeat, MetadataAck, MetadataEvent, MetadataProposal,
    MetadataRequest, MetadataResponse, MetadataSyncRequest, MetadataSyncResponse,
};

/// Protocol identifier for metadata synchronization
pub const METADATA_PROTOCOL_ID: &str = "/wormfs/metadata/1.0.0";

/// Maximum message size (10MB)
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Wrapper for all metadata protocol messages
#[derive(Debug, Clone)]
pub enum MetadataMessage {
    /// Metadata event broadcast
    Event(MetadataEvent),
    /// Operation proposal to master
    Proposal(MetadataProposal),
    /// Response from master
    Response(MetadataResponse),
    /// Acknowledgment of received event
    Ack(MetadataAck),
    /// Request for missing events
    Request(MetadataRequest),
    /// Request for full sync
    SyncRequest(MetadataSyncRequest),
    /// Response with event range
    SyncResponse(MetadataSyncResponse),
    /// Master election announcement
    MasterAnnounce(MasterAnnouncement),
    /// Master heartbeat
    MasterHeartbeat(MasterHeartbeat),
}

impl MetadataMessage {
    /// Encode the message to bytes
    pub fn encode(&self) -> Result<Vec<u8>> {
        // Use a tag byte to identify message type
        let mut buf = Vec::new();

        match self {
            MetadataMessage::Event(msg) => {
                buf.push(0x01);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode Event: {}", e))?;
            }
            MetadataMessage::Proposal(msg) => {
                buf.push(0x02);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode Proposal: {}", e))?;
            }
            MetadataMessage::Response(msg) => {
                buf.push(0x03);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode Response: {}", e))?;
            }
            MetadataMessage::Ack(msg) => {
                buf.push(0x04);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode Ack: {}", e))?;
            }
            MetadataMessage::Request(msg) => {
                buf.push(0x05);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode Request: {}", e))?;
            }
            MetadataMessage::SyncRequest(msg) => {
                buf.push(0x06);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode SyncRequest: {}", e))?;
            }
            MetadataMessage::SyncResponse(msg) => {
                buf.push(0x07);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode SyncResponse: {}", e))?;
            }
            MetadataMessage::MasterAnnounce(msg) => {
                buf.push(0x08);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode MasterAnnounce: {}", e))?;
            }
            MetadataMessage::MasterHeartbeat(msg) => {
                buf.push(0x09);
                msg.encode(&mut buf)
                    .map_err(|e| anyhow!("Failed to encode MasterHeartbeat: {}", e))?;
            }
        }

        Ok(buf)
    }

    /// Decode the message from bytes
    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(anyhow!("Empty message"));
        }

        let tag = bytes[0];
        bytes = &bytes[1..];

        match tag {
            0x01 => {
                let msg = MetadataEvent::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode Event: {}", e))?;
                Ok(MetadataMessage::Event(msg))
            }
            0x02 => {
                let msg = MetadataProposal::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode Proposal: {}", e))?;
                Ok(MetadataMessage::Proposal(msg))
            }
            0x03 => {
                let msg = MetadataResponse::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode Response: {}", e))?;
                Ok(MetadataMessage::Response(msg))
            }
            0x04 => {
                let msg = MetadataAck::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode Ack: {}", e))?;
                Ok(MetadataMessage::Ack(msg))
            }
            0x05 => {
                let msg = MetadataRequest::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode Request: {}", e))?;
                Ok(MetadataMessage::Request(msg))
            }
            0x06 => {
                let msg = MetadataSyncRequest::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode SyncRequest: {}", e))?;
                Ok(MetadataMessage::SyncRequest(msg))
            }
            0x07 => {
                let msg = MetadataSyncResponse::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode SyncResponse: {}", e))?;
                Ok(MetadataMessage::SyncResponse(msg))
            }
            0x08 => {
                let msg = MasterAnnouncement::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode MasterAnnounce: {}", e))?;
                Ok(MetadataMessage::MasterAnnounce(msg))
            }
            0x09 => {
                let msg = MasterHeartbeat::decode(bytes)
                    .map_err(|e| anyhow!("Failed to decode MasterHeartbeat: {}", e))?;
                Ok(MetadataMessage::MasterHeartbeat(msg))
            }
            _ => Err(anyhow!("Unknown message tag: {}", tag)),
        }
    }
}

/// Codec for metadata protocol messages
#[derive(Debug, Clone, Default)]
pub struct MetadataCodec;

#[async_trait::async_trait]
impl request_response::Codec for MetadataCodec {
    type Protocol = StreamProtocol;
    type Request = MetadataMessage;
    type Response = MetadataMessage;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_length_prefixed(io, MAX_MESSAGE_SIZE)
            .await
            .and_then(|buf| {
                MetadataMessage::decode(&buf).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Decode error: {}", e))
                })
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
        read_length_prefixed(io, MAX_MESSAGE_SIZE)
            .await
            .and_then(|buf| {
                MetadataMessage::decode(&buf).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Decode error: {}", e))
                })
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
        let data = req.encode().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Encode error: {}", e))
        })?;

        write_length_prefixed(io, &data).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data = res.encode().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Encode error: {}", e))
        })?;

        write_length_prefixed(io, &data).await
    }
}

/// Read a length-prefixed message from the stream
async fn read_length_prefixed<T>(io: &mut T, max_size: usize) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    use futures::io::AsyncReadExt;

    // Read the length prefix (4 bytes, big-endian)
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > max_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Message too large: {} > {}", len, max_size),
        ));
    }

    // Read the message data
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;

    Ok(buf)
}

/// Write a length-prefixed message to the stream
async fn write_length_prefixed<T>(io: &mut T, data: &[u8]) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    use futures::io::AsyncWriteExt;

    let len = data.len() as u32;
    let len_buf = len.to_be_bytes();

    // Write length prefix
    io.write_all(&len_buf).await?;
    // Write data
    io.write_all(data).await?;
    io.flush().await?;

    Ok(())
}

/// Create a metadata request-response behaviour
pub fn create_metadata_behaviour() -> request_response::Behaviour<MetadataCodec> {
    let protocols = std::iter::once((
        StreamProtocol::new(METADATA_PROTOCOL_ID),
        ProtocolSupport::Full,
    ));

    let cfg = request_response::Config::default();

    request_response::Behaviour::new(protocols, cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_protocol::{create_protocol_version, MetadataPeerInfo};
    use uuid::Uuid;

    #[test]
    fn test_message_encode_decode_event() {
        let event = MetadataEvent {
            version: Some(create_protocol_version()),
            sequence_number: 42,
            originator: Some(MetadataPeerInfo {
                peer_id: "test_peer".to_string(),
                node_id: Uuid::new_v4().to_string(),
                timestamp: 1000,
            }),
            timestamp: 1000,
            event: None,
        };

        let msg = MetadataMessage::Event(event);
        let encoded = msg.encode().unwrap();
        let decoded = MetadataMessage::decode(&encoded).unwrap();

        match decoded {
            MetadataMessage::Event(e) => {
                assert_eq!(e.sequence_number, 42);
            }
            _ => panic!("Expected Event message"),
        }
    }

    #[test]
    fn test_message_encode_decode_all_types() {
        let test_cases = vec![
            MetadataMessage::Event(MetadataEvent::default()),
            MetadataMessage::Proposal(MetadataProposal::default()),
            MetadataMessage::Response(MetadataResponse::default()),
            MetadataMessage::Ack(MetadataAck::default()),
            MetadataMessage::Request(MetadataRequest::default()),
            MetadataMessage::SyncRequest(MetadataSyncRequest::default()),
            MetadataMessage::SyncResponse(MetadataSyncResponse::default()),
            MetadataMessage::MasterAnnounce(MasterAnnouncement::default()),
            MetadataMessage::MasterHeartbeat(MasterHeartbeat::default()),
        ];

        for msg in test_cases {
            let encoded = msg.encode().unwrap();
            let decoded = MetadataMessage::decode(&encoded).unwrap();

            // Verify the types match
            assert_eq!(
                std::mem::discriminant(&msg),
                std::mem::discriminant(&decoded)
            );
        }
    }

    #[test]
    fn test_decode_invalid_tag() {
        let invalid_data = vec![0xFF, 0x01, 0x02, 0x03];
        let result = MetadataMessage::decode(&invalid_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_empty_message() {
        let result = MetadataMessage::decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_malformed_data() {
        // Tag is valid but data is malformed
        let invalid_data = vec![0x01, 0xFF, 0xFF, 0xFF];
        let result = MetadataMessage::decode(&invalid_data);
        assert!(result.is_err());
    }
}
