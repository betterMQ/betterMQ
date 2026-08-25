//! Durable log record encoding (length-prefixed, CRC32-checked).

use crate::retry::RetryBackoff;
use crate::RECORD_MAGIC;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use thiserror::Error;
use uuid::Uuid;

/// Max bincode header size accepted when decoding a frame.
pub const MAX_FRAME_HEADER_BYTES: usize = 1024 * 1024;
/// Default max payload size (aligned with the HTTP body cap).
pub const DEFAULT_MAX_FRAME_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

pub fn max_frame_payload_bytes() -> usize {
    std::env::var("BETTERMQ_MAX_HTTP_BODY_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(DEFAULT_MAX_FRAME_PAYLOAD_BYTES)
}

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("invalid magic")]
    InvalidMagic,
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("frame too large: header={header_len} payload={payload_len}")]
    FrameTooLarge {
        header_len: usize,
        payload_len: usize,
    },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("encode error: {0}")]
    Encode(#[from] bincode_next::error::EncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] bincode_next::error::DecodeError),
}

fn default_priority() -> u8 {
    5
}

fn default_max_retries() -> u32 {
    3
}

/// Metadata stored in the log before payload bytes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogRecord {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub routing_key: String,
    pub idempotency_key: Option<String>,
    pub published_at_ms: i64,
    /// 0 (lowest) .. 9 (highest). Used for webhook ordering when parallelism = 1.
    #[serde(default = "default_priority")]
    pub priority: u8,
    /// Per-message flow control override (parallel in-flight per routing key).
    #[serde(default)]
    pub flow_parallelism: Option<u32>,
    #[serde(default)]
    pub flow_key: Option<String>,
    #[serde(default)]
    pub flow_rate: Option<u32>,
    #[serde(default)]
    pub flow_period_secs: Option<u64>,
    /// Queue used at enqueue; destination URL/secret are fixed for this message.
    #[serde(default)]
    pub queue_id: Option<Uuid>,
    /// Fan-out group (one publish → many destinations).
    #[serde(default)]
    pub group_id: Option<Uuid>,
    #[serde(default)]
    pub group_member_id: Option<Uuid>,
    #[serde(default)]
    pub flow_profile_id: Option<Uuid>,
    #[serde(default)]
    pub destination_url: Option<String>,
    #[serde(default)]
    pub destination_secret: Option<String>,
    /// Extra delivery attempts after the first failure (`0` = single try, no retry).
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Backoff between attempts; when absent at read time, dispatch uses broker defaults.
    #[serde(default)]
    pub retry_backoff: Option<RetryBackoff>,
    /// Outbound HTTP method (default POST). Snapshotted at enqueue.
    #[serde(default)]
    pub http_method: Option<String>,
    /// JSON object of request headers. Snapshotted at enqueue.
    #[serde(default)]
    pub http_headers_json: Option<String>,
    /// Add BetterMQ HMAC signature headers when delivering.
    #[serde(default)]
    pub http_sign: Option<bool>,
    /// JSON `PayloadRef` when body stored in object store (CP3b).
    #[serde(default)]
    pub payload_ref_json: Option<String>,
}

/// Pre-priority on-disk header (read compatibility).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LogRecordLegacy {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub routing_key: String,
    pub idempotency_key: Option<String>,
    pub published_at_ms: i64,
}

impl LogRecord {
    pub fn decode_bytes(bytes: &[u8]) -> Result<Self, bincode_next::error::DecodeError> {
        match crate::codec::decode::<Self>(bytes) {
            Ok(r) => Ok(r),
            Err(_) => {
                let leg: LogRecordLegacy = crate::codec::decode(bytes)?;
                Ok(Self {
                    id: leg.id,
                    tenant_id: leg.tenant_id,
                    topic: leg.topic,
                    routing_key: leg.routing_key,
                    idempotency_key: leg.idempotency_key,
                    published_at_ms: leg.published_at_ms,
                    priority: default_priority(),
                    flow_parallelism: None,
                    flow_key: None,
                    flow_rate: None,
                    flow_period_secs: None,
                    queue_id: None,
                    group_id: None,
                    group_member_id: None,
                    flow_profile_id: None,
                    destination_url: None,
                    destination_secret: None,
                    max_retries: default_max_retries(),
                    retry_backoff: None,
                    http_method: None,
                    http_headers_json: None,
                    http_sign: None,
                    payload_ref_json: None,
                })
            }
        }
    }
}

/// Full message returned to consumers (includes assigned offset and payload).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredMessage {
    pub id: Uuid,
    pub tenant_id: String,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub routing_key: String,
    pub payload: Vec<u8>,
    pub published_at_ms: i64,
    pub priority: u8,
    pub flow_parallelism: Option<u32>,
    pub flow_key: Option<String>,
    pub flow_rate: Option<u32>,
    pub flow_period_secs: Option<u64>,
    pub queue_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
    pub group_member_id: Option<Uuid>,
    pub flow_profile_id: Option<Uuid>,
    pub destination_url: Option<String>,
    pub destination_secret: Option<String>,
    pub max_retries: u32,
    pub retry_backoff: Option<RetryBackoff>,
    pub http_method: Option<String>,
    pub http_headers_json: Option<String>,
    pub http_sign: Option<bool>,
    /// JSON `PayloadRef` when body is stored in object storage (CP3b).
    #[serde(default)]
    pub payload_ref_json: Option<String>,
}

/// Encode a frame into a new buffer (replication / batch append).
pub fn encode_frame_vec(header: &LogRecord, payload: &[u8]) -> Result<Vec<u8>, RecordError> {
    let mut buf = Vec::new();
    encode_frame(header, payload, &mut buf)?;
    Ok(buf)
}

/// CRC32 of concatenated frames (commit-epoch / replication batch).
pub fn batch_crc(frames: &[u8]) -> u32 {
    crc32fast::hash(frames)
}

/// On-disk frame: magic | header_len | payload_len | header | payload | crc32
pub fn encode_frame(
    header: &LogRecord,
    payload: &[u8],
    writer: &mut impl Write,
) -> Result<(), RecordError> {
    let header_bytes = crate::codec::encode(header)?;
    if header_bytes.len() > MAX_FRAME_HEADER_BYTES || payload.len() > max_frame_payload_bytes() {
        return Err(RecordError::FrameTooLarge {
            header_len: header_bytes.len(),
            payload_len: payload.len(),
        });
    }
    let mut body = Vec::with_capacity(12 + header_bytes.len() + payload.len());
    body.extend_from_slice(&RECORD_MAGIC);
    body.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
    body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    body.extend_from_slice(&header_bytes);
    body.extend_from_slice(payload);
    let checksum = crc32fast::hash(&body);
    writer.write_all(&body)?;
    writer.write_all(&checksum.to_be_bytes())?;
    Ok(())
}

pub fn decode_frame(mut reader: impl Read) -> Result<(LogRecord, Vec<u8>), RecordError> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    if magic != RECORD_MAGIC {
        return Err(RecordError::InvalidMagic);
    }

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let header_len = u32::from_be_bytes(len_buf) as usize;
    reader.read_exact(&mut len_buf)?;
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    if header_len > MAX_FRAME_HEADER_BYTES || payload_len > max_frame_payload_bytes() {
        return Err(RecordError::FrameTooLarge {
            header_len,
            payload_len,
        });
    }

    let mut header_bytes = vec![0u8; header_len];
    reader.read_exact(&mut header_bytes)?;
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;

    let mut checksum_bytes = [0u8; 4];
    reader.read_exact(&mut checksum_bytes)?;
    let expected = u32::from_be_bytes(checksum_bytes);

    let mut body = Vec::with_capacity(12 + header_len + payload_len);
    body.extend_from_slice(&RECORD_MAGIC);
    body.extend_from_slice(&(header_len as u32).to_be_bytes());
    body.extend_from_slice(&(payload_len as u32).to_be_bytes());
    body.extend_from_slice(&header_bytes);
    body.extend_from_slice(&payload);
    if crc32fast::hash(&body) != expected {
        return Err(RecordError::ChecksumMismatch);
    }

    let header = LogRecord::decode_bytes(&header_bytes)?;
    Ok((header, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_frame() {
        let header = LogRecord {
            id: Uuid::new_v4(),
            tenant_id: "default".into(),
            topic: "orders".into(),
            routing_key: "rk1".into(),
            idempotency_key: Some("idem-1".into()),
            published_at_ms: 1_700_000_000_000,
            priority: 5,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        };
        let payload = b"hello".to_vec();
        let mut buf = Vec::new();
        encode_frame(&header, &payload, &mut buf).unwrap();
        let (h, p) = decode_frame(std::io::Cursor::new(buf)).unwrap();
        assert_eq!(h, header);
        assert_eq!(p, payload);
    }

    #[test]
    fn decode_rejects_oversize_lengths() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&RECORD_MAGIC);
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        let err = decode_frame(std::io::Cursor::new(buf)).unwrap_err();
        assert!(matches!(err, RecordError::FrameTooLarge { .. }));
    }
}
