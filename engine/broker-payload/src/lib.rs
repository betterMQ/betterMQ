//! Payload storage: inline bytes in the log or blob objects (CP3b).

mod fs;
mod inline;
mod r#ref;
mod store;

#[cfg(feature = "s3")]
mod s3;

pub use fs::FsBlobStore;
pub use inline::InlinePayloadStore;
pub use r#ref::PayloadRef;
pub use store::{hydrate_payload, inline_max_bytes, max_blob_bytes, prepare_for_log, BlobStore};

#[cfg(feature = "s3")]
pub use s3::S3BlobStore;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum PayloadError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("store error: {0}")]
    Store(String),
    #[error("payload too large: {size} bytes exceeds limit {limit}")]
    TooLarge { size: u64, limit: u64 },
    #[error("invalid blob key: {0}")]
    InvalidKey(#[from] broker_proto::PathSegmentError),
}

/// Threshold above which callers should use blob storage (see `inline_max_bytes()`).
pub const INLINE_MAX_BYTES: usize = 1024 * 1024;

/// Legacy in-memory store (tests).
#[async_trait]
pub trait PayloadStore: Send + Sync {
    async fn put_inline(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: Bytes,
    ) -> Result<(), PayloadError>;
    async fn get_inline(
        &self,
        tenant_id: &str,
        message_id: Uuid,
    ) -> Result<Option<Bytes>, PayloadError>;
    async fn put_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: Bytes,
    ) -> Result<PayloadRef, PayloadError>;
    async fn open_stream(&self, reference: &PayloadRef) -> Result<Bytes, PayloadError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn fs_blob_roundtrip() {
        let dir = tempdir().unwrap();
        let store = BlobStore::open_local(dir.path()).unwrap();
        let id = Uuid::new_v4();
        let body = vec![0u8; 300_000];
        let reference = store.put_blob("default", id, &body).unwrap();
        assert_eq!(reference.size, 300_000);
        let loaded = store.get_blob(&reference).unwrap();
        assert_eq!(loaded, body);
    }

    #[test]
    fn prepare_for_log_splits_large_body() {
        let dir = tempdir().unwrap();
        let store = BlobStore::open_local(dir.path()).unwrap();
        let id = Uuid::new_v4();
        let body = vec![1u8; inline_max_bytes() + 1];
        let (inline, json) = prepare_for_log(&store, "default", id, body.clone()).unwrap();
        assert!(inline.is_empty());
        let json = json.expect("ref json");
        let mut restored = inline;
        hydrate_payload(&store, &mut restored, Some(&json)).unwrap();
        assert_eq!(restored, body);
    }

    #[test]
    fn blob_key_rejects_traversal() {
        assert!(PayloadRef::key_for("../etc", Uuid::new_v4()).is_err());
        let dir = tempdir().unwrap();
        let store = FsBlobStore::open(dir.path()).unwrap();
        let bad = PayloadRef {
            tenant_id: "t".into(),
            message_id: Uuid::new_v4(),
            bucket_key: "../secret".into(),
            size: 1,
            sha256: None,
        };
        assert!(store.get_blob(&bad).is_err());
    }

    #[test]
    fn fs_blob_streams_in_and_out_with_checksum() {
        let dir = tempdir().unwrap();
        let store = FsBlobStore::open(dir.path()).unwrap();
        let id = Uuid::new_v4();
        let input = vec![7u8; 2 * 1024 * 1024 + 17];
        let reference = store
            .put_blob_reader("default", id, std::io::Cursor::new(&input))
            .unwrap();
        let mut output = Vec::new();
        let written = store.get_blob_to_writer(&reference, &mut output).unwrap();
        assert_eq!(written, input.len() as u64);
        assert_eq!(output, input);
    }
}
