//! Unified blob store + ingest helpers.

use crate::{fs::FsBlobStore, PayloadError, PayloadRef};
use std::path::Path;
use uuid::Uuid;

#[cfg(feature = "s3")]
use crate::s3::S3BlobStore;

/// Default inline threshold — bodies larger than this go to blob storage.
pub fn inline_max_bytes() -> usize {
    std::env::var("BETTERMQ_INLINE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256 * 1024)
}

#[derive(Clone)]
pub enum BlobStore {
    Fs(FsBlobStore),
    #[cfg(feature = "s3")]
    S3(S3BlobStore),
}

impl BlobStore {
    pub fn open_local(data_dir: impl AsRef<Path>) -> Result<Self, PayloadError> {
        Ok(Self::Fs(FsBlobStore::open(
            data_dir.as_ref().join("payload-blobs"),
        )?))
    }

    #[cfg(feature = "s3")]
    pub fn open_s3(store: std::sync::Arc<dyn object_store::ObjectStore>) -> Self {
        Self::S3(S3BlobStore::new(store))
    }

    pub fn put_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: &[u8],
    ) -> Result<PayloadRef, PayloadError> {
        match self {
            Self::Fs(fs) => fs.put_blob(tenant_id, message_id, data),
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.put_blob(tenant_id, message_id, data),
        }
    }

    pub fn get_blob(&self, reference: &PayloadRef) -> Result<Vec<u8>, PayloadError> {
        match self {
            Self::Fs(fs) => fs.get_blob(reference),
            #[cfg(feature = "s3")]
            Self::S3(s3) => s3.get_blob(reference),
        }
    }
}

/// Split payload for log append: inline bytes + optional external ref JSON.
pub fn prepare_for_log(
    store: &BlobStore,
    tenant_id: &str,
    message_id: Uuid,
    payload: Vec<u8>,
) -> Result<(Vec<u8>, Option<String>), PayloadError> {
    let inline_max = inline_max_bytes();
    if payload.len() <= inline_max {
        return Ok((payload, None));
    }
    let reference = store.put_blob(tenant_id, message_id, &payload)?;
    let json = serde_json::to_string(&reference).map_err(|e| PayloadError::Store(e.to_string()))?;
    Ok((Vec::new(), Some(json)))
}

pub fn hydrate_payload(
    store: &BlobStore,
    payload: &mut Vec<u8>,
    payload_ref_json: Option<&str>,
) -> Result<(), PayloadError> {
    let Some(json) = payload_ref_json else {
        return Ok(());
    };
    if !payload.is_empty() {
        return Ok(());
    }
    let reference: PayloadRef =
        serde_json::from_str(json).map_err(|e| PayloadError::Store(e.to_string()))?;
    *payload = store.get_blob(&reference)?;
    Ok(())
}
