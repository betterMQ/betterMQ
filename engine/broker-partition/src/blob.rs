//! Large payload blob storage (local FS or S3 payload bucket).

use broker_payload::BlobStore;
#[cfg(feature = "slate")]
use broker_storage::open_payload_object_store_from_env;
use broker_storage::StorageMode;
use std::path::Path;

pub fn open_blob_store(
    data_dir: &Path,
    storage: StorageMode,
) -> Result<BlobStore, broker_payload::PayloadError> {
    if storage == StorageMode::Slate {
        #[cfg(feature = "slate")]
        {
            let s3 = open_payload_object_store_from_env().map_err(|e| {
                broker_payload::PayloadError::Store(format!(
                    "slate mode requires S3_PAYLOAD_BUCKET (separate payload object store): {e}"
                ))
            })?;
            tracing::info!("large message bodies → S3 payload bucket");
            return Ok(BlobStore::open_s3(s3));
        }
        #[cfg(not(feature = "slate"))]
        {
            return Err(broker_payload::PayloadError::Store(
                "rebuild with --features slate for Slate storage".into(),
            ));
        }
    }
    BlobStore::open_local(data_dir)
}
