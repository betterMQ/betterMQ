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
            match open_payload_object_store_from_env() {
                Ok(s3) => {
                    tracing::info!("large message bodies → S3 payload bucket");
                    return Ok(BlobStore::open_s3(s3));
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "S3_PAYLOAD_BUCKET unavailable; large payloads use local payload-blobs/"
                    );
                }
            }
        }
    }
    BlobStore::open_local(data_dir)
}
