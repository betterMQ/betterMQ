//! S3 / R2 / MinIO blob store for large message bodies.

use crate::{PayloadError, PayloadRef};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct S3BlobStore {
    store: Arc<dyn ObjectStore>,
}

impl S3BlobStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn put_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: &[u8],
    ) -> Result<PayloadRef, PayloadError> {
        let bucket_key = PayloadRef::key_for(tenant_id, message_id);
        let path = ObjectPath::from(bucket_key.as_str());
        let rt = payload_runtime();
        let body = bytes::Bytes::copy_from_slice(data);
        rt.block_on(self.store.put(&path, PutPayload::from_bytes(body)))
            .map_err(map_store_err)?;
        Ok(PayloadRef {
            tenant_id: tenant_id.to_string(),
            message_id,
            bucket_key,
            size: data.len() as u64,
            sha256: Some(sha256_hex(data)),
        })
    }

    pub fn get_blob(&self, reference: &PayloadRef) -> Result<Vec<u8>, PayloadError> {
        let path = ObjectPath::from(reference.bucket_key.as_str());
        let rt = payload_runtime();
        let data = rt.block_on(async {
            let result = self.store.get(&path).await.map_err(map_store_err)?;
            let bytes = result
                .bytes()
                .await
                .map_err(|e| PayloadError::Io(std::io::Error::other(e)))?;
            Ok::<Vec<u8>, PayloadError>(bytes.to_vec())
        })?;
        if let Some(expected) = &reference.sha256 {
            let actual = sha256_hex(&data);
            if actual != *expected {
                return Err(PayloadError::ChecksumMismatch);
            }
        }
        Ok(data)
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    hex::encode(digest)
}

fn map_store_err(e: object_store::Error) -> PayloadError {
    match e {
        object_store::Error::NotFound { path, .. } => PayloadError::NotFound(path),
        other => PayloadError::Store(other.to_string()),
    }
}

fn payload_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("payload-io")
            .enable_all()
            .build()
            .expect("payload tokio runtime")
    })
}
