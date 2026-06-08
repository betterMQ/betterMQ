//! Local filesystem blob store (self-host / dev).

use crate::{PayloadError, PayloadRef};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Clone)]
pub struct FsBlobStore {
    root: PathBuf,
}

impl FsBlobStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, PayloadError> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root
            .join(key.replace('/', std::path::MAIN_SEPARATOR_STR))
    }

    pub fn put_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: &[u8],
    ) -> Result<PayloadRef, PayloadError> {
        let bucket_key = PayloadRef::key_for(tenant_id, message_id);
        let path = self.path_for(&bucket_key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, data)?;
        std::fs::rename(tmp, &path)?;
        Ok(PayloadRef {
            tenant_id: tenant_id.to_string(),
            message_id,
            bucket_key,
            size: data.len() as u64,
            sha256: Some(sha256_hex(data)),
        })
    }

    pub fn get_blob(&self, reference: &PayloadRef) -> Result<Vec<u8>, PayloadError> {
        let path = self.path_for(&reference.bucket_key);
        let data = std::fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                PayloadError::NotFound(reference.bucket_key.clone())
            } else {
                PayloadError::Io(e)
            }
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
