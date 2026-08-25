//! Local filesystem blob store (self-host / dev).

use crate::{max_blob_bytes, PayloadError, PayloadRef};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
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

    fn path_for(&self, key: &str) -> Result<PathBuf, PayloadError> {
        Ok(PayloadRef::fs_path(&self.root, key)?)
    }

    pub fn put_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: &[u8],
    ) -> Result<PayloadRef, PayloadError> {
        self.put_blob_reader(tenant_id, message_id, std::io::Cursor::new(data))
    }

    /// Stream a payload to disk while hashing and enforcing a hard size cap.
    pub fn put_blob_reader(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        mut reader: impl Read,
    ) -> Result<PayloadRef, PayloadError> {
        let bucket_key = PayloadRef::key_for(tenant_id, message_id)?;
        let path = self.path_for(&bucket_key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        let result = (|| {
            use std::fs::File;
            let mut f = File::create(&tmp)?;
            let mut hasher = Sha256::new();
            let mut size = 0u64;
            let limit = max_blob_bytes();
            let mut buffer = vec![0u8; 1024 * 1024];
            loop {
                let read = reader.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                size += read as u64;
                if size > limit {
                    return Err(PayloadError::TooLarge { size, limit });
                }
                hasher.update(&buffer[..read]);
                f.write_all(&buffer[..read])?;
            }
            f.sync_all()?;
            Ok((size, hex::encode(hasher.finalize())))
        })();
        let (size, sha256) = match result {
            Ok(result) => result,
            Err(error) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(error);
            }
        };
        std::fs::rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(PayloadRef {
            tenant_id: tenant_id.to_string(),
            message_id,
            bucket_key,
            size,
            sha256: Some(sha256),
        })
    }

    pub fn get_blob(&self, reference: &PayloadRef) -> Result<Vec<u8>, PayloadError> {
        let path = self.path_for(&reference.bucket_key)?;
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

    /// Stream a verified local blob to a writer without materializing it.
    pub fn get_blob_to_writer(
        &self,
        reference: &PayloadRef,
        mut writer: impl Write,
    ) -> Result<u64, PayloadError> {
        let path = self.path_for(&reference.bucket_key)?;
        let mut file = std::fs::File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                PayloadError::NotFound(reference.bucket_key.clone())
            } else {
                PayloadError::Io(e)
            }
        })?;
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            writer.write_all(&buffer[..read])?;
            size += read as u64;
        }
        if let Some(expected) = reference.sha256.as_deref() {
            let actual = hex::encode(hasher.finalize());
            if actual != expected {
                return Err(PayloadError::ChecksumMismatch);
            }
        }
        Ok(size)
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    hex::encode(digest)
}
