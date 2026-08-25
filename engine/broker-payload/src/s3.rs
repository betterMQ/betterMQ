//! S3 / R2 / MinIO blob store for large message bodies.

use crate::{max_blob_bytes, PayloadError, PayloadRef};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use sha2::{Digest, Sha256};
use std::fmt::Display;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;

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
        self.put_blob_owned(tenant_id, message_id, data.to_vec())
    }

    pub fn put_blob_owned(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: Vec<u8>,
    ) -> Result<PayloadRef, PayloadError> {
        let data = Bytes::from(data);
        let chunks: Vec<Result<Bytes, std::io::Error>> = (0..data.len())
            .step_by(MULTIPART_CHUNK_BYTES)
            .map(|start| Ok(data.slice(start..(start + MULTIPART_CHUNK_BYTES).min(data.len()))))
            .collect();
        block_on_payload(self.put_blob_stream(tenant_id, message_id, futures::stream::iter(chunks)))
    }

    /// Async zero-copy upload entry point for streaming HTTP callers.
    pub async fn put_blob_bytes(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: Bytes,
    ) -> Result<PayloadRef, PayloadError> {
        let limit = max_blob_bytes();
        if data.len() as u64 > limit {
            return Err(PayloadError::TooLarge {
                size: data.len() as u64,
                limit,
            });
        }
        let bucket_key = PayloadRef::key_for(tenant_id, message_id)?;
        let path = ObjectPath::from(bucket_key.as_str());
        let size = data.len() as u64;
        let sha256 = sha256_hex(&data);
        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(map_store_err)?;
        Ok(PayloadRef {
            tenant_id: tenant_id.to_string(),
            message_id,
            bucket_key,
            size,
            sha256: Some(sha256),
        })
    }

    /// Stream chunks into a bounded multipart upload while calculating SHA-256.
    pub async fn put_blob_stream<S, E>(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        mut stream: S,
    ) -> Result<PayloadRef, PayloadError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: Display,
    {
        let bucket_key = PayloadRef::key_for(tenant_id, message_id)?;
        let path = ObjectPath::from(bucket_key.as_str());
        let mut upload = self
            .store
            .put_multipart(&path)
            .await
            .map_err(map_store_err)?;
        let limit = max_blob_bytes();
        let mut size = 0u64;
        let mut hasher = Sha256::new();
        let mut pending = BytesMut::with_capacity(MULTIPART_CHUNK_BYTES);

        while let Some(chunk) = stream.next().await {
            let mut chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = upload.abort().await;
                    return Err(PayloadError::Store(error.to_string()));
                }
            };
            size = size.saturating_add(chunk.len() as u64);
            if size > limit {
                let _ = upload.abort().await;
                return Err(PayloadError::TooLarge { size, limit });
            }
            hasher.update(&chunk);
            if pending.is_empty() {
                while chunk.len() >= MULTIPART_CHUNK_BYTES {
                    let part = chunk.split_to(MULTIPART_CHUNK_BYTES);
                    if let Err(error) = upload.put_part(part.into()).await {
                        let _ = upload.abort().await;
                        return Err(map_store_err(error));
                    }
                }
            }
            pending.extend_from_slice(&chunk);
            while pending.len() >= MULTIPART_CHUNK_BYTES {
                let part = pending.split_to(MULTIPART_CHUNK_BYTES).freeze();
                if let Err(error) = upload.put_part(part.into()).await {
                    let _ = upload.abort().await;
                    return Err(map_store_err(error));
                }
            }
        }
        if size == 0 {
            let _ = upload.abort().await;
            self.store
                .put(&path, PutPayload::from_static(b""))
                .await
                .map_err(map_store_err)?;
        } else {
            if !pending.is_empty() {
                if let Err(error) = upload.put_part(pending.freeze().into()).await {
                    let _ = upload.abort().await;
                    return Err(map_store_err(error));
                }
            }
            if let Err(error) = upload.complete().await {
                let _ = upload.abort().await;
                return Err(map_store_err(error));
            }
        }
        Ok(PayloadRef {
            tenant_id: tenant_id.to_string(),
            message_id,
            bucket_key,
            size,
            sha256: Some(hex::encode(hasher.finalize())),
        })
    }

    pub fn get_blob(&self, reference: &PayloadRef) -> Result<Vec<u8>, PayloadError> {
        Ok(block_on_payload(self.get_blob_bytes(reference))?.to_vec())
    }

    pub fn get_blob_to_writer<W: Write + Unpin>(
        &self,
        reference: &PayloadRef,
        writer: &mut W,
    ) -> Result<u64, PayloadError> {
        let mut writer = SyncAsyncWriter(writer);
        block_on_payload(self.download_blob_to_writer(reference, &mut writer))
    }

    /// Async bounded download; dispatch can forward `Bytes` without another copy.
    pub async fn get_blob_bytes(&self, reference: &PayloadRef) -> Result<Bytes, PayloadError> {
        let limit = max_blob_bytes();
        if reference.size > limit {
            return Err(PayloadError::TooLarge {
                size: reference.size,
                limit,
            });
        }
        let path = ObjectPath::from(reference.bucket_key.as_str());
        let result = self.store.get(&path).await.map_err(map_store_err)?;
        let data = result
            .bytes()
            .await
            .map_err(|e| PayloadError::Io(std::io::Error::other(e)))?;
        if data.len() as u64 > limit {
            return Err(PayloadError::TooLarge {
                size: data.len() as u64,
                limit,
            });
        }
        if let Some(expected) = &reference.sha256 {
            let actual = sha256_hex(&data);
            if actual != *expected {
                return Err(PayloadError::ChecksumMismatch);
            }
        }
        Ok(data)
    }

    /// Stream an object to an async writer with a bounded chunk and final hash check.
    pub async fn download_blob_to_writer<W>(
        &self,
        reference: &PayloadRef,
        writer: &mut W,
    ) -> Result<u64, PayloadError>
    where
        W: AsyncWrite + Unpin,
    {
        let limit = max_blob_bytes();
        if reference.size > limit {
            return Err(PayloadError::TooLarge {
                size: reference.size,
                limit,
            });
        }
        let path = ObjectPath::from(reference.bucket_key.as_str());
        let result = self.store.get(&path).await.map_err(map_store_err)?;
        if result.meta.size > limit {
            return Err(PayloadError::TooLarge {
                size: result.meta.size,
                limit,
            });
        }
        let mut stream = result.into_stream();
        let mut size = 0u64;
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(map_store_err)?;
            size = size.saturating_add(chunk.len() as u64);
            if size > limit {
                return Err(PayloadError::TooLarge { size, limit });
            }
            hasher.update(&chunk);
            writer.write_all(&chunk).await?;
        }
        writer.flush().await?;
        if size != reference.size {
            return Err(PayloadError::Store(format!(
                "payload size mismatch: expected {}, got {size}",
                reference.size
            )));
        }
        if let Some(expected) = reference.sha256.as_deref() {
            if hex::encode(hasher.finalize()) != expected {
                return Err(PayloadError::ChecksumMismatch);
            }
        }
        Ok(size)
    }
}

struct SyncAsyncWriter<'a, W>(&'a mut W);

impl<W: Write + Unpin> AsyncWrite for SyncAsyncWriter<'_, W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Poll::Ready(self.0.write(buffer))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(self.0.flush())
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
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

fn block_on_payload<F: Future>(future: F) -> F::Output {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| payload_runtime().block_on(future))
    } else {
        payload_runtime().block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use object_store::memory::InMemory;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn multipart_upload_and_download_are_streamed() {
        let store = S3BlobStore::new(Arc::new(InMemory::new()));
        let chunks = vec![
            Ok::<_, std::io::Error>(Bytes::from(vec![1u8; 5 * 1024 * 1024])),
            Ok(Bytes::from(vec![2u8; 5 * 1024 * 1024])),
        ];
        let reference = store
            .put_blob_stream("tenant", Uuid::new_v4(), stream::iter(chunks))
            .await
            .unwrap();
        assert_eq!(reference.size, 10 * 1024 * 1024);

        let (reader, mut writer) = tokio::io::duplex(64 * 1024);
        let download = store.download_blob_to_writer(&reference, &mut writer);
        let read = async {
            let mut output = Vec::new();
            reader
                .take(reference.size)
                .read_to_end(&mut output)
                .await
                .unwrap();
            output
        };
        let (downloaded, output) = tokio::join!(download, read);
        assert_eq!(downloaded.unwrap(), reference.size);
        assert_eq!(output.len() as u64, reference.size);
        assert_eq!(output[0], 1);
        assert_eq!(*output.last().unwrap(), 2);
    }

    #[test]
    fn synchronous_dispatch_writer_streams_without_materializing() {
        let store = S3BlobStore::new(Arc::new(InMemory::new()));
        let reference = store
            .put_blob_owned("tenant", Uuid::new_v4(), vec![4u8; 9 * 1024 * 1024])
            .unwrap();
        let mut output = Vec::new();
        assert_eq!(
            store.get_blob_to_writer(&reference, &mut output).unwrap(),
            reference.size
        );
        assert_eq!(output.len() as u64, reference.size);
    }
}
