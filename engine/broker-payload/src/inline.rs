use crate::{PayloadError, PayloadRef, PayloadStore, INLINE_MAX_BYTES};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Default)]
pub struct InlinePayloadStore {
    inline: Mutex<HashMap<(String, Uuid), Bytes>>,
    blobs: Mutex<HashMap<String, Bytes>>,
}

#[async_trait]
impl PayloadStore for InlinePayloadStore {
    async fn put_inline(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: Bytes,
    ) -> Result<(), PayloadError> {
        self.inline
            .lock()
            .insert((tenant_id.to_string(), message_id), data);
        Ok(())
    }

    async fn get_inline(
        &self,
        tenant_id: &str,
        message_id: Uuid,
    ) -> Result<Option<Bytes>, PayloadError> {
        Ok(self
            .inline
            .lock()
            .get(&(tenant_id.to_string(), message_id))
            .cloned())
    }

    async fn put_blob(
        &self,
        tenant_id: &str,
        message_id: Uuid,
        data: Bytes,
    ) -> Result<PayloadRef, PayloadError> {
        let key = format!("{tenant_id}/{message_id}");
        let size = data.len() as u64;
        self.blobs.lock().insert(key.clone(), data);
        Ok(PayloadRef {
            tenant_id: tenant_id.to_string(),
            message_id,
            bucket_key: key,
            size,
            sha256: None,
        })
    }

    async fn open_stream(&self, reference: &PayloadRef) -> Result<Bytes, PayloadError> {
        self.blobs
            .lock()
            .get(&reference.bucket_key)
            .cloned()
            .ok_or_else(|| PayloadError::NotFound(reference.bucket_key.clone()))
    }
}

impl InlinePayloadStore {
    pub fn choose_storage(len: usize) -> &'static str {
        if len <= INLINE_MAX_BYTES {
            "inline"
        } else {
            "blob"
        }
    }
}
