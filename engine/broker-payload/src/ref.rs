use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PayloadRef {
    pub tenant_id: String,
    pub message_id: Uuid,
    pub bucket_key: String,
    pub size: u64,
    #[serde(default)]
    pub sha256: Option<String>,
}

impl PayloadRef {
    pub fn key_for(tenant_id: &str, message_id: Uuid) -> String {
        format!("payloads/{tenant_id}/{message_id}")
    }
}
