use broker_proto::{join_under_root, sanitize_path_segment};
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
    pub fn key_for(
        tenant_id: &str,
        message_id: Uuid,
    ) -> Result<String, broker_proto::PathSegmentError> {
        let tenant = sanitize_path_segment(tenant_id)?;
        Ok(format!("payloads/{tenant}/{message_id}"))
    }

    pub fn fs_path(
        root: &std::path::Path,
        bucket_key: &str,
    ) -> Result<std::path::PathBuf, broker_proto::PathSegmentError> {
        let parts: Vec<&str> = bucket_key.split('/').collect();
        join_under_root(root, &parts)
    }
}
