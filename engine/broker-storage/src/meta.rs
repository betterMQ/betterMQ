use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogMeta {
    pub next_offset: u64,
    pub segment_roll_count: u64,
}

impl LogMeta {
    pub fn path(partition_dir: &Path) -> PathBuf {
        partition_dir.join("meta.json")
    }

    pub fn load(partition_dir: &Path) -> std::io::Result<Self> {
        let path = Self::path(partition_dir);
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn save(&self, partition_dir: &Path) -> std::io::Result<()> {
        let path = Self::path(partition_dir);
        let tmp = partition_dir.join("meta.json.tmp");
        std::fs::write(&tmp, serde_json::to_vec(self).unwrap())?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}
