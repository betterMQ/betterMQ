use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Atomic durable write: temp → fsync → rename → parent-dir fsync.
pub fn atomic_write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogMeta {
    pub next_offset: u64,
    pub segment_roll_count: u64,
    /// Offsets tombstoned by purge (local log bytes are not rewritten).
    #[serde(default)]
    pub purged_offsets: BTreeSet<u64>,
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
        let bytes = serde_json::to_vec(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        atomic_write_file(&path, &bytes)
    }
}
