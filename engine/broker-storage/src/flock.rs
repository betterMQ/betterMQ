//! Exclusive file locks for shared-meta CAS updates (HA M2).

use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::Path;

/// RAII exclusive lock on `{path}.lock` (created if missing).
pub struct FileLock {
    file: File,
}

impl FileLock {
    pub fn exclusive(path: &Path) -> std::io::Result<Self> {
        let lock_path = Path::new(&format!("{}.lock", path.display())).to_path_buf();
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        file.lock_exclusive()?;
        Ok(Self { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}
