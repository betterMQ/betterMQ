use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum CatalogJournalError {
    #[error("catalog journal io: {0}")]
    Io(#[from] std::io::Error),
    #[error("catalog journal serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("unsupported catalog journal version {saw}, expected {expected}")]
    UnsupportedVersion { saw: u16, expected: u16 },
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalRecord<C> {
    version: u16,
    sequence: u64,
    command: C,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogJournal {
    path: PathBuf,
    version: u16,
}

impl CatalogJournal {
    pub(crate) fn new(snapshot_path: &Path, version: u16) -> Self {
        Self {
            path: snapshot_path.with_extension("journal"),
            version,
        }
    }

    pub(crate) fn replay<C>(
        &self,
        snapshot_sequence: u64,
        mut apply: impl FnMut(C),
    ) -> Result<(u64, usize), CatalogJournalError>
    where
        C: DeserializeOwned,
    {
        if !self.path.exists() {
            return Ok((snapshot_sequence, 0));
        }
        let bytes = std::fs::read(&self.path)?;
        let lines: Vec<&[u8]> = bytes.split(|byte| *byte == b'\n').collect();
        let mut sequence = snapshot_sequence;
        let mut operations = 0usize;
        for (index, line) in lines.iter().enumerate() {
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            let record = match serde_json::from_slice::<JournalRecord<C>>(line) {
                Ok(record) => record,
                Err(error) if index + 1 == lines.len() => {
                    tracing::warn!(%error, path = %self.path.display(), "ignoring torn catalog journal tail");
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            if record.version != self.version {
                return Err(CatalogJournalError::UnsupportedVersion {
                    saw: record.version,
                    expected: self.version,
                });
            }
            if record.sequence <= sequence {
                continue;
            }
            sequence = record.sequence;
            operations += 1;
            apply(record.command);
        }
        Ok((sequence, operations))
    }

    pub(crate) fn append<C>(&self, sequence: u64, command: &C) -> Result<(), CatalogJournalError>
    where
        C: Serialize,
    {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        serde_json::to_writer(
            &mut file,
            &JournalRecord {
                version: self.version,
                sequence,
                command,
            },
        )?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        broker_storage::set_secret_file_mode(&self.path);
        Ok(())
    }

    pub(crate) fn compact<T>(
        &self,
        snapshot_path: &Path,
        snapshot: &T,
    ) -> Result<(), CatalogJournalError>
    where
        T: Serialize,
    {
        let bytes = serde_json::to_vec_pretty(snapshot)?;
        broker_storage::atomic_write_file(snapshot_path, &bytes)?;
        broker_storage::set_secret_file_mode(snapshot_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct Command(u64);

    #[test]
    fn replay_skips_snapshot_covered_records_and_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("catalog.json");
        let journal = CatalogJournal::new(&snapshot, 1);
        journal.append(1, &Command(1)).unwrap();
        journal.append(2, &Command(2)).unwrap();
        let mut file = OpenOptions::new()
            .append(true)
            .open(snapshot.with_extension("journal"))
            .unwrap();
        file.write_all(br#"{"version":1"#).unwrap();
        file.sync_data().unwrap();

        let mut applied = Vec::new();
        let (sequence, operations) = journal
            .replay(1, |command: Command| applied.push(command.0))
            .unwrap();
        assert_eq!(sequence, 2);
        assert_eq!(operations, 1);
        assert_eq!(applied, vec![2]);
    }
}
