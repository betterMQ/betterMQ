//! Tombstones for catalog deletes (CP6c) — prevents resurrection on union-merge.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CatalogKind {
    Flow,
    Queue,
    Group,
    GroupMember,
    Cron,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogTombstone {
    pub id: Uuid,
    pub kind: CatalogKind,
    pub deleted_at_ms: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TombstoneFile {
    #[serde(default)]
    tombstones: Vec<CatalogTombstone>,
}

#[derive(Clone)]
pub struct CatalogTombstones {
    path: PathBuf,
}

impl CatalogTombstones {
    pub fn open(data_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = data_dir.as_ref().join("catalog-tombstones.json");
        if !path.exists() {
            let file = TombstoneFile::default();
            std::fs::write(&path, serde_json::to_vec_pretty(&file)?)?;
        }
        Ok(Self { path })
    }

    fn load(&self) -> std::io::Result<TombstoneFile> {
        let bytes = std::fs::read(&self.path)?;
        Ok(serde_json::from_slice(&bytes).unwrap_or_default())
    }

    fn save(&self, file: &TombstoneFile) -> std::io::Result<()> {
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(file)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }

    pub fn record(&self, id: Uuid, kind: CatalogKind) -> std::io::Result<()> {
        let mut file = self.load()?;
        let now = Utc::now().timestamp_millis();
        if let Some(existing) = file.tombstones.iter_mut().find(|t| t.id == id) {
            existing.deleted_at_ms = now;
            existing.kind = kind;
        } else {
            file.tombstones.push(CatalogTombstone {
                id,
                kind,
                deleted_at_ms: now,
            });
        }
        self.save(&file)
    }

    pub fn is_deleted(&self, id: Uuid) -> bool {
        self.load()
            .map(|f| f.tombstones.iter().any(|t| t.id == id))
            .unwrap_or(false)
    }

    pub fn deleted_at(&self, id: Uuid) -> Option<i64> {
        self.load()
            .ok()?
            .tombstones
            .iter()
            .find(|t| t.id == id)
            .map(|t| t.deleted_at_ms)
    }

    pub fn snapshot(&self) -> HashMap<Uuid, i64> {
        self.load()
            .map(|f| {
                f.tombstones
                    .into_iter()
                    .map(|t| (t.id, t.deleted_at_ms))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn merge_remote(&self, remote: &HashMap<Uuid, i64>) -> std::io::Result<()> {
        if remote.is_empty() {
            return Ok(());
        }
        let mut file = self.load()?;
        let mut changed = false;
        for (id, deleted_at_ms) in remote {
            if let Some(existing) = file.tombstones.iter_mut().find(|t| t.id == *id) {
                if *deleted_at_ms > existing.deleted_at_ms {
                    existing.deleted_at_ms = *deleted_at_ms;
                    changed = true;
                }
            } else {
                file.tombstones.push(CatalogTombstone {
                    id: *id,
                    kind: CatalogKind::Queue,
                    deleted_at_ms: *deleted_at_ms,
                });
                changed = true;
            }
        }
        if changed {
            self.save(&file)?;
        }
        Ok(())
    }
}
