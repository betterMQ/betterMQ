//! Fan-out groups: one publish → many webhook destinations with per-member flow limits.

use crate::catalog_journal::CatalogJournal;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum GroupError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("group not found: {0}")]
    GroupNotFound(Uuid),
    #[error("member not found: {0}")]
    MemberNotFound(Uuid),
    #[error("group has no active members")]
    NoActiveMembers,
    #[error("duplicate group name: {0}")]
    DuplicateName(String),
    #[error("unsupported group catalog version: {0}")]
    UnsupportedVersion(u16),
    #[error("group catalog journal: {0}")]
    Journal(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DispatchGroup {
    pub id: Uuid,
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupMember {
    pub id: Uuid,
    pub group_id: Uuid,
    pub tenant_id: String,
    pub name: String,
    pub url: String,
    pub secret: String,
    #[serde(default)]
    pub paused: bool,
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
    #[serde(default)]
    pub rate: u32,
    #[serde(default = "default_period_secs")]
    pub period_secs: u64,
    /// Optional fixed flow key for this member (defaults to message routing key).
    #[serde(default)]
    pub flow_key: Option<String>,
    #[serde(default)]
    pub updated_at_ms: i64,
}

fn default_parallelism() -> u32 {
    1
}

fn default_period_secs() -> u64 {
    60
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct GroupFile {
    #[serde(default)]
    version: u16,
    #[serde(default)]
    revision: u64,
    #[serde(default)]
    groups: Vec<DispatchGroup>,
    #[serde(default)]
    members: Vec<GroupMember>,
}

const GROUP_CATALOG_VERSION: u16 = 1;
const GROUP_COMPACT_OPS: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum GroupCommand {
    Patch {
        group_upserts: Vec<DispatchGroup>,
        group_deletes: Vec<Uuid>,
        member_upserts: Vec<GroupMember>,
        member_deletes: Vec<Uuid>,
    },
}

fn apply_command(file: &mut GroupFile, command: GroupCommand) {
    match command {
        GroupCommand::Patch {
            group_upserts,
            group_deletes,
            member_upserts,
            member_deletes,
        } => {
            file.groups
                .retain(|group| !group_deletes.contains(&group.id));
            file.members
                .retain(|member| !member_deletes.contains(&member.id));
            for group in group_upserts {
                if let Some(slot) = file
                    .groups
                    .iter_mut()
                    .find(|existing| existing.id == group.id)
                {
                    *slot = group;
                } else {
                    file.groups.push(group);
                }
            }
            for member in member_upserts {
                if let Some(slot) = file
                    .members
                    .iter_mut()
                    .find(|existing| existing.id == member.id)
                {
                    *slot = member;
                } else {
                    file.members.push(member);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct GroupRegistry {
    path: PathBuf,
    journal: CatalogJournal,
}

fn meta_file_path(data_dir: &Path, name: &str) -> PathBuf {
    if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
        let dir = PathBuf::from(shared);
        let _ = std::fs::create_dir_all(&dir);
        return dir.join(name);
    }
    data_dir.join(name)
}

impl GroupRegistry {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, GroupError> {
        let path = meta_file_path(data_dir.as_ref(), "groups.json");
        if !path.exists() {
            let file = GroupFile {
                version: GROUP_CATALOG_VERSION,
                ..GroupFile::default()
            };
            std::fs::write(&path, serde_json::to_vec_pretty(&file)?)?;
        }
        let journal = CatalogJournal::new(&path, GROUP_CATALOG_VERSION);
        Ok(Self { path, journal })
    }

    fn load(&self) -> Result<GroupFile, GroupError> {
        self.load_state().map(|state| state.0)
    }

    fn load_state(&self) -> Result<(GroupFile, usize), GroupError> {
        let bytes = std::fs::read(&self.path)?;
        match serde_json::from_slice::<GroupFile>(&bytes) {
            Ok(mut file) => {
                if file.version != 0 && file.version != GROUP_CATALOG_VERSION {
                    return Err(GroupError::UnsupportedVersion(file.version));
                }
                file.version = GROUP_CATALOG_VERSION;
                let (revision, operations) = self
                    .journal
                    .replay(file.revision, |command| apply_command(&mut file, command))
                    .map_err(|error| GroupError::Journal(error.to_string()))?;
                file.revision = revision;
                Ok((file, operations))
            }
            Err(_) if broker_proto::allow_empty_metadata_recovery() => {
                let mut file = GroupFile {
                    version: GROUP_CATALOG_VERSION,
                    ..GroupFile::default()
                };
                let (revision, operations) = self
                    .journal
                    .replay(0, |command| apply_command(&mut file, command))
                    .map_err(|error| GroupError::Journal(error.to_string()))?;
                file.revision = revision;
                Ok((file, operations))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn mutate<T>(
        &self,
        f: impl FnOnce(&mut GroupFile) -> Result<T, GroupError>,
    ) -> Result<T, GroupError> {
        let _lock = broker_storage::FileLock::exclusive(&self.path)?;
        let (mut file, journal_ops) = self.load_state()?;
        let before_groups = file.groups.clone();
        let before_members = file.members.clone();
        let out = f(&mut file)?;
        let before_groups: std::collections::HashMap<_, _> = before_groups
            .into_iter()
            .map(|item| (item.id, item))
            .collect();
        let after_groups: std::collections::HashMap<_, _> = file
            .groups
            .iter()
            .cloned()
            .map(|item| (item.id, item))
            .collect();
        let before_members: std::collections::HashMap<_, _> = before_members
            .into_iter()
            .map(|item| (item.id, item))
            .collect();
        let after_members: std::collections::HashMap<_, _> = file
            .members
            .iter()
            .cloned()
            .map(|item| (item.id, item))
            .collect();
        let group_upserts: Vec<_> = after_groups
            .iter()
            .filter(|(id, item)| before_groups.get(id) != Some(*item))
            .map(|(_, item)| item.clone())
            .collect();
        let group_deletes: Vec<_> = before_groups
            .keys()
            .filter(|id| !after_groups.contains_key(id))
            .copied()
            .collect();
        let member_upserts: Vec<_> = after_members
            .iter()
            .filter(|(id, item)| before_members.get(id) != Some(*item))
            .map(|(_, item)| item.clone())
            .collect();
        let member_deletes: Vec<_> = before_members
            .keys()
            .filter(|id| !after_members.contains_key(id))
            .copied()
            .collect();
        if group_upserts.is_empty()
            && group_deletes.is_empty()
            && member_upserts.is_empty()
            && member_deletes.is_empty()
        {
            return Ok(out);
        }
        let revision = file
            .revision
            .checked_add(1)
            .ok_or_else(|| GroupError::Journal("revision exhausted".into()))?;
        self.journal
            .append(
                revision,
                &GroupCommand::Patch {
                    group_upserts,
                    group_deletes,
                    member_upserts,
                    member_deletes,
                },
            )
            .map_err(|error| GroupError::Journal(error.to_string()))?;
        file.revision = revision;
        if journal_ops.saturating_add(1) >= GROUP_COMPACT_OPS {
            self.journal
                .compact(&self.path, &file)
                .map_err(|error| GroupError::Journal(error.to_string()))?;
        }
        Ok(out)
    }

    pub fn create_group(&self, tenant_id: &str, name: String) -> Result<DispatchGroup, GroupError> {
        self.mutate(|file| {
            if file
                .groups
                .iter()
                .any(|g| g.tenant_id == tenant_id && g.name == name)
            {
                return Err(GroupError::DuplicateName(name.clone()));
            }
            let group = DispatchGroup {
                id: Uuid::new_v4(),
                tenant_id: tenant_id.to_string(),
                name,
                paused: false,
                updated_at_ms: Utc::now().timestamp_millis(),
            };
            file.groups.push(group.clone());
            Ok(group)
        })
    }

    pub fn upsert_group(&self, mut group: DispatchGroup) -> Result<(), GroupError> {
        if group.updated_at_ms == 0 {
            group.updated_at_ms = Utc::now().timestamp_millis();
        }
        self.mutate(|file| {
            if let Some(pos) = file
                .groups
                .iter()
                .position(|g| g.tenant_id == group.tenant_id && g.id == group.id)
            {
                if file.groups[pos].updated_at_ms > group.updated_at_ms {
                    return Ok(());
                }
                file.groups[pos] = group;
            } else {
                file.groups.push(group);
            }
            Ok(())
        })
    }

    pub fn get_group(
        &self,
        tenant_id: &str,
        id: Uuid,
    ) -> Result<Option<DispatchGroup>, GroupError> {
        let file = self.load()?;
        Ok(file
            .groups
            .into_iter()
            .find(|g| g.tenant_id == tenant_id && g.id == id))
    }

    pub fn get_group_by_name(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<DispatchGroup>, GroupError> {
        let file = self.load()?;
        Ok(file
            .groups
            .into_iter()
            .find(|g| g.tenant_id == tenant_id && g.name == name))
    }

    pub fn list_groups(&self, tenant_id: &str) -> Result<Vec<DispatchGroup>, GroupError> {
        let file = self.load()?;
        Ok(file
            .groups
            .into_iter()
            .filter(|g| g.tenant_id == tenant_id)
            .collect())
    }

    pub fn delete_group(&self, tenant_id: &str, id: Uuid) -> Result<DispatchGroup, GroupError> {
        self.mutate(|file| {
            let pos = file
                .groups
                .iter()
                .position(|g| g.tenant_id == tenant_id && g.id == id)
                .ok_or(GroupError::GroupNotFound(id))?;
            let removed = file.groups.remove(pos);
            file.members.retain(|m| m.group_id != id);
            Ok(removed)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_member(
        &self,
        tenant_id: &str,
        group_id: Uuid,
        name: String,
        url: String,
        secret: String,
        parallelism: u32,
        rate: u32,
        period_secs: u64,
        flow_key: Option<String>,
    ) -> Result<GroupMember, GroupError> {
        self.mutate(|file| {
            if !file
                .groups
                .iter()
                .any(|g| g.tenant_id == tenant_id && g.id == group_id)
            {
                return Err(GroupError::GroupNotFound(group_id));
            }
            let member = GroupMember {
                id: Uuid::new_v4(),
                group_id,
                tenant_id: tenant_id.to_string(),
                name,
                url,
                secret,
                paused: false,
                parallelism: parallelism.max(1),
                rate,
                period_secs: period_secs.max(1),
                flow_key,
                updated_at_ms: Utc::now().timestamp_millis(),
            };
            file.members.push(member.clone());
            Ok(member)
        })
    }

    pub fn upsert_member(&self, mut member: GroupMember) -> Result<(), GroupError> {
        if member.updated_at_ms == 0 {
            member.updated_at_ms = Utc::now().timestamp_millis();
        }
        self.mutate(|file| {
            if let Some(pos) = file
                .members
                .iter()
                .position(|m| m.tenant_id == member.tenant_id && m.id == member.id)
            {
                if file.members[pos].updated_at_ms > member.updated_at_ms {
                    return Ok(());
                }
                file.members[pos] = member;
            } else {
                file.members.push(member);
            }
            Ok(())
        })
    }

    pub fn get_member(&self, tenant_id: &str, id: Uuid) -> Result<Option<GroupMember>, GroupError> {
        let file = self.load()?;
        Ok(file
            .members
            .into_iter()
            .find(|m| m.tenant_id == tenant_id && m.id == id))
    }

    pub fn list_members(
        &self,
        tenant_id: &str,
        group_id: Uuid,
    ) -> Result<Vec<GroupMember>, GroupError> {
        let file = self.load()?;
        Ok(file
            .members
            .into_iter()
            .filter(|m| m.tenant_id == tenant_id && m.group_id == group_id)
            .collect())
    }

    pub fn list_all_members(&self, tenant_id: &str) -> Result<Vec<GroupMember>, GroupError> {
        let file = self.load()?;
        Ok(file
            .members
            .into_iter()
            .filter(|m| m.tenant_id == tenant_id)
            .collect())
    }

    pub fn delete_member(&self, tenant_id: &str, id: Uuid) -> Result<GroupMember, GroupError> {
        self.mutate(|file| {
            let pos = file
                .members
                .iter()
                .position(|m| m.tenant_id == tenant_id && m.id == id)
                .ok_or(GroupError::MemberNotFound(id))?;
            Ok(file.members.remove(pos))
        })
    }

    pub fn active_members(
        &self,
        tenant_id: &str,
        group_id: Uuid,
    ) -> Result<Vec<GroupMember>, GroupError> {
        let file = self.load()?;
        let group = file
            .groups
            .iter()
            .find(|g| g.tenant_id == tenant_id && g.id == group_id);
        if group.is_none() {
            return Err(GroupError::GroupNotFound(group_id));
        }
        if group.map(|g| g.paused).unwrap_or(false) {
            return Ok(Vec::new());
        }
        Ok(file
            .members
            .into_iter()
            .filter(|m| m.tenant_id == tenant_id && m.group_id == group_id && !m.paused)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_lifecycle_survives_restart_and_newer_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let registry = GroupRegistry::open(dir.path()).unwrap();
        let group = registry.create_group("tenant", "alerts".into()).unwrap();
        let member = registry
            .add_member(
                "tenant",
                group.id,
                "primary".into(),
                "https://example.com/hook".into(),
                "secret".into(),
                1,
                0,
                60,
                None,
            )
            .unwrap();
        drop(registry);
        let reopened = GroupRegistry::open(dir.path()).unwrap();
        assert!(reopened.get_group("tenant", group.id).unwrap().is_some());
        assert!(reopened.get_member("tenant", member.id).unwrap().is_some());

        std::fs::write(
            dir.path().join("groups.json"),
            br#"{"version":99,"revision":2,"groups":[],"members":[]}"#,
        )
        .unwrap();
        assert!(matches!(
            reopened.load(),
            Err(GroupError::UnsupportedVersion(99))
        ));
    }
}
