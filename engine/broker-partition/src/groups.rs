//! Fan-out groups: one publish → many webhook destinations with per-member flow limits.

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchGroup {
    pub id: Uuid,
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    groups: Vec<DispatchGroup>,
    #[serde(default)]
    members: Vec<GroupMember>,
}

#[derive(Clone)]
pub struct GroupRegistry {
    path: PathBuf,
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
            let file = GroupFile::default();
            std::fs::write(&path, serde_json::to_vec_pretty(&file)?)?;
        }
        Ok(Self { path })
    }

    fn load(&self) -> Result<GroupFile, GroupError> {
        let bytes = std::fs::read(&self.path)?;
        Ok(serde_json::from_slice(&bytes).unwrap_or_default())
    }

    fn save(&self, file: &GroupFile) -> Result<(), GroupError> {
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(file)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }

    pub fn create_group(&self, tenant_id: &str, name: String) -> Result<DispatchGroup, GroupError> {
        let mut file = self.load()?;
        if file
            .groups
            .iter()
            .any(|g| g.tenant_id == tenant_id && g.name == name)
        {
            return Err(GroupError::DuplicateName(name));
        }
        let group = DispatchGroup {
            id: Uuid::new_v4(),
            tenant_id: tenant_id.to_string(),
            name,
            paused: false,
            updated_at_ms: Utc::now().timestamp_millis(),
        };
        file.groups.push(group.clone());
        self.save(&file)?;
        Ok(group)
    }

    pub fn upsert_group(&self, mut group: DispatchGroup) -> Result<(), GroupError> {
        if group.updated_at_ms == 0 {
            group.updated_at_ms = Utc::now().timestamp_millis();
        }
        let mut file = self.load()?;
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
        self.save(&file)
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
        let mut file = self.load()?;
        let pos = file
            .groups
            .iter()
            .position(|g| g.tenant_id == tenant_id && g.id == id)
            .ok_or(GroupError::GroupNotFound(id))?;
        let removed = file.groups.remove(pos);
        file.members.retain(|m| m.group_id != id);
        self.save(&file)?;
        Ok(removed)
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
        let mut file = self.load()?;
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
        self.save(&file)?;
        Ok(member)
    }

    pub fn upsert_member(&self, mut member: GroupMember) -> Result<(), GroupError> {
        if member.updated_at_ms == 0 {
            member.updated_at_ms = Utc::now().timestamp_millis();
        }
        let mut file = self.load()?;
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
        self.save(&file)
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
        let mut file = self.load()?;
        let pos = file
            .members
            .iter()
            .position(|m| m.tenant_id == tenant_id && m.id == id)
            .ok_or(GroupError::MemberNotFound(id))?;
        let removed = file.members.remove(pos);
        self.save(&file)?;
        Ok(removed)
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
