//! Recurring schedules: cron expression or fixed interval, file-backed.

use crate::ScheduledPublishRequest;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum CronError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid cron expression: {0}")]
    InvalidExpression(String),
    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),
    #[error("cron job not found: {0}")]
    NotFound(Uuid),
}

/// Either a cron pattern or a fixed interval between runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScheduleKind {
    Cron { cron: String },
    Interval { every_seconds: u64 },
}

impl ScheduleKind {
    pub fn from_cron(expr: impl Into<String>) -> Self {
        Self::Cron { cron: expr.into() }
    }

    pub fn from_interval(every_seconds: u64) -> Result<Self, CronError> {
        if every_seconds == 0 {
            return Err(CronError::InvalidSchedule(
                "every_seconds must be >= 1".into(),
            ));
        }
        Ok(Self::Interval { every_seconds })
    }

    fn label(&self) -> String {
        match self {
            Self::Cron { cron } => cron.clone(),
            Self::Interval { every_seconds } => format!("every {every_seconds}s"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    pub id: Uuid,
    /// Display / legacy: cron expr or `every Ns` for intervals.
    pub cron: String,
    #[serde(default)]
    pub every_seconds: Option<u64>,
    pub paused: bool,
    pub next_run_at_ms: i64,
    pub created_at_ms: i64,
    #[serde(default)]
    pub last_run_at_ms: Option<i64>,
    /// Last catalog mutation time (ms since epoch) for cluster LWW merge (CP6c).
    #[serde(default)]
    pub updated_at_ms: i64,
    pub request: ScheduledPublishRequest,
}

impl CronJob {
    pub fn schedule_kind(&self) -> ScheduleKind {
        if let Some(secs) = self.every_seconds {
            ScheduleKind::Interval {
                every_seconds: secs,
            }
        } else {
            ScheduleKind::Cron {
                cron: self.cron.clone(),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronFile {
    jobs: Vec<CronJob>,
}

#[derive(Clone)]
pub struct CronRegistry {
    path: PathBuf,
    inner: Arc<parking_lot::Mutex<CronFile>>,
}

fn meta_file_path(data_dir: &Path, name: &str) -> PathBuf {
    if let Ok(shared) = std::env::var("BETTERMQ_SHARED_META_DIR") {
        let dir = PathBuf::from(shared);
        let _ = std::fs::create_dir_all(&dir);
        return dir.join(name);
    }
    data_dir.join(name)
}

impl CronRegistry {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, CronError> {
        let path = meta_file_path(data_dir.as_ref(), "crons.json");
        let file = if path.exists() {
            let bytes = std::fs::read(&path)?;
            serde_json::from_slice(&bytes)?
        } else {
            CronFile { jobs: Vec::new() }
        };
        Ok(Self {
            path,
            inner: Arc::new(parking_lot::Mutex::new(file)),
        })
    }

    pub fn create(
        &self,
        cron_expr: &str,
        request: ScheduledPublishRequest,
    ) -> Result<CronJob, CronError> {
        self.create_with_kind(ScheduleKind::from_cron(cron_expr), request)
    }

    pub fn create_with_kind(
        &self,
        kind: ScheduleKind,
        request: ScheduledPublishRequest,
    ) -> Result<CronJob, CronError> {
        let now = Utc::now().timestamp_millis();
        let (cron, every_seconds) = match &kind {
            ScheduleKind::Cron { cron } => {
                let normalized = normalize_cron(cron);
                parse_schedule(&normalized)?;
                (cron.clone(), None)
            }
            ScheduleKind::Interval { every_seconds } => {
                ScheduleKind::from_interval(*every_seconds)?;
                (kind.label(), Some(*every_seconds))
            }
        };

        let next_run_at_ms = next_run_for_job(&cron, every_seconds, now)?;

        let job = CronJob {
            id: Uuid::new_v4(),
            cron,
            every_seconds,
            paused: false,
            next_run_at_ms,
            created_at_ms: now,
            last_run_at_ms: None,
            updated_at_ms: now,
            request,
        };

        let mut file = self.inner.lock();
        file.jobs.push(job.clone());
        drop(file);
        self.persist()?;
        Ok(job)
    }

    pub fn get(&self, id: Uuid) -> Result<CronJob, CronError> {
        let file = self.inner.lock();
        file.jobs
            .iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))
    }

    pub fn list(&self) -> Vec<CronJob> {
        self.inner.lock().jobs.clone()
    }

    pub fn pause(&self, id: Uuid) -> Result<CronJob, CronError> {
        self.set_paused(id, true)
    }

    pub fn resume(&self, id: Uuid) -> Result<CronJob, CronError> {
        let job = self.set_paused(id, false)?;
        let now = Utc::now().timestamp_millis();
        let next = next_run_for_job(&job.cron, job.every_seconds, now)?;
        self.update_next_run(id, next)
    }

    pub fn delete(&self, id: Uuid) -> Result<CronJob, CronError> {
        let mut file = self.inner.lock();
        let pos = file
            .jobs
            .iter()
            .position(|j| j.id == id)
            .ok_or(CronError::NotFound(id))?;
        let removed = file.jobs.remove(pos);
        drop(file);
        self.persist()?;
        Ok(removed)
    }

    /// Insert or replace by `id` (cluster catalog sync, LWW).
    pub fn upsert(&self, mut job: CronJob) -> Result<(), CronError> {
        if job.updated_at_ms == 0 {
            job.updated_at_ms = Utc::now().timestamp_millis();
        }
        let mut file = self.inner.lock();
        if let Some(pos) = file.jobs.iter().position(|j| j.id == job.id) {
            if file.jobs[pos].updated_at_ms > job.updated_at_ms {
                return Ok(());
            }
            file.jobs[pos] = job;
        } else {
            file.jobs.push(job);
        }
        drop(file);
        self.persist()
    }

    /// Jobs that should fire now (not paused, `next_run_at_ms <= now`).
    pub fn pop_due(&self, now_ms: i64) -> Vec<CronJob> {
        let mut file = self.inner.lock();
        let mut due = Vec::new();
        for job in &mut file.jobs {
            if job.paused || job.next_run_at_ms > now_ms {
                continue;
            }
            due.push(job.clone());
            job.last_run_at_ms = Some(now_ms);
            if let Ok(next) = next_run_for_job(&job.cron, job.every_seconds, now_ms) {
                job.next_run_at_ms = next;
            }
        }
        drop(file);
        if !due.is_empty() {
            let _ = self.persist();
        }
        due
    }

    fn set_paused(&self, id: Uuid, paused: bool) -> Result<CronJob, CronError> {
        let mut file = self.inner.lock();
        let job = file
            .jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or(CronError::NotFound(id))?;
        job.paused = paused;
        let out = job.clone();
        drop(file);
        self.persist()?;
        Ok(out)
    }

    fn update_next_run(&self, id: Uuid, next_run_at_ms: i64) -> Result<CronJob, CronError> {
        let mut file = self.inner.lock();
        let job = file
            .jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or(CronError::NotFound(id))?;
        job.next_run_at_ms = next_run_at_ms;
        let out = job.clone();
        drop(file);
        self.persist()?;
        Ok(out)
    }

    fn persist(&self) -> Result<(), CronError> {
        let file = self.inner.lock();
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&*file)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }
}

fn next_run_for_job(
    cron: &str,
    every_seconds: Option<u64>,
    after_ms: i64,
) -> Result<i64, CronError> {
    if let Some(secs) = every_seconds {
        let step_ms = (secs as i64).saturating_mul(1000);
        return Ok(after_ms.saturating_add(step_ms));
    }
    next_run_after_cron(cron, after_ms)
}

/// Accept 5-field (`min hour dom month dow`) or 6-field (`sec min hour dom month dow`) cron.
pub fn normalize_cron(expr: &str) -> String {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    match parts.len() {
        5 => format!(
            "0 {} {} {} {} {}",
            parts[0], parts[1], parts[2], parts[3], parts[4]
        ),
        _ => expr.to_string(),
    }
}

fn parse_schedule(expr: &str) -> Result<Schedule, CronError> {
    Schedule::from_str(expr).map_err(|e| CronError::InvalidExpression(e.to_string()))
}

fn next_run_after_cron(expr: &str, after_ms: i64) -> Result<i64, CronError> {
    let normalized = normalize_cron(expr);
    let schedule = parse_schedule(&normalized)?;
    let after: DateTime<Utc> = DateTime::from_timestamp_millis(after_ms).unwrap_or_else(Utc::now);
    schedule
        .after(&after)
        .next()
        .map(|dt| dt.timestamp_millis())
        .ok_or_else(|| CronError::InvalidExpression("no upcoming run".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    fn sample_request() -> ScheduledPublishRequest {
        ScheduledPublishRequest {
            topic: "q".into(),
            routing_key: "k".into(),
            payload: "x".into(),
            payload_encoding: None,
            idempotency_key: None,
            priority: None,
            parallelism: None,
            flow_id: None,
            queue_id: None,
            flow: None,
            destination: None,
            max_retries: None,
            retry_backoff: None,
            method: None,
            headers: None,
            sign: None,
            request: None,
        }
    }

    #[test]
    fn create_pause_resume_delete() {
        let dir = temp_dir().join(format!("bettermq-cron-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg.create("0 9 * * *", sample_request()).unwrap();
        assert!(!job.paused);
        assert!(job.every_seconds.is_none());
        reg.pause(job.id).unwrap();
        assert!(reg.get(job.id).unwrap().paused);
        reg.resume(job.id).unwrap();
        assert!(!reg.get(job.id).unwrap().paused);
        reg.delete(job.id).unwrap();
        assert!(reg.get(job.id).is_err());
    }

    #[test]
    fn interval_schedule_advances() {
        let dir = temp_dir().join(format!("bettermq-interval-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        assert_eq!(job.every_seconds, Some(10));
        assert_eq!(job.cron, "every 10s");

        let now = job.created_at_ms;
        let due = reg.pop_due(now + 10_000);
        assert_eq!(due.len(), 1);
        let updated = reg.get(job.id).unwrap();
        assert_eq!(updated.next_run_at_ms, now + 20_000);
    }
}
