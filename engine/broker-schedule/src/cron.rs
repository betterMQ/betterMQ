//! Recurring schedules: cron expression or fixed interval, file-backed.

use crate::persist::{
    load_json_with_recovery, persist_json_atomic, JsonLoadSource, MetadataLoadError,
};
use crate::ScheduledPublishRequest;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;
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
    #[error(transparent)]
    MetadataLoad(#[from] MetadataLoadError),
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
    #[serde(default)]
    version: u16,
    #[serde(default)]
    sequence: u64,
    jobs: Vec<CronJob>,
}

#[derive(Clone)]
pub struct CronRegistry {
    path: PathBuf,
    journal_path: PathBuf,
    inner: Arc<parking_lot::Mutex<CronState>>,
}

struct CronState {
    file: CronFile,
    sequence: u64,
    journal_ops: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Persisted commands are infrequent and mirror the snapshot schema.
enum CronCommand {
    Upsert { job: CronJob },
    Delete { id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronJournalRecord {
    version: u16,
    sequence: u64,
    command: CronCommand,
}

const CRON_JOURNAL_VERSION: u16 = 1;
const CRON_COMPACT_OPS: usize = 1024;

fn apply_command(file: &mut CronFile, command: CronCommand) {
    match command {
        CronCommand::Upsert { job } => {
            if let Some(slot) = file.jobs.iter_mut().find(|existing| existing.id == job.id) {
                *slot = job;
            } else {
                file.jobs.push(job);
            }
        }
        CronCommand::Delete { id } => file.jobs.retain(|job| job.id != id),
    }
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
        let journal_path = path.with_extension("journal");
        let mut loaded = load_json_with_recovery(&path, || CronFile {
            version: CRON_JOURNAL_VERSION,
            sequence: 0,
            jobs: Vec::new(),
        })?;
        if !matches!(
            loaded.source,
            JsonLoadSource::Main | JsonLoadSource::Missing
        ) {
            info!(
                file = %path.display(),
                source = ?loaded.source,
                count = loaded.value.jobs.len(),
                "cron registry recovered after metadata read failure"
            );
        }
        if loaded.value.version != 0 && loaded.value.version != CRON_JOURNAL_VERSION {
            return Err(CronError::InvalidSchedule(format!(
                "unsupported cron snapshot version {}",
                loaded.value.version
            )));
        }
        loaded.value.version = CRON_JOURNAL_VERSION;
        let mut sequence = loaded.value.sequence;
        let mut journal_ops = 0usize;
        if journal_path.exists() {
            let bytes = std::fs::read(&journal_path)?;
            let lines: Vec<&[u8]> = bytes.split(|b| *b == b'\n').collect();
            for (index, line) in lines.iter().enumerate() {
                if line.iter().all(|b| b.is_ascii_whitespace()) {
                    continue;
                }
                let record = match serde_json::from_slice::<CronJournalRecord>(line) {
                    Ok(record) => record,
                    Err(error) if index + 1 == lines.len() => {
                        tracing::warn!(%error, "ignoring torn cron journal tail");
                        break;
                    }
                    Err(error) => return Err(CronError::Serde(error)),
                };
                if record.version != CRON_JOURNAL_VERSION {
                    return Err(CronError::InvalidSchedule(format!(
                        "unsupported cron journal version {}",
                        record.version
                    )));
                }
                if record.sequence <= sequence {
                    continue;
                }
                sequence = sequence.max(record.sequence);
                journal_ops += 1;
                apply_command(&mut loaded.value, record.command);
            }
        }
        loaded.value.sequence = sequence;
        Ok(Self {
            path,
            journal_path,
            inner: Arc::new(parking_lot::Mutex::new(CronState {
                file: loaded.value,
                sequence,
                journal_ops,
            })),
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
                parse_schedule(&cron_fields_for_parser(cron)?)?;
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

        let mut state = self.inner.lock();
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job: job.clone() });
        self.maybe_compact(&mut state);
        Ok(job)
    }

    pub fn get(&self, id: Uuid) -> Result<CronJob, CronError> {
        let state = self.inner.lock();
        state
            .file
            .jobs
            .iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))
    }

    pub fn list(&self) -> Vec<CronJob> {
        self.inner.lock().file.jobs.clone()
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
        let mut state = self.inner.lock();
        let pos = state
            .file
            .jobs
            .iter()
            .position(|j| j.id == id)
            .ok_or(CronError::NotFound(id))?;
        let removed = state.file.jobs[pos].clone();
        self.append_command(&mut state, CronCommand::Delete { id })?;
        apply_command(&mut state.file, CronCommand::Delete { id });
        self.maybe_compact(&mut state);
        Ok(removed)
    }

    /// Insert or replace by `id` (cluster catalog sync, LWW).
    pub fn upsert(&self, mut job: CronJob) -> Result<(), CronError> {
        if job.updated_at_ms == 0 {
            job.updated_at_ms = Utc::now().timestamp_millis();
        }
        let mut state = self.inner.lock();
        if let Some(pos) = state.file.jobs.iter().position(|j| j.id == job.id) {
            if state.file.jobs[pos].updated_at_ms > job.updated_at_ms {
                return Ok(());
            }
        }
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job });
        self.maybe_compact(&mut state);
        Ok(())
    }

    /// Jobs due now — advances next_run only in memory. Call [`Self::commit_fire`]
    /// after successful publish, or [`Self::revert_fire`] on failure.
    pub fn pop_due(&self, now_ms: i64) -> Vec<CronJob> {
        let mut state = self.inner.lock();
        let mut due = Vec::new();
        for job in &mut state.file.jobs {
            if job.paused || job.next_run_at_ms > now_ms {
                continue;
            }
            let previous = job.clone();
            job.last_run_at_ms = Some(now_ms);
            if let Ok(next) = next_run_for_job(&job.cron, job.every_seconds, now_ms) {
                job.next_run_at_ms = next;
            }
            // Return the pre-advance snapshot stamped with fire time for idempotency.
            let mut fired = previous;
            fired.last_run_at_ms = Some(now_ms);
            due.push(fired);
        }
        // Do not persist here — commit_fire / revert_fire owns durability.
        due
    }

    /// Persist advanced next_run after a successful cron publish.
    pub fn commit_fire(&self, id: Uuid) -> Result<(), CronError> {
        let mut state = self.inner.lock();
        let job = state
            .file
            .jobs
            .iter()
            .find(|job| job.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))?;
        self.append_command(&mut state, CronCommand::Upsert { job })?;
        self.maybe_compact(&mut state);
        Ok(())
    }

    /// Revert next_run after a failed publish so the tick retries.
    pub fn revert_fire(&self, job: &CronJob) -> Result<(), CronError> {
        let mut state = self.inner.lock();
        let mut restored = state
            .file
            .jobs
            .iter()
            .find(|existing| existing.id == job.id)
            .cloned()
            .ok_or(CronError::NotFound(job.id))?;
        restored.next_run_at_ms = job.next_run_at_ms;
        restored.last_run_at_ms = job.last_run_at_ms;
        self.append_command(
            &mut state,
            CronCommand::Upsert {
                job: restored.clone(),
            },
        )?;
        apply_command(&mut state.file, CronCommand::Upsert { job: restored });
        self.maybe_compact(&mut state);
        Ok(())
    }

    fn set_paused(&self, id: Uuid, paused: bool) -> Result<CronJob, CronError> {
        let mut state = self.inner.lock();
        let mut job = state
            .file
            .jobs
            .iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))?;
        job.paused = paused;
        let out = job.clone();
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job });
        self.maybe_compact(&mut state);
        Ok(out)
    }

    fn update_next_run(&self, id: Uuid, next_run_at_ms: i64) -> Result<CronJob, CronError> {
        let mut state = self.inner.lock();
        let mut job = state
            .file
            .jobs
            .iter()
            .find(|j| j.id == id)
            .cloned()
            .ok_or(CronError::NotFound(id))?;
        job.next_run_at_ms = next_run_at_ms;
        let out = job.clone();
        self.append_command(&mut state, CronCommand::Upsert { job: job.clone() })?;
        apply_command(&mut state.file, CronCommand::Upsert { job });
        self.maybe_compact(&mut state);
        Ok(out)
    }

    fn append_command(&self, state: &mut CronState, command: CronCommand) -> Result<(), CronError> {
        let sequence = state.sequence.saturating_add(1);
        let record = CronJournalRecord {
            version: CRON_JOURNAL_VERSION,
            sequence,
            command,
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        serde_json::to_writer(&mut file, &record)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        broker_storage::set_secret_file_mode(&self.journal_path);
        state.sequence = sequence;
        state.file.sequence = sequence;
        state.journal_ops += 1;
        Ok(())
    }

    fn maybe_compact(&self, state: &mut CronState) {
        if state.journal_ops < CRON_COMPACT_OPS {
            return;
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&state.file) else {
            return;
        };
        if persist_json_atomic(&self.path, &bytes).is_err() {
            return;
        }
        broker_storage::set_secret_file_mode(&self.path);
        match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.journal_path)
            .and_then(|file| file.sync_data())
        {
            Ok(()) => state.journal_ops = 0,
            Err(error) => tracing::warn!(%error, "cron journal compaction failed"),
        }
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

/// Split an optional `CRON_TZ=` / `TZ=` prefix from a crontab expression.
/// Default timezone is UTC.
fn split_cron_timezone(expr: &str) -> Result<(chrono_tz::Tz, &str), CronError> {
    let expr = expr.trim();
    let rest = if let Some(rest) = expr
        .strip_prefix("CRON_TZ=")
        .or_else(|| expr.strip_prefix("cron_tz="))
    {
        rest
    } else if let Some(rest) = expr
        .strip_prefix("TZ=")
        .or_else(|| expr.strip_prefix("tz="))
    {
        rest
    } else {
        return Ok((chrono_tz::Tz::UTC, expr));
    };
    let (name, fields) = rest.split_once(char::is_whitespace).ok_or_else(|| {
        CronError::InvalidExpression(
            "CRON_TZ requires a timezone and a cron expression, e.g. CRON_TZ=America/New_York */5 * * * *".into(),
        )
    })?;
    let tz = name.parse::<chrono_tz::Tz>().map_err(|_| {
        CronError::InvalidExpression(format!(
            "unknown timezone {name}; use an IANA name like America/New_York (default is UTC)"
        ))
    })?;
    let fields = fields.trim();
    if fields.is_empty() {
        return Err(CronError::InvalidExpression(
            "cron expression missing after timezone".into(),
        ));
    }
    Ok((tz, fields))
}

/// Accept 5-field (`min hour dom month dow`) or 6-field (`sec min hour dom month dow`) cron.
/// Strips an optional `CRON_TZ=` prefix first.
pub fn normalize_cron(expr: &str) -> String {
    let fields = split_cron_timezone(expr)
        .map(|(_, fields)| fields.to_string())
        .unwrap_or_else(|_| expr.trim().to_string());
    normalize_cron_fields(&fields)
}

fn normalize_cron_fields(expr: &str) -> String {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    match parts.len() {
        5 => format!(
            "0 {} {} {} {} {}",
            parts[0], parts[1], parts[2], parts[3], parts[4]
        ),
        _ => expr.to_string(),
    }
}

fn cron_fields_for_parser(expr: &str) -> Result<String, CronError> {
    let (_, fields) = split_cron_timezone(expr)?;
    Ok(normalize_cron_fields(fields))
}

fn parse_schedule(expr: &str) -> Result<Schedule, CronError> {
    Schedule::from_str(expr).map_err(|e| CronError::InvalidExpression(e.to_string()))
}

fn next_run_after_cron(expr: &str, after_ms: i64) -> Result<i64, CronError> {
    let (tz, fields) = split_cron_timezone(expr)?;
    let schedule = parse_schedule(&normalize_cron_fields(fields))?;
    let after_utc: DateTime<Utc> =
        DateTime::from_timestamp_millis(after_ms).unwrap_or_else(Utc::now);
    let after_local = after_utc.with_timezone(&tz);
    schedule
        .after(&after_local)
        .next()
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis())
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
    fn cron_tz_prefix_shifts_next_run() {
        let after = chrono::DateTime::parse_from_rfc3339("2024-01-15T12:00:00Z")
            .unwrap()
            .timestamp_millis();
        let utc = next_run_after_cron("0 9 * * *", after).unwrap();
        assert_eq!(
            utc,
            chrono::DateTime::parse_from_rfc3339("2024-01-16T09:00:00Z")
                .unwrap()
                .timestamp_millis()
        );
        let ny = next_run_after_cron("CRON_TZ=America/New_York 0 9 * * *", after).unwrap();
        assert_eq!(
            ny,
            chrono::DateTime::parse_from_rfc3339("2024-01-15T14:00:00Z")
                .unwrap()
                .timestamp_millis(),
            "09:00 EST is 14:00 UTC in January"
        );
    }

    #[test]
    fn unknown_cron_timezone_is_rejected() {
        assert!(next_run_after_cron("CRON_TZ=Not/AZone */5 * * * *", 0).is_err());
    }

    #[test]
    fn normalize_cron_strips_timezone_prefix() {
        assert_eq!(
            normalize_cron("CRON_TZ=America/New_York */5 * * * *"),
            "0 */5 * * * *"
        );
        assert_eq!(normalize_cron("*/5 * * * *"), "0 */5 * * * *");
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

    #[test]
    fn journal_replays_committed_fire_after_restart() {
        let dir = temp_dir().join(format!("bettermq-cron-restart-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let reg = CronRegistry::open(&dir).unwrap();
        let job = reg
            .create_with_kind(
                ScheduleKind::Interval { every_seconds: 10 },
                sample_request(),
            )
            .unwrap();
        let due = reg.pop_due(job.created_at_ms + 10_000);
        assert_eq!(due.len(), 1);
        reg.commit_fire(job.id).unwrap();
        drop(reg);

        let reopened = CronRegistry::open(&dir).unwrap();
        let restored = reopened.get(job.id).unwrap();
        assert_eq!(restored.next_run_at_ms, job.created_at_ms + 20_000);
        assert_eq!(restored.last_run_at_ms, Some(job.created_at_ms + 10_000));
        assert!(dir.join("crons.journal").exists());
    }

    #[test]
    fn unsupported_journal_version_fails_closed() {
        let dir = temp_dir().join(format!("bettermq-cron-version-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("crons.json"), br#"{"jobs":[]}"#).unwrap();
        std::fs::write(
            dir.join("crons.journal"),
            br#"{"version":99,"sequence":1,"command":{"command":"delete","id":"00000000-0000-0000-0000-000000000000"}}"#,
        )
        .unwrap();
        assert!(matches!(
            CronRegistry::open(&dir),
            Err(CronError::InvalidSchedule(message))
                if message.contains("unsupported cron journal version")
        ));
    }
}
