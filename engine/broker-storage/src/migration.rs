//! Offline, validated V1-to-V2 WAL migration.

use crate::manifest::{WalManifest, WAL_FORMAT_V1, WAL_FORMAT_V2};
use crate::meta::atomic_write_file;
use crate::{FsyncMode, PartitionLog, PartitionLogConfig};
use broker_proto::{decode_epoch, decode_frame, encode_frame_vec, LogRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::{Path, PathBuf};
use thiserror::Error;

const ROLLBACK_FILE: &str = "migration-rollback.json";
const REPORT_FILE: &str = "migration-report.json";
const COPY_BATCH_RECORDS: usize = 1000;

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("record error: {0}")]
    Record(#[from] broker_proto::RecordError),
    #[error("epoch error: {0}")]
    Epoch(#[from] broker_proto::EpochError),
    #[error("log error: {0}")]
    Log(#[from] crate::LogError),
    #[error("invalid migration: {0}")]
    Invalid(String),
    #[error("insufficient free space: available={available} required={required}")]
    InsufficientSpace { available: u64, required: u64 },
    #[error("source changed during migration")]
    SourceChanged,
    #[error("migrated WAL validation failed")]
    ValidationFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalInspection {
    pub format_version: u16,
    pub record_count: u64,
    pub total_bytes: u64,
    pub digest_hex: String,
    pub files: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MigrationReport {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub shard_id: u32,
    pub source_inspection: WalInspection,
    pub destination_inspection: WalInspection,
    pub required_free_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RollbackMetadata {
    version: u16,
    source: PathBuf,
    destination: PathBuf,
    source_inspection: WalInspection,
    destination_inspection: WalInspection,
    source_manifest: Option<Vec<u8>>,
}

pub fn inspect_wal(dir: impl AsRef<Path>) -> Result<WalInspection, MigrationError> {
    let dir = dir.as_ref();
    let format_version = read_format_version(dir)?;
    let files = wal_files(dir)?;
    let total_bytes = files.iter().try_fold(0u64, |total, path| {
        Ok::<_, std::io::Error>(total.saturating_add(path.metadata()?.len()))
    })?;
    let relative_files = files
        .iter()
        .map(|path| path.strip_prefix(dir).unwrap_or(path).to_path_buf())
        .collect();
    let mut digest = Sha256::new();
    let mut record_count = 0u64;
    scan_records(dir, format_version, |header, payload| {
        let frame = encode_frame_vec(&header, &payload)?;
        digest.update((frame.len() as u64).to_be_bytes());
        digest.update(frame);
        record_count = record_count.saturating_add(1);
        Ok(())
    })?;
    Ok(WalInspection {
        format_version,
        record_count,
        total_bytes,
        digest_hex: hex::encode(digest.finalize()),
        files: relative_files,
    })
}

/// Copy a V1 directory into a validated V2 directory.
///
/// The source remains untouched. The destination only becomes visible after
/// validation and an atomic staging-directory rename.
pub fn migrate_v1_to_v2(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    shard_id: u32,
) -> Result<MigrationReport, MigrationError> {
    let source = source.as_ref();
    let destination = destination.as_ref();
    if source == destination {
        return Err(MigrationError::Invalid(
            "source and destination must differ".into(),
        ));
    }
    if destination.exists() {
        return Err(MigrationError::Invalid(format!(
            "destination already exists: {}",
            destination.display()
        )));
    }
    let source_inspection = inspect_wal(source)?;
    if source_inspection.format_version != WAL_FORMAT_V1 {
        return Err(MigrationError::Invalid(format!(
            "source WAL format is {}, expected V1",
            source_inspection.format_version
        )));
    }
    let parent = destination.parent().ok_or_else(|| {
        MigrationError::Invalid("destination must have a parent directory".into())
    })?;
    std::fs::create_dir_all(parent)?;
    let required_free_bytes = source_inspection
        .total_bytes
        .saturating_add(source_inspection.total_bytes / 10)
        .saturating_add(1024 * 1024);
    let available = fs2::available_space(parent)?;
    if available < required_free_bytes {
        return Err(MigrationError::InsufficientSpace {
            available,
            required: required_free_bytes,
        });
    }

    let staging = staging_path(destination);
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }
    let result = migrate_into_staging(
        source,
        destination,
        &staging,
        shard_id,
        source_inspection,
        required_free_bytes,
    );
    if result.is_err() && staging.exists() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    result
}

fn migrate_into_staging(
    source: &Path,
    destination: &Path,
    staging: &Path,
    shard_id: u32,
    source_inspection: WalInspection,
    required_free_bytes: u64,
) -> Result<MigrationReport, MigrationError> {
    std::fs::create_dir_all(staging)?;
    let config = PartitionLogConfig {
        fsync: FsyncMode::Group,
        ..PartitionLogConfig::default()
    };
    let mut target = PartitionLog::open_for_shard(staging, config, shard_id, WAL_FORMAT_V2)?;
    let mut batch = Vec::with_capacity(COPY_BATCH_RECORDS);
    scan_records(source, WAL_FORMAT_V1, |header, payload| {
        batch.push((header, payload));
        if batch.len() >= COPY_BATCH_RECORDS {
            target.append_batch(shard_id, std::mem::take(&mut batch), None)?;
        }
        Ok(())
    })?;
    if !batch.is_empty() {
        target.append_batch(shard_id, batch, None)?;
    }
    target.sync()?;
    drop(target);

    if inspect_wal(source)? != source_inspection {
        return Err(MigrationError::SourceChanged);
    }
    let destination_inspection = inspect_wal(staging)?;
    if destination_inspection.format_version != WAL_FORMAT_V2
        || destination_inspection.record_count != source_inspection.record_count
        || destination_inspection.digest_hex != source_inspection.digest_hex
    {
        return Err(MigrationError::ValidationFailed);
    }

    let source_manifest = std::fs::read(WalManifest::path(source)).ok();
    let rollback = RollbackMetadata {
        version: 1,
        source: source.to_path_buf(),
        destination: destination.to_path_buf(),
        source_inspection: source_inspection.clone(),
        destination_inspection: destination_inspection.clone(),
        source_manifest,
    };
    atomic_write_file(
        &staging.join(ROLLBACK_FILE),
        &serde_json::to_vec_pretty(&rollback)?,
    )?;
    let report = MigrationReport {
        source: source.to_path_buf(),
        destination: destination.to_path_buf(),
        shard_id,
        source_inspection,
        destination_inspection,
        required_free_bytes,
    };
    atomic_write_file(
        &staging.join(REPORT_FILE),
        &serde_json::to_vec_pretty(&report)?,
    )?;

    // The complete V2 directory, including its WAL manifest and rollback
    // metadata, becomes visible in one rename.
    std::fs::rename(staging, destination)?;
    sync_directory(parent_of(destination)?)?;
    Ok(report)
}

/// Atomically remove a migrated destination while retaining it for inspection.
/// The original V1 source was never modified and remains the active rollback.
pub fn rollback_v1_to_v2(destination: impl AsRef<Path>) -> Result<PathBuf, MigrationError> {
    let destination = destination.as_ref();
    let bytes = std::fs::read(destination.join(ROLLBACK_FILE))?;
    let rollback: RollbackMetadata = serde_json::from_slice(&bytes)?;
    if inspect_wal(&rollback.source)? != rollback.source_inspection
        || inspect_wal(destination)? != rollback.destination_inspection
    {
        return Err(MigrationError::ValidationFailed);
    }
    let rolled_back = rollback_path(destination);
    std::fs::rename(destination, &rolled_back)?;
    sync_directory(parent_of(destination)?)?;
    Ok(rolled_back)
}

fn scan_records(
    dir: &Path,
    format_version: u16,
    mut visit: impl FnMut(LogRecord, Vec<u8>) -> Result<(), MigrationError>,
) -> Result<(), MigrationError> {
    let mut expected_offset = 0u64;
    for path in wal_files(dir)? {
        let len = path.metadata()?.len();
        let mut reader = BufReader::new(File::open(&path)?);
        while reader.stream_position()? < len {
            match format_version {
                WAL_FORMAT_V1 => {
                    let (header, payload) = decode_frame(&mut reader)?;
                    visit(header, payload)?;
                    expected_offset = expected_offset.saturating_add(1);
                }
                WAL_FORMAT_V2 => {
                    let (epoch, body) = decode_epoch(&mut reader)?;
                    if epoch.first_offset != expected_offset {
                        return Err(MigrationError::Invalid(format!(
                            "non-contiguous V2 epoch in {}: expected {}, found {}",
                            path.display(),
                            expected_offset,
                            epoch.first_offset
                        )));
                    }
                    let mut frames = std::io::Cursor::new(body);
                    for _ in 0..epoch.record_count {
                        let (header, payload) = decode_frame(&mut frames)?;
                        visit(header, payload)?;
                        expected_offset = expected_offset.saturating_add(1);
                    }
                    if frames.position() != frames.get_ref().len() as u64
                        || expected_offset != epoch.committed_hwm
                    {
                        return Err(MigrationError::Invalid(
                            "V2 epoch record count/length mismatch".into(),
                        ));
                    }
                }
                version => {
                    return Err(MigrationError::Invalid(format!(
                        "unsupported WAL format {version}"
                    )))
                }
            }
        }
    }
    Ok(())
}

fn read_format_version(dir: &Path) -> Result<u16, MigrationError> {
    let path = WalManifest::path(dir);
    if !path.exists() {
        return Ok(WAL_FORMAT_V1);
    }
    let manifest: WalManifest = serde_json::from_slice(&std::fs::read(path)?)?;
    if !matches!(manifest.wal_format_version, WAL_FORMAT_V1 | WAL_FORMAT_V2) {
        return Err(MigrationError::Invalid(format!(
            "unsupported WAL format {}",
            manifest.wal_format_version
        )));
    }
    Ok(manifest.wal_format_version)
}

fn wal_files(dir: &Path) -> Result<Vec<PathBuf>, MigrationError> {
    let mut files = Vec::new();
    let segments = dir.join("segments");
    if segments.exists() {
        let mut sealed = std::fs::read_dir(segments)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "log"))
            .collect::<Vec<_>>();
        sealed.sort();
        files.extend(sealed);
    }
    let active = dir.join("active.wal");
    if active.exists() {
        files.push(active);
    }
    if files.is_empty() {
        return Err(MigrationError::Invalid(format!(
            "no WAL files found in {}",
            dir.display()
        )));
    }
    Ok(files)
}

fn staging_path(destination: &Path) -> PathBuf {
    destination.with_extension(format!("migrating-{}", std::process::id()))
}

fn rollback_path(destination: &Path) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    destination.with_extension(format!("rolled-back-{timestamp}"))
}

fn parent_of(path: &Path) -> Result<&Path, MigrationError> {
    path.parent()
        .ok_or_else(|| MigrationError::Invalid("path has no parent directory".into()))
}

fn sync_directory(path: &Path) -> Result<(), MigrationError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_proto::LogRecord;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn record(index: u64) -> LogRecord {
        LogRecord {
            id: Uuid::from_u128(index as u128 + 1),
            tenant_id: "tenant".into(),
            topic: if index % 2 == 0 { "a" } else { "b" }.into(),
            routing_key: "lane".into(),
            idempotency_key: Some(format!("key-{index}")),
            published_at_ms: index as i64,
            priority: 5,
            flow_parallelism: None,
            flow_key: None,
            flow_rate: None,
            flow_period_secs: None,
            queue_id: None,
            group_id: None,
            group_member_id: None,
            flow_profile_id: None,
            destination_url: None,
            destination_secret: None,
            max_retries: 0,
            retry_backoff: None,
            http_method: None,
            http_headers_json: None,
            http_sign: None,
            payload_ref_json: None,
        }
    }

    fn create_v1(path: &Path, records: u64) {
        let mut log =
            PartitionLog::open_for_shard(path, PartitionLogConfig::default(), 3, WAL_FORMAT_V1)
                .unwrap();
        let batch = (0..records)
            .map(|index| (record(index), vec![(index % 251) as u8; 64]))
            .collect();
        log.append_batch(3, batch, None).unwrap();
        log.sync().unwrap();
    }

    #[test]
    fn migration_validates_and_atomically_switches() {
        let root = tempdir().unwrap();
        let source = root.path().join("v1");
        let destination = root.path().join("v2");
        create_v1(&source, 2500);

        let report = migrate_v1_to_v2(&source, &destination, 3).unwrap();
        assert!(source.exists());
        assert!(destination.exists());
        assert_eq!(report.source_inspection.record_count, 2500);
        assert_eq!(
            report.source_inspection.digest_hex,
            report.destination_inspection.digest_hex
        );
        assert_eq!(
            inspect_wal(&destination).unwrap().format_version,
            WAL_FORMAT_V2
        );
        let log = PartitionLog::open_for_shard(
            &destination,
            PartitionLogConfig::default(),
            3,
            WAL_FORMAT_V2,
        )
        .unwrap();
        assert_eq!(log.high_watermark(), 2500);
        assert_eq!(log.read_range(3, 2490, 20).unwrap().len(), 10);
    }

    #[test]
    fn rollback_keeps_source_and_quarantines_v2() {
        let root = tempdir().unwrap();
        let source = root.path().join("v1");
        let destination = root.path().join("v2");
        create_v1(&source, 20);
        migrate_v1_to_v2(&source, &destination, 3).unwrap();

        let rolled_back = rollback_v1_to_v2(&destination).unwrap();
        assert!(source.exists());
        assert!(!destination.exists());
        assert!(rolled_back.exists());
        assert_eq!(inspect_wal(&source).unwrap().record_count, 20);
        assert_eq!(inspect_wal(&rolled_back).unwrap().record_count, 20);
    }

    #[test]
    fn checksum_failure_never_publishes_destination() {
        let root = tempdir().unwrap();
        let source = root.path().join("v1");
        let destination = root.path().join("v2");
        create_v1(&source, 20);
        let wal = source.join("active.wal");
        let mut bytes = std::fs::read(&wal).unwrap();
        bytes[32] ^= 0xff;
        std::fs::write(wal, bytes).unwrap();

        assert!(migrate_v1_to_v2(&source, &destination, 3).is_err());
        assert!(!destination.exists());
    }
}
