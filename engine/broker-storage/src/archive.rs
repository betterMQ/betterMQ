//! Bounded asynchronous sealed-segment archive.
//!
//! The NVMe WAL remains the ACK path. One process-wide worker copies sealed
//! segments after roll, verifies SHA-256 while streaming, and writes a durable
//! sidecar manifest. Slate/object storage is deliberately not part of fast ACK.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::{info, warn};

#[cfg(feature = "s3")]
use bytes::Bytes;
#[cfg(feature = "s3")]
use futures::{StreamExt, TryStreamExt};
#[cfg(feature = "s3")]
use object_store::path::Path as ObjectPath;
#[cfg(feature = "s3")]
use object_store::ObjectStore;

const COPY_BUFFER_BYTES: usize = 1024 * 1024;
#[cfg(feature = "s3")]
const MULTIPART_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveManifest {
    pub version: u32,
    pub segment: String,
    pub bytes: u64,
    pub sha256: String,
    pub archived_at_unix_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArchiveLagStatus {
    pub enabled: bool,
    pub queued_records: u64,
    pub queued_bytes: u64,
    pub failed_records: u64,
    pub failed_bytes: u64,
    pub oldest_age_ms: u64,
    pub capacity: usize,
    pub admission_blocked: bool,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ArchiveToolReport {
    pub manifests: usize,
    pub verified: usize,
    pub restored: usize,
    #[serde(default)]
    pub skipped: usize,
    pub bytes: u64,
    pub errors: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest: {0}")]
    Manifest(String),
    #[cfg(feature = "s3")]
    #[error("object store: {0}")]
    ObjectStore(String),
}

#[derive(Default)]
struct ArchiveCounters {
    queued_records: AtomicU64,
    queued_bytes: AtomicU64,
    oldest_unix_ms: AtomicU64,
    failed_records: AtomicU64,
    failed_bytes: AtomicU64,
}

struct ArchiveJob {
    path: PathBuf,
    bytes: u64,
    pending_marker: PathBuf,
}

enum ArchiveBackend {
    Local(PathBuf),
    #[cfg(feature = "s3")]
    Object {
        store: Arc<dyn ObjectStore>,
        prefix: String,
    },
}

struct ArchiveService {
    sender: mpsc::SyncSender<ArchiveJob>,
    counters: Arc<ArchiveCounters>,
    capacity: usize,
}

static SERVICE: OnceLock<Option<ArchiveService>> = OnceLock::new();

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn archive_backend() -> Option<ArchiveBackend> {
    if std::env::var("BETTERMQ_ARCHIVE_BUCKET").is_ok()
        || std::env::var("S3_ARCHIVE_BUCKET").is_ok()
        || std::env::var("R2_ARCHIVE_BUCKET").is_ok()
    {
        #[cfg(feature = "s3")]
        {
            match crate::open_archive_object_store_from_env() {
                Ok(store) => {
                    let prefix = std::env::var("BETTERMQ_ARCHIVE_PREFIX")
                        .unwrap_or_else(|_| "bettermq-archive".into());
                    return Some(ArchiveBackend::Object { store, prefix });
                }
                Err(error) => {
                    warn!(%error, "archive object-store configuration invalid");
                    return None;
                }
            }
        }
        #[cfg(not(feature = "s3"))]
        {
            warn!("archive bucket configured but broker-storage was built without feature `s3`");
            return None;
        }
    }
    std::env::var("BETTERMQ_ARCHIVE_DIR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(ArchiveBackend::Local)
}

fn service() -> Option<&'static ArchiveService> {
    SERVICE
        .get_or_init(|| {
            let backend = archive_backend()?;
            let capacity = env_usize("BETTERMQ_ARCHIVE_QUEUE_CAPACITY", 128);
            let retries = env_usize("BETTERMQ_ARCHIVE_MAX_RETRIES", 5);
            let counters = Arc::new(ArchiveCounters::default());
            let worker_counters = counters.clone();
            let (sender, receiver) = mpsc::sync_channel(capacity);
            std::thread::Builder::new()
                .name("bettermq-archive".into())
                .spawn(move || archive_worker(receiver, backend, retries, worker_counters))
                .ok()?;
            let service = ArchiveService {
                sender,
                counters,
                capacity,
            };
            if let Some(root) = std::env::var_os("BETTERMQ_DATA_DIR").map(PathBuf::from) {
                visit_recovery_segments(&root, &mut |path| {
                    queue_archive_job(&service, path);
                });
            }
            Some(service)
        })
        .as_ref()
}

/// Initialize archive recovery during broker bootstrap so pending sealed
/// segments are requeued even when no new segment rolls after restart.
pub fn start_archive_service() {
    let _ = service();
}

pub fn archive_lag_status() -> ArchiveLagStatus {
    let Some(service) = SERVICE.get().and_then(Option::as_ref) else {
        return ArchiveLagStatus {
            enabled: false,
            queued_records: 0,
            queued_bytes: 0,
            failed_records: 0,
            failed_bytes: 0,
            oldest_age_ms: 0,
            capacity: 0,
            admission_blocked: false,
        };
    };
    let records = service.counters.queued_records.load(Ordering::Acquire);
    let bytes = service.counters.queued_bytes.load(Ordering::Acquire);
    let failed_records = service.counters.failed_records.load(Ordering::Acquire);
    let failed_bytes = service.counters.failed_bytes.load(Ordering::Acquire);
    let oldest = service.counters.oldest_unix_ms.load(Ordering::Acquire);
    let age = if oldest == 0 {
        0
    } else {
        unix_ms().saturating_sub(oldest)
    };
    let max_bytes = env_u64("BETTERMQ_ARCHIVE_MAX_QUEUED_BYTES", 4 * 1024 * 1024 * 1024);
    let max_age = env_u64("BETTERMQ_ARCHIVE_MAX_LAG_MS", 5 * 60 * 1_000);
    let high_water = ((service.capacity as u64) * 3 / 4).max(1);
    ArchiveLagStatus {
        enabled: true,
        queued_records: records,
        queued_bytes: bytes,
        failed_records,
        failed_bytes,
        oldest_age_ms: age,
        capacity: service.capacity,
        admission_blocked: failed_records > 0
            || records >= high_water
            || bytes >= max_bytes
            || age >= max_age,
    }
}

pub fn archive_admission_blocked() -> bool {
    archive_lag_status().admission_blocked
}

/// Queue a sealed segment on one bounded worker. Admission starts rejecting
/// before the queue fills; this final blocking send prevents silently losing an
/// archive job if already-admitted writes race the high-water mark.
pub fn enqueue_sealed_segment(path: &Path) {
    let Some(service) = service() else {
        return;
    };
    if pending_marker(path).exists() {
        return;
    }
    queue_archive_job(service, path);
}

fn pending_marker(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.log");
    path.with_file_name(format!("{name}.archive.pending"))
}

fn complete_marker(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.log");
    path.with_file_name(format!("{name}.archive.complete.json"))
}

fn write_pending_marker(path: &Path) -> std::io::Result<PathBuf> {
    let marker = pending_marker(path);
    crate::atomic_write_file(&marker, format!("{}\n", path.display()).as_bytes())?;
    Ok(marker)
}

fn mark_archive_complete(job: &ArchiveJob, manifest: &ArchiveManifest) -> Result<(), ArchiveError> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| ArchiveError::Manifest(error.to_string()))?;
    crate::atomic_write_file(&complete_marker(&job.path), &bytes)?;
    match std::fs::remove_file(&job.pending_marker) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn queue_archive_job(service: &ArchiveService, path: &Path) {
    if complete_marker(path).exists() {
        return;
    }
    let marker = match write_pending_marker(path) {
        Ok(marker) => marker,
        Err(error) => {
            warn!(segment = %path.display(), %error, "failed to persist archive pending marker");
            pending_marker(path)
        }
    };
    let bytes = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    service
        .counters
        .queued_records
        .fetch_add(1, Ordering::AcqRel);
    service
        .counters
        .queued_bytes
        .fetch_add(bytes, Ordering::AcqRel);
    let _ = service.counters.oldest_unix_ms.compare_exchange(
        0,
        unix_ms(),
        Ordering::AcqRel,
        Ordering::Relaxed,
    );
    let started = Instant::now();
    if let Err(error) = service.sender.send(ArchiveJob {
        path: path.to_path_buf(),
        bytes,
        pending_marker: marker,
    }) {
        finish_job(&service.counters, bytes, true);
        warn!(segment = %error.0.path.display(), "archive worker unavailable");
    } else if started.elapsed() > Duration::from_millis(10) {
        warn!(
            segment = %path.display(),
            blocked_ms = started.elapsed().as_millis(),
            "archive queue applied segment-roll backpressure"
        );
    }
}

fn visit_recovery_segments(root: &Path, visitor: &mut impl FnMut(&Path)) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            visit_recovery_segments(&path, visitor);
            continue;
        }
        let is_segment = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("segment-") && name.ends_with(".log"));
        if is_segment && !complete_marker(&path).exists() {
            visitor(&path);
        }
    }
}

#[cfg(test)]
fn discover_recovery_segments(root: &Path) -> Vec<PathBuf> {
    let mut output = Vec::new();
    visit_recovery_segments(root, &mut |path| output.push(path.to_path_buf()));
    output.sort();
    output
}

fn finish_job(counters: &ArchiveCounters, bytes: u64, failed: bool) {
    if failed {
        counters.failed_records.fetch_add(1, Ordering::AcqRel);
        counters.failed_bytes.fetch_add(bytes, Ordering::AcqRel);
    }
    counters.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
    if counters.queued_records.fetch_sub(1, Ordering::AcqRel) == 1 {
        counters.oldest_unix_ms.store(0, Ordering::Release);
    }
}

fn archive_worker(
    receiver: mpsc::Receiver<ArchiveJob>,
    backend: ArchiveBackend,
    retries: usize,
    counters: Arc<ArchiveCounters>,
) {
    #[cfg(feature = "s3")]
    let runtime = matches!(&backend, ArchiveBackend::Object { .. }).then(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("archive object-store runtime")
    });
    while let Ok(job) = receiver.recv() {
        let mut delay = Duration::from_millis(100);
        let mut archived = false;
        for attempt in 1..=retries {
            let result = match &backend {
                ArchiveBackend::Local(root) => archive_local_once(&job.path, root),
                #[cfg(feature = "s3")]
                ArchiveBackend::Object { store, prefix } => runtime
                    .as_ref()
                    .expect("object backend runtime")
                    .block_on(archive_object_once(store, prefix, &job.path)),
            };
            match result {
                Ok(manifest) => {
                    if let Err(error) = mark_archive_complete(&job, &manifest) {
                        warn!(
                            segment = %job.path.display(),
                            %error,
                            "archive completed but durable completion marker failed"
                        );
                        continue;
                    }
                    info!(
                        src = %job.path.display(),
                        bytes = manifest.bytes,
                        sha256 = %manifest.sha256,
                        "archived sealed segment"
                    );
                    archived = true;
                    break;
                }
                Err(error) if attempt < retries => {
                    warn!(
                        segment = %job.path.display(),
                        attempt,
                        error = %error,
                        "archive attempt failed; retrying"
                    );
                    std::thread::sleep(delay);
                    delay = delay.saturating_mul(2).min(Duration::from_secs(5));
                }
                Err(error) => warn!(
                    segment = %job.path.display(),
                    attempts = retries,
                    error = %error,
                    "archive retries exhausted; segment remains on local disk"
                ),
            }
        }
        finish_job(&counters, job.bytes, !archived);
    }
}

fn archive_local_once(src: &Path, dest_root: &Path) -> Result<ArchiveManifest, ArchiveError> {
    let key = archive_key(src);
    let dest = dest_root.join(&key);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let part = dest.with_extension("archive-part");
    let (bytes, sha256) = copy_and_hash(File::open(src)?, File::create(&part)?)?;
    std::fs::rename(&part, &dest)?;

    let manifest = new_manifest(&key, bytes, sha256);
    let manifest_path = local_manifest_path(&dest);
    let manifest_part = manifest_path.with_extension("json.part");
    let mut file = File::create(&manifest_part)?;
    serde_json::to_writer_pretty(&mut file, &manifest)
        .map_err(|error| ArchiveError::Manifest(error.to_string()))?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(manifest_part, manifest_path)?;
    if let Some(parent) = dest.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(manifest)
}

fn copy_and_hash(mut input: File, mut output: File) -> Result<(u64, String), ArchiveError> {
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        output.write_all(&buffer[..read])?;
        bytes += read as u64;
    }
    output.sync_all()?;
    Ok((bytes, hex::encode(hasher.finalize())))
}

fn new_manifest(key: &Path, bytes: u64, sha256: String) -> ArchiveManifest {
    ArchiveManifest {
        version: 1,
        segment: key.to_string_lossy().replace('\\', "/"),
        bytes,
        sha256,
        archived_at_unix_ms: unix_ms() as u128,
    }
}

fn local_manifest_path(segment: &Path) -> PathBuf {
    let name = segment
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.log");
    segment.with_file_name(format!("{name}.manifest.json"))
}

#[cfg(feature = "s3")]
async fn archive_object_once(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    src: &Path,
) -> Result<ArchiveManifest, ArchiveError> {
    let key = archive_key(src);
    let object_key = prefixed_key(prefix, &key);
    let path = ObjectPath::from(object_key.as_str());
    let mut upload = store.put_multipart(&path).await.map_err(object_error)?;
    let mut file = File::open(src)?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    loop {
        let mut buffer = vec![0u8; MULTIPART_BYTES];
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        buffer.truncate(read);
        hasher.update(&buffer);
        bytes += read as u64;
        if let Err(error) = upload.put_part(Bytes::from(buffer).into()).await {
            let _ = upload.abort().await;
            return Err(object_error(error));
        }
    }
    upload.complete().await.map_err(object_error)?;
    let manifest = new_manifest(&key, bytes, hex::encode(hasher.finalize()));
    let manifest_key = format!("{object_key}.manifest.json");
    store
        .put(
            &ObjectPath::from(manifest_key),
            serde_json::to_vec_pretty(&manifest)
                .map_err(|error| ArchiveError::Manifest(error.to_string()))?
                .into(),
        )
        .await
        .map_err(object_error)?;
    Ok(manifest)
}

#[cfg(feature = "s3")]
fn prefixed_key(prefix: &str, key: &Path) -> String {
    let key = key.to_string_lossy().replace('\\', "/");
    format!("{}/{}", prefix.trim_matches('/'), key)
        .trim_matches('/')
        .to_string()
}

#[cfg(feature = "s3")]
fn object_error(error: object_store::Error) -> ArchiveError {
    ArchiveError::ObjectStore(error.to_string())
}

fn archive_key(src: &Path) -> PathBuf {
    if let Some(relative) = std::env::var_os("BETTERMQ_DATA_DIR")
        .and_then(|root| {
            src.strip_prefix(PathBuf::from(root))
                .ok()
                .map(Path::to_path_buf)
        })
        .filter(|path| {
            path.components()
                .all(|component| matches!(component, std::path::Component::Normal(_)))
        })
    {
        return relative;
    }
    let parent = src.parent().unwrap_or_else(|| Path::new("."));
    let digest = Sha256::digest(parent.to_string_lossy().as_bytes());
    let prefix = &hex::encode(digest)[..16];
    PathBuf::from(prefix).join(
        src.file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("segment.log")),
    )
}

fn safe_archive_key(value: &str) -> Result<PathBuf, ArchiveError> {
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty()
        || !path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return Err(ArchiveError::Manifest(format!(
            "unsafe archive segment key {value:?}"
        )));
    }
    Ok(path)
}

fn collect_manifest_paths(root: &Path, output: &mut Vec<PathBuf>) -> Result<(), ArchiveError> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_manifest_paths(&path, output)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".manifest.json"))
        {
            output.push(path);
        }
    }
    Ok(())
}

fn read_local_manifest(path: &Path) -> Result<ArchiveManifest, ArchiveError> {
    serde_json::from_slice(&std::fs::read(path)?)
        .map_err(|error| ArchiveError::Manifest(format!("{}: {error}", path.display())))
}

fn verify_local_manifest(root: &Path, manifest: &ArchiveManifest) -> Result<(), ArchiveError> {
    let key = safe_archive_key(&manifest.segment)?;
    let mut file = File::open(root.join(key))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes += read as u64;
    }
    if bytes != manifest.bytes || hex::encode(hasher.finalize()) != manifest.sha256 {
        return Err(ArchiveError::Manifest(format!(
            "checksum/size mismatch for {}",
            manifest.segment
        )));
    }
    Ok(())
}

pub fn scrub_local_archive(root: &Path) -> ArchiveToolReport {
    let mut report = ArchiveToolReport::default();
    let mut paths = Vec::new();
    if let Err(error) = collect_manifest_paths(root, &mut paths) {
        report.errors.push(error.to_string());
        return report;
    }
    for path in paths {
        report.manifests += 1;
        match read_local_manifest(&path).and_then(|manifest| {
            verify_local_manifest(root, &manifest)?;
            Ok(manifest)
        }) {
            Ok(manifest) => {
                report.verified += 1;
                report.bytes += manifest.bytes;
            }
            Err(error) => report.errors.push(error.to_string()),
        }
    }
    report
}

pub fn restore_local_archive(root: &Path, destination: &Path) -> ArchiveToolReport {
    restore_local_archive_until(root, destination, None)
}

/// Restore sealed segments whose `archived_at_unix_ms` is `<= until_unix_ms`.
/// `None` restores the whole archive. S3/object restore is the same contract.
pub fn restore_local_archive_until(
    root: &Path,
    destination: &Path,
    until_unix_ms: Option<u128>,
) -> ArchiveToolReport {
    let mut report = ArchiveToolReport::default();
    let mut paths = Vec::new();
    if let Err(error) = collect_manifest_paths(root, &mut paths) {
        report.errors.push(error.to_string());
        return report;
    }
    for path in paths {
        report.manifests += 1;
        let result = (|| {
            let manifest = read_local_manifest(&path)?;
            if until_unix_ms.is_some_and(|until| manifest.archived_at_unix_ms > until) {
                report.skipped += 1;
                return Ok(());
            }
            verify_local_manifest(root, &manifest)?;
            report.verified += 1;
            let key = safe_archive_key(&manifest.segment)?;
            let source = root.join(&key);
            let target = destination.join(&key);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let part = target.with_extension("restore-part");
            let (bytes, sha256) = copy_and_hash(File::open(source)?, File::create(&part)?)?;
            if bytes != manifest.bytes || sha256 != manifest.sha256 {
                let _ = std::fs::remove_file(part);
                return Err(ArchiveError::Manifest(format!(
                    "restore checksum mismatch for {}",
                    manifest.segment
                )));
            }
            std::fs::rename(part, target)?;
            report.restored += 1;
            report.bytes += bytes;
            Ok::<(), ArchiveError>(())
        })();
        if let Err(error) = result {
            report.errors.push(error.to_string());
        }
    }
    report
}

#[cfg(feature = "s3")]
async fn object_manifests(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<Vec<(ObjectPath, ArchiveManifest)>, ArchiveError> {
    let prefix_path =
        (!prefix.trim_matches('/').is_empty()).then(|| ObjectPath::from(prefix.trim_matches('/')));
    let paths = store
        .list(prefix_path.as_ref())
        .try_filter(|meta| {
            futures::future::ready(meta.location.as_ref().ends_with(".manifest.json"))
        })
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await
        .map_err(object_error)?;
    let mut manifests = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = store
            .get(&path)
            .await
            .map_err(object_error)?
            .bytes()
            .await
            .map_err(object_error)?;
        let manifest = serde_json::from_slice(&bytes)
            .map_err(|error| ArchiveError::Manifest(format!("{path}: {error}")))?;
        manifests.push((path, manifest));
    }
    Ok(manifests)
}

#[cfg(feature = "s3")]
async fn verify_object_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    manifest: &ArchiveManifest,
) -> Result<(), ArchiveError> {
    let key = safe_archive_key(&manifest.segment)?;
    let path = ObjectPath::from(prefixed_key(prefix, &key));
    let mut stream = store.get(&path).await.map_err(object_error)?.into_stream();
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(object_error)?;
        hasher.update(&chunk);
        bytes += chunk.len() as u64;
    }
    if bytes != manifest.bytes || hex::encode(hasher.finalize()) != manifest.sha256 {
        return Err(ArchiveError::Manifest(format!(
            "checksum/size mismatch for {}",
            manifest.segment
        )));
    }
    Ok(())
}

#[cfg(feature = "s3")]
pub async fn scrub_object_archive(store: Arc<dyn ObjectStore>, prefix: &str) -> ArchiveToolReport {
    let mut report = ArchiveToolReport::default();
    let manifests = match object_manifests(&store, prefix).await {
        Ok(manifests) => manifests,
        Err(error) => {
            report.errors.push(error.to_string());
            return report;
        }
    };
    for (_, manifest) in manifests {
        report.manifests += 1;
        match verify_object_manifest(&store, prefix, &manifest).await {
            Ok(()) => {
                report.verified += 1;
                report.bytes += manifest.bytes;
            }
            Err(error) => report.errors.push(error.to_string()),
        }
    }
    report
}

#[cfg(feature = "s3")]
pub async fn restore_object_archive(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    destination: &Path,
) -> ArchiveToolReport {
    restore_object_archive_until(store, prefix, destination, None).await
}

#[cfg(feature = "s3")]
pub async fn restore_object_archive_until(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    destination: &Path,
    until_unix_ms: Option<u128>,
) -> ArchiveToolReport {
    let mut report = ArchiveToolReport::default();
    let manifests = match object_manifests(&store, prefix).await {
        Ok(manifests) => manifests,
        Err(error) => {
            report.errors.push(error.to_string());
            return report;
        }
    };
    for (_, manifest) in manifests {
        report.manifests += 1;
        if until_unix_ms.is_some_and(|until| manifest.archived_at_unix_ms > until) {
            report.skipped += 1;
            continue;
        }
        let result = async {
            verify_object_manifest(&store, prefix, &manifest).await?;
            report.verified += 1;
            let key = safe_archive_key(&manifest.segment)?;
            let target = destination.join(&key);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let part = target.with_extension("restore-part");
            let mut file = File::create(&part)?;
            let mut stream = store
                .get(&ObjectPath::from(prefixed_key(prefix, &key)))
                .await
                .map_err(object_error)?
                .into_stream();
            let mut hasher = Sha256::new();
            let mut bytes = 0u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(object_error)?;
                hasher.update(&chunk);
                file.write_all(&chunk)?;
                bytes += chunk.len() as u64;
            }
            file.sync_all()?;
            if bytes != manifest.bytes || hex::encode(hasher.finalize()) != manifest.sha256 {
                let _ = std::fs::remove_file(part);
                return Err(ArchiveError::Manifest(format!(
                    "restore checksum mismatch for {}",
                    manifest.segment
                )));
            }
            std::fs::rename(part, target)?;
            report.restored += 1;
            report.bytes += bytes;
            Ok::<(), ArchiveError>(())
        }
        .await;
        if let Err(error) = result {
            report.errors.push(error.to_string());
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn archive_writes_checksum_manifest() {
        let source = tempdir().unwrap();
        let archive = tempdir().unwrap();
        let seg = source.path().join("segment-000000.log");
        std::fs::write(&seg, b"hello").unwrap();
        let manifest = archive_local_once(&seg, archive.path()).unwrap();
        let key = archive_key(&seg);
        let archived = archive.path().join(&key);
        assert_eq!(manifest.bytes, 5);
        assert_eq!(
            manifest.sha256,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_eq!(std::fs::read(&archived).unwrap(), b"hello");
        assert!(archived
            .with_file_name("segment-000000.log.manifest.json")
            .exists());
        let scrub = scrub_local_archive(archive.path());
        assert_eq!(scrub.verified, 1);
        assert!(scrub.errors.is_empty());

        let restored = tempdir().unwrap();
        let restore = restore_local_archive(archive.path(), restored.path());
        assert_eq!(restore.restored, 1);
        assert_eq!(std::fs::read(restored.path().join(key)).unwrap(), b"hello");

        let before = restore_local_archive_until(archive.path(), restored.path(), Some(0));
        assert_eq!(before.skipped, 1);
        assert_eq!(before.restored, 0);
    }

    #[test]
    fn startup_rescan_recovers_incomplete_archive_job() {
        let data = tempdir().unwrap();
        let archive = tempdir().unwrap();
        let segments = data.path().join("partitions/default/jobs/0/segments");
        std::fs::create_dir_all(&segments).unwrap();
        let segment = segments.join("segment-000003.log");
        std::fs::write(&segment, b"recover-me").unwrap();
        let marker = write_pending_marker(&segment).unwrap();

        assert_eq!(
            discover_recovery_segments(data.path()),
            vec![segment.clone()]
        );
        let manifest = archive_local_once(&segment, archive.path()).unwrap();
        let job = ArchiveJob {
            path: segment.clone(),
            bytes: 10,
            pending_marker: marker,
        };
        mark_archive_complete(&job, &manifest).unwrap();

        assert!(discover_recovery_segments(data.path()).is_empty());
        assert!(!pending_marker(&segment).exists());
        assert!(complete_marker(&segment).exists());
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn object_archive_scrubs_and_restores() {
        let source = tempdir().unwrap();
        let restored = tempdir().unwrap();
        let segment = source.path().join("segment-000001.log");
        std::fs::write(&segment, vec![3u8; 10 * 1024 * 1024]).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());

        archive_object_once(&store, "archive-test", &segment)
            .await
            .unwrap();
        let scrub = scrub_object_archive(store.clone(), "archive-test").await;
        assert_eq!(scrub.verified, 1);
        assert!(scrub.errors.is_empty());

        let restore = restore_object_archive(store, "archive-test", restored.path()).await;
        assert_eq!(restore.restored, 1);
        assert!(restore.errors.is_empty());
        assert_eq!(
            std::fs::metadata(restored.path().join(archive_key(&segment)))
                .unwrap()
                .len(),
            10 * 1024 * 1024
        );
    }
}
