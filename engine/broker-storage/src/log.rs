//! Append-only partition log: active WAL + sealed segments.

use crate::meta::LogMeta;
use broker_proto::record::{decode_frame, encode_frame, RecordError};
use broker_proto::{LogRecord, StoredMessage};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::info;

const WAL_FILE: &str = "active.wal";
const SEGMENTS_DIR: &str = "segments";

#[derive(Debug, Clone)]
pub struct PartitionLogConfig {
    /// Roll WAL into a segment when it exceeds this many bytes.
    pub segment_max_bytes: u64,
    /// fsync after every append (dev/tests).
    pub fsync_every_record: bool,
}

impl Default for PartitionLogConfig {
    fn default() -> Self {
        Self {
            segment_max_bytes: 8 * 1024 * 1024,
            fsync_every_record: true,
        }
    }
}

#[derive(Debug, Clone)]
struct LogPosition {
    path: PathBuf,
    byte_offset: u64,
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("record error: {0}")]
    Record(#[from] RecordError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("offset {0} not found")]
    OffsetNotFound(u64),
    #[error("slate: {0}")]
    Slate(String),
}

/// Durable log for one partition directory.
pub struct PartitionLog {
    dir: PathBuf,
    config: PartitionLogConfig,
    meta: LogMeta,
    wal: File,
    wal_size: u64,
    index: BTreeMap<u64, LogPosition>,
}

impl PartitionLog {
    pub fn open(dir: impl AsRef<Path>, config: PartitionLogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        std::fs::create_dir_all(dir.join(SEGMENTS_DIR))?;

        let meta = LogMeta::load(&dir).unwrap_or_default();
        let wal_path = dir.join(WAL_FILE);
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;

        let mut log = Self {
            dir: dir.clone(),
            config,
            meta,
            wal,
            wal_size: 0,
            index: BTreeMap::new(),
        };

        log.rebuild_from_disk()?;

        info!(
            dir = %dir.display(),
            next_offset = log.meta.next_offset,
            records = log.index.len(),
            "partition log opened"
        );

        Ok(log)
    }

    fn rebuild_from_disk(&mut self) -> Result<(), LogError> {
        self.index.clear();
        let mut next = 0u64;

        let mut segment_files: Vec<PathBuf> = std::fs::read_dir(self.dir.join(SEGMENTS_DIR))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file())
            .collect();
        segment_files.sort();

        for path in &segment_files {
            next = self.scan_file(path, next)?;
        }

        let wal_path = self.dir.join(WAL_FILE);
        next = self.scan_file(&wal_path, next)?;

        self.wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        self.meta.next_offset = next;
        self.meta.save(&self.dir)?;
        Ok(())
    }

    fn scan_file(&mut self, path: &Path, mut next_offset: u64) -> Result<u64, LogError> {
        let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            return Ok(next_offset);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut byte_pos = 0u64;

        while byte_pos < len {
            let frame_start = byte_pos;
            match decode_frame(&mut reader) {
                Ok((_header, _payload)) => {
                    self.index.insert(
                        next_offset,
                        LogPosition {
                            path: path.to_path_buf(),
                            byte_offset: frame_start,
                        },
                    );
                    next_offset += 1;
                    byte_pos = reader.stream_position()?;
                }
                Err(RecordError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(_) => {
                    // Partial tail after crash — ignore remainder
                    break;
                }
            }
        }
        Ok(next_offset)
    }

    /// Append a pre-encoded broker-proto frame (follower replication).
    pub fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
    ) -> Result<StoredMessage, LogError> {
        let (header, payload) = {
            let mut cursor = std::io::Cursor::new(frame);
            decode_frame(&mut cursor)?
        };
        let offset = self.meta.next_offset;
        let byte_offset = self.wal_size;
        let wal_path = self.dir.join(WAL_FILE);

        use std::io::Write;
        self.wal.write_all(frame)?;
        if self.config.fsync_every_record {
            self.wal.sync_all()?;
        }

        self.index.insert(
            offset,
            LogPosition {
                path: wal_path,
                byte_offset,
            },
        );
        self.wal_size += frame.len() as u64;
        self.meta.next_offset = offset + 1;
        self.meta.save(&self.dir)?;

        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }

        Ok(StoredMessage {
            id: header.id,
            tenant_id: header.tenant_id,
            topic: header.topic,
            partition,
            offset,
            routing_key: header.routing_key,
            payload,
            published_at_ms: header.published_at_ms,
            priority: header.priority,
            flow_parallelism: header.flow_parallelism,
            flow_key: header.flow_key.clone(),
            flow_rate: header.flow_rate,
            flow_period_secs: header.flow_period_secs,
            queue_id: header.queue_id,
            group_id: header.group_id,
            group_member_id: header.group_member_id,
            flow_profile_id: header.flow_profile_id,
            destination_url: header.destination_url.clone(),
            destination_secret: header.destination_secret.clone(),
            max_retries: header.max_retries,
            retry_backoff: header.retry_backoff.clone(),
            http_method: header.http_method.clone(),
            http_headers_json: header.http_headers_json.clone(),
            http_sign: header.http_sign,
            payload_ref_json: header.payload_ref_json.clone(),
        })
    }

    pub fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        let offset = self.meta.next_offset;
        let byte_offset = self.wal_size;
        let wal_path = self.dir.join(WAL_FILE);

        let mut frame = Vec::new();
        encode_frame(&header, &payload, &mut frame)?;
        use std::io::Write;
        self.wal.write_all(&frame)?;
        if self.config.fsync_every_record {
            self.wal.sync_all()?;
        }

        self.index.insert(
            offset,
            LogPosition {
                path: wal_path,
                byte_offset,
            },
        );
        self.wal_size += frame.len() as u64;
        self.meta.next_offset = offset + 1;
        self.meta.save(&self.dir)?;

        if self.wal_size >= self.config.segment_max_bytes {
            self.roll_segment()?;
        }

        let stored = StoredMessage {
            id: header.id,
            tenant_id: header.tenant_id,
            topic: header.topic,
            partition,
            offset,
            routing_key: header.routing_key,
            payload,
            published_at_ms: header.published_at_ms,
            priority: header.priority,
            flow_parallelism: header.flow_parallelism,
            flow_key: header.flow_key.clone(),
            flow_rate: header.flow_rate,
            flow_period_secs: header.flow_period_secs,
            queue_id: header.queue_id,
            group_id: header.group_id,
            group_member_id: header.group_member_id,
            flow_profile_id: header.flow_profile_id,
            destination_url: header.destination_url.clone(),
            destination_secret: header.destination_secret.clone(),
            max_retries: header.max_retries,
            retry_backoff: header.retry_backoff.clone(),
            http_method: header.http_method.clone(),
            http_headers_json: header.http_headers_json.clone(),
            http_sign: header.http_sign,
            payload_ref_json: header.payload_ref_json.clone(),
        };
        Ok((stored, frame))
    }

    fn roll_segment(&mut self) -> Result<(), LogError> {
        if self.wal_size == 0 {
            return Ok(());
        }

        let roll = self.meta.segment_roll_count;
        let seg_path = self
            .dir
            .join(SEGMENTS_DIR)
            .join(format!("segment-{roll:06}.log"));
        let wal_path = self.dir.join(WAL_FILE);

        self.wal.sync_all()?;

        // Close the WAL handle before rename. Do not truncate active.wal first.
        let placeholder = self.dir.join(".wal_fd_placeholder");
        let _closed = std::mem::replace(
            &mut self.wal,
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&placeholder)?,
        );

        std::fs::rename(&wal_path, &seg_path)?;
        let _ = std::fs::remove_file(&placeholder);

        self.meta.segment_roll_count += 1;
        self.meta.save(&self.dir)?;

        self.wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;
        self.wal_size = 0;
        self.rebuild_from_disk()?;
        Ok(())
    }

    pub fn read_range(
        &self,
        partition: u32,
        start_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        let mut out = Vec::new();
        for (&offset, pos) in self.index.range(start_offset..) {
            if out.len() >= max_messages {
                break;
            }
            let (header, payload) = self.read_at(pos)?;
            out.push(StoredMessage {
                id: header.id,
                tenant_id: header.tenant_id,
                topic: header.topic,
                partition,
                offset,
                routing_key: header.routing_key,
                payload,
                published_at_ms: header.published_at_ms,
                priority: header.priority,
                flow_parallelism: header.flow_parallelism,
                flow_key: header.flow_key.clone(),
                flow_rate: header.flow_rate,
                flow_period_secs: header.flow_period_secs,
                queue_id: header.queue_id,
                group_id: header.group_id,
                group_member_id: header.group_member_id,
                flow_profile_id: header.flow_profile_id,
                destination_url: header.destination_url.clone(),
                destination_secret: header.destination_secret.clone(),
                max_retries: header.max_retries,
                retry_backoff: header.retry_backoff.clone(),
                http_method: header.http_method.clone(),
                http_headers_json: header.http_headers_json.clone(),
                http_sign: header.http_sign,
                payload_ref_json: header.payload_ref_json.clone(),
            });
        }
        Ok(out)
    }

    pub fn high_watermark(&self) -> u64 {
        self.meta.next_offset
    }

    /// Remove a delivered message from the in-memory index after delivery.
    /// Segment bytes are not rewritten; records are no longer readable via [`Self::read_range`].
    pub fn purge_offset(&mut self, offset: u64) -> bool {
        self.index.remove(&offset).is_some()
    }

    fn read_at(&self, pos: &LogPosition) -> Result<(LogRecord, Vec<u8>), LogError> {
        let mut file = File::open(&pos.path)?;
        file.seek(SeekFrom::Start(pos.byte_offset))?;
        decode_frame(&mut file).map_err(LogError::from)
    }

    pub fn sync(&mut self) -> Result<(), LogError> {
        self.wal.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn sample_record(topic: &str) -> LogRecord {
        LogRecord {
            id: Uuid::new_v4(),
            tenant_id: "default".into(),
            topic: topic.into(),
            routing_key: "rk".into(),
            idempotency_key: None,
            published_at_ms: 0,
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

    #[test]
    fn append_and_read_back() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        log.append(0, sample_record("t"), b"two".to_vec()).unwrap();

        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].offset, 0);
        assert_eq!(msgs[1].payload, b"two");
    }

    #[test]
    fn reopen_recovers() {
        let dir = tempdir().unwrap();
        {
            let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
            for i in 0..5u8 {
                log.append(0, sample_record("t"), vec![i]).unwrap();
            }
            log.sync().unwrap();
        }
        let log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert_eq!(msgs.len(), 5);
        assert_eq!(log.high_watermark(), 5);
    }

    #[test]
    fn purge_offset_removes_from_reads() {
        let dir = tempdir().unwrap();
        let mut log = PartitionLog::open(dir.path(), PartitionLogConfig::default()).unwrap();
        log.append(0, sample_record("t"), b"one".to_vec()).unwrap();
        assert!(log.purge_offset(0));
        let msgs = log.read_range(0, 0, 10).unwrap();
        assert!(msgs.is_empty());
    }

    #[test]
    fn segment_roll_preserves_records() {
        let dir = tempdir().unwrap();
        let cfg = PartitionLogConfig {
            segment_max_bytes: 256,
            fsync_every_record: true,
        };
        let mut log = PartitionLog::open(dir.path(), cfg.clone()).unwrap();
        for i in 0..20u8 {
            log.append(0, sample_record("t"), vec![i; 32]).unwrap();
        }
        let log = PartitionLog::open(dir.path(), cfg).unwrap();
        let msgs = log.read_range(0, 0, 50).unwrap();
        assert_eq!(msgs.len(), 20);
    }
}
