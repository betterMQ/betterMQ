//! Durable SlateDB partition log on object storage (HA M3).

#[cfg(feature = "slate")]
use crate::log::PartitionLogConfig;
#[cfg(feature = "slate")]
use crate::LogError;
#[cfg(feature = "slate")]
use broker_proto::record::{decode_frame, encode_frame};
#[cfg(feature = "slate")]
use broker_proto::{LogRecord, StoredMessage};
#[cfg(feature = "slate")]
use bytes::Bytes;
#[cfg(feature = "slate")]
use object_store::ObjectStore;
#[cfg(feature = "slate")]
use slatedb::config::WriteOptions;
#[cfg(feature = "slate")]
use slatedb::{Db, WriteBatch};
#[cfg(feature = "slate")]
use std::future::Future;
#[cfg(feature = "slate")]
use std::path::Path;
#[cfg(feature = "slate")]
use std::sync::{Arc, OnceLock};
#[cfg(feature = "slate")]
use tokio::runtime::{Handle, Runtime};

/// Dedicated runtime for SlateDB — never `block_on` the server's Tokio pool (deadlocks HTTP).
#[cfg(feature = "slate")]
fn slate_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("slate-io")
            .enable_all()
            .build()
            .expect("slate tokio runtime")
    })
}

/// Run SlateDB async IO from sync partition APIs (called under `#[tokio::main]`).
#[cfg(feature = "slate")]
pub(crate) fn block_on_slate<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let rt = slate_runtime();
    if Handle::try_current().is_ok() {
        std::thread::scope(|scope| scope.spawn(|| rt.block_on(future)).join().unwrap())
    } else {
        rt.block_on(future)
    }
}

#[cfg(feature = "slate")]
fn frame_key(offset: u64) -> Vec<u8> {
    format!("fr/{offset:020}").into_bytes()
}

#[cfg(feature = "slate")]
const META_NEXT_OFFSET: &[u8] = b"meta/next_offset";
#[cfg(feature = "slate")]
const META_LEADER_GEN: &[u8] = b"meta/leader_generation";

/// Stable object-store prefix shared by every broker in a cluster (not per-node `data_dir`).
#[cfg(feature = "slate")]
pub fn slate_db_path(tenant_id: &str, topic: &str, partition: u32) -> String {
    format!("bettermq/{tenant_id}/topics/{topic}/p{partition}")
}

#[cfg(feature = "slate")]
fn durable_write_opts() -> WriteOptions {
    WriteOptions {
        await_durable: true,
        seqnum: 0,
    }
}

#[cfg(feature = "slate")]
pub struct SlatePartitionLog {
    db: Arc<Db>,
    next_offset: u64,
    leader_generation: u64,
    _config: PartitionLogConfig,
}

#[cfg(feature = "slate")]
impl SlatePartitionLog {
    pub fn open(
        db_path: String,
        local_cache_dir: impl AsRef<Path>,
        object_store: Arc<dyn ObjectStore>,
        partition: u32,
        config: PartitionLogConfig,
    ) -> Result<Self, LogError> {
        let _ = std::fs::create_dir_all(local_cache_dir.as_ref());
        let db =
            Arc::new(block_on_slate(Db::open(db_path.clone(), object_store)).map_err(slate_err)?);

        let next_offset = Self::recover_next_offset(Arc::clone(&db), partition)?;
        let leader_generation = Self::read_leader_generation(Arc::clone(&db))?;

        Ok(Self {
            db,
            next_offset,
            leader_generation,
            _config: config,
        })
    }

    fn read_leader_generation(db: Arc<Db>) -> Result<u64, LogError> {
        block_on_slate(async move {
            match db.get(META_LEADER_GEN).await.map_err(slate_err)? {
                Some(bytes) if bytes.len() >= 8 => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&bytes[..8]);
                    Ok(u64::from_be_bytes(buf))
                }
                _ => Ok(0u64),
            }
        })
    }

    /// Prefer `meta/next_offset`; scan frame keys so a durable frame is never overwritten after crash.
    fn recover_next_offset(db: Arc<Db>, _partition: u32) -> Result<u64, LogError> {
        let db_meta = Arc::clone(&db);
        let from_meta = block_on_slate(async move {
            match db_meta.get(META_NEXT_OFFSET).await.map_err(slate_err)? {
                Some(bytes) if bytes.len() >= 8 => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&bytes[..8]);
                    Ok::<Option<u64>, LogError>(Some(u64::from_be_bytes(buf)))
                }
                _ => Ok::<Option<u64>, LogError>(None),
            }
        })?;

        let mut next = from_meta.unwrap_or(0);
        let scan_until = next.saturating_add(4096);
        for off in 0..scan_until {
            let db_scan = Arc::clone(&db);
            let key = frame_key(off);
            let exists = block_on_slate(async move {
                Ok::<bool, LogError>(db_scan.get(&key).await.map_err(slate_err)?.is_some())
            })?;
            if exists {
                next = next.max(off + 1);
            }
        }
        Ok(next)
    }

    /// Reject stale leaders; stamp a higher generation into the next durable batch.
    pub fn require_fence(&mut self, generation: u64) -> Result<(), LogError> {
        if generation < self.leader_generation {
            return Err(LogError::Slate(format!(
                "stale leader fence: our generation {generation} < stored {}",
                self.leader_generation
            )));
        }
        Ok(())
    }

    pub fn append(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        let mut frame = Vec::new();
        encode_frame(&header, &payload, &mut frame)?;
        self.append_frame(partition, header, payload, frame, None)
    }

    /// Append with an optional leadership fence token (HA M3).
    pub fn append_fenced(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
        fence_generation: Option<u64>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        if let Some(gen) = fence_generation {
            self.require_fence(gen)?;
        }
        let mut frame = Vec::new();
        encode_frame(&header, &payload, &mut frame)?;
        self.append_frame(partition, header, payload, frame, fence_generation)
    }

    fn append_frame(
        &mut self,
        partition: u32,
        header: LogRecord,
        payload: Vec<u8>,
        frame: Vec<u8>,
        fence_generation: Option<u64>,
    ) -> Result<(StoredMessage, Vec<u8>), LogError> {
        let offset = self.next_offset;
        let next = offset + 1;
        let db = Arc::clone(&self.db);
        let key = frame_key(offset);
        let frame_bytes = Bytes::from(frame.clone());
        let next_bytes = next.to_be_bytes();
        let stamp_gen = fence_generation.filter(|g| *g > self.leader_generation);
        let gen_bytes = stamp_gen.map(|g| g.to_be_bytes());

        block_on_slate(async move {
            let mut batch = WriteBatch::new();
            batch.put_bytes(Bytes::from(key), frame_bytes);
            batch.put_bytes(
                Bytes::copy_from_slice(META_NEXT_OFFSET),
                Bytes::copy_from_slice(&next_bytes),
            );
            if let Some(gb) = gen_bytes {
                batch.put_bytes(
                    Bytes::copy_from_slice(META_LEADER_GEN),
                    Bytes::copy_from_slice(&gb),
                );
            }
            let opts = durable_write_opts();
            let _h = db
                .write_with_options(batch, &opts)
                .await
                .map_err(slate_err)?;
            // Explicit flush so ACK is only after object-store durability.
            db.flush().await.map_err(slate_err)?;
            Ok::<(), LogError>(())
        })?;

        self.next_offset = next;
        if let Some(g) = stamp_gen {
            self.leader_generation = g;
        }
        let stored = stored_from_header(header, partition, offset, payload);
        Ok((stored, frame))
    }

    pub fn append_raw_frame(
        &mut self,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<StoredMessage, LogError> {
        if let Some(want) = expected_offset {
            let have = self.next_offset;
            if have != want {
                return Err(LogError::OffsetMismatch { have, want });
            }
        }
        let (header, payload) = {
            let mut cursor = std::io::Cursor::new(frame);
            decode_frame(&mut cursor)?
        };
        let (stored, _) = self.append_frame(partition, header, payload, frame.to_vec(), None)?;
        Ok(stored)
    }

    pub fn read_range(
        &self,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        let mut out = Vec::new();
        for i in 0..max_messages {
            let off = offset + i as u64;
            if off >= self.next_offset {
                break;
            }
            let db = Arc::clone(&self.db);
            let key = frame_key(off);
            let frame = block_on_slate(async move { db.get(&key).await.map_err(slate_err) })?
                .ok_or(LogError::OffsetNotFound(off))?;
            let (header, payload) = {
                let mut cursor = std::io::Cursor::new(frame.as_ref());
                decode_frame(&mut cursor)?
            };
            out.push(stored_from_header(header, partition, off, payload));
        }
        Ok(out)
    }

    pub fn high_watermark(&self) -> u64 {
        self.next_offset
    }

    pub fn purge_offset(&mut self, offset: u64) -> bool {
        if offset >= self.next_offset {
            return false;
        }
        let db = Arc::clone(&self.db);
        let key = frame_key(offset);
        block_on_slate(async move {
            let opts = durable_write_opts();
            let mut batch = WriteBatch::new();
            batch.delete(key);
            let _h = db
                .write_with_options(batch, &opts)
                .await
                .map_err(slate_err)?;
            db.flush().await.map_err(slate_err)?;
            Ok::<(), LogError>(())
        })
        .is_ok()
    }

    pub fn sync(&mut self) -> Result<(), LogError> {
        let db = Arc::clone(&self.db);
        let next = self.next_offset.to_be_bytes();
        block_on_slate(async move {
            let mut batch = WriteBatch::new();
            batch.put_bytes(
                Bytes::copy_from_slice(META_NEXT_OFFSET),
                Bytes::copy_from_slice(&next),
            );
            let opts = durable_write_opts();
            let _h = db
                .write_with_options(batch, &opts)
                .await
                .map_err(slate_err)?;
            db.flush().await.map_err(slate_err)?;
            Ok::<(), LogError>(())
        })
    }
}

#[cfg(feature = "slate")]
fn stored_from_header(
    header: LogRecord,
    partition: u32,
    offset: u64,
    payload: Vec<u8>,
) -> StoredMessage {
    StoredMessage {
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
        flow_key: header.flow_key,
        flow_rate: header.flow_rate,
        flow_period_secs: header.flow_period_secs,
        queue_id: header.queue_id,
        group_id: header.group_id,
        group_member_id: header.group_member_id,
        flow_profile_id: header.flow_profile_id,
        destination_url: header.destination_url,
        destination_secret: header.destination_secret,
        max_retries: header.max_retries,
        retry_backoff: header.retry_backoff.clone(),
        http_method: header.http_method,
        http_headers_json: header.http_headers_json,
        http_sign: header.http_sign,
        payload_ref_json: header.payload_ref_json,
    }
}

#[cfg(feature = "slate")]
fn slate_err(e: slatedb::Error) -> LogError {
    LogError::Slate(e.to_string())
}
