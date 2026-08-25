//! Bounded, single-owner shard runtime and shared commit coordinator.

use broker_proto::{LogRecord, StoredMessage};
use broker_storage::{FsyncMode, LogError, PartitionBackend};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

fn commit_linger(mode: FsyncMode) -> Duration {
    if mode == FsyncMode::Always {
        return Duration::ZERO;
    }
    let ms = std::env::var("BETTERMQ_COMMIT_LINGER_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1u64)
        .min(20);
    Duration::from_millis(ms)
}

type Reply<T> = mpsc::SyncSender<Result<T, LogError>>;
type Appended = Vec<(StoredMessage, Vec<u8>)>;

enum ShardCommand {
    AppendBatch {
        partition: u32,
        items: Vec<(LogRecord, Vec<u8>)>,
        fence_generation: Option<u64>,
        reply: Reply<Appended>,
    },
    AppendRaw {
        partition: u32,
        frame: Vec<u8>,
        expected_offset: Option<u64>,
        reply: Reply<StoredMessage>,
    },
    ReadRange {
        partition: u32,
        offset: u64,
        max: usize,
        reply: Reply<Vec<StoredMessage>>,
    },
    HighWatermark {
        reply: Reply<u64>,
    },
    Purge {
        offset: u64,
        reply: Reply<bool>,
    },
    GcSealed {
        watermark: u64,
        reply: Reply<usize>,
    },
    Commit {
        offset: u64,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Flush {
        reply: Reply<u64>,
    },
    FlushIfDue {
        reply: Reply<()>,
    },
    FlushCount {
        reply: Reply<u64>,
    },
    InjectFsyncFailure {
        reply: Reply<()>,
    },
}

impl ShardCommand {
    fn queued_work(&self) -> Option<(u32, u64, u64)> {
        match self {
            Self::AppendBatch {
                partition, items, ..
            } => Some((
                *partition,
                items.len() as u64,
                items.iter().map(|(_, payload)| payload.len() as u64).sum(),
            )),
            Self::AppendRaw {
                partition, frame, ..
            } => Some((*partition, 1, frame.len() as u64)),
            _ => None,
        }
    }
}

/// Future returned by the commit coordinator for one requested offset.
pub struct CommitTicket {
    offset: u64,
    receiver: oneshot::Receiver<Result<u64, String>>,
}

impl CommitTicket {
    pub async fn wait(self) -> Result<u64, LogError> {
        match self.receiver.await {
            Ok(Ok(hwm)) => Ok(hwm),
            Ok(Err(message)) => Err(actor_error(message)),
            Err(_) => Err(actor_error(format!(
                "shard actor stopped while committing offset {}",
                self.offset
            ))),
        }
    }
}

/// Handle for one physical shard. A dedicated bounded actor owns the backend.
pub struct ShardHandle {
    sender: mpsc::SyncSender<ShardCommand>,
    committed_hwm: Arc<AtomicU64>,
}

impl ShardHandle {
    pub fn new(backend: PartitionBackend) -> Arc<Self> {
        let hwm = backend.committed_hwm();
        let linger = commit_linger(backend.fsync_mode());
        let queue_capacity = std::env::var("BETTERMQ_SHARD_QUEUE_CAPACITY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024usize)
            .clamp(1, 65_536);
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let handle = Arc::new(Self {
            sender,
            committed_hwm: Arc::new(AtomicU64::new(hwm)),
        });
        let actor_hwm = Arc::clone(&handle.committed_hwm);
        std::thread::Builder::new()
            .name("bettermq-shard-io".into())
            .spawn(move || run_actor(backend, receiver, actor_hwm, linger))
            .expect("failed to start shard I/O actor");
        handle
    }

    pub fn committed_hwm(&self) -> u64 {
        self.committed_hwm.load(Ordering::Acquire)
    }

    pub fn append_batch(
        &self,
        partition: u32,
        items: Vec<(LogRecord, Vec<u8>)>,
        fence_generation: Option<u64>,
    ) -> Result<Appended, LogError> {
        self.call(|reply| ShardCommand::AppendBatch {
            partition,
            items,
            fence_generation,
            reply,
        })
    }

    pub fn append_raw_frame(
        &self,
        partition: u32,
        frame: &[u8],
        expected_offset: Option<u64>,
    ) -> Result<StoredMessage, LogError> {
        self.call(|reply| ShardCommand::AppendRaw {
            partition,
            frame: frame.to_vec(),
            expected_offset,
            reply,
        })
    }

    pub fn read_range(
        &self,
        partition: u32,
        offset: u64,
        max: usize,
    ) -> Result<Vec<StoredMessage>, LogError> {
        self.call(|reply| ShardCommand::ReadRange {
            partition,
            offset,
            max,
            reply,
        })
    }

    pub fn high_watermark(&self) -> Result<u64, LogError> {
        self.call(|reply| ShardCommand::HighWatermark { reply })
    }

    pub fn purge_offset(&self, offset: u64) -> Result<bool, LogError> {
        self.call(|reply| ShardCommand::Purge { offset, reply })
    }

    pub fn gc_sealed_below(&self, watermark: u64) -> Result<usize, LogError> {
        self.call(|reply| ShardCommand::GcSealed { watermark, reply })
    }

    pub fn flush_durable(&self) -> Result<u64, LogError> {
        self.call(|reply| ShardCommand::Flush { reply })
    }

    pub fn flush_if_due(&self) -> Result<(), LogError> {
        self.call(|reply| ShardCommand::FlushIfDue { reply })
    }

    pub fn request_commit(&self, offset: u64) -> Result<CommitTicket, LogError> {
        if self.committed_hwm() > offset {
            let (sender, receiver) = oneshot::channel();
            let _ = sender.send(Ok(self.committed_hwm()));
            return Ok(CommitTicket { offset, receiver });
        }
        let (reply, receiver) = oneshot::channel();
        self.send(ShardCommand::Commit { offset, reply })?;
        Ok(CommitTicket { offset, receiver })
    }

    /// Wait until `offset` is included in the committed high watermark.
    /// Concurrent waiters are drained into one actor-owned commit epoch.
    pub async fn wait_committed(&self, offset: u64) -> Result<(), LogError> {
        self.request_commit(offset)?.wait().await?;
        Ok(())
    }

    pub fn flush_count(&self) -> Result<u64, LogError> {
        self.call(|reply| ShardCommand::FlushCount { reply })
    }

    pub fn inject_fsync_failure(&self) -> Result<(), LogError> {
        self.call(|reply| ShardCommand::InjectFsyncFailure { reply })
    }

    fn call<T>(&self, command: impl FnOnce(Reply<T>) -> ShardCommand) -> Result<T, LogError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.send(command(reply))?;
        let receive = || {
            receiver
                .recv()
                .map_err(|_| actor_error("shard actor stopped before replying"))
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(receive)
            }
            _ => receive(),
        }?
    }

    fn send(&self, command: ShardCommand) -> Result<(), LogError> {
        let queued_work = command.queued_work();
        if let Some((shard, records, bytes)) = queued_work {
            crate::telemetry::record_enqueue(shard, records, bytes);
        }
        self.sender.try_send(command).map_err(|error| {
            if let Some((shard, records, bytes)) = queued_work {
                crate::telemetry::record_dequeue(shard, records, bytes);
                crate::telemetry::record_rejected(shard);
            }
            match error {
                mpsc::TrySendError::Full(_) => LogError::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "shard owner queue is full",
                )),
                mpsc::TrySendError::Disconnected(_) => actor_error("shard actor is not running"),
            }
        })
    }
}

fn run_actor(
    mut backend: PartitionBackend,
    receiver: mpsc::Receiver<ShardCommand>,
    committed_hwm: Arc<AtomicU64>,
    linger: Duration,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            ShardCommand::Commit { offset, reply } => {
                let mut waiters = vec![(offset, reply)];
                let deadline = Instant::now() + linger;
                while Instant::now() < deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    match receiver.recv_timeout(remaining) {
                        Ok(ShardCommand::Commit { offset, reply }) => {
                            waiters.push((offset, reply));
                        }
                        Ok(other) => process_non_commit(other, &mut backend, &committed_hwm),
                        Err(mpsc::RecvTimeoutError::Timeout) => break,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                commit_waiters(&mut backend, &committed_hwm, waiters);
            }
            other => process_non_commit(other, &mut backend, &committed_hwm),
        }
    }
}

fn process_non_commit(
    command: ShardCommand,
    backend: &mut PartitionBackend,
    committed_hwm: &AtomicU64,
) {
    if let Some((shard, records, bytes)) = command.queued_work() {
        crate::telemetry::record_dequeue(shard, records, bytes);
    }
    match command {
        ShardCommand::AppendBatch {
            partition,
            items,
            fence_generation,
            reply,
        } => {
            let result = backend.append_batch(partition, items, fence_generation);
            publish_backend_hwm(backend, committed_hwm);
            let _ = reply.send(result);
        }
        ShardCommand::AppendRaw {
            partition,
            frame,
            expected_offset,
            reply,
        } => {
            let result = backend.append_raw_frame(partition, &frame, expected_offset);
            publish_backend_hwm(backend, committed_hwm);
            let _ = reply.send(result);
        }
        ShardCommand::ReadRange {
            partition,
            offset,
            max,
            reply,
        } => {
            let _ = reply.send(backend.read_range(partition, offset, max));
        }
        ShardCommand::HighWatermark { reply } => {
            let _ = reply.send(Ok(backend.high_watermark()));
        }
        ShardCommand::Purge { offset, reply } => {
            let _ = reply.send(Ok(backend.purge_offset(offset)));
        }
        ShardCommand::GcSealed { watermark, reply } => {
            let _ = reply.send(backend.gc_sealed_below(watermark));
        }
        ShardCommand::Flush { reply } => {
            let result = backend.sync().map(|()| backend.committed_hwm());
            publish_backend_hwm(backend, committed_hwm);
            let _ = reply.send(result);
        }
        ShardCommand::FlushIfDue { reply } => {
            let result = backend.flush_if_due();
            publish_backend_hwm(backend, committed_hwm);
            let _ = reply.send(result);
        }
        ShardCommand::FlushCount { reply } => {
            let _ = reply.send(Ok(backend.flush_count()));
        }
        ShardCommand::InjectFsyncFailure { reply } => {
            backend.inject_fsync_failure();
            let _ = reply.send(Ok(()));
        }
        ShardCommand::Commit { .. } => unreachable!("commit handled by coordinator"),
    }
}

fn commit_waiters(
    backend: &mut PartitionBackend,
    committed_hwm: &AtomicU64,
    waiters: Vec<(u64, oneshot::Sender<Result<u64, String>>)>,
) {
    let needs_flush = waiters
        .iter()
        .any(|(offset, _)| backend.committed_hwm() <= *offset);
    let result = if needs_flush {
        backend.sync().map(|()| backend.committed_hwm())
    } else {
        Ok(backend.committed_hwm())
    };
    publish_backend_hwm(backend, committed_hwm);
    match result {
        Ok(hwm) => {
            for (offset, waiter) in waiters {
                let result = if hwm > offset {
                    Ok(hwm)
                } else {
                    Err(format!(
                        "commit barrier stopped at {hwm}, below requested offset {offset}"
                    ))
                };
                let _ = waiter.send(result);
            }
        }
        Err(error) => {
            let message = error.to_string();
            for (_, waiter) in waiters {
                let _ = waiter.send(Err(message.clone()));
            }
        }
    }
}

fn publish_backend_hwm(backend: &PartitionBackend, committed_hwm: &AtomicU64) {
    committed_hwm.store(backend.committed_hwm(), Ordering::Release);
}

fn actor_error(message: impl Into<String>) -> LogError {
    LogError::Io(std::io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_storage::PartitionLogConfig;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn record(id: Uuid) -> LogRecord {
        LogRecord {
            id,
            tenant_id: "default".into(),
            topic: "orders".into(),
            routing_key: "lane".into(),
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

    fn handle() -> Arc<ShardHandle> {
        let dir = tempdir().unwrap().keep();
        let config = PartitionLogConfig {
            fsync: FsyncMode::Group,
            group_interval: Duration::from_secs(60),
            ..PartitionLogConfig::default()
        };
        ShardHandle::new(PartitionBackend::open_local(dir, config).unwrap())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_waiters_share_one_fsync() {
        let handle = handle();
        let appended = handle
            .append_batch(
                0,
                vec![
                    (record(Uuid::new_v4()), b"a".to_vec()),
                    (record(Uuid::new_v4()), b"b".to_vec()),
                ],
                None,
            )
            .unwrap();
        assert_eq!(appended.len(), 2);

        let first = handle.request_commit(0).unwrap();
        let second = handle.request_commit(1).unwrap();
        let (a, b) = tokio::join!(first.wait(), second.wait());
        assert_eq!(a.unwrap(), 2);
        assert_eq!(b.unwrap(), 2);
        assert_eq!(handle.flush_count().unwrap(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fsync_failure_reaches_all_epoch_waiters() {
        let handle = handle();
        handle
            .append_batch(
                0,
                vec![
                    (record(Uuid::new_v4()), b"a".to_vec()),
                    (record(Uuid::new_v4()), b"b".to_vec()),
                ],
                None,
            )
            .unwrap();
        handle.inject_fsync_failure().unwrap();

        let first = handle.request_commit(0).unwrap();
        let second = handle.request_commit(1).unwrap();
        let (a, b) = tokio::join!(first.wait(), second.wait());
        assert!(a
            .unwrap_err()
            .to_string()
            .contains("injected fsync failure"));
        assert!(b
            .unwrap_err()
            .to_string()
            .contains("injected fsync failure"));
        assert_eq!(handle.committed_hwm(), 0);
    }
}
