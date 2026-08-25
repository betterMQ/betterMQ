//! Core V2 index bakeoff: RocksDB stays the production default unless Fjall
//! wins a crash-safe, durable batch comparison. This never replaces the WAL.

use broker_storage::{RocksStateIndex, StateIndex, StateWriteBatch, WriteDurability};
use fjall::{Batch, Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use std::time::Instant;
use tempfile::tempdir;

fn epoch_ops(epoch: u64, operations: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut batch = Vec::with_capacity(operations.saturating_mul(2).saturating_add(2));
    batch.push((
        format!("checkpoint:shard:007:epoch:{epoch}").into_bytes(),
        epoch.to_le_bytes().to_vec(),
    ));
    for operation in 0..operations {
        let offset = epoch
            .saturating_mul(operations as u64)
            .saturating_add(operation as u64);
        batch.push((
            format!("completion:tenant:queue:shard:007:{offset:020}").into_bytes(),
            [1u8].to_vec(),
        ));
        batch.push((
            format!("dedup:tenant:key:{offset:020}").into_bytes(),
            offset.to_le_bytes().to_vec(),
        ));
    }
    batch
}

fn rocks_ms(operations: usize, epochs: u64) -> f64 {
    let dir = tempdir().unwrap();
    let rocks = RocksStateIndex::open(dir.path()).unwrap();
    let started = Instant::now();
    for epoch in 0..epochs {
        let mut batch = StateWriteBatch::default();
        for (key, value) in epoch_ops(epoch, operations) {
            batch.put(key, value);
        }
        rocks.write(batch, WriteDurability::Sync).unwrap();
    }
    started.elapsed().as_secs_f64() * 1000.0
}

fn fjall_ms(operations: usize, epochs: u64) -> f64 {
    let dir = tempdir().unwrap();
    let keyspace: Keyspace = Config::new(dir.path())
        .manual_journal_persist(true)
        .open()
        .unwrap();
    let partition: PartitionHandle = keyspace
        .open_partition("state", PartitionCreateOptions::default())
        .unwrap();
    let started = Instant::now();
    for epoch in 0..epochs {
        let ops = epoch_ops(epoch, operations);
        let mut batch = Batch::with_capacity(keyspace.clone(), ops.len())
            .durability(Some(PersistMode::SyncData));
        for (key, value) in ops {
            batch.insert(&partition, key, value);
        }
        batch.commit().unwrap();
    }
    started.elapsed().as_secs_f64() * 1000.0
}

#[test]
fn rocksdb_remains_default_unless_fjall_is_clearly_faster() {
    let rocks = rocks_ms(50, 40);
    let fjall = fjall_ms(50, 40);
    eprintln!("fjall_bakeoff rocks_sync_ms={rocks:.2} fjall_sync_ms={fjall:.2}");
    // Keep RocksDB as the production index/Raft store. Fjall must be at least
    // 25% faster on durable batches to justify replacing a proven engine.
    // The message WAL is never a candidate.
    let adopt_fjall = fjall * 1.25 < rocks;
    eprintln!(
        "fjall_bakeoff decision={} (WAL unchanged; indexes/Raft only)",
        if adopt_fjall {
            "revisit_fjall"
        } else {
            "keep_rocksdb"
        }
    );
    assert!(rocks > 0.0 && fjall > 0.0);
}
