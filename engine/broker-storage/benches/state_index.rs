use broker_storage::{RocksStateIndex, StateIndex, StateWriteBatch, WriteDurability};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fjall::{Batch, Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use tempfile::tempdir;

fn epoch_operations(epoch: u64, operations: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
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
    batch.push((
        b"cursor:tenant:queue:shard:007".to_vec(),
        epoch
            .saturating_mul(operations as u64)
            .to_le_bytes()
            .to_vec(),
    ));
    batch
}

fn rocks_batch(operations: Vec<(Vec<u8>, Vec<u8>)>) -> StateWriteBatch {
    let mut batch = StateWriteBatch::default();
    for (key, value) in operations {
        batch.put(key, value);
    }
    batch
}

fn fjall_batch(
    keyspace: &Keyspace,
    partition: &PartitionHandle,
    operations: Vec<(Vec<u8>, Vec<u8>)>,
    durability: WriteDurability,
) {
    let mode = match durability {
        WriteDurability::Derived => PersistMode::Buffer,
        WriteDurability::Sync => PersistMode::SyncData,
    };
    let mut batch = Batch::with_capacity(keyspace.clone(), operations.len()).durability(Some(mode));
    for (key, value) in operations {
        batch.insert(partition, key, value);
    }
    batch.commit().unwrap();
}

fn bench_state_index(c: &mut Criterion) {
    let rocks_dir = tempdir().unwrap();
    let rocks = RocksStateIndex::open(rocks_dir.path()).unwrap();
    let fjall_dir = tempdir().unwrap();
    let fjall = Config::new(fjall_dir.path())
        .manual_journal_persist(true)
        .open()
        .unwrap();
    let fjall_state = fjall
        .open_partition("state", PartitionCreateOptions::default())
        .unwrap();

    for durability in [WriteDurability::Derived, WriteDurability::Sync] {
        let durability_name = match durability {
            WriteDurability::Derived => "derived",
            WriteDurability::Sync => "sync_data",
        };
        let mut group = c.benchmark_group(format!("state_epoch_{durability_name}"));
        for engine in ["rocksdb", "fjall"] {
            for operations in [1usize, 100, 1000] {
                group.throughput(Throughput::Elements(operations as u64));
                let mut epoch = 0u64;
                group.bench_with_input(
                    BenchmarkId::new(engine, operations),
                    &operations,
                    |bencher, &operations| {
                        bencher.iter(|| {
                            epoch = epoch.saturating_add(1);
                            let workload = black_box(epoch_operations(epoch, operations));
                            match engine {
                                "rocksdb" => {
                                    rocks.write(rocks_batch(workload), durability).unwrap()
                                }
                                "fjall" => fjall_batch(&fjall, &fjall_state, workload, durability),
                                _ => unreachable!(),
                            }
                        });
                    },
                );
            }
        }
        group.finish();
    }

    let mut preload = Vec::with_capacity(10_000);
    for offset in 0..10_000u64 {
        preload.push((
            format!("lookup:tenant:shard:007:{offset:020}").into_bytes(),
            offset.to_le_bytes().to_vec(),
        ));
    }
    rocks
        .write(rocks_batch(preload.clone()), WriteDurability::Derived)
        .unwrap();
    fjall_batch(&fjall, &fjall_state, preload, WriteDurability::Derived);

    for engine in ["rocksdb", "fjall"] {
        c.bench_function(&format!("state_point_lookup/{engine}"), |bencher| {
            let mut offset = 0u64;
            bencher.iter(|| {
                offset = (offset + 7919) % 10_000;
                let key = format!("lookup:tenant:shard:007:{offset:020}");
                let value = match engine {
                    "rocksdb" => rocks.get(key.as_bytes()).unwrap(),
                    "fjall" => fjall_state
                        .get(key.as_bytes())
                        .unwrap()
                        .map(|value| value.to_vec()),
                    _ => unreachable!(),
                };
                black_box(value);
            });
        });
        c.bench_function(&format!("state_prefix_scan_1000/{engine}"), |bencher| {
            bencher.iter(|| {
                let values = match engine {
                    "rocksdb" => rocks
                        .scan_prefix(b"lookup:tenant:shard:007:", 1000)
                        .unwrap()
                        .len(),
                    "fjall" => fjall_state
                        .prefix(b"lookup:tenant:shard:007:")
                        .take(1000)
                        .collect::<fjall::Result<Vec<_>>>()
                        .unwrap()
                        .len(),
                    _ => unreachable!(),
                };
                black_box(values);
            });
        });
    }
}

criterion_group!(benches, bench_state_index);
criterion_main!(benches);
