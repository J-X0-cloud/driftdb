//! Engine ingest and query throughput.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use driftdb::{Db, Options, Point, Precision, SeriesKey};

const HOSTS: usize = 50;
const POINTS_PER_HOST: usize = 2_000;
const STEP: i64 = 10_000_000_000;

fn options() -> Options {
    Options {
        sync_wal: false,
        auto_compact: false,
        ..Options::default()
    }
}

fn batch(round: usize) -> Vec<Point> {
    (0..HOSTS)
        .flat_map(|h| {
            let key = SeriesKey::new("cpu", [("host", format!("host-{h:03}"))], "usage").unwrap();
            (0..100).map(move |i| {
                let n = round * 100 + i;
                Point::new(key.clone(), n as i64 * STEP, (n % 97) as f64 + h as f64)
            })
        })
        .collect()
}

fn loaded_db(dir: &std::path::Path) -> Db {
    let db = Db::open(dir, options()).unwrap();
    for round in 0..POINTS_PER_HOST / 100 {
        db.write_points(&batch(round)).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    db
}

/// Steady-state ingest into an open database whose series already exist.
/// Batches cycle, so later iterations overwrite earlier timestamps; the
/// memtable flushes to segments whenever it reaches its size limit.
fn ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest");
    group.sample_size(30);
    group.throughput(Throughput::Elements((HOSTS * 100) as u64));
    let batches: Vec<Vec<Point>> = (0..20).map(batch).collect();

    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), options()).unwrap();
    db.write_points(&batches[0]).unwrap();
    let mut round = 0usize;
    group.bench_function("write_points_batch_5000", |b| {
        b.iter(|| {
            db.write_points(&batches[round % batches.len()]).unwrap();
            round += 1;
        })
    });

    let bodies: Vec<String> = batches
        .iter()
        .map(|batch| batch.iter().map(|p| format!("{}\n", p.to_line())).collect())
        .collect();
    let mut round = 0usize;
    group.bench_function("write_lines_batch_5000", |b| {
        b.iter(|| {
            db.write_lines(&bodies[round % bodies.len()], Precision::Nanoseconds)
                .unwrap();
            round += 1;
        })
    });
    group.finish();
}

fn query(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let db = loaded_db(dir.path());
    let total = (HOSTS * POINTS_PER_HOST) as u64;
    let mut group = c.benchmark_group("query");
    group.sample_size(30);
    group.throughput(Throughput::Elements(total));
    group.bench_function("mean_by_5m_window_all_hosts", |b| {
        b.iter(|| {
            db.query("SELECT mean(usage) FROM cpu GROUP BY time(5m)")
                .unwrap()
        })
    });
    group.bench_function("max_by_host", |b| {
        b.iter(|| {
            db.query("SELECT max(usage) FROM cpu GROUP BY host")
                .unwrap()
        })
    });
    group.throughput(Throughput::Elements(POINTS_PER_HOST as u64));
    group.bench_function("raw_scan_one_host", |b| {
        b.iter(|| {
            db.query("SELECT usage FROM cpu WHERE host = 'host-007'")
                .unwrap()
        })
    });
    group.finish();
}

criterion_group!(benches, ingest, query);
criterion_main!(benches);
