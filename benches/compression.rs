//! Gorilla codec throughput on regular and noisy series.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use driftdb::encoding::gorilla;

const POINTS: usize = 1_000;

/// A 10s-interval gauge that drifts slowly, like a CPU metric.
fn gauge() -> Vec<(i64, f64)> {
    let start = 1_700_000_000_000_000_000i64;
    let mut v = 40.0f64;
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    (0..POINTS)
        .map(|i| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            v += ((state >> 11) as f64 / (1u64 << 53) as f64 - 0.5) * 0.5;
            (
                start + i as i64 * 10_000_000_000,
                (v * 100.0).round() / 100.0,
            )
        })
        .collect()
}

/// A monotonically increasing counter sampled with timestamp jitter.
fn counter() -> Vec<(i64, f64)> {
    let start = 1_700_000_000_000_000_000i64;
    (0..POINTS)
        .map(|i| {
            let jitter = ((i * 7919) % 1_000) as i64 * 1_000;
            (start + i as i64 * 10_000_000_000 + jitter, (i * 17) as f64)
        })
        .collect()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla");
    group.throughput(Throughput::Elements(POINTS as u64));
    for (name, data) in [("gauge", gauge()), ("counter", counter())] {
        let block = gorilla::encode_points(&data);
        eprintln!(
            "{name}: {} points -> {} bytes ({:.2} bytes/point)",
            data.len(),
            block.len(),
            block.len() as f64 / data.len() as f64
        );
        group.bench_with_input(BenchmarkId::new("encode", name), &data, |b, d| {
            b.iter(|| gorilla::encode_points(black_box(d)))
        });
        group.bench_with_input(BenchmarkId::new("decode", name), &block, |b, blk| {
            b.iter(|| gorilla::decode_points(black_box(blk)).unwrap())
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
