//! Property-based tests: codecs round-trip and the engine agrees with a
//! trivially correct in-memory model under arbitrary operation sequences.

use std::collections::{BTreeMap, HashMap};

use proptest::collection::vec;
use proptest::prelude::*;

use driftdb::encoding::gorilla;
use driftdb::encoding::varint::{put_ivarint, put_uvarint, Cursor};
use driftdb::lineproto::parse_lines;
use driftdb::query::time::{format_duration, format_rfc3339, parse_duration, parse_rfc3339};
use driftdb::storage::merge::merge_sorted;
use driftdb::storage::segment::{Segment, SegmentWriter};
use driftdb::{Db, Options, Point, Precision, SeriesKey, Value};

fn config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        ..ProptestConfig::default()
    }
}

/// Timestamps that look like real data: a start plus small, mostly regular
/// steps, occasionally jittered or jumping.
fn realistic_series() -> impl Strategy<Value = Vec<(i64, f64)>> {
    (
        any::<i64>(),
        vec((0i64..3, -1_000i64..1_000, any::<f64>(), 0u8..4), 0..400),
    )
        .prop_map(|(start, steps)| {
            let mut ts = start;
            let mut val = 0.0f64;
            steps
                .into_iter()
                .map(|(kind, jitter, raw, vkind)| {
                    ts = ts.wrapping_add(match kind {
                        0 => 10_000_000_000,
                        1 => 10_000_000_000 + jitter,
                        _ => jitter.wrapping_mul(1_000_003),
                    });
                    val = match vkind {
                        0 => val,
                        1 => val + 0.25,
                        2 => (val * 1.5).round(),
                        _ => raw,
                    };
                    (ts, val)
                })
                .collect()
        })
}

fn name_strategy() -> impl Strategy<Value = String> {
    // Includes every character that needs escaping in line protocol.
    "[a-z][a-z0-9 ,=\\\\_.-]{0,8}".prop_filter("reserved", |s| s != "time")
}

proptest! {
    #![proptest_config(config(256))]

    #[test]
    fn gorilla_roundtrips_arbitrary_points(points in vec((any::<i64>(), any::<u64>()), 0..300)) {
        let pts: Vec<(i64, f64)> = points.iter().map(|&(t, b)| (t, f64::from_bits(b))).collect();
        let decoded = gorilla::decode_points(&gorilla::encode_points(&pts)).unwrap();
        prop_assert_eq!(decoded.len(), pts.len());
        for (a, b) in decoded.iter().zip(&pts) {
            prop_assert_eq!(a.0, b.0);
            prop_assert_eq!(a.1.to_bits(), b.1.to_bits());
        }
    }

    #[test]
    fn gorilla_roundtrips_realistic_series(pts in realistic_series()) {
        let block = gorilla::encode_points(&pts);
        let decoded = gorilla::decode_points(&block).unwrap();
        prop_assert_eq!(decoded.len(), pts.len());
        for (a, b) in decoded.iter().zip(&pts) {
            prop_assert_eq!(a.0, b.0);
            prop_assert_eq!(a.1.to_bits(), b.1.to_bits());
        }
    }

    #[test]
    fn gorilla_never_panics_on_garbage(bytes in vec(any::<u8>(), 0..200)) {
        let _ = gorilla::decode_points(&bytes);
    }

    #[test]
    fn varints_roundtrip(u in vec(any::<u64>(), 0..50), i in vec(any::<i64>(), 0..50)) {
        let mut buf = Vec::new();
        for &v in &u { put_uvarint(&mut buf, v); }
        for &v in &i { put_ivarint(&mut buf, v); }
        let mut cur = Cursor::new(&buf);
        for &v in &u { prop_assert_eq!(cur.uvarint().unwrap(), v); }
        for &v in &i { prop_assert_eq!(cur.ivarint().unwrap(), v); }
        prop_assert!(cur.is_empty());
    }

    #[test]
    fn line_protocol_roundtrips(
        measurement in name_strategy(),
        tags in proptest::collection::btree_map(name_strategy(), name_strategy(), 0..4),
        field in name_strategy(),
        value in any::<f64>().prop_filter("finite", |v| v.is_finite()),
        ts in any::<i64>(),
    ) {
        let key = SeriesKey::new(measurement, tags, field).unwrap();
        let point = Point::new(key, ts, value);
        let parsed = parse_lines(&point.to_line(), Precision::Nanoseconds, 0).unwrap();
        prop_assert_eq!(parsed, vec![point]);
    }

    #[test]
    fn line_protocol_never_panics(body in "[a-z ,=\\\\\"0-9.\\n#ifte-]{0,60}") {
        let _ = parse_lines(&body, Precision::Seconds, 0);
    }

    #[test]
    fn merge_matches_last_writer_wins_model(
        runs in vec(proptest::collection::btree_map(-50i64..50, -100i32..100, 0..30), 0..6)
    ) {
        let mut model = BTreeMap::new();
        let sources: Vec<Vec<(i64, f64)>> = runs
            .iter()
            .map(|run| {
                for (&t, &v) in run { model.insert(t, f64::from(v)); }
                run.iter().map(|(&t, &v)| (t, f64::from(v))).collect()
            })
            .collect();
        let expected: Vec<(i64, f64)> = model.into_iter().collect();
        prop_assert_eq!(merge_sorted(sources), expected);
    }

    #[test]
    fn rfc3339_roundtrips(ts in any::<i64>()) {
        prop_assert_eq!(parse_rfc3339(&format_rfc3339(ts)), Some(ts));
    }

    #[test]
    fn durations_roundtrip(d in 1i64..i64::MAX) {
        prop_assert_eq!(parse_duration(&format_duration(d)), Some(d));
    }
}

proptest! {
    #![proptest_config(config(48))]

    #[test]
    fn segments_roundtrip(series in proptest::collection::btree_map(
        any::<u64>(),
        proptest::collection::btree_map(any::<i64>(), any::<i32>(), 1..1500),
        0..5,
    )) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.sst");
        let mut w = SegmentWriter::create(&path).unwrap();
        for (id, pts) in &series {
            let pts: Vec<(i64, f64)> = pts.iter().map(|(&t, &v)| (t, f64::from(v))).collect();
            w.add_series(*id, &pts).unwrap();
        }
        w.finish().unwrap();
        let seg = Segment::open(&path, 1).unwrap();
        for (id, pts) in &series {
            let expected: Vec<(i64, f64)> = pts.iter().map(|(&t, &v)| (t, f64::from(v))).collect();
            prop_assert_eq!(seg.read_all(*id).unwrap(), expected.clone());
            // A sub-range read must equal filtering the full series.
            let (lo, hi) = (expected[0].0 / 2, expected[expected.len() / 2].0);
            let filtered: Vec<(i64, f64)> =
                expected.iter().copied().filter(|(t, _)| *t >= lo && *t < hi).collect();
            prop_assert_eq!(seg.read_range(*id, lo, hi).unwrap(), filtered);
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    Write(Vec<(u8, i64, i32)>),
    Flush,
    Compact,
    CompactAll,
    Reopen,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => vec((0u8..4, 0i64..200, -1000i32..1000), 1..20).prop_map(Op::Write),
        2 => Just(Op::Flush),
        1 => Just(Op::Compact),
        1 => Just(Op::CompactAll),
        1 => Just(Op::Reopen),
    ]
}

fn key(i: u8) -> SeriesKey {
    SeriesKey::new("m", [("s", format!("{i}"))], "v").unwrap()
}

fn engine_options() -> Options {
    Options {
        sync_wal: false,
        auto_compact: false,
        compaction_fanout: 2,
        memtable_bytes: 2_000,
        ..Options::default()
    }
}

proptest! {
    #![proptest_config(config(40))]

    /// Whatever mix of writes, flushes, compactions and restarts happens,
    /// reads return exactly what a last-writer-wins map would.
    #[test]
    fn engine_matches_model(ops in vec(op_strategy(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Some(Db::open(dir.path(), engine_options()).unwrap());
        let mut model: HashMap<u8, BTreeMap<i64, f64>> = HashMap::new();
        for op in ops {
            let d = db.as_ref().unwrap();
            match op {
                Op::Write(batch) => {
                    let points: Vec<Point> = batch
                        .iter()
                        .map(|&(s, t, v)| Point::new(key(s), t, f64::from(v)))
                        .collect();
                    d.write_points(&points).unwrap();
                    for (s, t, v) in batch {
                        model.entry(s).or_default().insert(t, f64::from(v));
                    }
                }
                Op::Flush => { d.flush().unwrap(); }
                Op::Compact => { d.compact().unwrap(); }
                Op::CompactAll => { d.compact_all().unwrap(); }
                Op::Reopen => {
                    drop(db.take());
                    db = Some(Db::open(dir.path(), engine_options()).unwrap());
                }
            }
        }
        let d = db.as_ref().unwrap();
        for s in 0u8..4 {
            let expected: Vec<(i64, f64)> =
                model.get(&s).map(|m| m.iter().map(|(&t, &v)| (t, v)).collect()).unwrap_or_default();
            let id = d.with_index(|idx| idx.id_of(&key(s)));
            let actual = match id {
                Some(id) => d.read_series(&[id], i64::MIN, i64::MAX).unwrap().remove(0),
                None => Vec::new(),
            };
            prop_assert_eq!(actual, expected);
        }
    }

    /// GROUP BY time() aggregates equal a direct computation over the model.
    #[test]
    fn windowed_aggregates_match_model(
        points in proptest::collection::btree_map(0i64..10_000, -500i32..500, 1..300),
        interval in 1i64..2_000,
        flush_at in 0usize..300,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), engine_options()).unwrap();
        let all: Vec<(i64, i32)> = points.iter().map(|(&t, &v)| (t, v)).collect();
        let (first, second) = all.split_at(flush_at.min(all.len()));
        for chunk in [first, second] {
            let batch: Vec<Point> = chunk
                .iter()
                .map(|&(t, v)| Point::new(SeriesKey::new("m", Vec::<(String, String)>::new(), "v").unwrap(), t, f64::from(v)))
                .collect();
            db.write_points(&batch).unwrap();
            db.flush().unwrap();
        }
        let r = db
            .query(&format!(
                "SELECT count(v), sum(v), min(v), max(v), first(v), last(v) FROM m GROUP BY time({interval}ns) FILL(none)"
            ))
            .unwrap();
        let mut windows: BTreeMap<i64, Vec<(i64, f64)>> = BTreeMap::new();
        for &(t, v) in &all {
            windows.entry(t.div_euclid(interval) * interval).or_default().push((t, f64::from(v)));
        }
        let rows = &r.series[0].values;
        prop_assert_eq!(rows.len(), windows.len());
        for (row, (start, pts)) in rows.iter().zip(windows) {
            let vals: Vec<f64> = pts.iter().map(|p| p.1).collect();
            let expected = vec![
                Value::Int(start),
                Value::Float(vals.len() as f64),
                Value::Float(vals.iter().sum()),
                Value::Float(vals.iter().copied().fold(f64::INFINITY, f64::min)),
                Value::Float(vals.iter().copied().fold(f64::NEG_INFINITY, f64::max)),
                Value::Float(pts[0].1),
                Value::Float(pts[pts.len() - 1].1),
            ];
            prop_assert_eq!(row, &expected);
        }
    }
}
