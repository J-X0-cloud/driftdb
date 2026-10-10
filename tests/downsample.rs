//! Downsampling and retention working together.

use std::sync::Arc;

use driftdb::downsample::run_rule;
use driftdb::{Db, DownsampleRule, ManualClock, Options, Precision, Value};

const SEC: i64 = 1_000_000_000;
const MIN: i64 = 60 * SEC;

fn open(dir: &std::path::Path, clock: Arc<ManualClock>, retention: Option<i64>) -> Db {
    Db::open(
        dir,
        Options {
            sync_wal: false,
            auto_compact: false,
            retention,
            clock,
            ..Options::default()
        },
    )
    .unwrap()
}

/// One point every 10s for `minutes` minutes on two hosts; value = second.
fn write_minutes(db: &Db, from_min: i64, minutes: i64) {
    let mut body = String::new();
    for s in (from_min * 60..(from_min + minutes) * 60).step_by(10) {
        for host in ["a", "b"] {
            body.push_str(&format!("cpu,host={host} usage={s} {}\n", s * SEC));
        }
    }
    db.write_lines(&body, Precision::Nanoseconds).unwrap();
}

fn column(db: &Db, q: &str, col: usize) -> Vec<f64> {
    let r = db.query(q).unwrap();
    r.series
        .first()
        .map(|s| {
            s.values
                .iter()
                .map(|row| row[col].as_f64().unwrap())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn rollups_are_written_once_per_window() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(20 * MIN));
    let db = open(dir.path(), clock.clone(), None);
    write_minutes(&db, 0, 20);
    let rule: DownsampleRule = "cpu:5m:mean,max,count:cpu_5m:1m".parse().unwrap();

    // now = 20m, lag = 1m: windows ending at or before 15m are complete.
    let report = run_rule(&db, &rule).unwrap();
    assert_eq!(report.windows, 6);
    assert_eq!(report.points, 18);
    assert_eq!(report.watermark, Some(15 * MIN));
    assert_eq!(
        column(&db, "SELECT usage_mean FROM cpu_5m WHERE host = 'a'", 1),
        vec![145.0, 445.0, 745.0]
    );
    assert_eq!(
        column(&db, "SELECT usage_max FROM cpu_5m WHERE host = 'b'", 1),
        vec![290.0, 590.0, 890.0]
    );

    // Running again without time passing does nothing.
    let again = run_rule(&db, &rule).unwrap();
    assert_eq!(again.points, 0);

    // More data and time: only the new window is produced.
    write_minutes(&db, 20, 5);
    clock.set(26 * MIN);
    let report = run_rule(&db, &rule).unwrap();
    assert_eq!(report.windows, 4);
    assert_eq!(report.watermark, Some(25 * MIN));
    assert_eq!(
        column(&db, "SELECT usage_count FROM cpu_5m WHERE host = 'a'", 1),
        vec![30.0; 5]
    );
}

#[test]
fn watermark_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(11 * MIN));
    let rule: DownsampleRule = "cpu:5m:last".parse().unwrap();
    {
        let db = open(dir.path(), clock.clone(), None);
        write_minutes(&db, 0, 11);
        assert_eq!(run_rule(&db, &rule).unwrap().watermark, Some(5 * MIN));
    }
    let db = open(dir.path(), clock.clone(), None);
    assert_eq!(db.downsample_watermark("cpu_5m"), Some(5 * MIN));
    assert_eq!(run_rule(&db, &rule).unwrap().points, 0);
    clock.set(16 * MIN);
    let report = run_rule(&db, &rule).unwrap();
    assert_eq!(report.points, 2);
    let r = db
        .query("SELECT usage_last FROM cpu_5m WHERE host = 'a' ORDER BY time DESC LIMIT 1")
        .unwrap();
    assert_eq!(
        r.series[0].values[0],
        vec![Value::Int(5 * MIN), Value::Float(590.0)]
    );
}

#[test]
fn retention_expires_raw_data_but_keeps_recent_rollups() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(10 * MIN));
    let db = open(dir.path(), clock.clone(), Some(30 * MIN));
    write_minutes(&db, 0, 10);
    db.flush().unwrap();
    let rule: DownsampleRule = "cpu:5m:mean:cpu_5m:1m".parse().unwrap();
    clock.set(11 * MIN);
    assert_eq!(run_rule(&db, &rule).unwrap().watermark, Some(10 * MIN));
    db.flush().unwrap();

    clock.set(35 * MIN);
    // The cutoff is now 5m: raw points before it are invisible, and so is
    // the rollup stamped 0m, while the one stamped 5m survives.
    assert_eq!(
        column(&db, "SELECT count(usage) FROM cpu WHERE host = 'a'", 1),
        vec![30.0]
    );
    assert_eq!(
        column(&db, "SELECT usage_mean FROM cpu_5m WHERE host = 'a'", 1),
        vec![445.0]
    );

    clock.set(60 * MIN);
    assert_eq!(db.enforce_retention().unwrap(), 2);
    assert!(db.query("SELECT * FROM cpu").unwrap().series.is_empty());
    assert!(db.stats().unwrap().segments.is_empty());
}

#[test]
fn rule_waits_for_first_data_before_setting_a_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(60 * MIN));
    let db = open(dir.path(), clock, None);
    let rule: DownsampleRule = "cpu:5m:count".parse().unwrap();
    assert_eq!(run_rule(&db, &rule).unwrap().watermark, None);
    assert_eq!(db.downsample_watermark("cpu_5m"), None);
    // Backfilled history is still picked up on the next run.
    write_minutes(&db, 0, 10);
    let report = run_rule(&db, &rule).unwrap();
    assert_eq!(report.points, 4);
    assert_eq!(report.watermark, Some(55 * MIN));
}
