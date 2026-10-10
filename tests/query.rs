//! End-to-end query tests: line protocol in, query results out.

use std::sync::Arc;

use driftdb::{Db, ManualClock, Options, Precision, QueryResult, Value};

const SEC: i64 = 1_000_000_000;
const MIN: i64 = 60 * SEC;

fn open(dir: &std::path::Path, now: i64) -> (Db, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(now));
    let db = Db::open(
        dir,
        Options {
            sync_wal: false,
            auto_compact: false,
            clock: clock.clone(),
            ..Options::default()
        },
    )
    .unwrap();
    (db, clock)
}

/// Two hosts reporting every 30s for 10 minutes; host b reports double.
fn seed(db: &Db) {
    let mut body = String::new();
    for i in 0..20 {
        let ts = i * 30 * SEC;
        body.push_str(&format!(
            "cpu,host=a,dc=east usage={},idle={} {ts}\n",
            i,
            100 - i
        ));
        body.push_str(&format!("cpu,host=b,dc=west usage={} {ts}\n", i * 2));
    }
    body.push_str("mem,host=a free=512 0\n");
    db.write_lines(&body, Precision::Nanoseconds).unwrap();
}

fn floats(r: &QueryResult, series: usize, col: usize) -> Vec<Option<f64>> {
    r.series[series]
        .values
        .iter()
        .map(|row| row[col].as_f64())
        .collect()
}

/// Cells `cols` of the first row of `series`, as floats.
fn first_row(r: &QueryResult, series: usize, cols: std::ops::Range<usize>) -> Vec<Option<f64>> {
    let row = &r.series[series].values[0];
    cols.map(|c| row[c].as_f64()).collect()
}

fn times(r: &QueryResult, series: usize) -> Vec<i64> {
    r.series[series]
        .values
        .iter()
        .map(|row| row[0].as_i64().unwrap())
        .collect()
}

#[test]
fn raw_select_with_tag_and_time_filters() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    seed(&db);
    let r = db
        .query("SELECT usage FROM cpu WHERE host = 'b' AND time >= 60s AND time < 150s")
        .unwrap();
    assert_eq!(r.series.len(), 1);
    assert_eq!(r.series[0].columns, vec!["time", "usage"]);
    assert_eq!(times(&r, 0), vec![60 * SEC, 90 * SEC, 120 * SEC]);
    assert_eq!(floats(&r, 0, 1), vec![Some(4.0), Some(6.0), Some(8.0)]);
}

#[test]
fn wildcard_aligns_fields_of_one_series() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    seed(&db);
    let r = db
        .query("SELECT * FROM cpu WHERE host = 'a' LIMIT 2")
        .unwrap();
    assert_eq!(r.series[0].columns, vec!["time", "idle", "usage"]);
    assert_eq!(
        r.series[0].values[1],
        vec![Value::Int(30 * SEC), Value::Float(99.0), Value::Float(1.0)]
    );
    // Host b has no idle field: its rows carry nulls there.
    let r = db
        .query("SELECT * FROM cpu WHERE host = 'b' LIMIT 1")
        .unwrap();
    assert_eq!(r.series[0].values[0][1], Value::Null);
}

#[test]
fn aggregates_over_whole_range_and_by_tag() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    seed(&db);
    let r = db
        .query("SELECT count(usage), sum(usage), min(usage), max(usage), mean(usage) FROM cpu")
        .unwrap();
    assert_eq!(r.series.len(), 1);
    assert_eq!(
        first_row(&r, 0, 1..6),
        vec![Some(40.0), Some(570.0), Some(0.0), Some(38.0), Some(14.25)]
    );

    let r = db
        .query("SELECT last(usage) AS latest, spread(usage) FROM cpu GROUP BY host")
        .unwrap();
    assert_eq!(r.series.len(), 2);
    assert_eq!(r.series[0].tags["host"], "a");
    assert_eq!(r.series[0].columns, vec!["time", "latest", "spread"]);
    assert_eq!(r.series[0].values[0][1], Value::Float(19.0));
    assert_eq!(r.series[1].values[0][1], Value::Float(38.0));
    assert_eq!(r.series[1].values[0][2], Value::Float(38.0));
}

#[test]
fn group_by_time_windows() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    seed(&db);
    let r = db
        .query("SELECT mean(usage), count(usage) FROM cpu WHERE host = 'a' GROUP BY time(2m)")
        .unwrap();
    assert_eq!(times(&r, 0), vec![0, 2 * MIN, 4 * MIN, 6 * MIN, 8 * MIN]);
    assert_eq!(
        floats(&r, 0, 1),
        vec![Some(1.5), Some(5.5), Some(9.5), Some(13.5), Some(17.5)]
    );
    assert_eq!(floats(&r, 0, 2), vec![Some(4.0); 5]);

    // Offset windows shift the alignment.
    let r = db
        .query("SELECT count(usage) FROM cpu WHERE host = 'a' GROUP BY time(2m, 1m)")
        .unwrap();
    assert_eq!(times(&r, 0)[0], -MIN);
    assert_eq!(floats(&r, 0, 1)[0], Some(2.0));
}

#[test]
fn fill_policies_cover_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    db.write_lines("m v=1 0\nm v=4 180000000000\n", Precision::Nanoseconds)
        .unwrap();
    let q = |fill: &str| {
        db.query(&format!(
            "SELECT max(v) FROM m WHERE time >= 0 AND time < 4m GROUP BY time(1m) FILL({fill})"
        ))
        .unwrap()
    };
    assert_eq!(
        floats(&q("null"), 0, 1),
        vec![Some(1.0), None, None, Some(4.0)]
    );
    assert_eq!(floats(&q("none"), 0, 1), vec![Some(1.0), Some(4.0)]);
    assert_eq!(
        floats(&q("-1"), 0, 1),
        vec![Some(1.0), Some(-1.0), Some(-1.0), Some(4.0)]
    );
    assert_eq!(
        floats(&q("previous"), 0, 1),
        vec![Some(1.0), Some(1.0), Some(1.0), Some(4.0)]
    );
    assert_eq!(
        floats(&q("linear"), 0, 1),
        vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]
    );

    // count() reports zero for empty windows instead of null.
    let r = db
        .query("SELECT count(v) FROM m WHERE time >= 0 AND time < 4m GROUP BY time(1m)")
        .unwrap();
    assert_eq!(
        floats(&r, 0, 1),
        vec![Some(1.0), Some(0.0), Some(0.0), Some(1.0)]
    );
}

#[test]
fn ordering_limits_and_relative_time() {
    let dir = tempfile::tempdir().unwrap();
    let (db, clock) = open(dir.path(), 10 * MIN);
    seed(&db);
    let r = db
        .query("SELECT usage FROM cpu WHERE host = 'a' AND time > now() - 2m ORDER BY time DESC LIMIT 3")
        .unwrap();
    assert_eq!(times(&r, 0), vec![570 * SEC, 540 * SEC, 510 * SEC]);

    clock.set(5 * MIN);
    let r = db
        .query(
            "SELECT count(usage) FROM cpu WHERE host = 'a' AND time >= now() - 1m AND time < now()",
        )
        .unwrap();
    assert_eq!(floats(&r, 0, 1), vec![Some(2.0)]);
    assert_eq!(times(&r, 0), vec![4 * MIN]);
}

#[test]
fn queries_merge_memtable_and_segments() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    seed(&db);
    db.flush().unwrap();
    // Correct one value and add a new one after the flush.
    db.write_lines(
        "cpu,host=a,dc=east usage=100 0\ncpu,host=a,dc=east usage=20 600000000000\n",
        Precision::Nanoseconds,
    )
    .unwrap();
    let r = db
        .query("SELECT first(usage), last(usage), count(usage) FROM cpu WHERE host = 'a'")
        .unwrap();
    assert_eq!(
        first_row(&r, 0, 1..4),
        vec![Some(100.0), Some(20.0), Some(21.0)]
    );
    db.flush().unwrap();
    db.compact_all().unwrap();
    let r2 = db
        .query("SELECT first(usage), last(usage), count(usage) FROM cpu WHERE host = 'a'")
        .unwrap();
    assert_eq!(r, r2);
}

#[test]
fn show_statements_list_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    seed(&db);
    let col0 = |r: QueryResult, i: usize| -> Vec<String> {
        r.series[i]
            .values
            .iter()
            .map(|row| match &row[row.len() - 1] {
                Value::Str(s) => s.clone(),
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    };
    assert_eq!(
        col0(db.query("SHOW MEASUREMENTS").unwrap(), 0),
        vec!["cpu", "mem"]
    );
    assert_eq!(
        col0(db.query("SHOW TAG KEYS FROM cpu").unwrap(), 0),
        vec!["dc", "host"]
    );
    assert_eq!(
        col0(db.query("SHOW TAG VALUES WITH KEY = host").unwrap(), 0),
        vec!["a", "b"]
    );
    assert_eq!(
        col0(db.query("SHOW FIELD KEYS FROM cpu").unwrap(), 0),
        vec!["idle", "usage"]
    );
    assert_eq!(
        col0(db.query("SHOW SERIES").unwrap(), 0),
        vec!["cpu,dc=east,host=a", "cpu,dc=west,host=b", "mem,host=a"]
    );
    assert_eq!(
        col0(
            db.query("SHOW SERIES FROM cpu WHERE dc =~ /we/").unwrap(),
            0
        ),
        vec!["cpu,dc=west,host=b"]
    );
    assert!(db
        .query("SHOW TAG KEYS FROM nothing")
        .unwrap()
        .series
        .is_empty());
}

#[test]
fn explain_describes_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    seed(&db);
    db.flush().unwrap();
    let r = db
        .query("EXPLAIN SELECT mean(usage) FROM cpu WHERE dc = 'east' AND time >= 1h GROUP BY time(1m), host")
        .unwrap();
    let text: Vec<String> = r.series[0]
        .values
        .iter()
        .map(|row| match &row[0] {
            Value::Str(s) => s.clone(),
            _ => unreachable!(),
        })
        .collect();
    let text = text.join("\n");
    assert!(
        text.contains("Series: 1 matched in 1 group(s) by host"),
        "{text}"
    );
    assert!(text.contains("0 of 1 segment(s)"), "{text}");
}

#[test]
fn empty_and_missing_data() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    assert!(db.query("SELECT v FROM nothing").unwrap().series.is_empty());
    assert!(db
        .query("SELECT mean(v) FROM nothing GROUP BY time(1m)")
        .unwrap()
        .series
        .is_empty());
    seed(&db);
    assert!(db.query("SELECT nope FROM cpu").unwrap().series.is_empty());
    assert!(db
        .query("SELECT usage FROM cpu WHERE time > 1h")
        .unwrap()
        .series
        .is_empty());
    let err = db
        .query("SELECT mean(usage) FROM cpu GROUP BY time(1ns) FILL(0)")
        .unwrap_err();
    assert!(err.to_string().contains("windows"), "{err}");
    assert!(err.is_client_error());
}

#[test]
fn json_shape_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _) = open(dir.path(), 10 * MIN);
    db.write_lines("m,host=a v=1.5 1000", Precision::Nanoseconds)
        .unwrap();
    let r = db.query("SELECT v FROM m GROUP BY host").unwrap();
    assert_eq!(
        serde_json::to_string(&r).unwrap(),
        r#"{"series":[{"name":"m","tags":{"host":"a"},"columns":["time","v"],"values":[[1000,1.5]]}]}"#
    );
}
