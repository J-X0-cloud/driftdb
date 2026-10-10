//! Crash-recovery, corruption and concurrency tests for the engine.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use driftdb::{Db, Error, Options, Point, Precision, SeriesKey};

fn opts() -> Options {
    Options {
        sync_wal: true,
        auto_compact: false,
        ..Options::default()
    }
}

fn key(host: &str) -> SeriesKey {
    SeriesKey::new("cpu", [("host", host)], "usage").unwrap()
}

fn read(db: &Db, host: &str) -> Vec<(i64, f64)> {
    match db.with_index(|i| i.id_of(&key(host))) {
        Some(id) => db.read_series(&[id], i64::MIN, i64::MAX).unwrap().remove(0),
        None => Vec::new(),
    }
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else if entry.file_name() != "LOCK" {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn wal_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    files
}

#[test]
fn torn_wal_tail_loses_only_the_last_batch() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), opts()).unwrap();
        for i in 0..10 {
            db.write_points(&[Point::new(key("a"), i, i as f64)])
                .unwrap();
        }
    }
    let wal = wal_files(dir.path()).pop().unwrap();
    let len = fs::metadata(&wal).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&wal)
        .unwrap()
        .set_len(len - 3)
        .unwrap();

    let db = Db::open(dir.path(), opts()).unwrap();
    let pts = read(&db, "a");
    assert_eq!(pts.len(), 9);
    assert_eq!(pts.last(), Some(&(8, 8.0)));
    // The log keeps working after recovery.
    db.write_points(&[Point::new(key("a"), 100, 1.0)]).unwrap();
    drop(db);
    let db = Db::open(dir.path(), opts()).unwrap();
    assert_eq!(read(&db, "a").len(), 10);
}

#[test]
fn crash_between_segment_write_and_manifest_update() {
    let dir = tempfile::tempdir().unwrap();
    let crashed = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    let batch: Vec<Point> = (0..500)
        .map(|i| Point::new(key("a"), i, i as f64))
        .collect();
    db.write_points(&batch).unwrap();

    // Snapshot the directory as it was just before the flush...
    copy_dir(dir.path(), crashed.path());
    db.flush().unwrap();
    // ...then add the segment the flush produced, as if the process died
    // after renaming it into place but before the manifest was rewritten.
    for entry in fs::read_dir(dir.path().join("segments")).unwrap() {
        let entry = entry.unwrap();
        fs::copy(
            entry.path(),
            crashed.path().join("segments").join(entry.file_name()),
        )
        .unwrap();
    }

    let recovered = Db::open(crashed.path(), opts()).unwrap();
    assert_eq!(read(&recovered, "a"), read(&db, "a"));
    assert_eq!(
        recovered.stats().unwrap().segments.len(),
        0,
        "orphan must not be adopted"
    );
    assert_eq!(
        fs::read_dir(crashed.path().join("segments"))
            .unwrap()
            .count(),
        0
    );
    // Flushing the replayed WAL produces the segment for real.
    recovered.flush().unwrap();
    drop(recovered);
    let reopened = Db::open(crashed.path(), opts()).unwrap();
    assert_eq!(read(&reopened, "a").len(), 500);
    assert_eq!(reopened.stats().unwrap().memtable_points, 0);
}

#[test]
fn flushed_wal_files_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), opts()).unwrap();
    for round in 0..3 {
        db.write_points(&[Point::new(key("a"), round, 1.0)])
            .unwrap();
        db.flush().unwrap();
    }
    assert_eq!(wal_files(dir.path()).len(), 1);
    assert_eq!(db.stats().unwrap().wal_bytes, 0);
}

#[test]
fn corrupt_segment_block_surfaces_an_error() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), opts()).unwrap();
        let batch: Vec<Point> = (0..100)
            .map(|i| Point::new(key("a"), i, i as f64 * 0.5))
            .collect();
        db.write_points(&batch).unwrap();
        db.flush().unwrap();
    }
    let seg = fs::read_dir(dir.path().join("segments"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = fs::read(&seg).unwrap();
    bytes[20] ^= 0x55;
    fs::write(&seg, bytes).unwrap();

    let db = Db::open(dir.path(), opts()).unwrap();
    let id = db.with_index(|i| i.id_of(&key("a"))).unwrap();
    let err = db.read_series(&[id], 0, 1_000).unwrap_err();
    assert!(matches!(err, Error::Corruption(_)), "{err}");
    let err = db.query("SELECT count(usage) FROM cpu").unwrap_err();
    assert!(!err.is_client_error());
}

#[test]
fn unknown_series_in_wal_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), opts()).unwrap();
        db.write_points(&[Point::new(key("a"), 1, 1.0)]).unwrap();
    }
    // Lose the catalog but keep the WAL that references series 0.
    fs::write(dir.path().join("series.log"), b"").unwrap();
    assert!(matches!(
        Db::open(dir.path(), opts()),
        Err(Error::Corruption(_))
    ));
}

#[test]
fn concurrent_writers_readers_and_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Db::open(
            dir.path(),
            Options {
                sync_wal: false,
                memtable_bytes: 20_000,
                compaction_fanout: 3,
                auto_compact: true,
                ..Options::default()
            },
        )
        .unwrap(),
    );
    const WRITERS: usize = 4;
    const PER_WRITER: i64 = 2_000;
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let host = format!("h{w}");
            for chunk in 0..(PER_WRITER / 50) {
                let body: String = (0..50)
                    .map(|i| {
                        let ts = chunk * 50 + i;
                        format!("cpu,host={host} usage={ts} {ts}\n")
                    })
                    .collect();
                db.write_lines(&body, Precision::Nanoseconds).unwrap();
            }
        }));
    }
    let reader = {
        let db = db.clone();
        thread::spawn(move || {
            for _ in 0..50 {
                let r = db
                    .query("SELECT count(usage) FROM cpu GROUP BY host")
                    .unwrap();
                for s in &r.series {
                    let count = s.values[0][1].as_f64().unwrap();
                    assert!(count <= PER_WRITER as f64);
                }
            }
        })
    };
    let maintainer = {
        let db = db.clone();
        thread::spawn(move || {
            for _ in 0..20 {
                db.flush().unwrap();
                db.compact().unwrap();
            }
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    reader.join().unwrap();
    maintainer.join().unwrap();

    for w in 0..WRITERS {
        let pts = read(&db, &format!("h{w}"));
        assert_eq!(pts.len(), PER_WRITER as usize);
        assert!(pts.iter().all(|(t, v)| *t as f64 == *v));
    }
    let r = db.query("SELECT count(usage) FROM cpu").unwrap();
    assert_eq!(
        r.series[0].values[0][1].as_f64(),
        Some((WRITERS as i64 * PER_WRITER) as f64)
    );
}
