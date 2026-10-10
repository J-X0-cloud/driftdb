//! Using driftdb as an embedded library.
//!
//! Run with `cargo run --example embedded`.

use driftdb::{Db, Options, Point, Precision, SeriesKey};

fn main() -> driftdb::Result<()> {
    let dir = std::env::temp_dir().join(format!("driftdb-example-{}", std::process::id()));
    let db = Db::open(&dir, Options::default())?;

    // Line protocol, the same format the HTTP API accepts.
    db.write_lines(
        "weather,station=oslo temp=4.5,humidity=81 1700000000\n\
         weather,station=oslo temp=5.0,humidity=79 1700000600\n\
         weather,station=lima temp=19.5,humidity=70 1700000000\n",
        Precision::Seconds,
    )?;

    // Or typed points.
    let key = SeriesKey::new("weather", [("station", "lima")], "temp")?;
    db.write_points(&[Point::new(key, 1_700_000_600_000_000_000, 20.25)])?;

    // Persist the memtable as a compressed segment (normally automatic).
    db.flush()?;

    let result = db.query(
        "SELECT mean(temp), max(humidity) FROM weather \
         WHERE time >= '2023-11-14T22:00:00Z' GROUP BY time(30m), station",
    )?;
    print!("{}", result.to_table(true));

    let plan = db.query("EXPLAIN SELECT mean(temp) FROM weather WHERE station = 'oslo'")?;
    print!("\n{}", plan.to_table(false));

    drop(db);
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
