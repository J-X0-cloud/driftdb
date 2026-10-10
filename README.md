# driftdb

An embedded time-series storage engine written in Rust. It stores float
series in a small LSM tree (write-ahead log, memtable, immutable segments),
compresses them with Gorilla delta-of-delta and XOR encoding, indexes tags for
fast series selection, and answers an InfluxQL-flavoured query language with
range scans, aggregates and `GROUP BY time()`. It can be used as a library or
run as a single-binary HTTP server that accepts line protocol.

```text
$ driftdb generate --hosts 100 --points 10000 > cpu.lp
$ driftdb --no-sync write cpu.lp
wrote 2000000 points in 2.52s (792768 points/s)
$ driftdb query "SELECT mean(usage_user), max(usage_user), percentile(usage_user, 99)
                 FROM cpu WHERE region = 'eu-central' GROUP BY time(6h) FILL(none) LIMIT 2"
name: cpu
time                  mean                max  percentile
--------------------  ------------------  ---  ----------
2026-10-09T00:00:00Z  47.48525286525555   100  99.41
2026-10-09T06:00:00Z  54.82348759259259   100  99.55
```

## Features

- **Write-ahead log** with CRC-32 framed records, fsync per batch (optional),
  rotation on flush and torn-tail recovery: a crash mid-write loses at most the
  batch being written, never earlier data.
- **Memtable** of per-series B-trees with last-writer-wins overwrites and
  out-of-order inserts.
- **Segments** (SSTables): per-series blocks of up to 1000 points, each block
  Gorilla-compressed and checksummed, with a per-block time index so reads skip
  blocks outside the query range. Segments are written to a temp file, fsynced
  and atomically renamed.
- **Gorilla compression**: delta-of-delta timestamps (zig-zag, 7/9/12/64-bit
  buckets, so nanosecond timestamps and arbitrary `i64` values round-trip) and
  XOR float encoding that preserves NaN payloads and signed zeros.
- **Tiered compaction** of adjacent same-level segments, plus a full major
  compaction. Merging preserves recency order so overwrites stay correct.
- **Manifest** written atomically (temp file, fsync, rename, directory fsync);
  orphaned segments from interrupted flushes are cleaned up on open.
- **Tag index**: a durable series catalog with an inverted index
  (`tag=value -> series`) used to evaluate `=`, `!=`, `=~`, `!~` with `AND`/`OR`.
- **Retention**: expired data is invisible to reads immediately, dropped
  during flush and compaction, and whole segments are deleted once they fall
  out of the window.
- **Downsampling**: rules such as `cpu:5m:mean,max,p99` roll a measurement
  into a coarser one, tracked by a persisted watermark so each window is
  materialised once.
- **Query language**: lexer, recursive-descent parser, planner and executor.
  `count sum mean median min max first last stddev spread percentile`,
  `GROUP BY time(interval[, offset])` and tags, `FILL(null|none|previous|linear|<n>)`,
  `ORDER BY time DESC`, `LIMIT`, relative time (`now() - 1h`), RFC 3339 literals,
  `EXPLAIN`, and `SHOW MEASUREMENTS | TAG KEYS | TAG VALUES | FIELD KEYS | SERIES`.
- **HTTP API** (axum) for line-protocol ingest, queries (JSON or text tables),
  stats and admin actions, with background maintenance and graceful shutdown.
- **CLI** for serving, bulk ingest, queries, an interactive shell, compaction,
  retention, downsampling, segment inspection and synthetic data generation.

## Architecture

```text
                write_lines / write_points                     query("SELECT ...")
                           |                                          |
                           v                                          v
  line protocol --> SeriesKey --> Catalog (series.log)        lexer -> parser -> AST
                                   |  id <-> key, tag postings         |
                                   v                                   v
                       WAL (wal/*.wal, CRC framed) ----+         planner: time bounds,
                                   |                   |         tag set algebra, groups
                                   v                   |                |
                       active memtable                 | replay         v
                                   |  size limit       | on open   executor: k-way merge
                                   v                   |           of segments + memtables,
                       frozen memtable --flush--> L0 segment       windows, aggregates, fill
                                                       |
                         tiered compaction: L0 -> L1 -> L2 ... (adjacent runs only)
                                                       |
                                     MANIFEST.json (live segments, WAL floor,
                                                    downsampling watermarks)
```

| Module | Responsibility |
|--------|----------------|
| `encoding::{varint, bits, gorilla}` | LEB128/zig-zag, MSB-first bit streams, Gorilla block codec |
| `storage::record` | `len \| crc32 \| payload` framing shared by the WAL and catalog |
| `storage::catalog` | Durable series ids and the inverted tag index |
| `storage::wal` | Rotating write-ahead log and replay |
| `storage::memtable`, `storage::merge` | Write buffer and last-writer-wins k-way merge |
| `storage::segment` | Segment writer/reader, block index, checksums |
| `storage::manifest` | Atomic manifest of live segments |
| `db` | Engine: write path, read path, flush, compaction, retention, locking |
| `query::{lexer, parser, ast}` | Query language front end |
| `query::{planner, exec, aggregate}` | Planning, execution, accumulators and fill |
| `downsample` | Watermark-driven rollup rules |
| `server` | axum HTTP API and background maintenance |

### Read and write path details

- A write resolves series keys to ids (new keys are appended to `series.log`
  and fsynced before use), appends one WAL record for the batch, then inserts
  into the memtable while still holding the WAL lock so WAL order equals
  memtable order.
- When the memtable passes `memtable_bytes`, it is frozen and the WAL rotates.
  The frozen memtable is written as a level-0 segment outside the state lock,
  so reads and writes continue during the flush. Installing the segment and
  advancing the WAL floor in the manifest happen together; older WAL files are
  deleted afterwards.
- Every segment has a `seq` that orders it by recency. A compaction only merges
  segments that are adjacent in `seq` order and gives the output the largest
  input `seq`, so no segment can sit between inputs and change which value wins
  for a timestamp.
- Reads take a snapshot of the segment list (reference counted) and copy the
  requested memtable ranges, then decode only blocks whose time span overlaps
  the query. A segment replaced by compaction is deleted when its last reader
  drops it.

### On-disk layout

```text
<data-dir>/
  LOCK               advisory lock, one process per directory
  MANIFEST.json      live segments, WAL floor, downsampling watermarks
  series.log         series catalog (framed records)
  wal/0000000007.wal
  segments/0000000012.sst
```

## Quick start

```sh
cargo build --release
./target/release/driftdb --help

# Ingest synthetic data and query it
./target/release/driftdb generate --hosts 10 --points 5000 > cpu.lp
./target/release/driftdb write cpu.lp
./target/release/driftdb query "SHOW TAG VALUES WITH KEY = region"
./target/release/driftdb query "SELECT max(usage_user) FROM cpu GROUP BY host LIMIT 3"
./target/release/driftdb query "EXPLAIN SELECT mean(usage_user) FROM cpu WHERE host =~ /00[1-3]/ AND time > now() - 1h GROUP BY time(5m)"

# Interactive shell (statements end with ';')
./target/release/driftdb shell
```

### Server

```sh
PORT=8086 ./target/release/driftdb --retention 30d serve \
    --downsample "cpu:5m:mean,max:cpu_5m"

curl --data-binary 'cpu,host=a,region=eu usage_user=12.5,usage_system=3 1700000000' \
     'localhost:8086/write?precision=s'
curl -G localhost:8086/query --data-urlencode 'q=SELECT * FROM cpu'
curl -G localhost:8086/query --data-urlencode 'q=SHOW SERIES' --data-urlencode format=table
curl localhost:8086/stats
```

| Method | Path | Notes |
|--------|------|-------|
| `GET` | `/health` | Liveness probe |
| `POST` | `/write?precision=ns\|us\|ms\|s` | Line protocol body; `204` on success, `400` with `{"error": ...}` on bad input |
| `GET`/`POST` | `/query?q=...&format=json\|table` | The query may also be sent as the POST body |
| `GET` | `/stats` | Series count, memtable size, WAL bytes, segments |
| `POST` | `/admin/flush`, `/admin/compact[?full=true]`, `/admin/retention` | Manual maintenance |

Configuration is by flag or environment variable: `DRIFTDB_DATA_DIR`,
`PORT`, `DRIFTDB_BIND`, `DRIFTDB_RETENTION`, `DRIFTDB_MEMTABLE_MB`,
`DRIFTDB_NO_SYNC`, `DRIFTDB_DOWNSAMPLE` (rules separated by `;`).

### Docker

```sh
docker build -t driftdb .
docker run -p 8086:8086 -v driftdb-data:/data driftdb
```

The image runs `driftdb serve`, stores data in `/data` and listens on `$PORT`
(default 8086).

### As a library

```rust
use driftdb::{Db, Options, Precision};

let db = Db::open("data", Options::default())?;
db.write_lines("cpu,host=a usage=12.5 1700000000", Precision::Seconds)?;
let result = db.query("SELECT mean(usage) FROM cpu GROUP BY time(1m)")?;
print!("{}", result.to_table(true));
```

See `examples/embedded.rs` (`cargo run --example embedded`).

## Query language

```text
SELECT <projection> [, ...] FROM <measurement>
  [WHERE <condition>]
  [GROUP BY time(<interval>[, <offset>]) | <tag> | * [, ...]]
  [FILL(null | none | previous | linear | <number>)]
  [ORDER BY time ASC | DESC]
  [LIMIT <n>]

projection := * | field [AS alias] | func(field) [AS alias] | percentile(field, p)
condition  := tag = 'v' | tag != 'v' | tag =~ /re/ | tag !~ /re/
            | time <op> (now() | 'RFC3339' | <int ns> | <duration>) [+|- duration ...]
            | condition AND condition | condition OR condition | ( condition )
```

Semantics worth knowing:

- Each series stores one float field. A line with several fields fans out into
  several series sharing a tag set; raw `SELECT` re-aligns them by timestamp.
- A missing tag compares as the empty string, so `dc != 'east'` includes series
  without a `dc` tag and `dc = ''` selects exactly those.
- Time conditions must be joined with `AND` at the top level; they become a
  half-open range `[start, end)`.
- Without `GROUP BY time()`, an aggregate covers the whole range and is stamped
  with the lower bound (or 0 when unbounded).
- `count()` of an empty window is 0; other aggregates are null and then subject
  to `FILL`. Filling is capped at one million windows per group.
- `percentile` uses the nearest-rank method; `median` averages the two middle
  values for even counts; `stddev` is the sample standard deviation.

## Running tests

```sh
cargo test                      # unit, integration and property tests
cargo test --release --test properties   # property tests, faster
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The suite includes:

- unit tests next to each module (codecs, framing, catalog, WAL, segments,
  manifest, engine, lexer, parser, planner, aggregates, server handlers, CLI);
- `tests/query.rs`: end-to-end queries over line protocol input;
- `tests/durability.rs`: torn WAL tails, a crash between writing a segment and
  updating the manifest, corrupted blocks, and concurrent writers, readers and
  maintenance;
- `tests/downsample.rs`: rollups, watermarks across restarts, and retention;
- `tests/properties.rs` (proptest): codec round trips, parsers never panicking
  on garbage, the merge against a map model, segment range reads, an engine
  model test that interleaves writes, flushes, compactions and restarts, and
  windowed aggregates checked against a direct computation.

## Benchmarks

Measured with `cargo bench` (criterion) on an AMD Ryzen 7 7800X3D under WSL2
(Linux 6.18), rustc 1.93, release profile. Numbers are the criterion point
estimates; expect noticeable run-to-run variance on this setup.

| Benchmark | Result |
|-----------|--------|
| Gorilla encode, 1000-point gauge (random walk, 2 decimals) | ~37 us, ~27 M points/s |
| Gorilla decode, same block | ~20 us, ~50 M points/s |
| Gorilla encode, 1000-point counter with timestamp jitter | ~27 us, ~38 M points/s |
| Gorilla decode, same block | ~13 us, ~78 M points/s |
| `write_points`, 5000-point batches over 50 series, WAL fsync off | ~1.4 ms per batch, ~3.5 M points/s |
| `write_lines`, same data as line protocol | ~4.5 ms per batch, ~1.1 M points/s |
| `SELECT mean(usage) ... GROUP BY time(5m)` over 100k points in one segment | ~5.5 ms |
| `SELECT max(usage) ... GROUP BY host` over 100k points | ~2.2 ms |
| Raw scan of one 2000-point series | ~0.33 ms |

Compressed size is data dependent. The bench's gauge block takes 6.68
bytes/point and the counter block 3.31 bytes/point (raw pairs are 16 bytes).
Timestamps at a fixed interval cost about one bit each; values whose low
mantissa bits change every sample (such as the 2-decimal random walk produced
by `driftdb generate`) dominate the size. The CLI run at the top of this file
(2,000,000 points from 1,000,000 lines, `--no-sync`) wrote at about 790k
points/s and compacted into one 14.6 MB segment (7.3 bytes/point).

## Roadmap

Things that are deliberately not done yet:

- Deletes and tombstones (`DELETE FROM ... WHERE ...`). Data only leaves through
  retention.
- Series cardinality cleanup: the catalog never forgets a series, even after
  all of its points expire.
- Per-measurement retention policies, so rollups can outlive raw data.
- Re-aggregating late data: points that arrive behind a downsampling watermark
  are stored but not rolled up again.
- Streaming execution: queries materialise the selected points in memory
  before aggregating.
- Field-value predicates (`WHERE usage > 90`), arithmetic in projections,
  derivatives and rates, subqueries, multiple statements per request.
- A block cache or memory-mapped segment reads; every read goes through a file
  handle guarded by a mutex.
- Flush and compaction triggered by a write run on the writing thread; moving
  them to a dedicated background worker would smooth write latency.
- String, integer-typed and boolean fields are converted or rejected; only
  `f64` values are stored.
- Authentication, TLS and replication for the HTTP server.
- Obsolete segments are unlinked while readers may still hold them open, which
  relies on Unix file semantics.

## License

MIT, see [LICENSE](LICENSE).
