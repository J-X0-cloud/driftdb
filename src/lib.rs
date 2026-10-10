//! driftdb: an embedded time-series storage engine.
//!
//! The crate is organised bottom-up:
//!
//! * [`encoding`]: varints, bit streams and Gorilla compression.
//! * [`error`]: the shared error type.
//! * [`model`] and [`lineproto`]: series keys, points and line protocol.
//! * [`storage`]: record framing, series catalog, WAL, memtable, segments
//!   and the manifest.
//! * [`db`]: the engine that ties storage together (flush, compaction,
//!   retention).
//! * [`query`]: an InfluxQL-flavoured query language with a planner and
//!   executor supporting range scans, aggregates and `GROUP BY time()`.
//! * [`downsample`]: watermark-driven rollups into coarser measurements.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod clock;
pub mod db;
pub mod downsample;
pub mod encoding;
pub mod error;
pub mod lineproto;
pub mod model;
pub mod query;
pub mod storage;

pub use clock::{Clock, ManualClock, SystemClock};
pub use db::{CompactionReport, Db, DbStats, Options, WriteSummary};
pub use downsample::{DownsampleReport, DownsampleRule};
pub use error::{Error, Result};
pub use query::{QueryResult, ResultSeries, Value};
pub use model::{Point, Precision, SeriesId, SeriesKey, Timestamp};
