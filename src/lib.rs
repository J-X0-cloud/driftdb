//! driftdb: an embedded time-series storage engine.
//!
//! The crate is organised bottom-up:
//!
//! * [`encoding`]: varints, bit streams and Gorilla compression.
//! * [`error`]: the shared error type.
//! * [`model`] and [`lineproto`]: series keys, points and line protocol.
//! * [`storage`]: record framing, series catalog, WAL, memtable, segments
//!   and the manifest.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod encoding;
pub mod error;
pub mod lineproto;
pub mod model;
pub mod storage;

pub use error::{Error, Result};
pub use model::{Point, Precision, SeriesId, SeriesKey, Timestamp};
