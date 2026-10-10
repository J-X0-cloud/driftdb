//! driftdb: an embedded time-series storage engine.
//!
//! The crate is organised bottom-up:
//!
//! * [`encoding`]: varints, bit streams and Gorilla compression.
//! * [`error`]: the shared error type.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod encoding;
pub mod error;

pub use error::{Error, Result};
