//! driftdb: an embedded time-series storage engine.
//!
//! The crate is organised bottom-up:
//!
//! * [`error`]: the shared error type.

#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod error;

pub use error::{Error, Result};
