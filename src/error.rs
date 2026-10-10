//! Error type shared by every layer of the engine.

use std::io;
use std::path::PathBuf;

/// Convenience alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// All failures surfaced by driftdb.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An operating system level I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// An I/O failure that is tied to a specific file.
    #[error("I/O error on {path}: {source}")]
    IoAt {
        /// File that was being accessed.
        path: PathBuf,
        /// Underlying error.
        source: io::Error,
    },

    /// On-disk data failed validation (bad magic, checksum or framing).
    #[error("corruption: {0}")]
    Corruption(String),

    /// A compressed block could not be decoded.
    #[error("decode error: {0}")]
    Decode(String),

    /// Line protocol input was malformed.
    #[error("line protocol error on line {line}: {message}")]
    LineProtocol {
        /// One-based line number inside the request body.
        line: usize,
        /// Human readable description.
        message: String,
    },

    /// A query failed to lex or parse.
    #[error("parse error at offset {offset}: {message}")]
    Parse {
        /// Byte offset of the offending token.
        offset: usize,
        /// Human readable description.
        message: String,
    },

    /// A syntactically valid query cannot be planned or executed.
    #[error("query error: {0}")]
    Query(String),

    /// Caller supplied an invalid argument or configuration.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Manifest serialisation failed.
    #[error("manifest error: {0}")]
    Manifest(String),
}

impl Error {
    /// Attach a path to an I/O error.
    pub fn io_at(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Error::IoAt {
            path: path.into(),
            source,
        }
    }

    /// Build a corruption error from anything displayable.
    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }

    /// Build a query error from anything displayable.
    pub fn query(msg: impl Into<String>) -> Self {
        Error::Query(msg.into())
    }

    /// Whether this error was caused by user input rather than the engine.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Error::LineProtocol { .. }
                | Error::Parse { .. }
                | Error::Query(_)
                | Error::InvalidArgument(_)
        )
    }
}

/// Extension trait to attach file paths to `io::Result` values.
pub(crate) trait IoContext<T> {
    fn at(self, path: &std::path::Path) -> Result<T>;
}

impl<T> IoContext<T> for io::Result<T> {
    fn at(self, path: &std::path::Path) -> Result<T> {
        self.map_err(|e| Error::io_at(path, e))
    }
}
