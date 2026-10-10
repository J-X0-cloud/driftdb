//! Checksummed record framing shared by the WAL and the series catalog log.
//!
//! Every record is `u32 len | u32 crc32(payload) | payload`, little endian.
//! Readers stop at the first record that is truncated or fails its checksum
//! and report the byte offset of the last good record, so callers can
//! truncate a torn tail left behind by a crash mid-write.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{IoContext, Result};

/// Size of the fixed record header.
pub const HEADER_LEN: usize = 8;
/// Upper bound on a single record, guarding against garbage length fields.
pub const MAX_RECORD_LEN: usize = 64 << 20;

/// Frame `payload` and append it to `out`.
pub fn encode_record(out: &mut Vec<u8>, payload: &[u8]) {
    let len = u32::try_from(payload.len()).expect("record payload exceeds u32::MAX");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Why a scan of framed records stopped before the end of the buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TailState {
    /// Every byte belonged to a valid record.
    Clean,
    /// The final record was cut short (typical after a crash).
    Truncated,
    /// A record failed its checksum or declared an impossible length.
    Corrupt,
}

/// Result of scanning a buffer of framed records.
#[derive(Debug)]
pub struct Scan<'a> {
    /// Payloads of every valid record, in order.
    pub records: Vec<&'a [u8]>,
    /// Byte length of the valid prefix.
    pub valid_len: usize,
    /// How the scan ended.
    pub tail: TailState,
}

/// Split `buf` into framed records, stopping at the first invalid one.
pub fn scan_records(buf: &[u8]) -> Scan<'_> {
    let mut records = Vec::new();
    let mut pos = 0;
    let tail = loop {
        if pos == buf.len() {
            break TailState::Clean;
        }
        if buf.len() - pos < HEADER_LEN {
            break TailState::Truncated;
        }
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().expect("4 bytes"));
        if len > MAX_RECORD_LEN {
            break TailState::Corrupt;
        }
        let start = pos + HEADER_LEN;
        if buf.len() - start < len {
            break TailState::Truncated;
        }
        let payload = &buf[start..start + len];
        if crc32fast::hash(payload) != crc {
            break TailState::Corrupt;
        }
        records.push(payload);
        pos = start + len;
    };
    Scan {
        records,
        valid_len: pos,
        tail,
    }
}

/// An append-only file of framed records.
#[derive(Debug)]
pub struct RecordFile {
    path: PathBuf,
    file: File,
    len: u64,
}

impl RecordFile {
    /// Open (or create) `path`, returning the file positioned for appends
    /// along with every valid payload already stored in it. A torn or
    /// corrupt tail is truncated away so later appends start cleanly.
    pub fn open(path: &Path) -> Result<(Self, Vec<Vec<u8>>, TailState)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .at(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).at(path)?;
        let scan = scan_records(&buf);
        let payloads = scan.records.iter().map(|r| r.to_vec()).collect();
        if scan.valid_len < buf.len() {
            tracing::warn!(
                path = %path.display(),
                valid = scan.valid_len,
                total = buf.len(),
                tail = ?scan.tail,
                "truncating invalid record tail"
            );
            file.set_len(scan.valid_len as u64).at(path)?;
            file.sync_all().at(path)?;
        }
        let tail = scan.tail.clone();
        Ok((
            RecordFile {
                path: path.to_path_buf(),
                file,
                len: scan.valid_len as u64,
            },
            payloads,
            tail,
        ))
    }

    /// Append pre-framed bytes (one or more records) in a single write.
    pub fn append_framed(&mut self, framed: &[u8], sync: bool) -> Result<()> {
        self.file.write_all(framed).at(&self.path)?;
        if sync {
            self.file.sync_data().at(&self.path)?;
        }
        self.len += framed.len() as u64;
        Ok(())
    }

    /// Frame and append a single payload.
    pub fn append(&mut self, payload: &[u8], sync: bool) -> Result<()> {
        let mut framed = Vec::with_capacity(payload.len() + HEADER_LEN);
        encode_record(&mut framed, payload);
        self.append_framed(&framed, sync)
    }

    /// Flush file contents to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data().at(&self.path)
    }

    /// Bytes currently stored in the file.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the file holds no records.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Path of the underlying file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_round_trips_records() {
        let mut buf = Vec::new();
        encode_record(&mut buf, b"one");
        encode_record(&mut buf, b"");
        encode_record(&mut buf, b"three");
        let scan = scan_records(&buf);
        assert_eq!(scan.records, vec![&b"one"[..], b"", b"three"]);
        assert_eq!(scan.valid_len, buf.len());
        assert_eq!(scan.tail, TailState::Clean);
    }

    #[test]
    fn torn_tail_is_reported() {
        let mut buf = Vec::new();
        encode_record(&mut buf, b"good");
        let good = buf.len();
        encode_record(&mut buf, b"partial record");
        for cut in good + 1..buf.len() {
            let scan = scan_records(&buf[..cut]);
            assert_eq!(scan.records.len(), 1);
            assert_eq!(scan.valid_len, good);
            assert_eq!(scan.tail, TailState::Truncated);
        }
    }

    #[test]
    fn checksum_mismatch_is_corrupt() {
        let mut buf = Vec::new();
        encode_record(&mut buf, b"aaaa");
        encode_record(&mut buf, b"bbbb");
        let last = buf.len() - 1;
        buf[last] ^= 0x01;
        let scan = scan_records(&buf);
        assert_eq!(scan.records.len(), 1);
        assert_eq!(scan.tail, TailState::Corrupt);
    }

    #[test]
    fn absurd_length_is_corrupt() {
        let mut buf = vec![0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0];
        buf.extend_from_slice(&[0; 16]);
        assert_eq!(scan_records(&buf).tail, TailState::Corrupt);
    }

    #[test]
    fn record_file_truncates_bad_tail_and_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        {
            let (mut f, existing, _) = RecordFile::open(&path).unwrap();
            assert!(existing.is_empty());
            f.append(b"first", true).unwrap();
            f.append(b"second", true).unwrap();
        }
        // Simulate a torn write.
        let mut raw = std::fs::read(&path).unwrap();
        raw.truncate(raw.len() - 2);
        std::fs::write(&path, &raw).unwrap();

        let (mut f, existing, tail) = RecordFile::open(&path).unwrap();
        assert_eq!(existing, vec![b"first".to_vec()]);
        assert_eq!(tail, TailState::Truncated);
        f.append(b"third", true).unwrap();
        drop(f);

        let (_, existing, tail) = RecordFile::open(&path).unwrap();
        assert_eq!(existing, vec![b"first".to_vec(), b"third".to_vec()]);
        assert_eq!(tail, TailState::Clean);
    }
}
