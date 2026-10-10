//! Write-ahead log.
//!
//! The WAL is a directory of numbered files (`0000000001.wal`, ...). Each
//! file is a sequence of checksummed records (see [`super::record`]) and
//! each record holds one write batch of `(series id, timestamp, value)`
//! triples. When the memtable is flushed the WAL rotates to a fresh file;
//! once the segment produced by the flush is recorded in the manifest every
//! older file is deleted.

use std::fs;
use std::path::{Path, PathBuf};

use crate::encoding::varint::{put_ivarint, put_uvarint, Cursor};
use crate::error::{Error, IoContext, Result};
use crate::model::{SeriesId, Timestamp};
use crate::storage::record::{RecordFile, TailState};

const BATCH_KIND: u8 = 1;

/// One observation as stored in the WAL.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WalEntry {
    /// Series the value belongs to.
    pub series: SeriesId,
    /// Timestamp in nanoseconds.
    pub timestamp: Timestamp,
    /// Observed value.
    pub value: f64,
}

/// Encode a batch into a WAL record payload.
pub fn encode_batch(entries: &[WalEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + entries.len() * 14);
    out.push(BATCH_KIND);
    put_uvarint(&mut out, entries.len() as u64);
    for e in entries {
        put_uvarint(&mut out, e.series);
        put_ivarint(&mut out, e.timestamp);
        out.extend_from_slice(&e.value.to_bits().to_le_bytes());
    }
    out
}

/// Decode a WAL record payload.
pub fn decode_batch(payload: &[u8]) -> Result<Vec<WalEntry>> {
    let mut cur = Cursor::new(payload);
    let kind = cur.u8()?;
    if kind != BATCH_KIND {
        return Err(Error::corruption(format!("unknown WAL record kind {kind}")));
    }
    let n = cur.usize()?;
    // Each entry needs at least 10 bytes; refuse to pre-allocate beyond that.
    let mut out = Vec::with_capacity(n.min(cur.remaining() / 10 + 1));
    for _ in 0..n {
        let series = cur.uvarint()?;
        let timestamp = cur.ivarint()?;
        let value = cur.f64_le()?;
        out.push(WalEntry {
            series,
            timestamp,
            value,
        });
    }
    if !cur.is_empty() {
        return Err(Error::corruption("trailing bytes in WAL batch"));
    }
    Ok(out)
}

/// Replayed contents of the WAL.
#[derive(Debug, Default)]
pub struct Replay {
    /// Every entry recovered, in write order.
    pub entries: Vec<WalEntry>,
    /// Number of files that had to be truncated.
    pub truncated_files: usize,
}

/// Append-only, rotating write-ahead log.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    current: RecordFile,
    current_seq: u64,
    sync: bool,
}

fn file_name(seq: u64) -> String {
    format!("{seq:010}.wal")
}

fn parse_seq(name: &str) -> Option<u64> {
    name.strip_suffix(".wal")?.parse().ok()
}

/// Sorted sequence numbers of every WAL file in `dir`.
fn list_files(dir: &Path) -> Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in fs::read_dir(dir).at(dir)? {
        let entry = entry.at(dir)?;
        if let Some(seq) = entry.file_name().to_str().and_then(parse_seq) {
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

impl Wal {
    /// Open the WAL in `dir`, discarding files below `floor` (already
    /// flushed) and replaying the rest.
    pub fn open(dir: &Path, floor: u64, sync: bool) -> Result<(Self, Replay)> {
        fs::create_dir_all(dir).at(dir)?;
        let mut replay = Replay::default();
        let mut current = None;
        for seq in list_files(dir)? {
            let path = dir.join(file_name(seq));
            if seq < floor {
                fs::remove_file(&path).at(&path)?;
                continue;
            }
            let (file, payloads, tail) = RecordFile::open(&path)?;
            if tail != TailState::Clean {
                replay.truncated_files += 1;
            }
            for payload in payloads {
                match decode_batch(&payload) {
                    Ok(entries) => replay.entries.extend(entries),
                    Err(e) => {
                        // A checksummed record that does not decode means a
                        // format bug, not a torn write; refuse to guess.
                        return Err(Error::corruption(format!(
                            "{}: undecodable WAL batch: {e}",
                            path.display()
                        )));
                    }
                }
            }
            current = Some((seq, file));
        }
        let (current_seq, current) = match current {
            Some(c) => c,
            None => {
                let seq = floor.max(1);
                let (file, _, _) = RecordFile::open(&dir.join(file_name(seq)))?;
                (seq, file)
            }
        };
        Ok((
            Wal {
                dir: dir.to_path_buf(),
                current,
                current_seq,
                sync,
            },
            replay,
        ))
    }

    /// Durably append one batch.
    pub fn append(&mut self, entries: &[WalEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.current.append(&encode_batch(entries), self.sync)
    }

    /// Sequence number of the file currently receiving appends.
    pub fn current_seq(&self) -> u64 {
        self.current_seq
    }

    /// Start a new file and return its sequence number. Everything written
    /// before this call lives in files with a smaller sequence number.
    pub fn rotate(&mut self) -> Result<u64> {
        self.current.sync()?;
        let seq = self.current_seq + 1;
        let (file, _, _) = RecordFile::open(&self.dir.join(file_name(seq)))?;
        self.current = file;
        self.current_seq = seq;
        Ok(seq)
    }

    /// Delete every file with a sequence number below `floor`.
    pub fn remove_below(&self, floor: u64) -> Result<usize> {
        let mut removed = 0;
        for seq in list_files(&self.dir)? {
            if seq < floor && seq != self.current_seq {
                let path = self.dir.join(file_name(seq));
                fs::remove_file(&path).at(&path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Total bytes across all live WAL files.
    pub fn disk_bytes(&self) -> Result<u64> {
        let mut total = 0;
        for seq in list_files(&self.dir)? {
            let path = self.dir.join(file_name(seq));
            total += fs::metadata(&path).at(&path)?.len();
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(series: SeriesId, ts: Timestamp, value: f64) -> WalEntry {
        WalEntry {
            series,
            timestamp: ts,
            value,
        }
    }

    #[test]
    fn batch_codec_roundtrips() {
        let batch = vec![entry(0, -1, 1.5), entry(u64::MAX, i64::MAX, f64::NAN)];
        let decoded = decode_batch(&encode_batch(&batch)).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], batch[0]);
        assert_eq!(decoded[1].series, u64::MAX);
        assert!(decoded[1].value.is_nan());
    }

    #[test]
    fn replays_across_rotation_and_respects_floor() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let (mut wal, replay) = Wal::open(&wal_dir, 0, true).unwrap();
            assert!(replay.entries.is_empty());
            wal.append(&[entry(1, 10, 1.0)]).unwrap();
            let next = wal.rotate().unwrap();
            assert_eq!(next, wal.current_seq());
            wal.append(&[entry(2, 20, 2.0), entry(3, 30, 3.0)]).unwrap();
        }
        let (_, replay) = Wal::open(&wal_dir, 0, true).unwrap();
        assert_eq!(replay.entries.len(), 3);

        // Floor 2 means file 1 was flushed and must not be replayed.
        let (wal, replay) = Wal::open(&wal_dir, 2, true).unwrap();
        assert_eq!(replay.entries, vec![entry(2, 20, 2.0), entry(3, 30, 3.0)]);
        assert!(!wal_dir.join(file_name(1)).exists());
        assert!(wal.disk_bytes().unwrap() > 0);
    }

    #[test]
    fn torn_tail_is_dropped_and_appends_continue() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let (mut wal, _) = Wal::open(&wal_dir, 0, false).unwrap();
            wal.append(&[entry(1, 1, 1.0)]).unwrap();
            wal.append(&[entry(1, 2, 2.0)]).unwrap();
        }
        let path = wal_dir.join(file_name(1));
        let mut raw = fs::read(&path).unwrap();
        raw.truncate(raw.len() - 5);
        fs::write(&path, raw).unwrap();

        let (mut wal, replay) = Wal::open(&wal_dir, 0, false).unwrap();
        assert_eq!(replay.entries, vec![entry(1, 1, 1.0)]);
        assert_eq!(replay.truncated_files, 1);
        wal.append(&[entry(1, 3, 3.0)]).unwrap();
        drop(wal);
        let (_, replay) = Wal::open(&wal_dir, 0, false).unwrap();
        assert_eq!(replay.entries, vec![entry(1, 1, 1.0), entry(1, 3, 3.0)]);
    }

    #[test]
    fn remove_below_keeps_current_file() {
        let dir = tempfile::tempdir().unwrap();
        let (mut wal, _) = Wal::open(dir.path(), 0, false).unwrap();
        wal.append(&[entry(1, 1, 1.0)]).unwrap();
        wal.rotate().unwrap();
        wal.rotate().unwrap();
        assert_eq!(wal.remove_below(3).unwrap(), 2);
        assert_eq!(list_files(dir.path()).unwrap(), vec![3]);
        assert_eq!(wal.remove_below(100).unwrap(), 0);
    }
}
