//! Immutable, sorted segment files (the "SSTables" of the LSM tree).
//!
//! ```text
//! +----------------+---------------------------+-------+--------+
//! | magic (8)      | block 0 | block 1 | ...   | index | footer |
//! +----------------+---------------------------+-------+--------+
//!
//! index  := uvarint series_count
//!           { uvarint series_id  uvarint block_count
//!             { uvarint offset  uvarint len  ivarint min_ts  ivarint max_ts
//!               uvarint count   u32 crc32 } }
//! footer := u64 index_offset | u32 index_len | u32 index_crc | magic (8)
//! ```
//!
//! Each block holds up to [`POINTS_PER_BLOCK`] Gorilla-compressed points of a
//! single series. Series appear in ascending id order and the per-block time
//! bounds let readers skip blocks that cannot overlap a query range.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::encoding::gorilla;
use crate::encoding::varint::{put_ivarint, put_uvarint, Cursor};
use crate::error::{Error, IoContext, Result};
use crate::model::{SeriesId, Timestamp};

/// File magic, also used as the format version.
pub const MAGIC: &[u8; 8] = b"DRFTSEG1";
/// Maximum points stored in one compressed block.
pub const POINTS_PER_BLOCK: usize = 1000;
const FOOTER_LEN: usize = 8 + 4 + 4 + 8;

/// Location and summary of one compressed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    /// Byte offset of the block in the file.
    pub offset: u64,
    /// Encoded length in bytes.
    pub len: u32,
    /// Smallest timestamp in the block.
    pub min_ts: Timestamp,
    /// Largest timestamp in the block.
    pub max_ts: Timestamp,
    /// Number of points in the block.
    pub count: u32,
    /// CRC-32 of the encoded bytes.
    pub crc: u32,
}

impl BlockHandle {
    fn overlaps(&self, start: Timestamp, end: Timestamp) -> bool {
        self.min_ts < end && self.max_ts >= start
    }
}

/// Summary statistics of a finished segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SegmentStats {
    /// Smallest timestamp stored.
    pub min_ts: Timestamp,
    /// Largest timestamp stored.
    pub max_ts: Timestamp,
    /// Total number of points.
    pub points: u64,
    /// Number of distinct series.
    pub series: u64,
    /// File size in bytes.
    pub bytes: u64,
}

/// Streams series into a new segment file.
///
/// Data is written to `<path>.tmp` and atomically renamed into place by
/// [`SegmentWriter::finish`], so a crash never leaves a half-written segment
/// under its final name.
#[derive(Debug)]
pub struct SegmentWriter {
    out: BufWriter<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
    offset: u64,
    index: Vec<(SeriesId, Vec<BlockHandle>)>,
    stats: SegmentStats,
}

impl SegmentWriter {
    /// Begin writing a segment that will live at `path`.
    pub fn create(path: &Path) -> Result<Self> {
        let tmp_path = path.with_extension("tmp");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .at(&tmp_path)?;
        let mut out = BufWriter::with_capacity(256 << 10, file);
        out.write_all(MAGIC).at(&tmp_path)?;
        Ok(SegmentWriter {
            out,
            tmp_path,
            final_path: path.to_path_buf(),
            offset: MAGIC.len() as u64,
            index: Vec::new(),
            stats: SegmentStats {
                min_ts: Timestamp::MAX,
                max_ts: Timestamp::MIN,
                ..SegmentStats::default()
            },
        })
    }

    /// Append every point of one series. Series must be added in strictly
    /// increasing id order and points must be strictly sorted by time.
    pub fn add_series(&mut self, series: SeriesId, points: &[(Timestamp, f64)]) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }
        if let Some((last, _)) = self.index.last() {
            if *last >= series {
                return Err(Error::InvalidArgument(format!(
                    "series {series} added after {last}; segment series must ascend"
                )));
            }
        }
        if points.windows(2).any(|w| w[0].0 >= w[1].0) {
            return Err(Error::InvalidArgument(format!(
                "points for series {series} are not strictly sorted"
            )));
        }
        let mut handles = Vec::with_capacity(points.len().div_ceil(POINTS_PER_BLOCK));
        for chunk in points.chunks(POINTS_PER_BLOCK) {
            let encoded = gorilla::encode_points(chunk);
            self.out.write_all(&encoded).at(&self.tmp_path)?;
            let handle = BlockHandle {
                offset: self.offset,
                len: u32::try_from(encoded.len())
                    .map_err(|_| Error::InvalidArgument("block too large".into()))?,
                min_ts: chunk[0].0,
                max_ts: chunk[chunk.len() - 1].0,
                count: chunk.len() as u32,
                crc: crc32fast::hash(&encoded),
            };
            self.offset += encoded.len() as u64;
            handles.push(handle);
        }
        self.stats.min_ts = self.stats.min_ts.min(points[0].0);
        self.stats.max_ts = self.stats.max_ts.max(points[points.len() - 1].0);
        self.stats.points += points.len() as u64;
        self.stats.series += 1;
        self.index.push((series, handles));
        Ok(())
    }

    /// Number of series added so far.
    pub fn series_count(&self) -> usize {
        self.index.len()
    }

    /// Write the index and footer, fsync, and rename into place.
    pub fn finish(mut self) -> Result<SegmentStats> {
        let mut index = Vec::new();
        put_uvarint(&mut index, self.index.len() as u64);
        for (series, handles) in &self.index {
            put_uvarint(&mut index, *series);
            put_uvarint(&mut index, handles.len() as u64);
            for h in handles {
                put_uvarint(&mut index, h.offset);
                put_uvarint(&mut index, u64::from(h.len));
                put_ivarint(&mut index, h.min_ts);
                put_ivarint(&mut index, h.max_ts);
                put_uvarint(&mut index, u64::from(h.count));
                index.extend_from_slice(&h.crc.to_le_bytes());
            }
        }
        let index_len = u32::try_from(index.len())
            .map_err(|_| Error::InvalidArgument("segment index too large".into()))?;
        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(&self.offset.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&crc32fast::hash(&index).to_le_bytes());
        footer.extend_from_slice(MAGIC);

        self.out.write_all(&index).at(&self.tmp_path)?;
        self.out.write_all(&footer).at(&self.tmp_path)?;
        let file = self
            .out
            .into_inner()
            .map_err(|e| Error::io_at(&self.tmp_path, e.into_error()))?;
        file.sync_all().at(&self.tmp_path)?;
        let bytes = file.metadata().at(&self.tmp_path)?.len();
        drop(file);
        fs::rename(&self.tmp_path, &self.final_path).at(&self.final_path)?;
        if let Some(dir) = self.final_path.parent() {
            sync_dir(dir)?;
        }
        let mut stats = self.stats;
        stats.bytes = bytes;
        if stats.points == 0 {
            stats.min_ts = 0;
            stats.max_ts = 0;
        }
        Ok(stats)
    }

    /// Abandon the segment and remove the temporary file.
    pub fn abort(self) {
        let path = self.tmp_path.clone();
        drop(self.out);
        let _ = fs::remove_file(path);
    }
}

/// fsync a directory so renames and unlinks inside it are durable.
pub fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir).and_then(|d| d.sync_all()).at(dir)
}

/// An open, immutable segment file.
#[derive(Debug)]
pub struct Segment {
    id: u64,
    path: PathBuf,
    file: Mutex<File>,
    index: BTreeMap<SeriesId, Vec<BlockHandle>>,
    stats: SegmentStats,
    obsolete: AtomicBool,
}

impl Segment {
    /// Open and validate the segment at `path`.
    pub fn open(path: &Path, id: u64) -> Result<Self> {
        let mut file = File::open(path).at(path)?;
        let len = file.metadata().at(path)?.len();
        if len < (MAGIC.len() + FOOTER_LEN) as u64 {
            return Err(Error::corruption(format!(
                "{}: file too short for a segment ({len} bytes)",
                path.display()
            )));
        }
        let mut header = [0u8; 8];
        file.read_exact(&mut header).at(path)?;
        if &header != MAGIC {
            return Err(Error::corruption(format!("{}: bad header magic", path.display())));
        }
        file.seek(SeekFrom::Start(len - FOOTER_LEN as u64)).at(path)?;
        let mut footer = [0u8; FOOTER_LEN];
        file.read_exact(&mut footer).at(path)?;
        let mut cur = Cursor::new(&footer);
        let index_offset = cur.u64_le()?;
        let index_len = cur.u32_le()? as u64;
        let index_crc = cur.u32_le()?;
        if cur.bytes(8)? != MAGIC {
            return Err(Error::corruption(format!("{}: bad footer magic", path.display())));
        }
        if index_offset < MAGIC.len() as u64 || index_offset + index_len + FOOTER_LEN as u64 != len {
            return Err(Error::corruption(format!(
                "{}: index bounds do not match file length",
                path.display()
            )));
        }
        file.seek(SeekFrom::Start(index_offset)).at(path)?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes).at(path)?;
        if crc32fast::hash(&index_bytes) != index_crc {
            return Err(Error::corruption(format!("{}: index checksum mismatch", path.display())));
        }
        let (index, mut stats) = parse_index(&index_bytes, index_offset)
            .map_err(|e| Error::corruption(format!("{}: {e}", path.display())))?;
        stats.bytes = len;
        Ok(Segment {
            id,
            path: path.to_path_buf(),
            file: Mutex::new(file),
            index,
            stats,
            obsolete: AtomicBool::new(false),
        })
    }

    /// Segment id assigned by the manifest.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Location on disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Summary statistics.
    pub fn stats(&self) -> SegmentStats {
        self.stats
    }

    /// Whether the segment stores any data for `series`.
    pub fn contains(&self, series: SeriesId) -> bool {
        self.index.contains_key(&series)
    }

    /// Series ids stored in this segment, ascending.
    pub fn series_ids(&self) -> impl Iterator<Item = SeriesId> + '_ {
        self.index.keys().copied()
    }

    /// Block handles for `series`.
    pub fn blocks(&self, series: SeriesId) -> &[BlockHandle] {
        self.index.get(&series).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Whether any block of `series` may hold points in `[start, end)`.
    pub fn may_overlap(&self, series: SeriesId, start: Timestamp, end: Timestamp) -> bool {
        self.blocks(series).iter().any(|b| b.overlaps(start, end))
    }

    /// Points of `series` with `start <= ts < end`, sorted by time.
    pub fn read_range(&self, series: SeriesId, start: Timestamp, end: Timestamp) -> Result<Vec<(Timestamp, f64)>> {
        let mut out = Vec::new();
        if start >= end {
            return Ok(out);
        }
        for handle in self.blocks(series).iter().filter(|b| b.overlaps(start, end)) {
            let points = self.read_block(handle)?;
            if handle.min_ts >= start && handle.max_ts < end {
                out.extend(points);
            } else {
                out.extend(points.into_iter().filter(|(t, _)| *t >= start && *t < end));
            }
        }
        Ok(out)
    }

    /// Every point of `series`, sorted by time.
    pub fn read_all(&self, series: SeriesId) -> Result<Vec<(Timestamp, f64)>> {
        let mut out = Vec::new();
        for handle in self.blocks(series) {
            out.extend(self.read_block(handle)?);
        }
        Ok(out)
    }

    /// Read, verify and decode one block.
    pub fn read_block(&self, handle: &BlockHandle) -> Result<Vec<(Timestamp, f64)>> {
        let mut buf = vec![0u8; handle.len as usize];
        {
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(handle.offset)).at(&self.path)?;
            file.read_exact(&mut buf).at(&self.path)?;
        }
        if crc32fast::hash(&buf) != handle.crc {
            return Err(Error::corruption(format!(
                "{}: block at offset {} failed checksum",
                self.path.display(),
                handle.offset
            )));
        }
        let points = gorilla::decode_points(&buf)?;
        if points.len() != handle.count as usize {
            return Err(Error::corruption(format!(
                "{}: block at offset {} decoded {} points, index says {}",
                self.path.display(),
                handle.offset,
                points.len(),
                handle.count
            )));
        }
        Ok(points)
    }

    /// Mark the segment as replaced. The file is deleted once the last
    /// reader holding this segment drops it.
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::SeqCst);
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if self.obsolete.load(Ordering::SeqCst) {
            if let Err(e) = fs::remove_file(&self.path) {
                tracing::warn!(path = %self.path.display(), error = %e, "failed to delete obsolete segment");
            }
        }
    }
}

fn parse_index(
    bytes: &[u8],
    data_end: u64,
) -> Result<(BTreeMap<SeriesId, Vec<BlockHandle>>, SegmentStats)> {
    let mut cur = Cursor::new(bytes);
    let n_series = cur.usize()?;
    let mut index = BTreeMap::new();
    let mut stats = SegmentStats {
        min_ts: Timestamp::MAX,
        max_ts: Timestamp::MIN,
        ..SegmentStats::default()
    };
    let mut prev_series: Option<SeriesId> = None;
    for _ in 0..n_series {
        let series = cur.uvarint()?;
        if prev_series.is_some_and(|p| p >= series) {
            return Err(Error::Decode("series ids in index are not ascending".into()));
        }
        prev_series = Some(series);
        let n_blocks = cur.usize()?;
        let mut handles = Vec::with_capacity(n_blocks.min(cur.remaining()));
        for _ in 0..n_blocks {
            let offset = cur.uvarint()?;
            let len = u32::try_from(cur.uvarint()?)
                .map_err(|_| Error::Decode("block length overflows u32".into()))?;
            let min_ts = cur.ivarint()?;
            let max_ts = cur.ivarint()?;
            let count = u32::try_from(cur.uvarint()?)
                .map_err(|_| Error::Decode("block count overflows u32".into()))?;
            let crc = cur.u32_le()?;
            if offset < MAGIC.len() as u64 || offset + u64::from(len) > data_end {
                return Err(Error::Decode(format!("block at {offset} lies outside the data region")));
            }
            if min_ts > max_ts || count == 0 {
                return Err(Error::Decode(format!("block at {offset} has invalid bounds")));
            }
            stats.min_ts = stats.min_ts.min(min_ts);
            stats.max_ts = stats.max_ts.max(max_ts);
            stats.points += u64::from(count);
            handles.push(BlockHandle {
                offset,
                len,
                min_ts,
                max_ts,
                count,
                crc,
            });
        }
        index.insert(series, handles);
    }
    if !cur.is_empty() {
        return Err(Error::Decode("trailing bytes after segment index".into()));
    }
    stats.series = index.len() as u64;
    if stats.points == 0 {
        stats.min_ts = 0;
        stats.max_ts = 0;
    }
    Ok((index, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series_points(n: usize, start: Timestamp, step: Timestamp) -> Vec<(Timestamp, f64)> {
        (0..n)
            .map(|i| (start + i as Timestamp * step, (i as f64).sin()))
            .collect()
    }

    fn write_segment(dir: &Path, data: &[(SeriesId, Vec<(Timestamp, f64)>)]) -> (PathBuf, SegmentStats) {
        let path = dir.join("seg.sst");
        let mut w = SegmentWriter::create(&path).unwrap();
        for (id, pts) in data {
            w.add_series(*id, pts).unwrap();
        }
        let stats = w.finish().unwrap();
        (path, stats)
    }

    #[test]
    fn write_then_read_multiple_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let a = series_points(2500, 0, 10);
        let b = series_points(3, 100, 1);
        let (path, stats) = write_segment(dir.path(), &[(1, a.clone()), (7, b.clone())]);
        assert_eq!(stats.points, 2503);
        assert_eq!(stats.series, 2);
        assert_eq!(stats.min_ts, 0);
        assert_eq!(stats.max_ts, 24_990);
        assert!(!path.with_extension("tmp").exists());

        let seg = Segment::open(&path, 1).unwrap();
        assert_eq!(seg.stats().points, 2503);
        assert_eq!(seg.blocks(1).len(), 3);
        assert_eq!(seg.read_all(1).unwrap(), a);
        assert_eq!(seg.read_all(7).unwrap(), b);
        assert!(seg.read_all(3).unwrap().is_empty());
        assert_eq!(seg.series_ids().collect::<Vec<_>>(), vec![1, 7]);

        let window = seg.read_range(1, 9_995, 10_030).unwrap();
        assert_eq!(window.iter().map(|p| p.0).collect::<Vec<_>>(), vec![10_000, 10_010, 10_020]);
        assert!(seg.may_overlap(1, 24_990, 25_000));
        assert!(!seg.may_overlap(1, 25_000, 30_000));
        assert!(seg.read_range(1, 50, 50).unwrap().is_empty());
    }

    #[test]
    fn rejects_unsorted_input() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(&dir.path().join("s.sst")).unwrap();
        w.add_series(5, &[(1, 1.0)]).unwrap();
        assert!(w.add_series(5, &[(2, 1.0)]).is_err());
        assert!(w.add_series(6, &[(2, 1.0), (2, 2.0)]).is_err());
        w.abort();
        assert!(!dir.path().join("s.tmp").exists());
    }

    #[test]
    fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = write_segment(dir.path(), &[(1, series_points(50, 0, 1))]);
        let pristine = fs::read(&path).unwrap();

        // Flip a byte inside the first block: open succeeds, read fails.
        let mut bytes = pristine.clone();
        bytes[MAGIC.len() + 2] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let seg = Segment::open(&path, 1).unwrap();
        assert!(matches!(seg.read_all(1), Err(Error::Corruption(_))));

        // Damage the index: open fails.
        let mut bytes = pristine.clone();
        let idx = bytes.len() - FOOTER_LEN - 2;
        bytes[idx] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert!(matches!(Segment::open(&path, 1), Err(Error::Corruption(_))));

        // Truncated file.
        fs::write(&path, &pristine[..pristine.len() - 1]).unwrap();
        assert!(Segment::open(&path, 1).is_err());

        // Wrong magic.
        let mut bytes = pristine;
        bytes[0] = b'X';
        fs::write(&path, &bytes).unwrap();
        assert!(Segment::open(&path, 1).is_err());
    }

    #[test]
    fn empty_segment_is_valid() {
        let dir = tempfile::tempdir().unwrap();
        let (path, stats) = write_segment(dir.path(), &[]);
        assert_eq!(stats.points, 0);
        let seg = Segment::open(&path, 9).unwrap();
        assert_eq!(seg.stats().series, 0);
        assert_eq!(seg.id(), 9);
    }

    #[test]
    fn obsolete_segment_is_deleted_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let (path, _) = write_segment(dir.path(), &[(1, series_points(5, 0, 1))]);
        let seg = Segment::open(&path, 1).unwrap();
        seg.mark_obsolete();
        assert!(path.exists());
        drop(seg);
        assert!(!path.exists());
    }
}
