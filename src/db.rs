//! The storage engine: ties the catalog, WAL, memtables and segments together.
//!
//! # Write path
//!
//! 1. Series keys are resolved to ids (new keys are durably registered).
//! 2. The batch is appended to the WAL (fsynced when `sync_wal` is set).
//! 3. Points go into the active memtable.
//! 4. When the memtable exceeds `memtable_bytes`, it is frozen, the WAL
//!    rotates, and the frozen memtable is written out as a level-0 segment.
//!
//! # Read path
//!
//! A read for one series merges, oldest to newest: every live segment that
//! may overlap the range (ordered by `seq`), the frozen memtable, then the
//! active memtable. Newer sources win on equal timestamps.
//!
//! # Locking
//!
//! Lock order is `maintenance -> wal -> state` and `manifest -> state`.
//! Writers hold `wal` while inserting into the memtable so WAL order and
//! memtable order agree. Flush, compaction and retention serialise on
//! `maintenance` and do their heavy I/O without holding `state`.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::clock::{Clock, SystemClock};
use crate::error::{Error, IoContext, Result};
use crate::lineproto;
use crate::model::{Point, Precision, SeriesId, SeriesKey, Timestamp};
use crate::storage::catalog::{Catalog, SeriesIndex};
use crate::storage::manifest::{parse_segment_file_name, segment_file_name, Manifest, SegmentMeta};
use crate::storage::memtable::Memtable;
use crate::storage::merge::merge_sorted;
use crate::storage::segment::{Segment, SegmentWriter};
use crate::storage::wal::{Wal, WalEntry};

/// A time-sorted run of points for one series.
type PointRun = Vec<(Timestamp, f64)>;

/// Engine configuration.
#[derive(Clone)]
pub struct Options {
    /// Flush the memtable once its estimated size reaches this many bytes.
    pub memtable_bytes: usize,
    /// fsync the WAL after every write batch.
    pub sync_wal: bool,
    /// Drop data older than this many nanoseconds (relative to the clock).
    pub retention: Option<i64>,
    /// Number of same-level segments merged by one tiered compaction.
    pub compaction_fanout: usize,
    /// Highest level produced by tiered compaction.
    pub max_level: u32,
    /// Run tiered compaction automatically after each automatic flush.
    pub auto_compact: bool,
    /// Time source for retention and query-relative times.
    pub clock: Arc<dyn Clock>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_bytes: 16 << 20,
            sync_wal: true,
            retention: None,
            compaction_fanout: 4,
            max_level: 3,
            auto_compact: true,
            clock: Arc::new(SystemClock),
        }
    }
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("memtable_bytes", &self.memtable_bytes)
            .field("sync_wal", &self.sync_wal)
            .field("retention", &self.retention)
            .field("compaction_fanout", &self.compaction_fanout)
            .field("max_level", &self.max_level)
            .field("auto_compact", &self.auto_compact)
            .finish_non_exhaustive()
    }
}

impl Options {
    fn validate(&self) -> Result<()> {
        if self.compaction_fanout < 2 {
            return Err(Error::InvalidArgument(
                "compaction_fanout must be at least 2".into(),
            ));
        }
        if self.memtable_bytes == 0 {
            return Err(Error::InvalidArgument(
                "memtable_bytes must be positive".into(),
            ));
        }
        if self.retention.is_some_and(|r| r <= 0) {
            return Err(Error::InvalidArgument("retention must be positive".into()));
        }
        Ok(())
    }
}

/// Outcome of a write call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct WriteSummary {
    /// Points stored.
    pub written: usize,
    /// Points discarded because they were already outside retention.
    pub expired: usize,
}

/// Outcome of one compaction.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CompactionReport {
    /// Ids of the merged input segments.
    pub inputs: Vec<u64>,
    /// Id of the produced segment, if any data survived.
    pub output: Option<u64>,
    /// Level of the output.
    pub level: u32,
    /// Points across all inputs.
    pub points_in: u64,
    /// Points in the output.
    pub points_out: u64,
    /// Bytes across all inputs.
    pub bytes_in: u64,
    /// Bytes of the output.
    pub bytes_out: u64,
}

/// Point-in-time engine statistics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DbStats {
    /// Registered series.
    pub series: usize,
    /// Points in the active memtable.
    pub memtable_points: usize,
    /// Estimated active memtable size.
    pub memtable_bytes: usize,
    /// Points in a frozen memtable awaiting flush.
    pub frozen_points: usize,
    /// Bytes across WAL files.
    pub wal_bytes: u64,
    /// Live segments, oldest first.
    pub segments: Vec<SegmentMeta>,
}

#[derive(Clone)]
struct Frozen {
    mem: Arc<Memtable>,
    wal_floor: u64,
}

struct State {
    active: Memtable,
    frozen: Option<Frozen>,
    /// Live segments in manifest (`seq`) order.
    segments: Vec<Arc<Segment>>,
}

/// An open database directory.
pub struct Db {
    dir: PathBuf,
    opts: Options,
    _lock: File,
    catalog: Catalog,
    wal: Mutex<Wal>,
    state: RwLock<State>,
    manifest: Mutex<Manifest>,
    maintenance: Mutex<()>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("dir", &self.dir)
            .field("opts", &self.opts)
            .finish_non_exhaustive()
    }
}

impl Db {
    /// Open (creating if needed) the database in `dir`.
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Db> {
        opts.validate()?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).at(&dir)?;

        let lock_path = dir.join("LOCK");
        let lock = File::create(&lock_path).at(&lock_path)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(Error::InvalidArgument(format!(
                    "{} is already open in another process",
                    dir.display()
                )))
            }
            Err(fs::TryLockError::Error(e)) => return Err(Error::io_at(&lock_path, e)),
        }

        let manifest = Manifest::load(&dir)?;
        let seg_dir = dir.join("segments");
        fs::create_dir_all(&seg_dir).at(&seg_dir)?;
        remove_orphans(&seg_dir, &manifest)?;

        let mut segments = Vec::with_capacity(manifest.segments.len());
        for meta in &manifest.segments {
            let path = seg_dir.join(segment_file_name(meta.id));
            segments.push(Arc::new(Segment::open(&path, meta.id)?));
        }

        let catalog = Catalog::open(&dir.join("series.log"))?;
        let (wal, replay) = Wal::open(&dir.join("wal"), manifest.wal_floor, opts.sync_wal)?;
        let known = catalog.len() as u64;
        let mut active = Memtable::new();
        for e in &replay.entries {
            if e.series >= known {
                return Err(Error::corruption(format!(
                    "WAL references unknown series id {}",
                    e.series
                )));
            }
            active.insert(e.series, e.timestamp, e.value);
        }
        if !replay.entries.is_empty() {
            tracing::info!(
                points = replay.entries.len(),
                truncated_files = replay.truncated_files,
                "replayed write-ahead log"
            );
        }

        let needs_flush = active.approx_bytes() >= opts.memtable_bytes;
        let db = Db {
            dir,
            opts,
            _lock: lock,
            catalog,
            wal: Mutex::new(wal),
            state: RwLock::new(State {
                active,
                frozen: None,
                segments,
            }),
            manifest: Mutex::new(manifest),
            maintenance: Mutex::new(()),
        };
        if needs_flush {
            db.flush()?;
        }
        Ok(db)
    }

    /// Directory holding the database.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Configuration in effect.
    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// Current time according to the configured clock.
    pub fn now(&self) -> Timestamp {
        self.opts.clock.now()
    }

    /// Oldest timestamp still inside the retention window.
    pub fn retention_cutoff(&self) -> Option<Timestamp> {
        self.opts.retention.map(|r| self.now().saturating_sub(r))
    }

    /// Run `f` with shared access to the series index.
    pub fn with_index<R>(&self, f: impl FnOnce(&SeriesIndex) -> R) -> R {
        self.catalog.read(f)
    }

    /// Key registered for `id`.
    pub fn series_key(&self, id: SeriesId) -> Option<SeriesKey> {
        self.catalog.key_of(id)
    }

    /// Parse line protocol and write it. Lines without a timestamp use the
    /// clock's current time.
    pub fn write_lines(&self, body: &str, precision: Precision) -> Result<WriteSummary> {
        let points = lineproto::parse_lines(body, precision, self.now())?;
        self.write_points(&points)
    }

    /// Write a batch of points atomically with respect to the WAL.
    pub fn write_points(&self, points: &[Point]) -> Result<WriteSummary> {
        let cutoff = self.retention_cutoff();
        let live: Vec<&Point> = points
            .iter()
            .filter(|p| cutoff.is_none_or(|c| p.timestamp >= c))
            .collect();
        let summary = WriteSummary {
            written: live.len(),
            expired: points.len() - live.len(),
        };
        if live.is_empty() {
            return Ok(summary);
        }
        let keys: Vec<&SeriesKey> = live.iter().map(|p| &p.key).collect();
        let ids = self.catalog.resolve(&keys)?;
        let entries: Vec<WalEntry> = live
            .iter()
            .zip(ids)
            .map(|(p, series)| WalEntry {
                series,
                timestamp: p.timestamp,
                value: p.value,
            })
            .collect();

        let needs_flush = {
            let mut wal = self.wal.lock();
            wal.append(&entries)?;
            let mut st = self.state.write();
            for e in &entries {
                st.active.insert(e.series, e.timestamp, e.value);
            }
            st.active.approx_bytes() >= self.opts.memtable_bytes
        };
        if needs_flush {
            self.flush()?;
            if self.opts.auto_compact {
                self.compact()?;
            }
        }
        Ok(summary)
    }

    /// Read points for each series in `ids` within `[start, end)`.
    ///
    /// The range is clamped to the retention window. The result has one
    /// vector per requested id, in the same order.
    pub fn read_series(
        &self,
        ids: &[SeriesId],
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Vec<Vec<(Timestamp, f64)>>> {
        let start = match self.retention_cutoff() {
            Some(c) => start.max(c),
            None => start,
        };
        if start >= end {
            return Ok(vec![Vec::new(); ids.len()]);
        }
        let (segments, memory) = {
            let st = self.state.read();
            let memory: Vec<(PointRun, PointRun)> = ids
                .iter()
                .map(|&id| {
                    let frozen = st
                        .frozen
                        .as_ref()
                        .map(|f| f.mem.range(id, start, end))
                        .unwrap_or_default();
                    (frozen, st.active.range(id, start, end))
                })
                .collect();
            (st.segments.clone(), memory)
        };
        let mut out = Vec::with_capacity(ids.len());
        for (&id, (frozen, active)) in ids.iter().zip(memory) {
            let mut sources = Vec::new();
            for seg in segments.iter().filter(|s| s.may_overlap(id, start, end)) {
                sources.push(seg.read_range(id, start, end)?);
            }
            sources.push(frozen);
            sources.push(active);
            out.push(merge_sorted(sources));
        }
        Ok(out)
    }

    /// Number of live segments whose time span overlaps `[start, end)`,
    /// together with the total number of live segments.
    pub fn segments_overlapping(&self, start: Timestamp, end: Timestamp) -> (usize, usize) {
        let m = self.manifest.lock();
        let hit = m
            .segments
            .iter()
            .filter(|s| s.min_ts < end && s.max_ts >= start)
            .count();
        (hit, m.segments.len())
    }

    /// Flush the memtable to a new level-0 segment.
    ///
    /// Returns the new segment's metadata, or `None` if there was nothing to
    /// write. If a previous flush failed, its frozen memtable is retried
    /// first.
    pub fn flush(&self) -> Result<Option<SegmentMeta>> {
        let _guard = self.maintenance.lock();
        let frozen = {
            let mut wal = self.wal.lock();
            let mut st = self.state.write();
            match &st.frozen {
                Some(f) => f.clone(),
                None => {
                    if st.active.is_empty() {
                        return Ok(None);
                    }
                    let wal_floor = wal.rotate()?;
                    let f = Frozen {
                        mem: Arc::new(std::mem::take(&mut st.active)),
                        wal_floor,
                    };
                    st.frozen = Some(f.clone());
                    f
                }
            }
        };

        let cutoff = self.retention_cutoff();
        let id = self.manifest.lock().allocate_segment_id();
        let path = self.segment_path(id);
        let mut writer = SegmentWriter::create(&path)?;
        for series in frozen.mem.sorted_series() {
            let points: Vec<(Timestamp, f64)> = frozen
                .mem
                .series_points(series)
                .filter(|(t, _)| cutoff.is_none_or(|c| *t >= c))
                .collect();
            if let Err(e) = writer.add_series(series, &points) {
                writer.abort();
                return Err(e);
            }
        }
        let built = if writer.series_count() == 0 {
            writer.abort();
            None
        } else {
            let stats = writer.finish()?;
            let segment = Arc::new(Segment::open(&path, id)?);
            let meta = SegmentMeta {
                id,
                seq: id,
                level: 0,
                min_ts: stats.min_ts,
                max_ts: stats.max_ts,
                points: stats.points,
                bytes: stats.bytes,
            };
            Some((meta, segment))
        };

        {
            let mut manifest = self.manifest.lock();
            let mut next = manifest.clone();
            if let Some((meta, _)) = &built {
                next.add_segment(meta.clone());
            }
            next.wal_floor = frozen.wal_floor;
            if let Err(e) = next.store(&self.dir) {
                if let Some((_, seg)) = &built {
                    seg.mark_obsolete();
                }
                return Err(e);
            }
            *manifest = next;
            let mut st = self.state.write();
            if let Some((_, seg)) = &built {
                st.segments = order_segments(&manifest, &st.segments, Some(seg.clone()));
            }
            st.frozen = None;
        }
        self.wal.lock().remove_below(frozen.wal_floor)?;
        let meta = built.map(|(m, _)| m);
        if let Some(m) = &meta {
            tracing::debug!(
                segment = m.id,
                points = m.points,
                bytes = m.bytes,
                "flushed memtable"
            );
        }
        Ok(meta)
    }

    /// Run tiered compaction until no level has `compaction_fanout`
    /// contiguous segments left to merge.
    pub fn compact(&self) -> Result<Vec<CompactionReport>> {
        let _guard = self.maintenance.lock();
        let mut reports = Vec::new();
        while let Some((inputs, level)) = self.pick_tiered() {
            reports.push(self.run_compaction(&inputs, level)?);
        }
        Ok(reports)
    }

    /// Merge every live segment into one (also purging expired points).
    pub fn compact_all(&self) -> Result<Option<CompactionReport>> {
        let _guard = self.maintenance.lock();
        let inputs = self.manifest.lock().segments.clone();
        if inputs.is_empty() {
            return Ok(None);
        }
        let level = inputs
            .iter()
            .map(|s| s.level + 1)
            .max()
            .unwrap_or(1)
            .min(self.opts.max_level.max(1));
        self.run_compaction(&inputs, level).map(Some)
    }

    /// Choose the first run of `fanout` adjacent segments sharing a level.
    ///
    /// Only adjacent segments (in `seq` order) may be merged: the output
    /// takes their place in the recency order, so no other segment can sit
    /// between inputs and change which value wins for a timestamp.
    fn pick_tiered(&self) -> Option<(Vec<SegmentMeta>, u32)> {
        let manifest = self.manifest.lock();
        let segs = &manifest.segments;
        let fanout = self.opts.compaction_fanout;
        for level in 0..self.opts.max_level {
            let mut run_start = 0;
            for i in 0..=segs.len() {
                let continues = i < segs.len() && segs[i].level == level;
                if continues {
                    if i + 1 - run_start == fanout {
                        return Some((segs[run_start..=i].to_vec(), level + 1));
                    }
                } else {
                    run_start = i + 1;
                }
            }
        }
        None
    }

    fn run_compaction(&self, inputs: &[SegmentMeta], level: u32) -> Result<CompactionReport> {
        let segments: Vec<Arc<Segment>> = {
            let st = self.state.read();
            inputs
                .iter()
                .map(|m| {
                    st.segments
                        .iter()
                        .find(|s| s.id() == m.id)
                        .cloned()
                        .ok_or_else(|| Error::corruption(format!("segment {} is not open", m.id)))
                })
                .collect::<Result<_>>()?
        };
        let cutoff = self.retention_cutoff();
        let id = self.manifest.lock().allocate_segment_id();
        let path = self.segment_path(id);
        let mut writer = SegmentWriter::create(&path)?;
        let series: BTreeSet<SeriesId> = segments.iter().flat_map(|s| s.series_ids()).collect();
        let merge_result: Result<()> = (|| {
            for sid in series {
                let sources = segments
                    .iter()
                    .filter(|s| s.contains(sid))
                    .map(|s| s.read_all(sid))
                    .collect::<Result<Vec<_>>>()?;
                let mut merged = merge_sorted(sources);
                if let Some(c) = cutoff {
                    merged.retain(|(t, _)| *t >= c);
                }
                writer.add_series(sid, &merged)?;
            }
            Ok(())
        })();
        if let Err(e) = merge_result {
            writer.abort();
            return Err(e);
        }

        let seq = inputs.iter().map(|m| m.seq).max().unwrap_or(id);
        let built = if writer.series_count() == 0 {
            writer.abort();
            None
        } else {
            let stats = writer.finish()?;
            let segment = Arc::new(Segment::open(&path, id)?);
            let meta = SegmentMeta {
                id,
                seq,
                level,
                min_ts: stats.min_ts,
                max_ts: stats.max_ts,
                points: stats.points,
                bytes: stats.bytes,
            };
            Some((meta, segment))
        };

        let input_ids: Vec<u64> = inputs.iter().map(|m| m.id).collect();
        {
            let mut manifest = self.manifest.lock();
            let mut next = manifest.clone();
            if next.remove_segments(&input_ids) != input_ids.len() {
                if let Some((_, seg)) = &built {
                    seg.mark_obsolete();
                }
                return Err(Error::corruption(
                    "compaction inputs vanished from manifest",
                ));
            }
            if let Some((meta, _)) = &built {
                next.add_segment(meta.clone());
            }
            if let Err(e) = next.store(&self.dir) {
                if let Some((_, seg)) = &built {
                    seg.mark_obsolete();
                }
                return Err(e);
            }
            *manifest = next;
            let mut st = self.state.write();
            st.segments =
                order_segments(&manifest, &st.segments, built.as_ref().map(|b| b.1.clone()));
        }
        for seg in &segments {
            seg.mark_obsolete();
        }

        let report = CompactionReport {
            inputs: input_ids,
            output: built.as_ref().map(|b| b.0.id),
            level,
            points_in: inputs.iter().map(|m| m.points).sum(),
            points_out: built.as_ref().map_or(0, |b| b.0.points),
            bytes_in: inputs.iter().map(|m| m.bytes).sum(),
            bytes_out: built.as_ref().map_or(0, |b| b.0.bytes),
        };
        tracing::debug!(?report, "compaction finished");
        Ok(report)
    }

    /// Drop segments that lie entirely outside the retention window and
    /// purge expired points from the active memtable. Returns the number
    /// of segments removed.
    pub fn enforce_retention(&self) -> Result<usize> {
        let Some(cutoff) = self.retention_cutoff() else {
            return Ok(0);
        };
        let _guard = self.maintenance.lock();
        self.state.write().active.drop_before(cutoff);
        let mut manifest = self.manifest.lock();
        let expired: Vec<u64> = manifest
            .segments
            .iter()
            .filter(|s| s.max_ts < cutoff)
            .map(|s| s.id)
            .collect();
        if expired.is_empty() {
            return Ok(0);
        }
        let mut next = manifest.clone();
        next.remove_segments(&expired);
        next.store(&self.dir)?;
        *manifest = next;
        let mut st = self.state.write();
        for seg in st.segments.iter().filter(|s| expired.contains(&s.id())) {
            seg.mark_obsolete();
        }
        st.segments = order_segments(&manifest, &st.segments, None);
        tracing::info!(
            removed = expired.len(),
            cutoff,
            "retention removed segments"
        );
        Ok(expired.len())
    }

    /// Watermark of a downsampling rule.
    pub fn downsample_watermark(&self, rule: &str) -> Option<Timestamp> {
        self.manifest
            .lock()
            .downsample_watermarks
            .get(rule)
            .copied()
    }

    /// Persist the watermark of a downsampling rule.
    pub fn set_downsample_watermark(&self, rule: &str, ts: Timestamp) -> Result<()> {
        let mut manifest = self.manifest.lock();
        let mut next = manifest.clone();
        next.downsample_watermarks.insert(rule.to_string(), ts);
        next.store(&self.dir)?;
        *manifest = next;
        Ok(())
    }

    /// Current engine statistics.
    pub fn stats(&self) -> Result<DbStats> {
        let wal_bytes = self.wal.lock().disk_bytes()?;
        let segments = self.manifest.lock().segments.clone();
        let st = self.state.read();
        Ok(DbStats {
            series: self.catalog.len(),
            memtable_points: st.active.len(),
            memtable_bytes: st.active.approx_bytes(),
            frozen_points: st.frozen.as_ref().map_or(0, |f| f.mem.len()),
            wal_bytes,
            segments,
        })
    }

    /// Flush outstanding writes and close the database.
    pub fn close(self) -> Result<()> {
        self.flush()?;
        Ok(())
    }

    fn segment_path(&self, id: u64) -> PathBuf {
        self.dir.join("segments").join(segment_file_name(id))
    }
}

/// Rebuild the open-segment list in manifest order, reusing open handles
/// and adding `extra` (a freshly written segment).
fn order_segments(
    manifest: &Manifest,
    current: &[Arc<Segment>],
    extra: Option<Arc<Segment>>,
) -> Vec<Arc<Segment>> {
    let mut by_id: HashMap<u64, Arc<Segment>> =
        current.iter().map(|s| (s.id(), s.clone())).collect();
    if let Some(seg) = extra {
        by_id.insert(seg.id(), seg);
    }
    manifest
        .segments
        .iter()
        .filter_map(|m| by_id.get(&m.id).cloned())
        .collect()
}

/// Delete temporary files and segments the manifest does not reference.
fn remove_orphans(seg_dir: &Path, manifest: &Manifest) -> Result<()> {
    let live: BTreeSet<u64> = manifest.segments.iter().map(|s| s.id).collect();
    for entry in fs::read_dir(seg_dir).at(seg_dir)? {
        let entry = entry.at(seg_dir)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let orphan = name.ends_with(".tmp")
            || parse_segment_file_name(name).is_some_and(|id| !live.contains(&id));
        if orphan {
            let path = entry.path();
            tracing::warn!(path = %path.display(), "removing orphaned segment file");
            fs::remove_file(&path).at(&path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    fn opts() -> Options {
        Options {
            sync_wal: false,
            auto_compact: false,
            ..Options::default()
        }
    }

    fn point(host: &str, ts: Timestamp, v: f64) -> Point {
        Point::new(
            SeriesKey::new("cpu", [("host", host)], "usage").unwrap(),
            ts,
            v,
        )
    }

    fn read_one(db: &Db, host: &str) -> Vec<(Timestamp, f64)> {
        let key = SeriesKey::new("cpu", [("host", host)], "usage").unwrap();
        let id = db.with_index(|i| i.id_of(&key)).expect("series exists");
        db.read_series(&[id], Timestamp::MIN, Timestamp::MAX)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn write_read_flush_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path(), opts()).unwrap();
            db.write_points(&[point("a", 1, 1.0), point("a", 2, 2.0), point("b", 1, 9.0)])
                .unwrap();
            assert_eq!(read_one(&db, "a"), vec![(1, 1.0), (2, 2.0)]);
            let meta = db.flush().unwrap().unwrap();
            assert_eq!(meta.points, 3);
            db.write_points(&[point("a", 3, 3.0)]).unwrap();
            assert_eq!(read_one(&db, "a"), vec![(1, 1.0), (2, 2.0), (3, 3.0)]);
        }
        let db = Db::open(dir.path(), opts()).unwrap();
        assert_eq!(read_one(&db, "a"), vec![(1, 1.0), (2, 2.0), (3, 3.0)]);
        assert_eq!(read_one(&db, "b"), vec![(1, 9.0)]);
        let stats = db.stats().unwrap();
        assert_eq!(stats.segments.len(), 1);
        assert_eq!(stats.memtable_points, 1);
    }

    #[test]
    fn newer_writes_override_flushed_data() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), opts()).unwrap();
        db.write_points(&[point("a", 1, 1.0), point("a", 2, 1.0)])
            .unwrap();
        db.flush().unwrap();
        db.write_points(&[point("a", 2, 5.0)]).unwrap();
        db.flush().unwrap();
        db.write_points(&[point("a", 1, 7.0)]).unwrap();
        assert_eq!(read_one(&db, "a"), vec![(1, 7.0), (2, 5.0)]);
        db.compact_all().unwrap();
        assert_eq!(read_one(&db, "a"), vec![(1, 7.0), (2, 5.0)]);
        db.flush().unwrap();
        db.compact_all().unwrap();
        assert_eq!(read_one(&db, "a"), vec![(1, 7.0), (2, 5.0)]);
        assert_eq!(db.stats().unwrap().segments.len(), 1);
    }

    #[test]
    fn tiered_compaction_merges_adjacent_runs() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options {
                compaction_fanout: 2,
                ..opts()
            },
        )
        .unwrap();
        for i in 0..5 {
            db.write_points(&[point("a", i, i as f64), point("a", 0, 100.0 + i as f64)])
                .unwrap();
            db.flush().unwrap();
        }
        let reports = db.compact().unwrap();
        assert!(!reports.is_empty());
        let levels: Vec<u32> = db
            .stats()
            .unwrap()
            .segments
            .iter()
            .map(|s| s.level)
            .collect();
        // 5 flushes with fanout 2: two L0 pairs -> two L1 -> one L2, one L0 left.
        assert_eq!(levels, vec![2, 0]);
        let pts = read_one(&db, "a");
        assert_eq!(pts[0], (0, 104.0));
        assert_eq!(pts.len(), 5);
        // Files of compacted inputs are gone.
        let files = fs::read_dir(dir.path().join("segments")).unwrap().count();
        assert_eq!(files, 2);
    }

    #[test]
    fn retention_drops_old_data_everywhere() {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(ManualClock::new(1_000));
        let db = Db::open(
            dir.path(),
            Options {
                retention: Some(100),
                clock: clock.clone(),
                ..opts()
            },
        )
        .unwrap();
        let summary = db
            .write_points(&[
                point("a", 850, 1.0),
                point("a", 950, 2.0),
                point("a", 990, 3.0),
            ])
            .unwrap();
        assert_eq!(
            summary,
            WriteSummary {
                written: 2,
                expired: 1
            }
        );
        db.flush().unwrap();
        db.write_points(&[point("a", 1_050, 4.0)]).unwrap();
        db.flush().unwrap();

        clock.set(1_060);
        // Point at 950 is now outside the window and invisible to reads.
        assert_eq!(read_one(&db, "a"), vec![(990, 3.0), (1_050, 4.0)]);
        assert_eq!(db.enforce_retention().unwrap(), 0);

        clock.set(1_100);
        assert_eq!(db.enforce_retention().unwrap(), 1);
        assert_eq!(read_one(&db, "a"), vec![(1_050, 4.0)]);
        assert_eq!(db.stats().unwrap().segments.len(), 1);
    }

    #[test]
    fn wal_replay_restores_unflushed_writes() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path(), opts()).unwrap();
            db.write_points(&[point("a", 1, 1.0)]).unwrap();
            db.flush().unwrap();
            db.write_points(&[point("a", 2, 2.0), point("a", 1, 3.0)])
                .unwrap();
            // Dropped without flushing.
        }
        let db = Db::open(dir.path(), opts()).unwrap();
        assert_eq!(read_one(&db, "a"), vec![(1, 3.0), (2, 2.0)]);
        assert_eq!(db.stats().unwrap().memtable_points, 2);
    }

    #[test]
    fn second_open_is_rejected_while_locked() {
        let dir = tempfile::tempdir().unwrap();
        let _db = Db::open(dir.path(), opts()).unwrap();
        let err = Db::open(dir.path(), opts()).unwrap_err();
        assert!(err.to_string().contains("already open"), "{err}");
    }

    #[test]
    fn orphaned_segments_are_cleaned_on_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path(), opts()).unwrap();
            db.write_points(&[point("a", 1, 1.0)]).unwrap();
            db.flush().unwrap();
        }
        let seg_dir = dir.path().join("segments");
        fs::write(seg_dir.join(segment_file_name(999)), b"junk").unwrap();
        fs::write(seg_dir.join("0000000005.tmp"), b"junk").unwrap();
        let db = Db::open(dir.path(), opts()).unwrap();
        assert_eq!(fs::read_dir(&seg_dir).unwrap().count(), 1);
        assert_eq!(read_one(&db, "a"), vec![(1, 1.0)]);
    }

    #[test]
    fn automatic_flush_and_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options {
                memtable_bytes: 4_000,
                compaction_fanout: 3,
                auto_compact: true,
                ..opts()
            },
        )
        .unwrap();
        for i in 0..2_000 {
            db.write_points(&[point("a", i, i as f64)]).unwrap();
        }
        let stats = db.stats().unwrap();
        assert!(!stats.segments.is_empty());
        assert!(stats.segments.iter().filter(|s| s.level == 0).count() < 3);
        let pts = read_one(&db, "a");
        assert_eq!(pts.len(), 2_000);
        assert!(pts.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn invalid_options_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        for bad in [
            Options {
                compaction_fanout: 1,
                ..opts()
            },
            Options {
                retention: Some(0),
                ..opts()
            },
            Options {
                memtable_bytes: 0,
                ..opts()
            },
        ] {
            assert!(Db::open(dir.path(), bad).is_err());
        }
    }
}
