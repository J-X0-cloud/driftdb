//! The manifest: the authoritative list of live segments.
//!
//! The manifest is a small JSON document rewritten atomically (write temp
//! file, fsync, rename, fsync directory) whenever the set of segments
//! changes. A segment file on disk that the manifest does not mention is an
//! orphan from an interrupted flush or compaction and is deleted on open.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, IoContext, Result};
use crate::model::Timestamp;
use crate::storage::segment::sync_dir;

/// File name of the manifest inside the data directory.
pub const MANIFEST_FILE: &str = "MANIFEST.json";
const FORMAT_VERSION: u32 = 1;

/// Manifest entry describing one live segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Unique file id.
    pub id: u64,
    /// Recency rank: when two segments hold the same point, the one with the
    /// larger `seq` wins. Flushes use their id; a compaction output inherits
    /// the largest `seq` of its (contiguous) inputs.
    pub seq: u64,
    /// Compaction level (0 = produced by a memtable flush).
    pub level: u32,
    /// Smallest timestamp stored.
    pub min_ts: Timestamp,
    /// Largest timestamp stored.
    pub max_ts: Timestamp,
    /// Number of points.
    pub points: u64,
    /// File size in bytes.
    pub bytes: u64,
}

/// Persistent engine state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// On-disk format version.
    pub version: u32,
    /// Next id to hand out to a new segment.
    pub next_segment_id: u64,
    /// WAL files with a sequence number below this are fully flushed.
    pub wal_floor: u64,
    /// Live segments ordered by `seq` (oldest first).
    pub segments: Vec<SegmentMeta>,
    /// Per downsampling rule: every window ending at or before this
    /// timestamp has been materialised.
    #[serde(default)]
    pub downsample_watermarks: BTreeMap<String, Timestamp>,
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            version: FORMAT_VERSION,
            next_segment_id: 1,
            wal_floor: 0,
            segments: Vec::new(),
            downsample_watermarks: BTreeMap::new(),
        }
    }
}

impl Manifest {
    /// Load the manifest from `dir`, or return a fresh one if absent.
    pub fn load(dir: &Path) -> Result<Self> {
        let path = dir.join(MANIFEST_FILE);
        match fs::read(&path) {
            Ok(bytes) => {
                let m: Manifest = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Manifest(format!("{}: {e}", path.display())))?;
                if m.version != FORMAT_VERSION {
                    return Err(Error::Manifest(format!(
                        "unsupported manifest version {} (expected {FORMAT_VERSION})",
                        m.version
                    )));
                }
                m.validate()?;
                Ok(m)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::default()),
            Err(e) => Err(Error::io_at(path, e)),
        }
    }

    /// Atomically persist the manifest into `dir`.
    pub fn store(&self, dir: &Path) -> Result<()> {
        let path = dir.join(MANIFEST_FILE);
        let tmp = dir.join(format!("{MANIFEST_FILE}.tmp"));
        let bytes = serde_json::to_vec_pretty(self).map_err(|e| Error::Manifest(e.to_string()))?;
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .at(&tmp)?;
            f.write_all(&bytes).at(&tmp)?;
            f.sync_all().at(&tmp)?;
        }
        fs::rename(&tmp, &path).at(&path)?;
        sync_dir(dir)
    }

    /// Allocate a fresh segment id.
    pub fn allocate_segment_id(&mut self) -> u64 {
        let id = self.next_segment_id;
        self.next_segment_id += 1;
        id
    }

    /// Insert a segment, keeping the list ordered by `seq`.
    pub fn add_segment(&mut self, meta: SegmentMeta) {
        let pos = self.segments.partition_point(|s| s.seq < meta.seq);
        self.segments.insert(pos, meta);
    }

    /// Remove the segments with the given ids. Returns how many were found.
    pub fn remove_segments(&mut self, ids: &[u64]) -> usize {
        let before = self.segments.len();
        self.segments.retain(|s| !ids.contains(&s.id));
        before - self.segments.len()
    }

    fn validate(&self) -> Result<()> {
        for w in self.segments.windows(2) {
            if w[0].seq >= w[1].seq {
                return Err(Error::Manifest("segment seqs are not strictly ascending".into()));
            }
        }
        let mut ids: Vec<u64> = self.segments.iter().map(|s| s.id).collect();
        ids.sort_unstable();
        if ids.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Manifest("duplicate segment id".into()));
        }
        if let Some(&max) = ids.last() {
            if max >= self.next_segment_id {
                return Err(Error::Manifest(format!(
                    "segment id {max} is not below next_segment_id {}",
                    self.next_segment_id
                )));
            }
        }
        Ok(())
    }
}

/// File name used for the segment with `id`.
pub fn segment_file_name(id: u64) -> String {
    format!("{id:010}.sst")
}

/// Parse a segment id out of a file name produced by [`segment_file_name`].
pub fn parse_segment_file_name(name: &str) -> Option<u64> {
    name.strip_suffix(".sst")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u64) -> SegmentMeta {
        SegmentMeta {
            id,
            seq: id,
            level: 0,
            min_ts: 0,
            max_ts: 10,
            points: 5,
            bytes: 100,
        }
    }

    #[test]
    fn missing_manifest_is_default() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(Manifest::load(dir.path()).unwrap(), Manifest::default());
    }

    #[test]
    fn store_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = Manifest::default();
        let a = m.allocate_segment_id();
        let b = m.allocate_segment_id();
        m.add_segment(meta(b));
        m.add_segment(meta(a));
        m.wal_floor = 4;
        m.downsample_watermarks.insert("cpu_5m".into(), 300);
        m.store(dir.path()).unwrap();
        let loaded = Manifest::load(dir.path()).unwrap();
        assert_eq!(loaded, m);
        assert_eq!(loaded.segments.iter().map(|s| s.id).collect::<Vec<_>>(), vec![1, 2]);
        assert!(!dir.path().join("MANIFEST.json.tmp").exists());
    }

    #[test]
    fn remove_segments_reports_count() {
        let mut m = Manifest::default();
        for _ in 0..3 {
            let id = m.allocate_segment_id();
            m.add_segment(meta(id));
        }
        assert_eq!(m.remove_segments(&[1, 3, 99]), 2);
        assert_eq!(m.segments.len(), 1);
    }

    #[test]
    fn invalid_manifests_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(MANIFEST_FILE), b"{not json").unwrap();
        assert!(matches!(Manifest::load(dir.path()), Err(Error::Manifest(_))));

        let mut m = Manifest::default();
        m.segments = vec![meta(5)];
        m.next_segment_id = 3;
        m.store(dir.path()).unwrap();
        assert!(Manifest::load(dir.path()).is_err());
    }

    #[test]
    fn segment_file_names() {
        assert_eq!(segment_file_name(42), "0000000042.sst");
        assert_eq!(parse_segment_file_name("0000000042.sst"), Some(42));
        assert_eq!(parse_segment_file_name("0000000042.tmp"), None);
    }
}
