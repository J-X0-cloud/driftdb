//! Series catalog and inverted tag index.
//!
//! Every distinct [`SeriesKey`] gets a dense [`SeriesId`]. The mapping is
//! persisted in an append-only, checksummed log (`series.log`) so ids are
//! stable across restarts; the WAL and segments only ever store ids.
//!
//! The in-memory index answers the questions the query planner asks:
//! which series belong to a measurement, which carry `tag = value`, which
//! carry a tag key at all, and which values a tag key takes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use parking_lot::{Mutex, RwLock};

use crate::encoding::varint::{put_str, put_uvarint, Cursor};
use crate::error::{Error, Result};
use crate::model::{SeriesId, SeriesKey};
use crate::storage::record::{encode_record, RecordFile};

/// Postings for one measurement.
#[derive(Debug, Default, Clone)]
pub struct MeasurementIndex {
    /// Every series in the measurement.
    pub series: BTreeSet<SeriesId>,
    /// Field name to the series that store it.
    pub fields: BTreeMap<String, BTreeSet<SeriesId>>,
    /// Tag key to tag value to series carrying that pair.
    pub tags: BTreeMap<String, BTreeMap<String, BTreeSet<SeriesId>>>,
}

impl MeasurementIndex {
    /// Series that carry tag `key` with exactly `value`.
    pub fn with_tag(&self, key: &str, value: &str) -> BTreeSet<SeriesId> {
        self.tags
            .get(key)
            .and_then(|vals| vals.get(value))
            .cloned()
            .unwrap_or_default()
    }

    /// Series that carry tag `key` with any value.
    pub fn with_tag_key(&self, key: &str) -> BTreeSet<SeriesId> {
        let mut out = BTreeSet::new();
        if let Some(vals) = self.tags.get(key) {
            for ids in vals.values() {
                out.extend(ids.iter().copied());
            }
        }
        out
    }

    /// Series whose value for tag `key` satisfies `pred`.
    pub fn with_tag_matching(&self, key: &str, mut pred: impl FnMut(&str) -> bool) -> BTreeSet<SeriesId> {
        let mut out = BTreeSet::new();
        if let Some(vals) = self.tags.get(key) {
            for (v, ids) in vals {
                if pred(v) {
                    out.extend(ids.iter().copied());
                }
            }
        }
        out
    }

    /// Series storing `field`.
    pub fn with_field(&self, field: &str) -> BTreeSet<SeriesId> {
        self.fields.get(field).cloned().unwrap_or_default()
    }
}

/// Pure in-memory catalog: key to id mapping plus postings.
#[derive(Debug, Default, Clone)]
pub struct SeriesIndex {
    by_key: HashMap<SeriesKey, SeriesId>,
    keys: Vec<SeriesKey>,
    measurements: BTreeMap<String, MeasurementIndex>,
}

impl SeriesIndex {
    /// Number of registered series.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether no series are registered.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Id of `key`, if registered.
    pub fn id_of(&self, key: &SeriesKey) -> Option<SeriesId> {
        self.by_key.get(key).copied()
    }

    /// Key of `id`, if registered.
    pub fn key_of(&self, id: SeriesId) -> Option<&SeriesKey> {
        usize::try_from(id).ok().and_then(|i| self.keys.get(i))
    }

    /// Postings for `measurement`.
    pub fn measurement(&self, measurement: &str) -> Option<&MeasurementIndex> {
        self.measurements.get(measurement)
    }

    /// All measurement names in sorted order.
    pub fn measurement_names(&self) -> impl Iterator<Item = &str> {
        self.measurements.keys().map(String::as_str)
    }

    /// Register `key` under the next id. Returns the id and whether it is new.
    fn insert(&mut self, key: SeriesKey) -> (SeriesId, bool) {
        if let Some(id) = self.by_key.get(&key) {
            return (*id, false);
        }
        let id = self.keys.len() as SeriesId;
        self.index_key(id, &key);
        self.by_key.insert(key.clone(), id);
        self.keys.push(key);
        (id, true)
    }

    /// Restore a persisted mapping. Ids must arrive densely and in order.
    fn restore(&mut self, id: SeriesId, key: SeriesKey) -> Result<()> {
        if id != self.keys.len() as SeriesId {
            return Err(Error::corruption(format!(
                "series catalog out of order: expected id {}, found {id}",
                self.keys.len()
            )));
        }
        if self.by_key.contains_key(&key) {
            return Err(Error::corruption(format!("series {key} registered twice")));
        }
        self.insert(key);
        Ok(())
    }

    fn index_key(&mut self, id: SeriesId, key: &SeriesKey) {
        let m = self.measurements.entry(key.measurement.clone()).or_default();
        m.series.insert(id);
        m.fields.entry(key.field.clone()).or_default().insert(id);
        for (k, v) in &key.tags {
            m.tags
                .entry(k.clone())
                .or_default()
                .entry(v.clone())
                .or_default()
                .insert(id);
        }
    }
}

fn encode_entry(id: SeriesId, key: &SeriesKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    put_uvarint(&mut out, id);
    put_str(&mut out, &key.measurement);
    put_uvarint(&mut out, key.tags.len() as u64);
    for (k, v) in &key.tags {
        put_str(&mut out, k);
        put_str(&mut out, v);
    }
    put_str(&mut out, &key.field);
    out
}

fn decode_entry(payload: &[u8]) -> Result<(SeriesId, SeriesKey)> {
    let mut cur = Cursor::new(payload);
    let id = cur.uvarint()?;
    let measurement = cur.string()?;
    let n = cur.usize()?;
    let mut tags = Vec::with_capacity(n.min(256));
    for _ in 0..n {
        let k = cur.string()?;
        let v = cur.string()?;
        tags.push((k, v));
    }
    let field = cur.string()?;
    if !cur.is_empty() {
        return Err(Error::corruption("trailing bytes in series catalog entry"));
    }
    let key = SeriesKey::new(measurement, tags, field)
        .map_err(|e| Error::corruption(format!("invalid series key in catalog: {e}")))?;
    Ok((id, key))
}

/// Durable, thread-safe series catalog.
#[derive(Debug)]
pub struct Catalog {
    index: RwLock<SeriesIndex>,
    log: Mutex<RecordFile>,
}

impl Catalog {
    /// Open the catalog stored at `path`, replaying every persisted entry.
    pub fn open(path: &Path) -> Result<Self> {
        let (log, payloads, _) = RecordFile::open(path)?;
        let mut index = SeriesIndex::default();
        for payload in payloads {
            let (id, key) = decode_entry(&payload)?;
            index.restore(id, key)?;
        }
        Ok(Catalog {
            index: RwLock::new(index),
            log: Mutex::new(log),
        })
    }

    /// Resolve every key to an id, durably registering unknown keys first.
    ///
    /// New entries are written in one batched, fsynced append before they
    /// become visible, so a WAL record can never reference an id that is
    /// missing from the catalog after a crash.
    pub fn resolve(&self, keys: &[&SeriesKey]) -> Result<Vec<SeriesId>> {
        {
            let index = self.index.read();
            if let Some(ids) = keys.iter().map(|k| index.id_of(k)).collect::<Option<Vec<_>>>() {
                return Ok(ids);
            }
        }
        // The log mutex serialises registrations, so the ids staged below
        // cannot be taken by a concurrent writer before they are installed.
        let mut log = self.log.lock();
        let (ids, framed) = {
            let index = self.index.read();
            let mut staged = index.staging();
            let mut framed = Vec::new();
            let mut ids = Vec::with_capacity(keys.len());
            for key in keys {
                let (id, new) = staged.insert((*key).clone());
                if new {
                    encode_record(&mut framed, &encode_entry(id, key));
                }
                ids.push(id);
            }
            (ids, framed)
        };
        if !framed.is_empty() {
            log.append_framed(&framed, true)?;
            let mut index = self.index.write();
            for key in keys {
                index.insert((*key).clone());
            }
        }
        Ok(ids)
    }

    /// Run `f` with shared access to the index.
    pub fn read<R>(&self, f: impl FnOnce(&SeriesIndex) -> R) -> R {
        f(&self.index.read())
    }

    /// Number of registered series.
    pub fn len(&self) -> usize {
        self.index.read().len()
    }

    /// Whether no series are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Key of `id`, cloned.
    pub fn key_of(&self, id: SeriesId) -> Option<SeriesKey> {
        self.index.read().key_of(id).cloned()
    }
}

impl SeriesIndex {
    /// An overlay used to assign ids while the log mutex is held. Only the
    /// key map and length matter for staging, so postings are not copied.
    fn staging(&self) -> StagingIndex<'_> {
        StagingIndex {
            base: self,
            added: HashMap::new(),
        }
    }
}

/// Overlay that assigns ids for new keys without mutating the shared index.
struct StagingIndex<'a> {
    base: &'a SeriesIndex,
    added: HashMap<SeriesKey, SeriesId>,
}

impl StagingIndex<'_> {
    fn insert(&mut self, key: SeriesKey) -> (SeriesId, bool) {
        if let Some(id) = self.base.id_of(&key) {
            return (id, false);
        }
        if let Some(id) = self.added.get(&key) {
            return (*id, false);
        }
        let id = (self.base.len() + self.added.len()) as SeriesId;
        self.added.insert(key, id);
        (id, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(m: &str, host: &str, field: &str) -> SeriesKey {
        SeriesKey::new(m, [("host", host)], field).unwrap()
    }

    #[test]
    fn assigns_dense_stable_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("series.log");
        let a = key("cpu", "a", "usage");
        let b = key("cpu", "b", "usage");
        let c = key("mem", "a", "free");
        {
            let cat = Catalog::open(&path).unwrap();
            assert_eq!(cat.resolve(&[&a, &b, &a]).unwrap(), vec![0, 1, 0]);
            assert_eq!(cat.resolve(&[&c]).unwrap(), vec![2]);
            assert_eq!(cat.resolve(&[&b]).unwrap(), vec![1]);
        }
        let cat = Catalog::open(&path).unwrap();
        assert_eq!(cat.len(), 3);
        assert_eq!(cat.resolve(&[&c, &a]).unwrap(), vec![2, 0]);
        assert_eq!(cat.key_of(1), Some(b));
    }

    #[test]
    fn postings_answer_tag_queries() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(&dir.path().join("series.log")).unwrap();
        let k1 = SeriesKey::new("cpu", [("host", "a"), ("dc", "x")], "usage").unwrap();
        let k2 = SeriesKey::new("cpu", [("host", "b")], "usage").unwrap();
        let k3 = SeriesKey::new("cpu", [("host", "b")], "idle").unwrap();
        cat.resolve(&[&k1, &k2, &k3]).unwrap();
        cat.read(|idx| {
            let m = idx.measurement("cpu").unwrap();
            assert_eq!(m.series.len(), 3);
            assert_eq!(m.with_tag("host", "b"), BTreeSet::from([1, 2]));
            assert_eq!(m.with_tag_key("dc"), BTreeSet::from([0]));
            assert_eq!(m.with_field("idle"), BTreeSet::from([2]));
            assert_eq!(m.with_tag_matching("host", |v| v > "a"), BTreeSet::from([1, 2]));
            assert!(m.with_tag("host", "zzz").is_empty());
            assert!(idx.measurement("disk").is_none());
            assert_eq!(idx.measurement_names().collect::<Vec<_>>(), vec!["cpu"]);
        });
    }

    #[test]
    fn corrupt_entries_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("series.log");
        {
            let (mut f, _, _) = RecordFile::open(&path).unwrap();
            // Id 5 when 0 is expected.
            f.append(&encode_entry(5, &key("m", "a", "v")), true).unwrap();
        }
        assert!(matches!(Catalog::open(&path), Err(Error::Corruption(_))));
    }

    #[test]
    fn entry_codec_roundtrips() {
        let k = SeriesKey::new("m e", [("a", "1"), ("b", "two words")], "f").unwrap();
        let (id, decoded) = decode_entry(&encode_entry(77, &k)).unwrap();
        assert_eq!(id, 77);
        assert_eq!(decoded, k);
        let mut bad = encode_entry(1, &k);
        bad.push(0);
        assert!(decode_entry(&bad).is_err());
    }
}
