//! In-memory write buffer.
//!
//! Points are kept per series in a `BTreeMap` keyed by timestamp, so a later
//! write to the same `(series, timestamp)` replaces the earlier one and
//! range scans come out sorted for free.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use crate::model::{SeriesId, Timestamp};

/// Rough per-point memory cost used for flush accounting: key and value
/// plus amortised B-tree node overhead.
const APPROX_BYTES_PER_POINT: usize = 40;
/// Fixed overhead charged for every distinct series.
const APPROX_BYTES_PER_SERIES: usize = 96;

/// Mutable, sorted, per-series buffer of recent writes.
#[derive(Debug, Default, Clone)]
pub struct Memtable {
    series: HashMap<SeriesId, BTreeMap<Timestamp, f64>>,
    points: usize,
    min_ts: Option<Timestamp>,
    max_ts: Option<Timestamp>,
}

impl Memtable {
    /// Create an empty memtable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite a point.
    pub fn insert(&mut self, series: SeriesId, ts: Timestamp, value: f64) {
        let points = self.series.entry(series).or_default();
        if points.insert(ts, value).is_none() {
            self.points += 1;
        }
        self.min_ts = Some(self.min_ts.map_or(ts, |m| m.min(ts)));
        self.max_ts = Some(self.max_ts.map_or(ts, |m| m.max(ts)));
    }

    /// Number of distinct points held.
    pub fn len(&self) -> usize {
        self.points
    }

    /// Whether the memtable holds no points.
    pub fn is_empty(&self) -> bool {
        self.points == 0
    }

    /// Number of distinct series held.
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Estimated heap footprint in bytes.
    pub fn approx_bytes(&self) -> usize {
        self.points * APPROX_BYTES_PER_POINT + self.series.len() * APPROX_BYTES_PER_SERIES
    }

    /// Smallest and largest timestamps held.
    pub fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        Some((self.min_ts?, self.max_ts?))
    }

    /// Whether `series` has any points.
    pub fn contains_series(&self, series: SeriesId) -> bool {
        self.series.contains_key(&series)
    }

    /// Points of `series` with `start <= ts < end`, sorted by time.
    pub fn range(
        &self,
        series: SeriesId,
        start: Timestamp,
        end: Timestamp,
    ) -> Vec<(Timestamp, f64)> {
        if start >= end {
            return Vec::new();
        }
        self.series
            .get(&series)
            .map(|points| {
                points
                    .range((Bound::Included(start), Bound::Excluded(end)))
                    .map(|(&t, &v)| (t, v))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every series id held, sorted ascending.
    pub fn sorted_series(&self) -> Vec<SeriesId> {
        let mut ids: Vec<SeriesId> = self.series.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// All points of one series in time order.
    pub fn series_points(&self, series: SeriesId) -> impl Iterator<Item = (Timestamp, f64)> + '_ {
        self.series
            .get(&series)
            .into_iter()
            .flat_map(|m| m.iter().map(|(&t, &v)| (t, v)))
    }

    /// Drop every point older than `cutoff`. Returns the number removed.
    pub fn drop_before(&mut self, cutoff: Timestamp) -> usize {
        let mut removed = 0;
        self.series.retain(|_, points| {
            let keep = points.split_off(&cutoff);
            removed += points.len();
            *points = keep;
            !points.is_empty()
        });
        self.points -= removed;
        if removed > 0 {
            self.recompute_bounds();
        }
        removed
    }

    fn recompute_bounds(&mut self) {
        self.min_ts = self
            .series
            .values()
            .filter_map(|m| m.keys().next().copied())
            .min();
        self.max_ts = self
            .series
            .values()
            .filter_map(|m| m.keys().next_back().copied())
            .max();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwrites_do_not_double_count() {
        let mut m = Memtable::new();
        m.insert(1, 10, 1.0);
        m.insert(1, 10, 2.0);
        m.insert(1, 5, 3.0);
        m.insert(2, 7, 4.0);
        assert_eq!(m.len(), 3);
        assert_eq!(m.series_count(), 2);
        assert_eq!(m.range(1, 0, 100), vec![(5, 3.0), (10, 2.0)]);
        assert_eq!(m.time_range(), Some((5, 10)));
        assert!(m.approx_bytes() > 0);
    }

    #[test]
    fn range_is_half_open() {
        let mut m = Memtable::new();
        for ts in 0..10 {
            m.insert(9, ts, ts as f64);
        }
        assert_eq!(m.range(9, 3, 6).len(), 3);
        assert!(m.range(9, 6, 6).is_empty());
        assert!(m.range(9, 7, 3).is_empty());
        assert!(m.range(8, 0, 10).is_empty());
    }

    #[test]
    fn drop_before_updates_counts_and_bounds() {
        let mut m = Memtable::new();
        m.insert(1, 1, 1.0);
        m.insert(1, 5, 1.0);
        m.insert(2, 2, 1.0);
        assert_eq!(m.drop_before(3), 2);
        assert_eq!(m.len(), 1);
        assert_eq!(m.series_count(), 1);
        assert_eq!(m.time_range(), Some((5, 5)));
        assert_eq!(m.drop_before(100), 1);
        assert!(m.is_empty());
        assert_eq!(m.time_range(), None);
    }

    #[test]
    fn sorted_series_and_iteration() {
        let mut m = Memtable::new();
        m.insert(5, 2, 2.0);
        m.insert(3, 1, 1.0);
        m.insert(5, 1, 1.0);
        assert_eq!(m.sorted_series(), vec![3, 5]);
        assert_eq!(
            m.series_points(5).collect::<Vec<_>>(),
            vec![(1, 1.0), (2, 2.0)]
        );
        assert_eq!(m.series_points(42).count(), 0);
    }
}
