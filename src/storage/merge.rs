//! K-way merge of time-sorted point runs with last-writer-wins semantics.
//!
//! Reads see the union of several sources (segments from oldest to newest,
//! then the immutable and active memtables). When two sources hold a point
//! at the same timestamp, the newer source wins, which is how overwrites
//! and out-of-order corrections become visible without rewriting segments.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::model::Timestamp;

#[derive(Debug, PartialEq, Eq)]
struct Head {
    ts: Reverse<Timestamp>,
    /// Index of the source; larger means newer.
    source: usize,
    pos: usize,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        // Smallest timestamp first; among equal timestamps the newest
        // source surfaces first so it is the one that gets emitted.
        self.ts
            .cmp(&other.ts)
            .then_with(|| self.source.cmp(&other.source))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Merge runs that are each sorted by strictly increasing timestamp.
///
/// `sources` are ordered oldest to newest; for duplicate timestamps the
/// value from the newest source is kept.
pub fn merge_sorted(sources: Vec<Vec<(Timestamp, f64)>>) -> Vec<(Timestamp, f64)> {
    let mut non_empty: Vec<Vec<(Timestamp, f64)>> =
        sources.into_iter().filter(|s| !s.is_empty()).collect();
    match non_empty.len() {
        0 => return Vec::new(),
        1 => return non_empty.pop().expect("one source"),
        _ => {}
    }
    for s in &non_empty {
        debug_assert!(
            s.windows(2).all(|w| w[0].0 < w[1].0),
            "merge sources must be strictly sorted"
        );
    }
    let total: usize = non_empty.iter().map(Vec::len).sum();
    let mut out = Vec::with_capacity(total);
    let mut heap: BinaryHeap<Head> = non_empty
        .iter()
        .enumerate()
        .map(|(source, run)| Head {
            ts: Reverse(run[0].0),
            source,
            pos: 0,
        })
        .collect();

    while let Some(head) = heap.pop() {
        let ts = head.ts.0;
        out.push(non_empty[head.source][head.pos]);
        advance(&mut heap, &non_empty, head);
        // Discard older duplicates of the timestamp just emitted.
        while heap.peek().is_some_and(|h| h.ts.0 == ts) {
            let dup = heap.pop().expect("peeked");
            advance(&mut heap, &non_empty, dup);
        }
    }
    out
}

fn advance(heap: &mut BinaryHeap<Head>, runs: &[Vec<(Timestamp, f64)>], head: Head) {
    let next = head.pos + 1;
    if let Some(&(ts, _)) = runs[head.source].get(next) {
        heap.push(Head {
            ts: Reverse(ts),
            source: head.source,
            pos: next,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newest_source_wins_ties() {
        let old = vec![(1, 1.0), (2, 1.0), (4, 1.0)];
        let mid = vec![(2, 2.0), (3, 2.0)];
        let new = vec![(2, 3.0), (4, 3.0), (9, 3.0)];
        let merged = merge_sorted(vec![old, mid, new]);
        assert_eq!(
            merged,
            vec![(1, 1.0), (2, 3.0), (3, 2.0), (4, 3.0), (9, 3.0)]
        );
    }

    #[test]
    fn trivial_cases() {
        assert!(merge_sorted(vec![]).is_empty());
        assert!(merge_sorted(vec![vec![], vec![]]).is_empty());
        let only = vec![(5, 5.0)];
        assert_eq!(merge_sorted(vec![vec![], only.clone()]), only);
    }

    #[test]
    fn disjoint_runs_interleave() {
        let a = vec![(1, 0.0), (3, 0.0), (5, 0.0)];
        let b = vec![(2, 1.0), (4, 1.0), (6, 1.0)];
        let ts: Vec<i64> = merge_sorted(vec![a, b]).iter().map(|p| p.0).collect();
        assert_eq!(ts, vec![1, 2, 3, 4, 5, 6]);
    }
}
