//! Streaming aggregate accumulators.

use crate::model::Timestamp;
use crate::query::ast::AggFunc;

/// Running state of one aggregate over one window.
#[derive(Debug, Clone)]
pub enum Accumulator {
    /// Number of points.
    Count(u64),
    /// Kahan-compensated sum and whether any value was seen.
    Sum {
        /// Running sum.
        sum: f64,
        /// Compensation term.
        comp: f64,
        /// Whether at least one value was added.
        seen: bool,
    },
    /// Compensated sum and count; the mean is computed once at the end,
    /// which is exact for sums that fit in a double's mantissa.
    Mean {
        /// Running sum.
        sum: f64,
        /// Compensation term.
        comp: f64,
        /// Points seen.
        n: u64,
    },
    /// Welford state for the sample standard deviation.
    Moments {
        /// Points seen.
        n: u64,
        /// Running mean.
        mean: f64,
        /// Sum of squared deviations.
        m2: f64,
    },
    /// Running minimum.
    Min(Option<f64>),
    /// Running maximum.
    Max(Option<f64>),
    /// Earliest point.
    First(Option<(Timestamp, f64)>),
    /// Latest point.
    Last(Option<(Timestamp, f64)>),
    /// Running min and max.
    Spread(Option<(f64, f64)>),
    /// Every value, for order statistics.
    Values {
        /// Collected values.
        values: Vec<f64>,
        /// `None` means median.
        percentile: Option<f64>,
    },
}

impl Accumulator {
    /// Fresh state for `func`.
    pub fn new(func: AggFunc) -> Self {
        match func {
            AggFunc::Count => Accumulator::Count(0),
            AggFunc::Sum => Accumulator::Sum {
                sum: 0.0,
                comp: 0.0,
                seen: false,
            },
            AggFunc::Mean => Accumulator::Mean {
                sum: 0.0,
                comp: 0.0,
                n: 0,
            },
            AggFunc::Stddev => Accumulator::Moments {
                n: 0,
                mean: 0.0,
                m2: 0.0,
            },
            AggFunc::Min => Accumulator::Min(None),
            AggFunc::Max => Accumulator::Max(None),
            AggFunc::First => Accumulator::First(None),
            AggFunc::Last => Accumulator::Last(None),
            AggFunc::Spread => Accumulator::Spread(None),
            AggFunc::Median => Accumulator::Values {
                values: Vec::new(),
                percentile: None,
            },
            AggFunc::Percentile(p) => Accumulator::Values {
                values: Vec::new(),
                percentile: Some(p),
            },
        }
    }

    /// Fold one point into the state.
    pub fn push(&mut self, ts: Timestamp, v: f64) {
        match self {
            Accumulator::Count(n) => *n += 1,
            Accumulator::Sum { sum, comp, seen } => {
                kahan_add(sum, comp, v);
                *seen = true;
            }
            Accumulator::Mean { sum, comp, n } => {
                kahan_add(sum, comp, v);
                *n += 1;
            }
            Accumulator::Moments { n, mean, m2, .. } => {
                *n += 1;
                let delta = v - *mean;
                *mean += delta / *n as f64;
                *m2 += delta * (v - *mean);
            }
            Accumulator::Min(m) => *m = Some(m.map_or(v, |x| x.min(v))),
            Accumulator::Max(m) => *m = Some(m.map_or(v, |x| x.max(v))),
            Accumulator::First(f) => {
                if f.is_none_or(|(t, _)| ts < t) {
                    *f = Some((ts, v));
                }
            }
            Accumulator::Last(l) => {
                if l.is_none_or(|(t, _)| ts >= t) {
                    *l = Some((ts, v));
                }
            }
            Accumulator::Spread(s) => {
                *s = Some(s.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
            }
            Accumulator::Values { values, .. } => values.push(v),
        }
    }

    /// Final value, or `None` if no point was seen.
    pub fn finish(self) -> Option<f64> {
        match self {
            Accumulator::Count(n) => (n > 0).then_some(n as f64),
            Accumulator::Sum { sum, seen, .. } => seen.then_some(sum),
            Accumulator::Mean { sum, n, .. } => (n > 0).then(|| sum / n as f64),
            Accumulator::Moments { n, m2, .. } => (n > 1).then(|| (m2 / (n - 1) as f64).sqrt()),
            Accumulator::Min(m) | Accumulator::Max(m) => m,
            Accumulator::First(p) | Accumulator::Last(p) => p.map(|(_, v)| v),
            Accumulator::Spread(s) => s.map(|(lo, hi)| hi - lo),
            Accumulator::Values {
                mut values,
                percentile,
            } => {
                if values.is_empty() {
                    return None;
                }
                values.sort_by(f64::total_cmp);
                let n = values.len();
                Some(match percentile {
                    None if n % 2 == 1 => values[n / 2],
                    None => (values[n / 2 - 1] + values[n / 2]) / 2.0,
                    Some(p) => {
                        // Nearest-rank method: the smallest value with at
                        // least p% of the data at or below it.
                        let rank = ((p / 100.0) * n as f64).ceil() as usize;
                        values[rank.clamp(1, n) - 1]
                    }
                })
            }
        }
    }
}

/// Kahan summation step: keeps long sums of small values accurate.
#[inline]
fn kahan_add(sum: &mut f64, comp: &mut f64, v: f64) {
    let y = v - *comp;
    let t = *sum + y;
    *comp = (t - *sum) - y;
    *sum = t;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(func: AggFunc, values: &[f64]) -> Option<f64> {
        let mut acc = Accumulator::new(func);
        for (i, v) in values.iter().enumerate() {
            acc.push(i as Timestamp, *v);
        }
        acc.finish()
    }

    #[test]
    fn basic_aggregates() {
        let v = [4.0, 1.0, 3.0, 2.0];
        assert_eq!(run(AggFunc::Count, &v), Some(4.0));
        assert_eq!(run(AggFunc::Sum, &v), Some(10.0));
        assert_eq!(run(AggFunc::Mean, &v), Some(2.5));
        assert_eq!(run(AggFunc::Min, &v), Some(1.0));
        assert_eq!(run(AggFunc::Max, &v), Some(4.0));
        assert_eq!(run(AggFunc::First, &v), Some(4.0));
        assert_eq!(run(AggFunc::Last, &v), Some(2.0));
        assert_eq!(run(AggFunc::Spread, &v), Some(3.0));
        assert_eq!(run(AggFunc::Median, &v), Some(2.5));
        assert_eq!(run(AggFunc::Median, &[5.0, 1.0, 3.0]), Some(3.0));
    }

    #[test]
    fn empty_input_is_null() {
        for f in [
            AggFunc::Count,
            AggFunc::Sum,
            AggFunc::Mean,
            AggFunc::Stddev,
            AggFunc::Min,
            AggFunc::First,
            AggFunc::Spread,
            AggFunc::Median,
            AggFunc::Percentile(50.0),
        ] {
            assert_eq!(run(f, &[]), None, "{f}");
        }
    }

    #[test]
    fn stddev_matches_sample_definition() {
        let v = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let sd = run(AggFunc::Stddev, &v).unwrap();
        assert!((sd - 2.138_089_935_299_395).abs() < 1e-12, "{sd}");
        assert_eq!(run(AggFunc::Stddev, &[3.0]), None);
    }

    #[test]
    fn percentiles_use_nearest_rank() {
        let v: Vec<f64> = (1..=10).map(f64::from).collect();
        assert_eq!(run(AggFunc::Percentile(0.0), &v), Some(1.0));
        assert_eq!(run(AggFunc::Percentile(50.0), &v), Some(5.0));
        assert_eq!(run(AggFunc::Percentile(90.0), &v), Some(9.0));
        assert_eq!(run(AggFunc::Percentile(91.0), &v), Some(10.0));
        assert_eq!(run(AggFunc::Percentile(100.0), &v), Some(10.0));
    }

    #[test]
    fn first_and_last_use_timestamps_not_arrival_order() {
        let mut first = Accumulator::new(AggFunc::First);
        let mut last = Accumulator::new(AggFunc::Last);
        for (ts, v) in [(5, 50.0), (1, 10.0), (9, 90.0), (3, 30.0)] {
            first.push(ts, v);
            last.push(ts, v);
        }
        assert_eq!(first.finish(), Some(10.0));
        assert_eq!(last.finish(), Some(90.0));
    }

    #[test]
    fn kahan_sum_is_accurate() {
        let v = vec![0.1; 1_000_000];
        let s = run(AggFunc::Sum, &v).unwrap();
        assert!((s - 100_000.0).abs() < 1e-6, "{s}");
    }
}
