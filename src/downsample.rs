//! Continuous downsampling into rollup measurements.
//!
//! A rule such as `cpu:5m:mean,max:cpu_5m` reads every series of `cpu`,
//! aggregates each into 5 minute windows and writes the results to the
//! `cpu_5m` measurement with the same tags and fields named
//! `<field>_<aggregate>` (for example `usage_mean`). Each rule keeps a
//! watermark in the manifest so every window is materialised exactly once.
//! Retention applies to rollups like any other measurement; a per-measurement
//! retention policy is listed on the roadmap.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::model::{Point, SeriesKey, Timestamp};
use crate::query::aggregate::Accumulator;
use crate::query::ast::{AggFunc, TimeWindow};
use crate::query::time::{format_duration, parse_duration};

/// One downsampling rule.
#[derive(Debug, Clone, PartialEq)]
pub struct DownsampleRule {
    /// Source measurement.
    pub source: String,
    /// Window width in nanoseconds.
    pub interval: i64,
    /// Aggregates computed per window.
    pub aggregates: Vec<AggFunc>,
    /// Destination measurement (also the rule's watermark key).
    pub target: String,
    /// Windows are only materialised once they ended at least this long ago,
    /// giving late points a chance to arrive.
    pub lag: i64,
}

impl DownsampleRule {
    /// Field name used for `field` aggregated with `func`.
    pub fn output_field(field: &str, func: AggFunc) -> String {
        match func {
            AggFunc::Percentile(p) => format!("{field}_p{}", format_percentile(p)),
            other => format!("{field}_{}", other.name()),
        }
    }

    fn window(&self) -> TimeWindow {
        TimeWindow {
            interval: self.interval,
            offset: 0,
        }
    }
}

fn format_percentile(p: f64) -> String {
    if p.fract() == 0.0 {
        format!("{}", p as i64)
    } else {
        p.to_string().replace('.', "_")
    }
}

fn parse_agg(name: &str) -> Option<AggFunc> {
    if let Some(p) = name.strip_prefix('p') {
        let v: f64 = p.parse().ok()?;
        return (0.0..=100.0).contains(&v).then_some(AggFunc::Percentile(v));
    }
    AggFunc::from_name(name)
}

impl FromStr for DownsampleRule {
    type Err = Error;

    /// Parse `source:interval:agg[,agg...][:target[:lag]]`.
    fn from_str(s: &str) -> Result<Self> {
        let bad = |msg: &str| {
            Error::InvalidArgument(format!(
                "downsample rule {s:?}: {msg} (expected source:interval:aggs[:target[:lag]])"
            ))
        };
        let parts: Vec<&str> = s.split(':').collect();
        if !(3..=5).contains(&parts.len()) {
            return Err(bad("wrong number of parts"));
        }
        let source = parts[0].trim();
        if source.is_empty() {
            return Err(bad("empty source measurement"));
        }
        let interval = parse_duration(parts[1].trim())
            .filter(|d| *d > 0)
            .ok_or_else(|| bad("invalid interval"))?;
        let aggregates = parts[2]
            .split(',')
            .map(|a| parse_agg(a.trim()).ok_or_else(|| bad(&format!("unknown aggregate {a:?}"))))
            .collect::<Result<Vec<_>>>()?;
        if aggregates.is_empty() {
            return Err(bad("no aggregates"));
        }
        let target = parts
            .get(3)
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| format!("{source}_{}", format_duration(interval)));
        if target == source {
            return Err(bad("target must differ from source"));
        }
        let lag = match parts.get(4) {
            Some(l) => parse_duration(l.trim()).ok_or_else(|| bad("invalid lag"))?,
            None => interval,
        };
        Ok(DownsampleRule {
            source: source.to_string(),
            interval,
            aggregates,
            target,
            lag,
        })
    }
}

impl fmt::Display for DownsampleRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let aggs: Vec<String> = self
            .aggregates
            .iter()
            .map(|a| match a {
                AggFunc::Percentile(p) => format!("p{}", p),
                other => other.name().to_string(),
            })
            .collect();
        write!(
            f,
            "{}:{}:{}:{}:{}",
            self.source,
            format_duration(self.interval),
            aggs.join(","),
            self.target,
            format_duration(self.lag)
        )
    }
}

/// What one run of a rule produced.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct DownsampleReport {
    /// Destination measurement.
    pub target: String,
    /// Windows aggregated (summed across series).
    pub windows: usize,
    /// Points written to the destination.
    pub points: usize,
    /// Watermark after the run, if one was recorded.
    pub watermark: Option<Timestamp>,
}

/// Materialise every complete, not yet processed window of `rule`.
pub fn run_rule(db: &Db, rule: &DownsampleRule) -> Result<DownsampleReport> {
    let window = rule.window();
    let horizon = window.window_start(db.now().saturating_sub(rule.lag));
    let from = db
        .downsample_watermark(&rule.target)
        .unwrap_or(Timestamp::MIN);
    let mut report = DownsampleReport {
        target: rule.target.clone(),
        watermark: Some(from).filter(|w| *w != Timestamp::MIN),
        ..DownsampleReport::default()
    };
    if from >= horizon {
        return Ok(report);
    }

    let ids: Vec<u64> = db.with_index(|idx| {
        idx.measurement(&rule.source)
            .map(|m| m.series.iter().copied().collect())
            .unwrap_or_default()
    });
    let data = db.read_series(&ids, from, horizon)?;
    let mut out = Vec::new();
    for (id, points) in ids.iter().zip(data) {
        if points.is_empty() {
            continue;
        }
        let key = db
            .series_key(*id)
            .ok_or_else(|| Error::corruption(format!("series {id} missing from catalog")))?;
        let mut windows: BTreeMap<Timestamp, Vec<Accumulator>> = BTreeMap::new();
        for (ts, v) in points {
            let accs = windows.entry(window.window_start(ts)).or_insert_with(|| {
                rule.aggregates
                    .iter()
                    .map(|a| Accumulator::new(*a))
                    .collect()
            });
            for acc in accs.iter_mut() {
                acc.push(ts, v);
            }
        }
        report.windows += windows.len();
        for (start, accs) in windows {
            for (func, acc) in rule.aggregates.iter().zip(accs) {
                if let Some(value) = acc.finish() {
                    let out_key = SeriesKey::new(
                        rule.target.clone(),
                        key.tags.clone(),
                        DownsampleRule::output_field(&key.field, *func),
                    )?;
                    out.push(Point::new(out_key, start, value));
                }
            }
        }
    }
    // Rollups go through the normal write path (WAL, memtable), so they are
    // durable before the watermark moves past them.
    let summary = db.write_points(&out)?;
    report.points = summary.written;
    db.set_downsample_watermark(&rule.target, horizon)?;
    report.watermark = Some(horizon);
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rule_specs() {
        let r: DownsampleRule = "cpu:5m:mean,max,p99".parse().unwrap();
        assert_eq!(r.source, "cpu");
        assert_eq!(r.interval, 300_000_000_000);
        assert_eq!(
            r.aggregates,
            vec![AggFunc::Mean, AggFunc::Max, AggFunc::Percentile(99.0)]
        );
        assert_eq!(r.target, "cpu_5m");
        assert_eq!(r.lag, r.interval);
        assert_eq!(r.to_string(), "cpu:5m:mean,max,p99:cpu_5m:5m");

        let r: DownsampleRule = "mem:1h:last:mem_hourly:10m".parse().unwrap();
        assert_eq!(r.target, "mem_hourly");
        assert_eq!(r.lag, 600_000_000_000);
        assert_eq!(r.to_string().parse::<DownsampleRule>().unwrap(), r);
    }

    #[test]
    fn rejects_bad_specs() {
        for bad in [
            "cpu",
            "cpu:5m",
            ":5m:mean",
            "cpu:0s:mean",
            "cpu:5x:mean",
            "cpu:5m:wat",
            "cpu:5m:p101",
            "cpu:5m:mean:cpu",
            "cpu:5m:mean:t:soon",
            "a:1m:mean:b:1m:extra",
        ] {
            assert!(bad.parse::<DownsampleRule>().is_err(), "{bad}");
        }
    }

    #[test]
    fn output_field_names() {
        assert_eq!(
            DownsampleRule::output_field("usage", AggFunc::Mean),
            "usage_mean"
        );
        assert_eq!(
            DownsampleRule::output_field("usage", AggFunc::Percentile(99.9)),
            "usage_p99_9"
        );
    }
}
