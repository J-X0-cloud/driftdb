//! Turns a parsed `SELECT` into an executable plan.
//!
//! Planning resolves everything that only depends on metadata: the time
//! range, which series match the tag filter, how they are grouped and which
//! output columns they feed. The executor then only has to read points and
//! fold them.

use std::collections::{BTreeMap, BTreeSet};

use regex::Regex;

use crate::error::{Error, Result};
use crate::model::{SeriesId, Timestamp};
use crate::query::ast::{AggFunc, CmpOp, Expr, Fill, Projection, SelectStatement, TagOp, TimeWindow};
use crate::query::time::format_rfc3339;
use crate::storage::catalog::{MeasurementIndex, SeriesIndex};

/// One output column of a select plan.
#[derive(Debug, Clone, PartialEq)]
pub struct OutputColumn {
    /// Name shown in results.
    pub name: String,
    /// Source field.
    pub field: String,
    /// Aggregate applied, if any.
    pub func: Option<AggFunc>,
}

/// A series that feeds one or more columns of a group.
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    /// Series id.
    pub id: SeriesId,
    /// Field stored by the series.
    pub field: String,
    /// Full tag set (`k=v,...`), distinguishing rows of raw queries.
    pub tagset: String,
}

/// Series sharing the same values for the `GROUP BY` tags.
#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    /// Group tag values (missing tags are reported as empty strings).
    pub tags: BTreeMap<String, String>,
    /// Series in the group.
    pub members: Vec<Member>,
}

/// Executable form of a `SELECT`.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectPlan {
    /// Source measurement.
    pub measurement: String,
    /// Inclusive start of the time range.
    pub start: Timestamp,
    /// Exclusive end of the time range.
    pub end: Timestamp,
    /// Whether the query gave a lower time bound.
    pub start_bounded: bool,
    /// Whether the query gave an upper time bound.
    pub end_bounded: bool,
    /// Output columns, excluding `time`.
    pub columns: Vec<OutputColumn>,
    /// Whether columns are aggregates.
    pub aggregate: bool,
    /// `GROUP BY time(...)` window.
    pub window: Option<TimeWindow>,
    /// Tag keys that define groups.
    pub group_tags: Vec<String>,
    /// Empty-window policy.
    pub fill: Fill,
    /// Newest rows first.
    pub descending: bool,
    /// Row limit per group.
    pub limit: Option<usize>,
    /// Matched series grouped by tag values.
    pub groups: Vec<Group>,
    /// Rendered tag filter, for `EXPLAIN`.
    pub filter: Option<String>,
}

impl SelectPlan {
    /// Distinct series ids the plan reads.
    pub fn series_ids(&self) -> Vec<SeriesId> {
        let set: BTreeSet<SeriesId> = self
            .groups
            .iter()
            .flat_map(|g| g.members.iter().map(|m| m.id))
            .collect();
        set.into_iter().collect()
    }

    /// Human-readable description of the plan.
    pub fn describe(&self, segments_hit: usize, segments_total: usize) -> Vec<String> {
        let fmt_bound = |bounded: bool, ts: Timestamp| {
            if bounded {
                format_rfc3339(ts)
            } else {
                "unbounded".to_string()
            }
        };
        let mut lines = vec![format!("Select from {}", self.measurement)];
        lines.push(format!(
            "  Range: [{}, {})",
            fmt_bound(self.start_bounded, self.start),
            fmt_bound(self.end_bounded, self.end)
        ));
        if let Some(filter) = &self.filter {
            lines.push(format!("  Filter: {filter}"));
        }
        let series = self.series_ids().len();
        let grouping = if self.group_tags.is_empty() {
            String::new()
        } else {
            format!(" by {}", self.group_tags.join(", "))
        };
        lines.push(format!(
            "  Series: {series} matched in {} group(s){grouping}",
            self.groups.len()
        ));
        lines.push(format!(
            "  Scan: {segments_hit} of {segments_total} segment(s) overlap the range, plus memtables"
        ));
        let cols: Vec<String> = self
            .columns
            .iter()
            .map(|c| match c.func {
                Some(f) => format!("{f}({}) as {}", c.field, c.name),
                None => format!("{} as {}", c.field, c.name),
            })
            .collect();
        if self.aggregate {
            let window = self
                .window
                .map_or_else(|| "whole range".to_string(), |w| w.to_string());
            lines.push(format!(
                "  Aggregate: {} window={window} fill={}",
                cols.join(", "),
                self.fill
            ));
        } else {
            lines.push(format!("  Project: {}", cols.join(", ")));
        }
        lines.push(format!(
            "  Order: time {}{}",
            if self.descending { "desc" } else { "asc" },
            self.limit.map_or_else(String::new, |l| format!(", limit {l}"))
        ));
        lines
    }
}

/// Split nested `AND`s into a flat list of conjuncts.
fn conjuncts(expr: Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::And(a, b) => {
            conjuncts(*a, out);
            conjuncts(*b, out);
        }
        other => out.push(other),
    }
}

/// Derive the half-open time range from top-level time conjuncts.
fn time_range(time_preds: &[(CmpOp, Timestamp)]) -> Result<(Timestamp, bool, Timestamp, bool)> {
    let (mut start, mut end) = (Timestamp::MIN, Timestamp::MAX);
    let (mut start_bounded, mut end_bounded) = (false, false);
    for &(op, ts) in time_preds {
        let (lo, hi) = match op {
            CmpOp::Gt => (Some(ts.saturating_add(1)), None),
            CmpOp::GtEq => (Some(ts), None),
            CmpOp::Lt => (None, Some(ts)),
            CmpOp::LtEq => (None, Some(ts.saturating_add(1))),
            CmpOp::Eq => (Some(ts), Some(ts.saturating_add(1))),
            CmpOp::NotEq => {
                return Err(Error::query("time != is not supported; use < and > instead"))
            }
        };
        if let Some(lo) = lo {
            start = start.max(lo);
            start_bounded = true;
        }
        if let Some(hi) = hi {
            end = end.min(hi);
            end_bounded = true;
        }
    }
    Ok((start, start_bounded, end, end_bounded))
}

/// Evaluate a tag-only expression to the set of matching series.
///
/// A series without tag `k` behaves as if `k` were the empty string, so
/// `k != 'v'` matches it and `k =~ /^$/` matches it.
pub fn eval_tags(expr: &Expr, m: &MeasurementIndex) -> Result<BTreeSet<SeriesId>> {
    Ok(match expr {
        Expr::And(a, b) => {
            let left = eval_tags(a, m)?;
            if left.is_empty() {
                return Ok(left);
            }
            left.intersection(&eval_tags(b, m)?).copied().collect()
        }
        Expr::Or(a, b) => {
            let mut left = eval_tags(a, m)?;
            left.extend(eval_tags(b, m)?);
            left
        }
        Expr::Tag { key, op, value } => {
            let missing = || -> BTreeSet<SeriesId> {
                m.series.difference(&m.with_tag_key(key)).copied().collect()
            };
            let positive = match op {
                TagOp::Eq | TagOp::NotEq => {
                    if value.is_empty() {
                        missing()
                    } else {
                        m.with_tag(key, value)
                    }
                }
                TagOp::Regex | TagOp::NotRegex => {
                    let re = Regex::new(value)
                        .map_err(|e| Error::query(format!("invalid regex /{value}/: {e}")))?;
                    let mut set = m.with_tag_matching(key, |v| re.is_match(v));
                    if re.is_match("") {
                        set.extend(missing());
                    }
                    set
                }
            };
            match op {
                TagOp::Eq | TagOp::Regex => positive,
                TagOp::NotEq | TagOp::NotRegex => m.series.difference(&positive).copied().collect(),
            }
        }
        Expr::Time { .. } => {
            return Err(Error::query(
                "time conditions must be combined with AND at the top level of WHERE",
            ))
        }
    })
}

/// Expand projections into concrete output columns.
fn output_columns(stmt: &SelectStatement, m: Option<&MeasurementIndex>) -> Result<(Vec<OutputColumn>, bool)> {
    let mut columns = Vec::new();
    let mut saw_raw = false;
    let mut saw_agg = false;
    for p in &stmt.projections {
        match p {
            Projection::Wildcard => {
                saw_raw = true;
                if let Some(m) = m {
                    for field in m.fields.keys() {
                        columns.push(OutputColumn {
                            name: field.clone(),
                            field: field.clone(),
                            func: None,
                        });
                    }
                }
            }
            Projection::Field { name, .. } => {
                saw_raw = true;
                columns.push(OutputColumn {
                    name: p.output_name(),
                    field: name.clone(),
                    func: None,
                });
            }
            Projection::Aggregate { func, field, .. } => {
                saw_agg = true;
                columns.push(OutputColumn {
                    name: p.output_name(),
                    field: field.clone(),
                    func: Some(*func),
                });
            }
        }
    }
    if saw_raw && saw_agg {
        return Err(Error::query(
            "cannot mix aggregate and raw field projections in one SELECT",
        ));
    }
    // Make column names unique the way InfluxQL does: mean, mean_1, mean_2.
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for col in &mut columns {
        let n = seen.entry(col.name.clone()).or_insert(0);
        if *n > 0 {
            col.name = format!("{}_{}", col.name, n);
        }
        *n += 1;
    }
    Ok((columns, saw_agg))
}

/// Build a plan for `stmt` against the series index at time `now`.
pub fn plan_select(stmt: &SelectStatement, index: &SeriesIndex, now: Timestamp) -> Result<SelectPlan> {
    let measurement = index.measurement(&stmt.from);
    let (columns, aggregate) = output_columns(stmt, measurement)?;
    if stmt.group_by.time.is_some() && !aggregate {
        return Err(Error::query("GROUP BY time() requires aggregate functions"));
    }

    let mut parts = Vec::new();
    if let Some(cond) = stmt.condition.clone() {
        conjuncts(cond, &mut parts);
    }
    let mut time_preds = Vec::new();
    let mut tag_exprs = Vec::new();
    for part in parts {
        match part {
            Expr::Time { op, value } => time_preds.push((op, value.resolve(now))),
            other if other.mentions_time() => {
                return Err(Error::query(
                    "time conditions must be combined with AND at the top level of WHERE",
                ))
            }
            other => tag_exprs.push(other),
        }
    }
    let (start, start_bounded, end, end_bounded) = time_range(&time_preds)?;
    let filter = tag_exprs
        .iter()
        .map(ToString::to_string)
        .reduce(|a, b| format!("{a} AND {b}"));

    let mut plan = SelectPlan {
        measurement: stmt.from.clone(),
        start,
        end,
        start_bounded,
        end_bounded,
        columns,
        aggregate,
        window: stmt.group_by.time,
        group_tags: Vec::new(),
        fill: stmt.fill,
        descending: stmt.descending,
        limit: stmt.limit,
        groups: Vec::new(),
        filter,
    };
    let Some(m) = measurement else {
        return Ok(plan);
    };

    plan.group_tags = if stmt.group_by.all_tags {
        let mut keys: Vec<String> = m.tags.keys().cloned().collect();
        for t in &stmt.group_by.tags {
            if !keys.contains(t) {
                keys.push(t.clone());
            }
        }
        keys.sort();
        keys
    } else {
        stmt.group_by.tags.clone()
    };

    let mut candidates = m.series.clone();
    for e in &tag_exprs {
        candidates = candidates.intersection(&eval_tags(e, m)?).copied().collect();
    }

    let fields: BTreeSet<&str> = plan.columns.iter().map(|c| c.field.as_str()).collect();
    let mut groups: BTreeMap<Vec<String>, Vec<Member>> = BTreeMap::new();
    for field in fields {
        for id in m.with_field(field).intersection(&candidates) {
            let key = index
                .key_of(*id)
                .ok_or_else(|| Error::corruption(format!("series {id} missing from catalog")))?;
            let group_key: Vec<String> = plan
                .group_tags
                .iter()
                .map(|t| key.tag(t).unwrap_or("").to_string())
                .collect();
            groups.entry(group_key).or_default().push(Member {
                id: *id,
                field: field.to_string(),
                tagset: key.tag_string(),
            });
        }
    }
    plan.groups = groups
        .into_iter()
        .map(|(values, members)| Group {
            tags: plan.group_tags.iter().cloned().zip(values).collect(),
            members,
        })
        .collect();
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::Statement;
    use crate::query::parser::parse;
    use crate::storage::catalog::Catalog;
    use crate::SeriesKey;

    fn catalog() -> (tempfile::TempDir, Catalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(&dir.path().join("series.log")).unwrap();
        let keys = [
            SeriesKey::new("cpu", [("host", "a"), ("dc", "east")], "usage").unwrap(),
            SeriesKey::new("cpu", [("host", "b"), ("dc", "west")], "usage").unwrap(),
            SeriesKey::new("cpu", [("host", "c")], "usage").unwrap(),
            SeriesKey::new("cpu", [("host", "a"), ("dc", "east")], "idle").unwrap(),
        ];
        let refs: Vec<&SeriesKey> = keys.iter().collect();
        cat.resolve(&refs).unwrap();
        (dir, cat)
    }

    fn plan(cat: &Catalog, q: &str) -> Result<SelectPlan> {
        let Statement::Select(stmt) = parse(q).unwrap() else {
            panic!("not a select")
        };
        cat.read(|idx| plan_select(&stmt, idx, 1_000))
    }

    fn ids(p: &SelectPlan) -> Vec<SeriesId> {
        p.series_ids()
    }

    #[test]
    fn tag_filters_select_series() {
        let (_d, cat) = catalog();
        assert_eq!(ids(&plan(&cat, "SELECT usage FROM cpu WHERE host = 'a'").unwrap()), vec![0]);
        assert_eq!(ids(&plan(&cat, "SELECT usage FROM cpu WHERE dc != 'east'").unwrap()), vec![1, 2]);
        assert_eq!(ids(&plan(&cat, "SELECT usage FROM cpu WHERE dc = ''").unwrap()), vec![2]);
        assert_eq!(ids(&plan(&cat, "SELECT usage FROM cpu WHERE dc =~ /^$/").unwrap()), vec![2]);
        assert_eq!(ids(&plan(&cat, "SELECT usage FROM cpu WHERE host =~ /[ab]/").unwrap()), vec![0, 1]);
        assert_eq!(ids(&plan(&cat, "SELECT usage FROM cpu WHERE host !~ /a/").unwrap()), vec![1, 2]);
        assert_eq!(
            ids(&plan(&cat, "SELECT usage FROM cpu WHERE host = 'a' OR dc = 'west'").unwrap()),
            vec![0, 1]
        );
        assert_eq!(
            ids(&plan(&cat, "SELECT * FROM cpu WHERE host = 'a' AND (dc = 'east' OR dc = 'x')").unwrap()),
            vec![0, 3]
        );
        assert!(ids(&plan(&cat, "SELECT usage FROM cpu WHERE host = 'zzz'").unwrap()).is_empty());
        assert!(plan(&cat, "SELECT usage FROM nope").unwrap().groups.is_empty());
    }

    #[test]
    fn time_bounds_are_half_open() {
        let (_d, cat) = catalog();
        let p = plan(&cat, "SELECT usage FROM cpu WHERE time > 10 AND time <= 20").unwrap();
        assert_eq!((p.start, p.end), (11, 21));
        assert!(p.start_bounded && p.end_bounded);
        let p = plan(&cat, "SELECT usage FROM cpu WHERE time >= now() - 100ns").unwrap();
        assert_eq!((p.start, p.end), (900, Timestamp::MAX));
        assert!(!p.end_bounded);
        let p = plan(&cat, "SELECT usage FROM cpu WHERE time = 5").unwrap();
        assert_eq!((p.start, p.end), (5, 6));
    }

    #[test]
    fn grouping_and_columns() {
        let (_d, cat) = catalog();
        let p = plan(&cat, "SELECT mean(usage), mean(usage), max(idle) FROM cpu GROUP BY dc").unwrap();
        let names: Vec<&str> = p.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["mean", "mean_1", "max"]);
        let groups: Vec<String> = p.groups.iter().map(|g| g.tags["dc"].clone()).collect();
        assert_eq!(groups, vec!["", "east", "west"]);
        let p = plan(&cat, "SELECT count(usage) FROM cpu GROUP BY *").unwrap();
        assert_eq!(p.group_tags, vec!["dc", "host"]);
        assert_eq!(p.groups.len(), 3);
        let p = plan(&cat, "SELECT * FROM cpu").unwrap();
        let names: Vec<&str> = p.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["idle", "usage"]);
    }

    #[test]
    fn semantic_errors() {
        let (_d, cat) = catalog();
        for bad in [
            "SELECT usage, mean(usage) FROM cpu",
            "SELECT usage FROM cpu GROUP BY time(1m)",
            "SELECT usage FROM cpu WHERE host = 'a' OR time > 5",
            "SELECT usage FROM cpu WHERE time != 5",
        ] {
            assert!(plan(&cat, bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn describe_mentions_key_facts() {
        let (_d, cat) = catalog();
        let p = plan(
            &cat,
            "SELECT mean(usage) FROM cpu WHERE host = 'a' AND time >= 0 GROUP BY time(1m) FILL(0) LIMIT 3",
        )
        .unwrap();
        let text = p.describe(1, 4).join("\n");
        assert!(text.contains("Select from cpu"), "{text}");
        assert!(text.contains("Filter: host = 'a'"), "{text}");
        assert!(text.contains("1 of 4 segment(s)"), "{text}");
        assert!(text.contains("window=time(1m) fill=0"), "{text}");
        assert!(text.contains("limit 3"), "{text}");
        assert!(text.contains("[1970-01-01T00:00:00Z, unbounded)"), "{text}");
    }
}
