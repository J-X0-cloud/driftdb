//! Executes plans and `SHOW` statements against a database.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::db::Db;
use crate::error::{Error, Result};
use crate::model::{SeriesId, Timestamp};
use crate::query::aggregate::Accumulator;
use crate::query::ast::{AggFunc, Fill, Statement};
use crate::query::parser::parse;
use crate::query::planner::{eval_tags, plan_select, Group, SelectPlan};
use crate::query::{QueryResult, ResultSeries, Value};

/// Upper bound on windows materialised for one group when filling gaps.
pub const MAX_FILLED_WINDOWS: i128 = 1_000_000;

/// Parse and run one statement.
pub fn execute(db: &Db, src: &str) -> Result<QueryResult> {
    let stmt = parse(src)?;
    let now = db.now();
    match stmt {
        Statement::Select(s) => {
            let plan = db.with_index(|idx| plan_select(&s, idx, now))?;
            run_select(db, &plan)
        }
        Statement::Explain(s) => {
            let plan = db.with_index(|idx| plan_select(&s, idx, now))?;
            let (hit, total) = db.segments_overlapping(plan.start, plan.end);
            Ok(QueryResult {
                series: vec![ResultSeries {
                    name: "explain".into(),
                    tags: BTreeMap::new(),
                    columns: vec!["plan".into()],
                    values: plan
                        .describe(hit, total)
                        .into_iter()
                        .map(|l| vec![Value::Str(l)])
                        .collect(),
                }],
            })
        }
        other => db.with_index(|idx| run_show(&other, idx)),
    }
}

/// Run a planned `SELECT`.
pub fn run_select(db: &Db, plan: &SelectPlan) -> Result<QueryResult> {
    let ids = plan.series_ids();
    let data = db.read_series(&ids, plan.start, plan.end)?;
    let points: HashMap<SeriesId, Vec<(Timestamp, f64)>> = ids.into_iter().zip(data).collect();

    let mut columns = vec!["time".to_string()];
    columns.extend(plan.columns.iter().map(|c| c.name.clone()));

    let mut series = Vec::with_capacity(plan.groups.len());
    for group in &plan.groups {
        let mut rows = if plan.aggregate {
            aggregate_group(plan, group, &points)?
        } else {
            raw_group(plan, group, &points)
        };
        if plan.descending {
            rows.reverse();
        }
        if let Some(limit) = plan.limit {
            rows.truncate(limit);
        }
        if rows.is_empty() {
            continue;
        }
        series.push(ResultSeries {
            name: plan.measurement.clone(),
            tags: group.tags.clone(),
            columns: columns.clone(),
            values: rows,
        });
    }
    Ok(QueryResult { series })
}

/// Raw rows: one row per (timestamp, tag set) with a cell per column.
fn raw_group(
    plan: &SelectPlan,
    group: &Group,
    points: &HashMap<SeriesId, Vec<(Timestamp, f64)>>,
) -> Vec<Vec<Value>> {
    let mut rows: BTreeMap<(Timestamp, &str), Vec<Option<f64>>> = BTreeMap::new();
    for member in &group.members {
        let Some(pts) = points.get(&member.id) else { continue };
        let targets: Vec<usize> = plan
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.field == member.field)
            .map(|(i, _)| i)
            .collect();
        for &(ts, v) in pts {
            let row = rows
                .entry((ts, member.tagset.as_str()))
                .or_insert_with(|| vec![None; plan.columns.len()]);
            for &i in &targets {
                row[i] = Some(v);
            }
        }
    }
    rows.into_iter()
        .map(|((ts, _), cells)| {
            let mut row = Vec::with_capacity(cells.len() + 1);
            row.push(Value::Int(ts));
            row.extend(cells.into_iter().map(Value::from));
            row
        })
        .collect()
}

/// Aggregated rows: one row per time window.
fn aggregate_group(
    plan: &SelectPlan,
    group: &Group,
    points: &HashMap<SeriesId, Vec<(Timestamp, f64)>>,
) -> Result<Vec<Vec<Value>>> {
    let funcs: Vec<AggFunc> = plan
        .columns
        .iter()
        .map(|c| c.func.expect("aggregate plan has functions"))
        .collect();
    let fresh = || funcs.iter().map(|f| Accumulator::new(*f)).collect::<Vec<_>>();
    // Without GROUP BY time the whole range is one bucket stamped with the
    // lower bound (or the epoch when unbounded).
    let bucket_of = |ts: Timestamp| match plan.window {
        Some(w) => w.window_start(ts),
        None => {
            if plan.start_bounded {
                plan.start
            } else {
                0
            }
        }
    };

    let mut windows: BTreeMap<Timestamp, Vec<Accumulator>> = BTreeMap::new();
    for member in &group.members {
        let Some(pts) = points.get(&member.id) else { continue };
        let targets: Vec<usize> = plan
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.field == member.field)
            .map(|(i, _)| i)
            .collect();
        for &(ts, v) in pts {
            let accs = windows.entry(bucket_of(ts)).or_insert_with(fresh);
            for &i in &targets {
                accs[i].push(ts, v);
            }
        }
    }

    let mut rows: Vec<(Timestamp, Vec<Option<f64>>)> = windows
        .into_iter()
        .map(|(ts, accs)| (ts, accs.into_iter().map(Accumulator::finish).collect()))
        .collect();

    if let (Some(w), false) = (plan.window, matches!(plan.fill, Fill::None)) {
        let first = if plan.start_bounded {
            Some(w.window_start(plan.start))
        } else {
            rows.first().map(|r| r.0)
        };
        let last = if plan.end_bounded {
            Some(w.window_start(plan.end.saturating_sub(1)))
        } else {
            rows.last().map(|r| r.0)
        };
        if let (Some(first), Some(last)) = (first, last) {
            if first <= last {
                let count = (i128::from(last) - i128::from(first)) / i128::from(w.interval) + 1;
                if count > MAX_FILLED_WINDOWS {
                    return Err(Error::query(format!(
                        "query would produce {count} windows (limit {MAX_FILLED_WINDOWS}); \
                         narrow the time range or use a larger interval"
                    )));
                }
                let existing: BTreeMap<Timestamp, Vec<Option<f64>>> = rows.into_iter().collect();
                let mut filled = Vec::with_capacity(count as usize);
                let mut ts = first;
                loop {
                    let cells = existing
                        .get(&ts)
                        .cloned()
                        .unwrap_or_else(|| vec![None; funcs.len()]);
                    filled.push((ts, cells));
                    match ts.checked_add(w.interval) {
                        Some(next) if next <= last => ts = next,
                        _ => break,
                    }
                }
                rows = filled;
            } else {
                rows.clear();
            }
        }
    }

    // count() of a window that exists but has no points is 0, not null.
    for (_, cells) in &mut rows {
        for (cell, func) in cells.iter_mut().zip(&funcs) {
            if cell.is_none() && matches!(func, AggFunc::Count) {
                *cell = Some(0.0);
            }
        }
    }
    apply_fill(plan.fill, &mut rows);

    Ok(rows
        .into_iter()
        .map(|(ts, cells)| {
            let mut row = Vec::with_capacity(cells.len() + 1);
            row.push(Value::Int(ts));
            row.extend(cells.into_iter().map(Value::from));
            row
        })
        .collect())
}

/// Replace null cells according to `fill`, column by column.
pub fn apply_fill(fill: Fill, rows: &mut [(Timestamp, Vec<Option<f64>>)]) {
    let ncols = rows.first().map_or(0, |r| r.1.len());
    match fill {
        Fill::Null | Fill::None => {}
        Fill::Value(v) => {
            for (_, cells) in rows.iter_mut() {
                for c in cells.iter_mut().filter(|c| c.is_none()) {
                    *c = Some(v);
                }
            }
        }
        Fill::Previous => {
            for col in 0..ncols {
                let mut prev = None;
                for (_, cells) in rows.iter_mut() {
                    match cells[col] {
                        Some(v) => prev = Some(v),
                        None => cells[col] = prev,
                    }
                }
            }
        }
        Fill::Linear => {
            for col in 0..ncols {
                let mut left: Option<(Timestamp, f64)> = None;
                let mut gap_start = None;
                for i in 0..rows.len() {
                    match rows[i].1[col] {
                        Some(v) => {
                            if let (Some((lt, lv)), Some(gs)) = (left, gap_start) {
                                let (rt, rv) = (rows[i].0, v);
                                let span = (rt - lt) as f64;
                                for row in &mut rows[gs..i] {
                                    let frac = (row.0 - lt) as f64 / span;
                                    row.1[col] = Some(lv + (rv - lv) * frac);
                                }
                            }
                            left = Some((rows[i].0, v));
                            gap_start = None;
                        }
                        None => {
                            if gap_start.is_none() {
                                gap_start = Some(i);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn single_series(name: &str, columns: &[&str], values: Vec<Vec<Value>>) -> ResultSeries {
    ResultSeries {
        name: name.to_string(),
        tags: BTreeMap::new(),
        columns: columns.iter().map(|c| c.to_string()).collect(),
        values,
    }
}

fn run_show(stmt: &Statement, idx: &crate::storage::catalog::SeriesIndex) -> Result<QueryResult> {
    let measurements: Vec<&str> = match stmt {
        Statement::ShowTagKeys { from }
        | Statement::ShowTagValues { from, .. }
        | Statement::ShowFieldKeys { from }
        | Statement::ShowSeries { from, .. } => match from {
            Some(m) => idx.measurement(m).map(|_| vec![m.as_str()]).unwrap_or_default(),
            None => idx.measurement_names().collect(),
        },
        _ => Vec::new(),
    };
    let mut series = Vec::new();
    match stmt {
        Statement::ShowMeasurements => {
            let values: Vec<Vec<Value>> = idx
                .measurement_names()
                .map(|m| vec![Value::Str(m.to_string())])
                .collect();
            if !values.is_empty() {
                series.push(single_series("measurements", &["name"], values));
            }
        }
        Statement::ShowTagKeys { .. } => {
            for m in measurements {
                let mi = idx.measurement(m).expect("listed measurement exists");
                let values: Vec<Vec<Value>> =
                    mi.tags.keys().map(|k| vec![Value::Str(k.clone())]).collect();
                if !values.is_empty() {
                    series.push(single_series(m, &["tagKey"], values));
                }
            }
        }
        Statement::ShowTagValues { key, .. } => {
            for m in measurements {
                let mi = idx.measurement(m).expect("listed measurement exists");
                let values: Vec<Vec<Value>> = mi
                    .tags
                    .get(key)
                    .map(|vals| {
                        vals.keys()
                            .map(|v| vec![Value::Str(key.clone()), Value::Str(v.clone())])
                            .collect()
                    })
                    .unwrap_or_default();
                if !values.is_empty() {
                    series.push(single_series(m, &["key", "value"], values));
                }
            }
        }
        Statement::ShowFieldKeys { .. } => {
            for m in measurements {
                let mi = idx.measurement(m).expect("listed measurement exists");
                let values: Vec<Vec<Value>> =
                    mi.fields.keys().map(|k| vec![Value::Str(k.clone())]).collect();
                series.push(single_series(m, &["fieldKey"], values));
            }
        }
        Statement::ShowSeries { condition, .. } => {
            let mut keys = BTreeSet::new();
            for m in measurements {
                let mi = idx.measurement(m).expect("listed measurement exists");
                let ids = match condition {
                    Some(c) => eval_tags(c, mi)?,
                    None => mi.series.clone(),
                };
                for id in ids {
                    let key = idx
                        .key_of(id)
                        .ok_or_else(|| Error::corruption(format!("series {id} missing")))?;
                    let tags = key.tag_string();
                    keys.insert(if tags.is_empty() {
                        key.measurement.clone()
                    } else {
                        format!("{},{tags}", key.measurement)
                    });
                }
            }
            if !keys.is_empty() {
                series.push(single_series(
                    "series",
                    &["key"],
                    keys.into_iter().map(|k| vec![Value::Str(k)]).collect(),
                ));
            }
        }
        Statement::Select(_) | Statement::Explain(_) => unreachable!("handled by execute"),
    }
    Ok(QueryResult { series })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(cells: &[Option<f64>]) -> Vec<(Timestamp, Vec<Option<f64>>)> {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| (i as Timestamp * 10, vec![*c]))
            .collect()
    }

    fn col(rows: &[(Timestamp, Vec<Option<f64>>)]) -> Vec<Option<f64>> {
        rows.iter().map(|r| r.1[0]).collect()
    }

    #[test]
    fn fill_previous_and_value() {
        let mut r = rows(&[None, Some(1.0), None, None, Some(4.0), None]);
        apply_fill(Fill::Previous, &mut r);
        assert_eq!(col(&r), vec![None, Some(1.0), Some(1.0), Some(1.0), Some(4.0), Some(4.0)]);
        let mut r = rows(&[None, Some(1.0)]);
        apply_fill(Fill::Value(-1.0), &mut r);
        assert_eq!(col(&r), vec![Some(-1.0), Some(1.0)]);
    }

    #[test]
    fn fill_linear_interpolates_interior_gaps_only() {
        let mut r = rows(&[None, Some(0.0), None, None, Some(3.0), None]);
        apply_fill(Fill::Linear, &mut r);
        assert_eq!(col(&r), vec![None, Some(0.0), Some(1.0), Some(2.0), Some(3.0), None]);
    }

    #[test]
    fn fill_null_is_identity() {
        let mut r = rows(&[None, Some(2.0)]);
        apply_fill(Fill::Null, &mut r);
        assert_eq!(col(&r), vec![None, Some(2.0)]);
        let mut empty: Vec<(Timestamp, Vec<Option<f64>>)> = Vec::new();
        apply_fill(Fill::Linear, &mut empty);
    }
}
