//! The query language: lexer, parser, planner and executor.
//!
//! The dialect is a small subset of InfluxQL:
//!
//! ```text
//! SELECT mean(usage) AS avg, max(usage) FROM cpu
//!   WHERE region =~ /^eu/ AND time >= now() - 6h
//!   GROUP BY time(5m), host FILL(previous) ORDER BY time DESC LIMIT 12
//! ```
//!
//! See [`parser`] for the full grammar.

pub mod aggregate;
pub mod ast;
pub mod exec;
pub mod lexer;
pub mod parser;
pub mod planner;
pub mod time;

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::Serialize;

use crate::db::Db;
use crate::error::Result;

/// A single result cell.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Value {
    /// Integer, used for timestamps.
    Int(i64),
    /// Floating point value.
    Float(f64),
    /// String, used by `SHOW` and `EXPLAIN`.
    Str(String),
    /// Missing value.
    Null,
}

impl From<Option<f64>> for Value {
    fn from(v: Option<f64>) -> Self {
        v.map_or(Value::Null, Value::Float)
    }
}

impl Value {
    /// Numeric view of the cell, if it has one.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Integer view of the cell, if it is an integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    fn render(&self, is_time: bool, rfc3339: bool) -> String {
        match self {
            Value::Int(i) if is_time && rfc3339 => time::format_rfc3339(*i),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Str(s) => s.clone(),
            Value::Null => String::new(),
        }
    }
}

/// One table of results (a group of a `SELECT`, or a `SHOW` listing).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResultSeries {
    /// Measurement or listing name.
    pub name: String,
    /// Group tag values.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    /// Column headers.
    pub columns: Vec<String>,
    /// Rows of cells.
    pub values: Vec<Vec<Value>>,
}

/// Output of one statement.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
pub struct QueryResult {
    /// Result tables; empty when nothing matched.
    pub series: Vec<ResultSeries>,
}

impl QueryResult {
    /// Render as aligned text tables. `rfc3339` formats `time` columns.
    pub fn to_table(&self, rfc3339: bool) -> String {
        let mut out = String::new();
        for (n, s) in self.series.iter().enumerate() {
            if n > 0 {
                out.push('\n');
            }
            let _ = write!(out, "name: {}", s.name);
            if !s.tags.is_empty() {
                let tags: Vec<String> = s.tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
                let _ = write!(out, "\ntags: {}", tags.join(", "));
            }
            out.push('\n');
            let cells: Vec<Vec<String>> = s
                .values
                .iter()
                .map(|row| {
                    row.iter()
                        .enumerate()
                        .map(|(i, v)| v.render(s.columns.get(i).is_some_and(|c| c == "time"), rfc3339))
                        .collect()
                })
                .collect();
            let widths: Vec<usize> = s
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    cells
                        .iter()
                        .filter_map(|r| r.get(i).map(|x| x.chars().count()))
                        .chain([c.chars().count()])
                        .max()
                        .unwrap_or(0)
                })
                .collect();
            let line = |vals: Vec<&str>| -> String {
                vals.iter()
                    .zip(&widths)
                    .map(|(v, w)| format!("{v:<w$}"))
                    .collect::<Vec<_>>()
                    .join("  ")
                    .trim_end()
                    .to_string()
            };
            out.push_str(&line(s.columns.iter().map(String::as_str).collect()));
            out.push('\n');
            let dashes: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
            out.push_str(&line(dashes.iter().map(String::as_str).collect()));
            out.push('\n');
            for row in &cells {
                out.push_str(&line(row.iter().map(String::as_str).collect()));
                out.push('\n');
            }
        }
        out
    }
}

impl Db {
    /// Parse and execute a query.
    pub fn query(&self, src: &str) -> Result<QueryResult> {
        exec::execute(self, src)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_rendering_aligns_columns() {
        let r = QueryResult {
            series: vec![ResultSeries {
                name: "cpu".into(),
                tags: BTreeMap::from([("host".to_string(), "a".to_string())]),
                columns: vec!["time".into(), "mean".into()],
                values: vec![
                    vec![Value::Int(0), Value::Float(1.5)],
                    vec![Value::Int(60_000_000_000), Value::Null],
                ],
            }],
        };
        let text = r.to_table(true);
        assert_eq!(
            text,
            "name: cpu\ntags: host=a\ntime                  mean\n--------------------  ----\n\
             1970-01-01T00:00:00Z  1.5\n1970-01-01T00:01:00Z\n"
        );
        assert!(r.to_table(false).contains("60000000000"));
    }

    #[test]
    fn values_serialize_untagged() {
        let v = vec![Value::Int(1), Value::Float(2.5), Value::Str("x".into()), Value::Null];
        assert_eq!(serde_json::to_string(&v).unwrap(), r#"[1,2.5,"x",null]"#);
    }
}
