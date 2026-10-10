//! Abstract syntax tree of the query language.

use std::fmt;

use crate::model::Timestamp;
use crate::query::time::{format_duration, format_rfc3339};

/// A parsed statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `SELECT ...`
    Select(SelectStatement),
    /// `EXPLAIN SELECT ...`
    Explain(SelectStatement),
    /// `SHOW MEASUREMENTS`
    ShowMeasurements,
    /// `SHOW TAG KEYS [FROM m]`
    ShowTagKeys {
        /// Restrict to one measurement.
        from: Option<String>,
    },
    /// `SHOW TAG VALUES [FROM m] WITH KEY = k`
    ShowTagValues {
        /// Restrict to one measurement.
        from: Option<String>,
        /// Tag key whose values are listed.
        key: String,
    },
    /// `SHOW FIELD KEYS [FROM m]`
    ShowFieldKeys {
        /// Restrict to one measurement.
        from: Option<String>,
    },
    /// `SHOW SERIES [FROM m] [WHERE tag-condition]`
    ShowSeries {
        /// Restrict to one measurement.
        from: Option<String>,
        /// Tag filter.
        condition: Option<Expr>,
    },
}

/// `SELECT projections FROM measurement [WHERE] [GROUP BY] [FILL] [ORDER BY] [LIMIT]`
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// Output columns.
    pub projections: Vec<Projection>,
    /// Source measurement.
    pub from: String,
    /// Filter on tags and time.
    pub condition: Option<Expr>,
    /// Grouping by time window and/or tags.
    pub group_by: GroupBy,
    /// How empty time windows are filled.
    pub fill: Fill,
    /// Return newest rows first.
    pub descending: bool,
    /// Maximum rows per output series.
    pub limit: Option<usize>,
}

/// One output column of a `SELECT`.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `*`: every field of the measurement.
    Wildcard,
    /// A raw field.
    Field {
        /// Field name.
        name: String,
        /// `AS` alias.
        alias: Option<String>,
    },
    /// An aggregate over a field.
    Aggregate {
        /// Aggregate function.
        func: AggFunc,
        /// Field name.
        field: String,
        /// `AS` alias.
        alias: Option<String>,
    },
}

impl Projection {
    /// Column name in results.
    pub fn output_name(&self) -> String {
        match self {
            Projection::Wildcard => "*".into(),
            Projection::Field { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
            Projection::Aggregate { func, alias, .. } => {
                alias.clone().unwrap_or_else(|| func.name().to_string())
            }
        }
    }
}

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggFunc {
    /// Number of points.
    Count,
    /// Sum of values.
    Sum,
    /// Arithmetic mean.
    Mean,
    /// Median (average of the two middle values for even counts).
    Median,
    /// Smallest value.
    Min,
    /// Largest value.
    Max,
    /// Value with the earliest timestamp.
    First,
    /// Value with the latest timestamp.
    Last,
    /// Sample standard deviation.
    Stddev,
    /// `max - min`.
    Spread,
    /// Nearest-rank percentile, `0 <= p <= 100`.
    Percentile(f64),
}

impl AggFunc {
    /// Resolve a function name (case-insensitive). `percentile` needs its
    /// argument and is handled by the parser.
    pub fn from_name(name: &str) -> Option<AggFunc> {
        Some(match name.to_ascii_lowercase().as_str() {
            "count" => AggFunc::Count,
            "sum" => AggFunc::Sum,
            "mean" | "avg" => AggFunc::Mean,
            "median" => AggFunc::Median,
            "min" => AggFunc::Min,
            "max" => AggFunc::Max,
            "first" => AggFunc::First,
            "last" => AggFunc::Last,
            "stddev" => AggFunc::Stddev,
            "spread" => AggFunc::Spread,
            _ => return None,
        })
    }

    /// Canonical lower-case name.
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Mean => "mean",
            AggFunc::Median => "median",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::First => "first",
            AggFunc::Last => "last",
            AggFunc::Stddev => "stddev",
            AggFunc::Spread => "spread",
            AggFunc::Percentile(_) => "percentile",
        }
    }
}

impl fmt::Display for AggFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggFunc::Percentile(p) => write!(f, "percentile({p})"),
            other => f.write_str(other.name()),
        }
    }
}

/// `GROUP BY` clause.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GroupBy {
    /// `time(interval[, offset])`, both in nanoseconds.
    pub time: Option<TimeWindow>,
    /// Tag keys to group by.
    pub tags: Vec<String>,
    /// `GROUP BY *`: group by every tag key.
    pub all_tags: bool,
}

/// Fixed-width time windows aligned to the epoch plus an offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    /// Window width in nanoseconds (positive).
    pub interval: i64,
    /// Alignment offset in nanoseconds.
    pub offset: i64,
}

impl TimeWindow {
    /// Start of the window containing `ts`.
    pub fn window_start(&self, ts: Timestamp) -> Timestamp {
        let rel = i128::from(ts) - i128::from(self.offset);
        let start = rel.div_euclid(i128::from(self.interval)) * i128::from(self.interval)
            + i128::from(self.offset);
        start.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
    }
}

impl fmt::Display for TimeWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.offset == 0 {
            write!(f, "time({})", format_duration(self.interval))
        } else {
            write!(
                f,
                "time({}, {})",
                format_duration(self.interval),
                format_duration(self.offset)
            )
        }
    }
}

/// Policy for windows without data.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Fill {
    /// Emit the window with null values (default).
    #[default]
    Null,
    /// Omit the window.
    None,
    /// Emit a constant.
    Value(f64),
    /// Repeat the previous non-null value.
    Previous,
    /// Interpolate linearly between neighbouring non-null windows.
    Linear,
}

impl fmt::Display for Fill {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fill::Null => f.write_str("null"),
            Fill::None => f.write_str("none"),
            Fill::Value(v) => write!(f, "{v}"),
            Fill::Previous => f.write_str("previous"),
            Fill::Linear => f.write_str("linear"),
        }
    }
}

/// Boolean filter expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Both sides must hold.
    And(Box<Expr>, Box<Expr>),
    /// Either side must hold.
    Or(Box<Expr>, Box<Expr>),
    /// A tag comparison.
    Tag {
        /// Tag key.
        key: String,
        /// Comparison.
        op: TagOp,
        /// String literal or regex source.
        value: String,
    },
    /// A time comparison.
    Time {
        /// Comparison.
        op: CmpOp,
        /// Right hand side.
        value: TimeExpr,
    },
}

impl Expr {
    /// Whether any `time` comparison appears in this expression.
    pub fn mentions_time(&self) -> bool {
        match self {
            Expr::And(a, b) | Expr::Or(a, b) => a.mentions_time() || b.mentions_time(),
            Expr::Time { .. } => true,
            Expr::Tag { .. } => false,
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::And(a, b) => write!(f, "({a} AND {b})"),
            Expr::Or(a, b) => write!(f, "({a} OR {b})"),
            Expr::Tag { key, op, value } => match op {
                TagOp::Regex | TagOp::NotRegex => write!(f, "{key} {op} /{value}/"),
                _ => write!(f, "{key} {op} '{}'", value.replace('\'', "\\'")),
            },
            Expr::Time { op, value } => write!(f, "time {op} {value}"),
        }
    }
}

/// Tag comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagOp {
    /// `=`
    Eq,
    /// `!=`
    NotEq,
    /// `=~`
    Regex,
    /// `!~`
    NotRegex,
}

impl fmt::Display for TagOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TagOp::Eq => "=",
            TagOp::NotEq => "!=",
            TagOp::Regex => "=~",
            TagOp::NotRegex => "!~",
        })
    }
}

/// Ordering comparison operators used with `time`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `=`
    Eq,
    /// `!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
}

impl fmt::Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            CmpOp::Eq => "=",
            CmpOp::NotEq => "!=",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
        })
    }
}

/// A point in time, possibly relative to `now()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeExpr {
    /// Anchor.
    pub base: TimeBase,
    /// Nanoseconds added to the anchor.
    pub offset: i64,
}

/// Anchor of a [`TimeExpr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeBase {
    /// The query's evaluation time.
    Now,
    /// An absolute timestamp.
    Absolute(Timestamp),
}

impl TimeExpr {
    /// Resolve against the evaluation time, saturating on overflow.
    pub fn resolve(&self, now: Timestamp) -> Timestamp {
        let base = match self.base {
            TimeBase::Now => now,
            TimeBase::Absolute(ts) => ts,
        };
        base.saturating_add(self.offset)
    }
}

impl fmt::Display for TimeExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.base {
            TimeBase::Now => f.write_str("now()")?,
            TimeBase::Absolute(ts) => write!(f, "'{}'", format_rfc3339(ts))?,
        }
        match self.offset.cmp(&0) {
            std::cmp::Ordering::Greater => write!(f, " + {}", format_duration(self.offset)),
            std::cmp::Ordering::Less => write!(f, " - {}", format_duration(-self.offset)),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_alignment_handles_negative_times_and_offsets() {
        let w = TimeWindow {
            interval: 10,
            offset: 0,
        };
        assert_eq!(w.window_start(0), 0);
        assert_eq!(w.window_start(19), 10);
        assert_eq!(w.window_start(-1), -10);
        let shifted = TimeWindow {
            interval: 10,
            offset: 3,
        };
        assert_eq!(shifted.window_start(3), 3);
        assert_eq!(shifted.window_start(2), -7);
        assert_eq!(w.window_start(i64::MIN), i64::MIN);
    }

    #[test]
    fn time_expr_resolution_saturates() {
        let e = TimeExpr {
            base: TimeBase::Now,
            offset: -5,
        };
        assert_eq!(e.resolve(100), 95);
        let e = TimeExpr {
            base: TimeBase::Absolute(i64::MAX),
            offset: 5,
        };
        assert_eq!(e.resolve(0), i64::MAX);
    }

    #[test]
    fn display_forms() {
        let e = Expr::And(
            Box::new(Expr::Tag {
                key: "host".into(),
                op: TagOp::Eq,
                value: "a'b".into(),
            }),
            Box::new(Expr::Time {
                op: CmpOp::GtEq,
                value: TimeExpr {
                    base: TimeBase::Now,
                    offset: -3_600_000_000_000,
                },
            }),
        );
        assert_eq!(e.to_string(), r"(host = 'a\'b' AND time >= now() - 1h)");
        assert!(e.mentions_time());
        assert_eq!(AggFunc::Percentile(95.0).to_string(), "percentile(95)");
        assert_eq!(
            TimeWindow {
                interval: 300_000_000_000,
                offset: 60_000_000_000
            }
            .to_string(),
            "time(5m, 1m)"
        );
    }
}
