//! Core data model: series keys, points and timestamp precision.

use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Dense identifier assigned to every distinct [`SeriesKey`].
pub type SeriesId = u64;

/// Timestamps are signed nanoseconds since the Unix epoch.
pub type Timestamp = i64;

/// Names that cannot be used as tag keys or field keys because the query
/// language reserves them.
pub const RESERVED_NAMES: &[&str] = &["time"];

/// Identity of a single series: measurement, sorted tag set and field name.
///
/// Each series stores exactly one float field; a line protocol entry with
/// several fields fans out into several series that share a tag set.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SeriesKey {
    /// Measurement name, e.g. `cpu`.
    pub measurement: String,
    /// Tag pairs sorted by key, with unique keys.
    pub tags: Vec<(String, String)>,
    /// Field name, e.g. `usage_user`.
    pub field: String,
}

impl SeriesKey {
    /// Build a validated key. Tags are sorted; duplicate keys are rejected.
    pub fn new<M, F, I, K, V>(measurement: M, tags: I, field: F) -> Result<Self>
    where
        M: Into<String>,
        F: Into<String>,
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let measurement = measurement.into();
        let field = field.into();
        if measurement.is_empty() {
            return Err(Error::InvalidArgument("measurement name is empty".into()));
        }
        validate_name("field key", &field)?;
        let mut tags: Vec<(String, String)> = tags
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        for (k, v) in &tags {
            validate_name("tag key", k)?;
            if v.is_empty() {
                return Err(Error::InvalidArgument(format!(
                    "tag {k:?} has an empty value"
                )));
            }
        }
        tags.sort();
        if let Some(w) = tags.windows(2).find(|w| w[0].0 == w[1].0) {
            return Err(Error::InvalidArgument(format!(
                "duplicate tag key {:?}",
                w[0].0
            )));
        }
        Ok(SeriesKey {
            measurement,
            tags,
            field,
        })
    }

    /// Value of tag `key`, if present.
    pub fn tag(&self, key: &str) -> Option<&str> {
        self.tags
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| self.tags[i].1.as_str())
    }

    /// The tag set rendered as `k=v,k=v` (used for grouping and display).
    pub fn tag_string(&self) -> String {
        let mut out = String::new();
        for (i, (k, v)) in self.tags.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            escape_into(&mut out, k, TAG_ESCAPES);
            out.push('=');
            escape_into(&mut out, v, TAG_ESCAPES);
        }
        out
    }
}

impl fmt::Display for SeriesKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::new();
        escape_into(&mut out, &self.measurement, MEASUREMENT_ESCAPES);
        for (k, v) in &self.tags {
            out.push(',');
            escape_into(&mut out, k, TAG_ESCAPES);
            out.push('=');
            escape_into(&mut out, v, TAG_ESCAPES);
        }
        out.push(' ');
        escape_into(&mut out, &self.field, TAG_ESCAPES);
        f.write_str(&out)
    }
}

fn validate_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidArgument(format!("{kind} is empty")));
    }
    if RESERVED_NAMES.contains(&name) {
        return Err(Error::InvalidArgument(format!(
            "{kind} {name:?} is reserved"
        )));
    }
    Ok(())
}

/// Characters that must be escaped in measurement names.
pub(crate) const MEASUREMENT_ESCAPES: &[char] = &[',', ' '];
/// Characters that must be escaped in tag keys, tag values and field keys.
pub(crate) const TAG_ESCAPES: &[char] = &[',', '=', ' '];

pub(crate) fn escape_into(out: &mut String, s: &str, escapes: &[char]) {
    for c in s.chars() {
        if c == '\\' || escapes.contains(&c) {
            out.push('\\');
        }
        out.push(c);
    }
}

/// A single timestamped float observation for one series.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    /// The series this observation belongs to.
    pub key: SeriesKey,
    /// Nanoseconds since the Unix epoch.
    pub timestamp: Timestamp,
    /// Observed value.
    pub value: f64,
}

impl Point {
    /// Construct a point.
    pub fn new(key: SeriesKey, timestamp: Timestamp, value: f64) -> Self {
        Point {
            key,
            timestamp,
            value,
        }
    }

    /// Render as a single line of line protocol with nanosecond precision.
    pub fn to_line(&self) -> String {
        format!("{}={} {}", self.key, self.value, self.timestamp)
    }
}

/// Precision of timestamps in line protocol input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Precision {
    /// Nanoseconds (the default).
    #[default]
    Nanoseconds,
    /// Microseconds.
    Microseconds,
    /// Milliseconds.
    Milliseconds,
    /// Seconds.
    Seconds,
}

impl Precision {
    /// Multiplier that converts a value in this precision to nanoseconds.
    pub fn nanos_per_unit(self) -> i64 {
        match self {
            Precision::Nanoseconds => 1,
            Precision::Microseconds => 1_000,
            Precision::Milliseconds => 1_000_000,
            Precision::Seconds => 1_000_000_000,
        }
    }

    /// Convert a raw timestamp to nanoseconds, failing on overflow.
    pub fn to_nanos(self, raw: i64) -> Option<Timestamp> {
        raw.checked_mul(self.nanos_per_unit())
    }
}

impl FromStr for Precision {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "n" | "ns" => Ok(Precision::Nanoseconds),
            "u" | "us" | "µs" => Ok(Precision::Microseconds),
            "ms" => Ok(Precision::Milliseconds),
            "s" => Ok(Precision::Seconds),
            other => Err(Error::InvalidArgument(format!(
                "unknown precision {other:?} (expected ns, us, ms or s)"
            ))),
        }
    }
}

/// Current wall-clock time in nanoseconds since the Unix epoch.
pub fn now_nanos() -> Timestamp {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_are_sorted_and_searchable() {
        let key = SeriesKey::new("cpu", [("region", "eu"), ("host", "a")], "usage").unwrap();
        assert_eq!(key.tags[0].0, "host");
        assert_eq!(key.tag("region"), Some("eu"));
        assert_eq!(key.tag("missing"), None);
        assert_eq!(key.tag_string(), "host=a,region=eu");
    }

    #[test]
    fn invalid_keys_are_rejected() {
        assert!(SeriesKey::new("", Vec::<(String, String)>::new(), "v").is_err());
        assert!(SeriesKey::new("m", Vec::<(String, String)>::new(), "").is_err());
        assert!(SeriesKey::new("m", [("time", "x")], "v").is_err());
        assert!(SeriesKey::new("m", [("a", "1"), ("a", "2")], "v").is_err());
        assert!(SeriesKey::new("m", [("a", "")], "v").is_err());
        assert!(SeriesKey::new("m", Vec::<(String, String)>::new(), "time").is_err());
    }

    #[test]
    fn display_escapes_special_characters() {
        let key = SeriesKey::new("my cpu", [("host name", "a,b=c")], "use d").unwrap();
        assert_eq!(key.to_string(), r"my\ cpu,host\ name=a\,b\=c use\ d");
        let key = SeriesKey::new("m", [("path", r"C:\x")], "v").unwrap();
        assert_eq!(key.to_string(), r"m,path=C:\\x v");
    }

    #[test]
    fn precision_parsing_and_conversion() {
        assert_eq!("ms".parse::<Precision>().unwrap(), Precision::Milliseconds);
        assert_eq!(Precision::Seconds.to_nanos(2), Some(2_000_000_000));
        assert_eq!(Precision::Seconds.to_nanos(i64::MAX), None);
        assert!("h".parse::<Precision>().is_err());
    }
}
