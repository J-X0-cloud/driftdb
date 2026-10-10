//! InfluxDB-style line protocol parser.
//!
//! ```text
//! measurement[,tag=value...] field=value[,field=value...] [timestamp]
//! ```
//!
//! Supported field values are floats (`1.5`, `-3e2`), signed integers (`5i`),
//! unsigned integers (`5u`) and booleans (`t`, `true`, `F`, ...). Integers
//! and booleans are stored as floats. String fields are rejected because the
//! storage engine only holds numeric series. Blank lines and lines starting
//! with `#` are ignored.

use crate::error::{Error, Result};
use crate::model::{Point, Precision, SeriesKey, Timestamp};

/// Parse a full request body into points.
///
/// `default_ts` is used for lines without a timestamp; it is already in
/// nanoseconds and is not scaled by `precision`.
pub fn parse_lines(body: &str, precision: Precision, default_ts: Timestamp) -> Result<Vec<Point>> {
    let mut points = Vec::new();
    for (idx, raw) in body.split('\n').enumerate() {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        parse_line(trimmed, precision, default_ts, &mut points).map_err(|message| {
            Error::LineProtocol {
                line: idx + 1,
                message,
            }
        })?;
    }
    Ok(points)
}

/// Parse one non-empty line, appending one point per field to `out`.
fn parse_line(
    line: &str,
    precision: Precision,
    default_ts: Timestamp,
    out: &mut Vec<Point>,
) -> std::result::Result<(), String> {
    let mut sc = Scanner::new(line);

    let measurement = sc.token(&[',', ' '], &[',', ' '])?;
    if measurement.is_empty() {
        return Err("missing measurement".into());
    }

    let mut tags = Vec::new();
    while sc.eat(',') {
        let key = sc.token(&['=', ',', ' '], &[',', '=', ' '])?;
        if !sc.eat('=') {
            return Err(format!("tag {key:?} is missing '='"));
        }
        let value = sc.token(&[',', ' '], &[',', '=', ' '])?;
        if key.is_empty() || value.is_empty() {
            return Err("tag keys and values must be non-empty".into());
        }
        tags.push((key, value));
    }

    if !sc.eat(' ') {
        return Err("expected a space before the field set".into());
    }

    let mut fields = Vec::new();
    loop {
        let key = sc.token(&['=', ',', ' '], &[',', '=', ' '])?;
        if key.is_empty() {
            return Err("missing field key".into());
        }
        if !sc.eat('=') {
            return Err(format!("field {key:?} is missing '='"));
        }
        let value = parse_field_value(&mut sc, &key)?;
        fields.push((key, value));
        if !sc.eat(',') {
            break;
        }
    }

    let timestamp = if sc.eat(' ') {
        let raw = sc.rest().trim();
        if raw.is_empty() {
            default_ts
        } else {
            let parsed: i64 = raw
                .parse()
                .map_err(|_| format!("invalid timestamp {raw:?}"))?;
            precision
                .to_nanos(parsed)
                .ok_or_else(|| format!("timestamp {raw} overflows nanosecond range"))?
        }
    } else if sc.at_end() {
        default_ts
    } else {
        return Err(format!("unexpected input {:?}", sc.rest()));
    };

    for (field, value) in fields {
        let key = SeriesKey::new(measurement.clone(), tags.clone(), field)
            .map_err(|e| e.to_string())?;
        out.push(Point::new(key, timestamp, value));
    }
    Ok(())
}

fn parse_field_value(sc: &mut Scanner<'_>, key: &str) -> std::result::Result<f64, String> {
    if sc.peek() == Some('"') {
        return Err(format!(
            "field {key:?} is a string; only numeric and boolean fields are supported"
        ));
    }
    let raw = sc.raw_until(&[',', ' ']);
    if raw.is_empty() {
        return Err(format!("field {key:?} has no value"));
    }
    match raw {
        "t" | "T" | "true" | "True" | "TRUE" => return Ok(1.0),
        "f" | "F" | "false" | "False" | "FALSE" => return Ok(0.0),
        _ => {}
    }
    if let Some(int) = raw.strip_suffix('i') {
        let v: i64 = int
            .parse()
            .map_err(|_| format!("invalid integer {raw:?} for field {key:?}"))?;
        return Ok(v as f64);
    }
    if let Some(uint) = raw.strip_suffix('u') {
        let v: u64 = uint
            .parse()
            .map_err(|_| format!("invalid unsigned integer {raw:?} for field {key:?}"))?;
        return Ok(v as f64);
    }
    if !raw
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E'))
    {
        return Err(format!("invalid number {raw:?} for field {key:?}"));
    }
    let v: f64 = raw
        .parse()
        .map_err(|_| format!("invalid number {raw:?} for field {key:?}"))?;
    if !v.is_finite() {
        return Err(format!("field {key:?} is not finite"));
    }
    Ok(v)
}

/// Character scanner with line-protocol escaping rules.
struct Scanner<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(src: &'a str) -> Self {
        Scanner { src, pos: 0 }
    }

    fn peek(&self) -> Option<char> {
        self.src[self.pos..].chars().next()
    }

    fn at_end(&self) -> bool {
        self.pos >= self.src.len()
    }

    fn rest(&self) -> &'a str {
        &self.src[self.pos..]
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }

    /// Read until an unescaped character from `stops`, unescaping any
    /// backslash that precedes a character in `escapable`. A backslash
    /// before any other character is kept literally.
    fn token(&mut self, stops: &[char], escapable: &[char]) -> std::result::Result<String, String> {
        let mut out = String::new();
        let mut chars = self.src[self.pos..].char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if c == '\\' {
                if let Some(&(_, next)) = chars.peek() {
                    if escapable.contains(&next) || next == '\\' {
                        out.push(next);
                        chars.next();
                        continue;
                    }
                }
                out.push('\\');
                continue;
            }
            if stops.contains(&c) {
                self.pos += i;
                return Ok(out);
            }
            if c == '\n' {
                return Err("unexpected newline".into());
            }
            out.push(c);
        }
        self.pos = self.src.len();
        Ok(out)
    }

    /// Read raw text until one of `stops` without interpreting escapes.
    fn raw_until(&mut self, stops: &[char]) -> &'a str {
        let rest = &self.src[self.pos..];
        let end = rest.find(|c| stops.contains(&c)).unwrap_or(rest.len());
        self.pos += end;
        &rest[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &str) -> Result<Vec<Point>> {
        parse_lines(body, Precision::Nanoseconds, 999)
    }

    #[test]
    fn parses_tags_fields_and_timestamp() {
        let pts = parse("cpu,host=a,region=eu usage=12.5,idle=80i 1700000000000000000").unwrap();
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[0].key.measurement, "cpu");
        assert_eq!(pts[0].key.field, "usage");
        assert_eq!(pts[0].key.tag("host"), Some("a"));
        assert_eq!(pts[0].value, 12.5);
        assert_eq!(pts[1].key.field, "idle");
        assert_eq!(pts[1].value, 80.0);
        assert_eq!(pts[1].timestamp, 1_700_000_000_000_000_000);
    }

    #[test]
    fn missing_timestamp_uses_default() {
        let pts = parse("mem free=1").unwrap();
        assert_eq!(pts[0].timestamp, 999);
        assert!(pts[0].key.tags.is_empty());
        let pts = parse("mem free=1 ").unwrap();
        assert_eq!(pts[0].timestamp, 999);
    }

    #[test]
    fn precision_scales_timestamps() {
        let pts = parse_lines("m v=1 1700000000", Precision::Seconds, 0).unwrap();
        assert_eq!(pts[0].timestamp, 1_700_000_000_000_000_000);
        let err = parse_lines("m v=1 99999999999999999", Precision::Seconds, 0).unwrap_err();
        assert!(err.to_string().contains("overflows"));
    }

    #[test]
    fn booleans_and_unsigned() {
        let pts = parse("m a=t,b=FALSE,c=7u").unwrap();
        let vals: Vec<f64> = pts.iter().map(|p| p.value).collect();
        assert_eq!(vals, vec![1.0, 0.0, 7.0]);
    }

    #[test]
    fn escapes_are_honoured() {
        let pts = parse(r"my\ cpu,host\ name=a\,b\=c us\ er=1 5").unwrap();
        assert_eq!(pts[0].key.measurement, "my cpu");
        assert_eq!(pts[0].key.tag("host name"), Some("a,b=c"));
        assert_eq!(pts[0].key.field, "us er");
        assert_eq!(pts[0].timestamp, 5);
    }

    #[test]
    fn literal_backslash_is_kept() {
        let pts = parse(r"m,path=C:\temp v=1 1").unwrap();
        assert_eq!(pts[0].key.tag("path"), Some(r"C:\temp"));
    }

    #[test]
    fn skips_comments_blank_lines_and_crlf() {
        let body = "# header\r\n\r\nm v=1 1\r\n  \nm v=2 2\n";
        let pts = parse(body).unwrap();
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[1].value, 2.0);
    }

    #[test]
    fn reports_line_numbers() {
        let err = parse("m v=1 1\nm v=oops 2").unwrap_err();
        match err {
            Error::LineProtocol { line, message } => {
                assert_eq!(line, 2);
                assert!(message.contains("oops"), "{message}");
            }
            other => panic!("unexpected error {other}"),
        }
    }

    #[test]
    fn rejects_malformed_input() {
        for bad in [
            "m",
            "m ",
            "m v",
            "m v=",
            "m,t v=1",
            "m,=x v=1",
            "m v=\"str\"",
            "m v=1 abc",
            "m v=1 1 extra",
            "m v=NaN",
            "m v=inf",
            "m v=12x",
            "m,time=1 v=1",
            "m time=1",
            ",t=1 v=1",
        ] {
            assert!(parse(bad).is_err(), "expected {bad:?} to fail");
        }
    }

    #[test]
    fn to_line_roundtrips() {
        let key = SeriesKey::new("we ird,m", [("k=1", "v 2")], "f,x").unwrap();
        let p = Point::new(key, -42, 0.1);
        let parsed = parse(&p.to_line()).unwrap();
        assert_eq!(parsed, vec![p]);
    }
}
