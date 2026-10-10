//! Duration literals and RFC 3339 timestamps.
//!
//! Calendar conversion uses Howard Hinnant's `days_from_civil` /
//! `civil_from_days` algorithms, which are exact for the proleptic
//! Gregorian calendar over the whole `i64` nanosecond range we accept.

use crate::model::Timestamp;

/// Nanoseconds in one second.
pub const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Convert a duration unit suffix to nanoseconds.
pub fn unit_nanos(unit: &str) -> Option<i64> {
    Some(match unit {
        "ns" => 1,
        "u" | "us" | "µs" => 1_000,
        "ms" => 1_000_000,
        "s" => NANOS_PER_SECOND,
        "m" => 60 * NANOS_PER_SECOND,
        "h" => 3_600 * NANOS_PER_SECOND,
        "d" => 86_400 * NANOS_PER_SECOND,
        "w" => 7 * 86_400 * NANOS_PER_SECOND,
        _ => return None,
    })
}

/// Parse a duration such as `90s`, `5m`, `1h30m` or `250ms` into nanoseconds.
pub fn parse_duration(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut total: i64 = 0;
    let mut rest = s;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        let value: i64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit_len = rest
            .char_indices()
            .find(|(_, c)| c.is_ascii_digit())
            .map_or(rest.len(), |(i, _)| i);
        let nanos = unit_nanos(&rest[..unit_len])?;
        total = total.checked_add(value.checked_mul(nanos)?)?;
        rest = &rest[unit_len..];
    }
    Some(total)
}

/// Render nanoseconds as the shortest exact duration literal (e.g. `90s`).
pub fn format_duration(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".into();
    }
    let sign = if nanos < 0 { "-" } else { "" };
    let mut rest = nanos.unsigned_abs();
    let mut out = String::from(sign);
    for (unit, size) in [
        ("w", 604_800_000_000_000u64),
        ("d", 86_400_000_000_000),
        ("h", 3_600_000_000_000),
        ("m", 60_000_000_000),
        ("s", 1_000_000_000),
        ("ms", 1_000_000),
        ("us", 1_000),
        ("ns", 1),
    ] {
        if rest >= size {
            out.push_str(&format!("{}{unit}", rest / size));
            rest %= size;
        }
    }
    out
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        _ => 28,
    }
}

/// Parse an RFC 3339 timestamp (or a bare `YYYY-MM-DD` date) to nanoseconds.
///
/// Accepted forms: `2024-03-01`, `2024-03-01T12:30:00Z`,
/// `2024-03-01 12:30:00.123456789+02:00`. Fractions may have 1-9 digits.
pub fn parse_rfc3339(s: &str) -> Option<Timestamp> {
    let b = s.as_bytes();
    let num = |range: std::ops::Range<usize>| -> Option<i64> {
        let part = s.get(range)?;
        if part.bytes().all(|c| c.is_ascii_digit()) {
            part.parse().ok()
        } else {
            None
        }
    };
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (year, month, day) = (num(0..4)?, num(5..7)?, num(8..10)?);
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    let days = days_from_civil(year, month, day);
    let (mut secs, mut nanos) = (days.checked_mul(86_400)?, 0i64);
    if b.len() > 10 {
        if !matches!(b[10], b'T' | b't' | b' ') || b.len() < 19 || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let (hh, mm, ss) = (num(11..13)?, num(14..16)?, num(17..19)?);
        if hh > 23 || mm > 59 || ss > 60 {
            return None;
        }
        secs += hh * 3_600 + mm * 60 + ss;
        let mut i = 19;
        if b.get(i) == Some(&b'.') {
            let start = i + 1;
            let mut end = start;
            while end < b.len() && b[end].is_ascii_digit() {
                end += 1;
            }
            let digits = end - start;
            if digits == 0 || digits > 9 {
                return None;
            }
            nanos = num(start..end)? * 10i64.pow((9 - digits) as u32);
            i = end;
        }
        match b.get(i) {
            Some(b'Z' | b'z') if i + 1 == b.len() => {}
            Some(&sign @ (b'+' | b'-')) if i + 6 == b.len() && b[i + 3] == b':' => {
                let (oh, om) = (num(i + 1..i + 3)?, num(i + 4..i + 6)?);
                if oh > 23 || om > 59 {
                    return None;
                }
                let offset = oh * 3_600 + om * 60;
                secs += if sign == b'+' { -offset } else { offset };
            }
            _ => return None,
        }
    }
    secs.checked_mul(NANOS_PER_SECOND)?.checked_add(nanos)
}

/// Render nanoseconds as an RFC 3339 UTC timestamp, trimming zero fractions.
pub fn format_rfc3339(ts: Timestamp) -> String {
    let secs = ts.div_euclid(NANOS_PER_SECOND);
    let nanos = ts.rem_euclid(NANOS_PER_SECOND);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let mut out = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        sod / 3_600,
        (sod / 60) % 60,
        sod % 60
    );
    if nanos != 0 {
        let frac = format!("{nanos:09}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("5m"), Some(300 * NANOS_PER_SECOND));
        assert_eq!(parse_duration("1h30m"), Some(5_400 * NANOS_PER_SECOND));
        assert_eq!(parse_duration("250ms"), Some(250_000_000));
        assert_eq!(parse_duration("7ns"), Some(7));
        assert_eq!(parse_duration("2w"), Some(14 * 86_400 * NANOS_PER_SECOND));
        for bad in ["", "m", "5", "5x", "5m3", "99999999999999999w"] {
            assert_eq!(parse_duration(bad), None, "{bad}");
        }
    }

    #[test]
    fn duration_formatting_roundtrips() {
        for d in [
            1,
            1_000,
            90 * NANOS_PER_SECOND,
            5_400 * NANOS_PER_SECOND,
            86_400 * NANOS_PER_SECOND + 1,
        ] {
            assert_eq!(parse_duration(&format_duration(d)), Some(d));
        }
        assert_eq!(format_duration(90 * NANOS_PER_SECOND), "1m30s");
        assert_eq!(format_duration(-NANOS_PER_SECOND), "-1s");
        assert_eq!(format_duration(0), "0s");
    }

    #[test]
    fn known_timestamps() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339("1970-01-01"), Some(0));
        assert_eq!(
            parse_rfc3339("2023-11-14T22:13:20Z"),
            Some(1_700_000_000 * NANOS_PER_SECOND)
        );
        assert_eq!(
            parse_rfc3339("2023-11-15T00:13:20+02:00"),
            Some(1_700_000_000 * NANOS_PER_SECOND)
        );
        assert_eq!(parse_rfc3339("1969-12-31T23:59:59.5Z"), Some(-500_000_000));
        assert_eq!(
            parse_rfc3339("2024-02-29 01:02:03.000000001Z"),
            Some(1_709_168_523_000_000_001)
        );
    }

    #[test]
    fn invalid_timestamps() {
        for bad in [
            "2023-02-29",
            "2024-13-01",
            "2024-01-32",
            "2024-01-01T25:00:00Z",
            "2024-01-01T00:00:00",
            "2024-01-01T00:00:00.Z",
            "2024-01-01T00:00:00.1234567890Z",
            "2024-01-01T00:00:00+0200",
            "2024/01/01",
            "24-01-01",
            "2024-01-01Tjunk",
        ] {
            assert_eq!(parse_rfc3339(bad), None, "{bad}");
        }
    }

    #[test]
    fn formatting_roundtrips() {
        for ts in [
            0,
            1,
            -1,
            1_700_000_000_123_000_000,
            -86_400 * NANOS_PER_SECOND * 365 * 100,
            i64::MAX / 2,
        ] {
            assert_eq!(parse_rfc3339(&format_rfc3339(ts)), Some(ts), "{ts}");
        }
        assert_eq!(
            format_rfc3339(1_700_000_000 * NANOS_PER_SECOND),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(format_rfc3339(1_500_000), "1970-01-01T00:00:00.0015Z");
    }
}
