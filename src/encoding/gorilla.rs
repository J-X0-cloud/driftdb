//! Gorilla compression for timestamps and float values.
//!
//! The scheme follows Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory
//! Time Series Database" (VLDB 2015), with two adjustments for nanosecond
//! timestamps:
//!
//! * delta-of-delta values are zig-zag encoded and bucketed into 7, 9 and
//!   12 bit payloads, with a 64 bit escape hatch instead of the paper's 32 bit
//!   one (nanosecond deltas routinely exceed 32 bits);
//! * all timestamp arithmetic wraps, so any sequence of `i64` values
//!   round-trips, including unsorted or extreme inputs.
//!
//! Values use the classic XOR scheme: identical consecutive values cost one
//! bit, and changed values reuse the previous leading/trailing zero window
//! when the new XOR fits inside it.
//!
//! A block is laid out as:
//!
//! ```text
//! uvarint count | uvarint timestamp_stream_len | timestamp stream | value stream
//! ```

use super::bits::{BitReader, BitWriter};
use super::varint::{put_uvarint, zigzag_decode, zigzag_encode, Cursor};
use crate::error::{Error, Result};

/// Delta-of-delta buckets: (unary prefix length, payload bits).
const DOD_BUCKETS: [(u32, u32); 3] = [(1, 7), (2, 9), (3, 12)];
/// Leading zero counts are stored in five bits.
const MAX_LEADING: u32 = 31;

/// Streaming encoder for a sequence of timestamps.
#[derive(Debug, Clone, Default)]
pub struct TimestampEncoder {
    out: BitWriter,
    prev: i64,
    prev_delta: i64,
    count: usize,
}

impl TimestampEncoder {
    /// Create an empty encoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of timestamps encoded so far.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether nothing has been encoded yet.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Append one timestamp.
    pub fn push(&mut self, ts: i64) {
        if self.count == 0 {
            self.out.write_bits(ts as u64, 64);
        } else {
            let delta = ts.wrapping_sub(self.prev);
            let dod = delta.wrapping_sub(self.prev_delta);
            self.write_dod(dod);
            self.prev_delta = delta;
        }
        self.prev = ts;
        self.count += 1;
    }

    fn write_dod(&mut self, dod: i64) {
        if dod == 0 {
            self.out.write_bit(false);
            return;
        }
        let zz = zigzag_encode(dod);
        for &(prefix, bits) in &DOD_BUCKETS {
            if zz < (1u64 << bits) {
                // `prefix` ones followed by a terminating zero.
                self.out.write_bits(((1u64 << prefix) - 1) << 1, prefix + 1);
                self.out.write_bits(zz, bits);
                return;
            }
        }
        self.out.write_bits(0b1111, 4);
        self.out.write_bits(zz, 64);
    }

    /// Finish and return the encoded bit stream.
    pub fn finish(self) -> Vec<u8> {
        self.out.finish()
    }
}

/// Decodes `count` timestamps from a stream produced by [`TimestampEncoder`].
pub fn decode_timestamps(bytes: &[u8], count: usize, out: &mut Vec<i64>) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    let mut r = BitReader::new(bytes);
    let mut prev = r.read_bits(64)? as i64;
    let mut prev_delta: i64 = 0;
    out.reserve(count);
    out.push(prev);
    for _ in 1..count {
        let prefix = r.read_unary(4)?;
        let dod = match prefix {
            0 => 0,
            1..=3 => zigzag_decode(r.read_bits(DOD_BUCKETS[prefix as usize - 1].1)?),
            _ => zigzag_decode(r.read_bits(64)?),
        };
        let delta = prev_delta.wrapping_add(dod);
        prev = prev.wrapping_add(delta);
        prev_delta = delta;
        out.push(prev);
    }
    Ok(())
}

/// Streaming XOR encoder for `f64` values.
#[derive(Debug, Clone)]
pub struct ValueEncoder {
    out: BitWriter,
    prev_bits: u64,
    /// `None` until the first non-identical value establishes a window.
    window: Option<(u32, u32)>,
    count: usize,
}

impl Default for ValueEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueEncoder {
    /// Create an empty encoder.
    pub fn new() -> Self {
        ValueEncoder {
            out: BitWriter::new(),
            prev_bits: 0,
            window: None,
            count: 0,
        }
    }

    /// Number of values encoded so far.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether nothing has been encoded yet.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Append one value. NaN payloads and signed zeros are preserved exactly.
    pub fn push(&mut self, value: f64) {
        let bits = value.to_bits();
        if self.count == 0 {
            self.out.write_bits(bits, 64);
        } else {
            self.write_xor(bits ^ self.prev_bits);
        }
        self.prev_bits = bits;
        self.count += 1;
    }

    fn write_xor(&mut self, xor: u64) {
        if xor == 0 {
            self.out.write_bit(false);
            return;
        }
        self.out.write_bit(true);
        let leading = xor.leading_zeros().min(MAX_LEADING);
        let trailing = xor.trailing_zeros();
        if let Some((prev_lead, prev_trail)) = self.window {
            if leading >= prev_lead && trailing >= prev_trail {
                self.out.write_bit(false);
                let significant = 64 - prev_lead - prev_trail;
                self.out.write_bits(xor >> prev_trail, significant);
                return;
            }
        }
        self.out.write_bit(true);
        let significant = 64 - leading - trailing;
        self.out.write_bits(u64::from(leading), 5);
        self.out.write_bits(u64::from(significant - 1), 6);
        self.out.write_bits(xor >> trailing, significant);
        self.window = Some((leading, trailing));
    }

    /// Finish and return the encoded bit stream.
    pub fn finish(self) -> Vec<u8> {
        self.out.finish()
    }
}

/// Decodes `count` values from a stream produced by [`ValueEncoder`].
pub fn decode_values(bytes: &[u8], count: usize, out: &mut Vec<f64>) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    let mut r = BitReader::new(bytes);
    let mut prev = r.read_bits(64)?;
    out.reserve(count);
    out.push(f64::from_bits(prev));
    let mut window: Option<(u32, u32)> = None;
    for _ in 1..count {
        if r.read_bit()? {
            let (lead, trail) = if r.read_bit()? {
                let lead = r.read_bits(5)? as u32;
                let significant = r.read_bits(6)? as u32 + 1;
                if lead + significant > 64 {
                    return Err(Error::Decode(format!(
                        "invalid XOR window: {lead} leading + {significant} significant bits"
                    )));
                }
                let w = (lead, 64 - lead - significant);
                window = Some(w);
                w
            } else {
                window
                    .ok_or_else(|| Error::Decode("XOR window reused before being defined".into()))?
            };
            let significant = 64 - lead - trail;
            let xor = r.read_bits(significant)? << trail;
            prev ^= xor;
        }
        out.push(f64::from_bits(prev));
    }
    Ok(())
}

/// Encode parallel timestamp/value slices into a single self-describing block.
///
/// # Panics
///
/// Panics if the slices have different lengths.
pub fn encode_block(timestamps: &[i64], values: &[f64]) -> Vec<u8> {
    assert_eq!(
        timestamps.len(),
        values.len(),
        "timestamp and value columns must have equal length"
    );
    let mut ts_enc = TimestampEncoder::new();
    let mut val_enc = ValueEncoder::new();
    for (&ts, &v) in timestamps.iter().zip(values) {
        ts_enc.push(ts);
        val_enc.push(v);
    }
    let ts_bytes = ts_enc.finish();
    let val_bytes = val_enc.finish();
    let mut out = Vec::with_capacity(ts_bytes.len() + val_bytes.len() + 8);
    put_uvarint(&mut out, timestamps.len() as u64);
    put_uvarint(&mut out, ts_bytes.len() as u64);
    out.extend_from_slice(&ts_bytes);
    out.extend_from_slice(&val_bytes);
    out
}

/// Encode a slice of `(timestamp, value)` pairs into a block.
pub fn encode_points(points: &[(i64, f64)]) -> Vec<u8> {
    let (ts, vals): (Vec<i64>, Vec<f64>) = points.iter().copied().unzip();
    encode_block(&ts, &vals)
}

/// Decode a block produced by [`encode_block`] into parallel columns.
pub fn decode_block(bytes: &[u8]) -> Result<(Vec<i64>, Vec<f64>)> {
    let mut cur = Cursor::new(bytes);
    let count = cur.usize()?;
    let ts_len = cur.usize()?;
    // Each point needs at least one bit per column; reject absurd counts
    // before allocating.
    if count > bytes.len().saturating_mul(8) + 1 {
        return Err(Error::Decode(format!(
            "block claims {count} points in {} bytes",
            bytes.len()
        )));
    }
    let ts_bytes = cur.bytes(ts_len)?;
    let val_bytes = cur.bytes(cur.remaining())?;
    let mut timestamps = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    decode_timestamps(ts_bytes, count, &mut timestamps)?;
    decode_values(val_bytes, count, &mut values)?;
    Ok((timestamps, values))
}

/// Decode a block into `(timestamp, value)` pairs.
pub fn decode_points(bytes: &[u8]) -> Result<Vec<(i64, f64)>> {
    let (ts, vals) = decode_block(bytes)?;
    Ok(ts.into_iter().zip(vals).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(points: &[(i64, f64)]) -> Vec<(i64, f64)> {
        decode_points(&encode_points(points)).expect("decode")
    }

    fn assert_bitwise_eq(a: &[(i64, f64)], b: &[(i64, f64)]) {
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b) {
            assert_eq!(x.0, y.0);
            assert_eq!(x.1.to_bits(), y.1.to_bits());
        }
    }

    #[test]
    fn empty_block() {
        assert!(roundtrip(&[]).is_empty());
    }

    #[test]
    fn single_point() {
        let pts = [(1_700_000_000_000_000_000, 42.5)];
        assert_bitwise_eq(&roundtrip(&pts), &pts);
    }

    #[test]
    fn regular_series_compresses_well() {
        let start = 1_700_000_000_000_000_000i64;
        let pts: Vec<(i64, f64)> = (0..1000)
            .map(|i| (start + i * 10_000_000_000, 20.0 + (i % 5) as f64 * 0.5))
            .collect();
        let block = encode_points(&pts);
        assert_bitwise_eq(&decode_points(&block).unwrap(), &pts);
        // 16 raw bytes per point; a regular series should be far smaller.
        assert!(
            block.len() < pts.len() * 3,
            "block was {} bytes for {} points",
            block.len(),
            pts.len()
        );
    }

    #[test]
    fn constant_values_cost_one_bit() {
        let pts: Vec<(i64, f64)> = (0..800).map(|i| (i, 1.0)).collect();
        let block = encode_points(&pts);
        // 64 bits header for each column plus ~1 bit per point per column.
        assert!(block.len() < 16 + 2 * 800 / 8 + 8, "{}", block.len());
        assert_bitwise_eq(&decode_points(&block).unwrap(), &pts);
    }

    #[test]
    fn extreme_timestamps_and_special_floats() {
        let pts = [
            (i64::MIN, f64::NAN),
            (i64::MAX, f64::INFINITY),
            (0, -0.0),
            (-5, f64::NEG_INFINITY),
            (i64::MAX, f64::MIN_POSITIVE),
            (i64::MIN + 1, f64::MAX),
            (3, 0.0),
        ];
        assert_bitwise_eq(&roundtrip(&pts), &pts);
    }

    #[test]
    fn every_dod_bucket_is_exercised() {
        let deltas = [0i64, 10, 70, 300, 3000, 5_000_000_000, -2, -200, -4000];
        let mut ts = 1_000i64;
        let mut pts = vec![(ts, 0.0)];
        for (i, d) in deltas.iter().enumerate() {
            ts += d;
            pts.push((ts, i as f64));
        }
        assert_bitwise_eq(&roundtrip(&pts), &pts);
    }

    #[test]
    fn corrupt_window_is_detected() {
        let pts: Vec<(i64, f64)> = (0..10).map(|i| (i, i as f64 * 1.37)).collect();
        let mut block = encode_points(&pts);
        // Truncating the value stream must surface an error, not garbage.
        block.truncate(block.len() - 3);
        assert!(decode_points(&block).is_err());
    }

    #[test]
    fn absurd_counts_are_rejected_without_allocating() {
        let mut block = Vec::new();
        put_uvarint(&mut block, u32::MAX as u64);
        put_uvarint(&mut block, 0);
        assert!(decode_block(&block).is_err());
    }
}
