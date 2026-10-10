//! LEB128 variable length integers and zig-zag signed mapping.
//!
//! These are used for every length prefix and index entry in the on-disk
//! formats, so small values cost a single byte.

use crate::error::{Error, Result};

/// Maximum number of bytes an encoded `u64` can occupy.
pub const MAX_VARINT_LEN: usize = 10;

/// Map a signed integer onto an unsigned one so small magnitudes stay small.
#[inline]
pub fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Inverse of [`zigzag_encode`].
#[inline]
pub fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Append `v` to `out` as an unsigned LEB128 varint.
#[inline]
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Append `v` to `out` as a zig-zag encoded varint.
#[inline]
pub fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, zigzag_encode(v));
}

/// Append a length-prefixed UTF-8 string.
pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put_uvarint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// A forward-only cursor over a byte slice with typed readers.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// Start reading at the beginning of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    /// Current byte offset.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Bytes that have not been consumed yet.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Whether every byte has been consumed.
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Read an unsigned LEB128 varint.
    pub fn uvarint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        for i in 0..MAX_VARINT_LEN {
            let Some(&byte) = self.buf.get(self.pos + i) else {
                return Err(Error::Decode("truncated varint".into()));
            };
            if i == MAX_VARINT_LEN - 1 && byte > 1 {
                return Err(Error::Decode("varint overflows u64".into()));
            }
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                self.pos += i + 1;
                return Ok(result);
            }
            shift += 7;
        }
        Err(Error::Decode("varint longer than 10 bytes".into()))
    }

    /// Read a zig-zag encoded signed varint.
    pub fn ivarint(&mut self) -> Result<i64> {
        self.uvarint().map(zigzag_decode)
    }

    /// Read a varint that must fit in `usize`.
    pub fn usize(&mut self) -> Result<usize> {
        let v = self.uvarint()?;
        usize::try_from(v).map_err(|_| Error::Decode(format!("length {v} does not fit usize")))
    }

    /// Read exactly `n` raw bytes.
    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(Error::Decode(format!(
                "wanted {n} bytes, only {} remain",
                self.remaining()
            )));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// Read a single byte.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    /// Read a little-endian `u32`.
    pub fn u32_le(&mut self) -> Result<u32> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Read a little-endian `u64`.
    pub fn u64_le(&mut self) -> Result<u64> {
        let b = self.bytes(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(b);
        Ok(u64::from_le_bytes(arr))
    }

    /// Read a little-endian IEEE-754 double.
    pub fn f64_le(&mut self) -> Result<f64> {
        self.u64_le().map(f64::from_bits)
    }

    /// Read a length-prefixed UTF-8 string.
    pub fn string(&mut self) -> Result<String> {
        let len = self.usize()?;
        let raw = self.bytes(len)?;
        String::from_utf8(raw.to_vec()).map_err(|_| Error::Decode("invalid UTF-8 in string".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_small_values_stay_small() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_decode(zigzag_encode(i64::MIN)), i64::MIN);
        assert_eq!(zigzag_decode(zigzag_encode(i64::MAX)), i64::MAX);
    }

    #[test]
    fn varint_boundaries_roundtrip() {
        let values = [0, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX];
        let mut buf = Vec::new();
        for &v in &values {
            put_uvarint(&mut buf, v);
        }
        let mut cur = Cursor::new(&buf);
        for &v in &values {
            assert_eq!(cur.uvarint().unwrap(), v);
        }
        assert!(cur.is_empty());
    }

    #[test]
    fn single_byte_encodings() {
        let mut buf = Vec::new();
        put_uvarint(&mut buf, 127);
        assert_eq!(buf, vec![0x7f]);
        buf.clear();
        put_uvarint(&mut buf, 128);
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    #[test]
    fn truncated_varint_is_rejected() {
        let mut cur = Cursor::new(&[0x80, 0x80]);
        assert!(cur.uvarint().is_err());
    }

    #[test]
    fn overlong_varint_is_rejected() {
        let buf = [0xff; 11];
        assert!(Cursor::new(&buf).uvarint().is_err());
        let mut overflow = vec![0xff; 9];
        overflow.push(0x02);
        assert!(Cursor::new(&overflow).uvarint().is_err());
    }

    #[test]
    fn strings_roundtrip_and_reject_bad_utf8() {
        let mut buf = Vec::new();
        put_str(&mut buf, "héllo");
        put_str(&mut buf, "");
        let mut cur = Cursor::new(&buf);
        assert_eq!(cur.string().unwrap(), "héllo");
        assert_eq!(cur.string().unwrap(), "");

        let bad = [2u8, 0xff, 0xfe];
        assert!(Cursor::new(&bad).string().is_err());
    }

    #[test]
    fn bytes_past_end_fail() {
        let mut cur = Cursor::new(&[1, 2, 3]);
        assert!(cur.bytes(4).is_err());
        assert_eq!(cur.bytes(3).unwrap(), &[1, 2, 3]);
    }
}
