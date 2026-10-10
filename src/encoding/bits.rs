//! MSB-first bit stream writer and reader used by the Gorilla codecs.

use crate::error::{Error, Result};

/// Appends bits most-significant first into a growable byte buffer.
#[derive(Debug, Clone)]
pub struct BitWriter {
    bytes: Vec<u8>,
    /// Number of bits already used in the last byte (8 means "full").
    used: u8,
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BitWriter {
    /// Create an empty writer.
    pub fn new() -> Self {
        BitWriter {
            bytes: Vec::new(),
            used: 8,
        }
    }

    /// Create an empty writer with room for `bytes` bytes.
    pub fn with_capacity(bytes: usize) -> Self {
        BitWriter {
            bytes: Vec::with_capacity(bytes),
            used: 8,
        }
    }

    /// Total number of bits written so far.
    pub fn bit_len(&self) -> usize {
        self.bytes.len() * 8 - (8 - self.used as usize)
    }

    /// Write a single bit.
    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if self.used == 8 {
            self.bytes.push(0);
            self.used = 0;
        }
        if bit {
            let last = self.bytes.len() - 1;
            self.bytes[last] |= 0x80 >> self.used;
        }
        self.used += 1;
    }

    /// Write the low `n` bits of `value`, most significant first.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `n > 64`.
    #[inline]
    pub fn write_bits(&mut self, value: u64, n: u32) {
        debug_assert!(n <= 64, "cannot write more than 64 bits at once");
        let mut remaining = n;
        let value = if n < 64 {
            value & ((1u64 << n) - 1)
        } else {
            value
        };
        while remaining > 0 {
            if self.used == 8 {
                self.bytes.push(0);
                self.used = 0;
            }
            let free = 8 - u32::from(self.used);
            let take = free.min(remaining);
            let shift = remaining - take;
            let chunk = ((value >> shift) & ((1u64 << take) - 1)) as u8;
            let last = self.bytes.len() - 1;
            self.bytes[last] |= chunk << (free - take);
            self.used += take as u8;
            remaining -= take;
        }
    }

    /// Consume the writer and return the padded byte buffer.
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Reads bits most-significant first from a byte slice.
#[derive(Debug, Clone)]
pub struct BitReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> BitReader<'a> {
    /// Start reading from the first bit of `bytes`.
    pub fn new(bytes: &'a [u8]) -> Self {
        BitReader { bytes, pos: 0 }
    }

    /// Number of unread bits, including trailing padding.
    pub fn remaining(&self) -> usize {
        self.bytes.len() * 8 - self.pos
    }

    /// Read a single bit.
    #[inline]
    pub fn read_bit(&mut self) -> Result<bool> {
        let byte = *self
            .bytes
            .get(self.pos / 8)
            .ok_or_else(|| Error::Decode("bit stream exhausted".into()))?;
        let bit = byte & (0x80 >> (self.pos % 8)) != 0;
        self.pos += 1;
        Ok(bit)
    }

    /// Read `n` bits (at most 64) into the low bits of the result.
    #[inline]
    pub fn read_bits(&mut self, n: u32) -> Result<u64> {
        debug_assert!(n <= 64, "cannot read more than 64 bits at once");
        if n as usize > self.remaining() {
            return Err(Error::Decode(format!(
                "wanted {n} bits, only {} remain",
                self.remaining()
            )));
        }
        let mut out: u64 = 0;
        let mut remaining = n;
        while remaining > 0 {
            let byte = self.bytes[self.pos / 8];
            let offset = (self.pos % 8) as u32;
            let avail = 8 - offset;
            let take = avail.min(remaining);
            let chunk = (u32::from(byte) >> (avail - take)) & ((1u32 << take) - 1);
            out = (out << take) | u64::from(chunk);
            self.pos += take as usize;
            remaining -= take;
        }
        Ok(out)
    }

    /// Count consecutive one bits, stopping after the first zero or at `max`.
    ///
    /// Used to decode unary control prefixes such as `0`, `10`, `110`.
    #[inline]
    pub fn read_unary(&mut self, max: u32) -> Result<u32> {
        let mut ones = 0;
        while ones < max {
            if !self.read_bit()? {
                return Ok(ones);
            }
            ones += 1;
        }
        Ok(ones)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_bits_msb_first() {
        let mut w = BitWriter::new();
        w.write_bit(true);
        w.write_bit(false);
        w.write_bits(0b111, 3);
        assert_eq!(w.bit_len(), 5);
        assert_eq!(w.finish(), vec![0b1011_1000]);
    }

    #[test]
    fn spans_byte_boundaries() {
        let mut w = BitWriter::new();
        w.write_bits(0b101, 3);
        w.write_bits(0xABCD, 16);
        w.write_bits(u64::MAX, 64);
        w.write_bits(0, 0);
        let len = w.bit_len();
        assert_eq!(len, 83);
        let bytes = w.finish();
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bits(3).unwrap(), 0b101);
        assert_eq!(r.read_bits(16).unwrap(), 0xABCD);
        assert_eq!(r.read_bits(64).unwrap(), u64::MAX);
        assert_eq!(r.read_bits(0).unwrap(), 0);
    }

    #[test]
    fn masks_high_bits_of_value() {
        let mut w = BitWriter::new();
        w.write_bits(0xFF, 4);
        assert_eq!(w.finish(), vec![0xF0]);
    }

    #[test]
    fn reading_past_end_errors() {
        let bytes = [0xAAu8];
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bits(8).unwrap(), 0xAA);
        assert!(r.read_bit().is_err());
        assert!(r.read_bits(1).is_err());
    }

    #[test]
    fn unary_prefix_decoding() {
        let mut w = BitWriter::new();
        w.write_bits(0b0, 1);
        w.write_bits(0b10, 2);
        w.write_bits(0b110, 3);
        w.write_bits(0b1111, 4);
        let bytes = w.finish();
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_unary(4).unwrap(), 0);
        assert_eq!(r.read_unary(4).unwrap(), 1);
        assert_eq!(r.read_unary(4).unwrap(), 2);
        assert_eq!(r.read_unary(4).unwrap(), 4);
    }
}
