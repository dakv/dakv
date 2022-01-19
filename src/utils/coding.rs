use crate::varint::EncodeVar;
use byteorder::{ByteOrder, LittleEndian};
use core::mem;
use num_traits::cast::cast;
use num_traits::int::PrimInt;

/// Put string length value and data
pub fn put_length_prefixed_slice(s: &mut Vec<u8>, value: &[u8]) {
    put_varint(s, value.len() as u64);
    s.extend_from_slice(value);
}

pub fn get_length_prefixed_slice<'a>(input: &mut &'a [u8], result: &mut &'a [u8]) -> bool {
    let mut len = 0;
    if get_varint32(input, &mut len) && input.len() >= len as usize {
        *result = &input[..len as usize];
        // remove prefixed
        *input = &input[len as usize..];
        true
    } else {
        false
    }
}

fn get_varint32_ptr_fallback<'a>(p: &'a [u8], limit: *const u8, v: &mut u32) -> (&'a [u8], bool) {
    let mut result = 0;
    let mut shift = 0;
    let mut pp = p;
    while shift <= 28 && pp.as_ptr() < limit {
        let byte = pp[0] as u32;
        pp = &pp[1..];
        if (byte & 128) != 0 {
            result |= (byte & 127) << shift;
        } else {
            result |= byte << shift;
            *v = result;
            return (pp, true);
        }
        shift += 7;
    }
    (p, false) // invalid
}

pub fn get_varint32_ptr<'a>(p: &'a [u8], limit: *const u8, v: &mut u32) -> (&'a [u8], bool) {
    if p.as_ptr() < limit {
        let result = p[0] as u32;
        if (result & 128) == 0 {
            *v = result;
            return (&p[1..], true);
        }
    }
    get_varint32_ptr_fallback(p, limit, v)
}

// TODO: replace with u32::decode_varint?
// read the varint value, and skip the varint prefix.
pub fn get_varint32(p: &mut &[u8], value: &mut u32) -> bool {
    let (ret, success) = get_varint32_ptr(p, p[p.len()..].as_ptr(), value);
    if !success {
        false
    } else {
        *p = ret;
        true
    }
}

// return false if parse failed.
fn get_varint64_ptr<'a>(p: &'a [u8], limit: &'a [u8], value: &mut u64) -> &'a [u8] {
    let mut result = 0;
    let mut shift = 0;
    let mut pp = p;
    while shift <= 63 && pp.as_ptr() < limit.as_ptr() {
        let byte = pp[0] as u64;
        pp = &pp[1..];
        if (byte & 128) != 0 {
            result |= (byte & 127) << shift;
        } else {
            result |= byte << shift;
            *value = result;
            return pp;
        }
        shift += 7;
    }
    p
}

pub fn get_varint64(p: &mut &[u8], value: &mut u64) -> bool {
    let ret = get_varint64_ptr(p, &p[p.len()..], value);
    if ret.as_ptr() == p.as_ptr() {
        false
    } else {
        *p = ret;
        true
    }
}

#[allow(dead_code)]
pub fn varint_length<T: PrimInt>(mut v: T) -> u64 {
    let mut len = 1;
    while cast::<T, u64>(v).unwrap() >= 128 {
        v = v.unsigned_shr(7);
        len += 1;
    }
    len
}

/// Encodes an unsigned 32 bit integer `v` to `buf` in little endian,
/// which is not memory-comparable.
/// input: buf: [0, 0, 0, 0], v: 1993 -> buf: [201, 7, 0, 0]
/// # Panics
///
/// Panics when `buf.len() < 4`.
#[inline]
pub fn encode_u32_le(buf: &mut [u8], v: u32) {
    LittleEndian::write_u32(buf, v)
}

#[inline]
pub fn encode_u64_le(buf: &mut [u8], v: u64) {
    LittleEndian::write_u64(buf, v)
}

/// Decodes an unsigned 32 bit integer from `buf` in little endian,
/// which is previously encoded via `encode_u32_le`.
///
/// # Panics
///
/// Panics when `buf.len() < 4`.
#[inline]
pub fn decode_u32_le(buf: &[u8]) -> u32 {
    LittleEndian::read_u32(buf)
}

/// Decodes an unsigned 64 bit integer from `buf` in little endian,
/// which is previously encoded via `encode_u64_le`.
///
/// # Panics
///
/// Panics when `buf.len() < 8`.
#[inline]
pub fn decode_u64_le(buf: &[u8]) -> u64 {
    LittleEndian::read_u64(buf)
}

#[inline]
pub fn put_varint<T: PrimInt>(s: &mut Vec<u8>, v: T) {
    let c = cast::<T, u64>(v).unwrap().encode_varint();
    s.extend(c);
}

#[inline]
pub fn put_fixed_32(dst: &mut Vec<u8>, v: u32) {
    let mut buf = [0; mem::size_of::<u32>()];
    encode_u32_le(&mut buf, v);
    dst.extend_from_slice(&buf);
}

#[inline]
pub fn put_fixed_64(dst: &mut Vec<u8>, v: u64) {
    let mut buf = [0; mem::size_of::<u64>()];
    encode_u64_le(&mut buf, v);
    dst.extend_from_slice(&buf);
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encode_u32() {
        let s = &mut String::from("abcdefghijk");
        unsafe {
            let b = s.as_bytes_mut();
            let bb = &mut b[6..];
            encode_u32_le(bb, 111);
            assert_eq!(s.as_bytes(), [97, 98, 99, 100, 101, 102, 111, 0, 0, 0, 107]);
        }
    }

    #[test]
    fn test_fixed32() {
        let mut s = vec![];

        for i in 0..100000 {
            put_fixed_32(&mut s, i);
        }
        let mut p = s.as_slice();
        for i in 0..100000 {
            assert_eq!(decode_u32_le(p), i);
            p = &p[4..]
        }
    }

    #[test]
    fn test_fixed64() {
        let mut s = vec![];

        for i in 0..64 {
            let v = 1 << i;
            put_fixed_64(&mut s, v - 1);
            put_fixed_64(&mut s, v);
            put_fixed_64(&mut s, v + 1);
        }
        let mut p = s.as_slice();
        for i in 0..64 {
            let v = 1 << i;
            assert_eq!(v - 1, decode_u64_le(p));
            p = &p[8..];
            assert_eq!(v, decode_u64_le(p));
            p = &p[8..];
            assert_eq!(v + 1, decode_u64_le(p));
            p = &p[8..];
        }
    }

    #[test]
    fn test_encoding() {
        let mut dst = vec![];
        put_fixed_32(&mut dst, 0x04030201);
        assert_eq!(0x01, dst.as_slice()[0]);
        assert_eq!(0x02, dst.as_slice()[1]);
        assert_eq!(0x03, dst.as_slice()[2]);
        assert_eq!(0x04, dst.as_slice()[3]);
        dst.clear();
        put_fixed_64(&mut dst, 0x0807060504030201);
        assert_eq!(0x01, dst.as_slice()[0]);
        assert_eq!(0x02, dst.as_slice()[1]);
        assert_eq!(0x03, dst.as_slice()[2]);
        assert_eq!(0x04, dst.as_slice()[3]);
        assert_eq!(0x05, dst.as_slice()[4]);
        assert_eq!(0x06, dst.as_slice()[5]);
        assert_eq!(0x07, dst.as_slice()[6]);
        assert_eq!(0x08, dst.as_slice()[7]);
    }

    #[test]
    fn test_varint32() {
        let mut s = vec![];
        for i in 0u32..32 * 32 {
            let v = (i / 32) << (i % 32);
            put_varint(&mut s, v);
        }
        let mut p = s.as_slice();
        let limit = p[p.len()..].as_ptr();
        for i in 0..32 * 32 {
            let mut actual = 0;
            let start = p;
            p = get_varint32_ptr(&p, limit, &mut actual).0;
            assert_ne!(p.as_ptr(), s.as_ptr());

            assert_eq!((i / 32) << (i % 32), actual);
            assert_eq!(
                varint_length(actual) as usize,
                p.as_ptr() as usize - start.as_ptr() as usize
            );
        }
    }

    #[test]
    fn test_varint64() {
        let mut values = vec![0, 100, std::u64::MAX, std::u64::MAX - 1];
        for k in 0..64 {
            let power = 1u64 << k;
            values.push(power);
            values.push(power - 1);
            values.push(power + 1);
        }

        let mut s = vec![];
        for i in &values {
            put_varint(&mut s, *i);
        }

        let mut p = s.as_slice();
        let limit = &p[p.len()..];
        for i in &values {
            assert!(p.as_ptr() < limit.as_ptr());
            let mut actual = 0;
            let start = p;
            p = get_varint64_ptr(p, limit, &mut actual);
            assert_ne!(p.as_ptr(), start.as_ptr());
            assert_eq!(*i, actual);
            assert_eq!(
                varint_length(actual) as usize,
                p.as_ptr() as usize - start.as_ptr() as usize
            );
        }
        assert_eq!(p.as_ptr(), limit.as_ptr());
    }

    #[test]
    fn test_put_fixed_32() {
        let mut s = vec![];
        let mut i = 0u32;
        while i < 100000 {
            put_fixed_32(&mut s, i);
            i = i * i + 1;
        }
        assert_eq!(
            s.as_slice(),
            [0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0, 26, 0, 0, 0, 165, 2, 0, 0]
        );
        let mut i = 0u32;
        while i < 100000 {
            let actual = decode_u32_le(s.as_slice());
            assert_eq!(i, actual);
            s.drain(..mem::size_of::<u32>());
            i = i * i + 1;
        }
    }

    #[test]
    fn test_parse_prefix() {
        let mut input = vec![];
        put_length_prefixed_slice(&mut input, b"");
        put_length_prefixed_slice(&mut input, b"foo");
        put_length_prefixed_slice(&mut input, b"bar");
        put_length_prefixed_slice(&mut input, "x".repeat(200).as_bytes());
        let mut result: &[u8] = &[];
        let mut tmp = input.as_slice();
        assert!(get_length_prefixed_slice(&mut tmp, &mut result));
        assert_eq!(result, b"");
        assert!(get_length_prefixed_slice(&mut tmp, &mut result));
        assert_eq!(result, b"foo");
        assert!(get_length_prefixed_slice(&mut tmp, &mut result));
        assert_eq!(result, b"bar");

        assert!(get_length_prefixed_slice(&mut tmp, &mut result));
        assert_eq!(result, vec![b'x'; 200].as_slice());
        assert_eq!(tmp.len(), 0);
    }

    #[test]
    fn test_large_value() {
        let large_value = (1u32 << 31) + 100;
        let mut s = vec![];
        put_varint(&mut s, large_value);
        assert_eq!(s.as_slice(), &[228, 128, 128, 128, 8]);

        let mut result = 0;
        let mut tmp = s.as_slice();
        assert!(get_varint32(&mut tmp, &mut result));
        assert_eq!(large_value as u32, result);

        let large_value = (1u64 << 63) + 100;
        let mut s = vec![];
        put_varint(&mut s, large_value);
        assert_eq!(
            s.as_slice(),
            &[228, 128, 128, 128, 128, 128, 128, 128, 128, 1]
        );
        let mut result = 0;
        let mut tmp = s.as_slice();
        assert!(get_varint64(&mut tmp, &mut result));
        assert_eq!(large_value as u64, result);
    }

    #[test]
    fn test_overflow() {
        let mut result = 0;
        let mut tmp: &[u8] = b"\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11";
        let limit = &tmp[tmp.len()..];
        assert_eq!(
            get_varint64_ptr(&mut tmp, limit, &mut result).as_ptr(),
            tmp.as_ptr()
        );

        let mut result = 0;
        let mut tmp: &[u8] = b"\x81\x82\x83\x84\x85\x11";
        let limit = tmp[tmp.len()..].as_ptr();

        let (ret, _) = get_varint32_ptr(&mut tmp, limit, &mut result);
        assert_eq!(ret.as_ptr(), tmp.as_ptr());
    }
}
