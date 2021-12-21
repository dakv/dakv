use crc32fast::Hasher;

const K_MASK_DELTA: u32 = 0xa282ead8;

#[inline]
pub fn extend(value: u32, data: &[u8]) -> u32 {
    let mut hasher = Hasher::new_with_initial(value);
    hasher.update(data);
    hasher.finalize()
}

#[inline]
pub fn value(data: &[u8]) -> u32 {
    extend(0, data)
}

/// Return masked value
#[inline]
pub fn mask(crc: u32) -> u32 {
    K_MASK_DELTA.wrapping_add(crc >> 15 | (crc << 17))
}

#[inline]
pub fn unmask(masked_crc: u32) -> u32 {
    let rot: u32 = masked_crc.wrapping_sub(K_MASK_DELTA);
    (rot >> 17) | (rot << 15)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_extend() {
        assert_eq!(value(b"hello world"), extend(value(b"hello "), b"world"))
    }

    #[test]
    fn test_mask() {
        let crc = value(b"foo");
        assert_ne!(crc, mask(crc));
        assert_ne!(crc, mask(mask(crc)));
        assert_eq!(crc, unmask(mask(crc)));
        assert_eq!(crc, unmask(unmask(mask(mask(crc)))));
    }
}
