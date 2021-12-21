use crate::utils::coding::decode_u32_le;

// Works when `data.len() >= 4`.
#[allow(clippy::many_single_char_names)]
pub fn hash(data: &[u8], n: usize, seed: u32) -> u32 {
    let m = 0xc6a4a793u32;
    let r = 24u32;

    let mut h = seed ^ (n.wrapping_mul(m as usize) as u32);
    let mut count = 0;
    while count + 4 <= n {
        let w = decode_u32_le(&data[count..]);
        count += 4;
        h = h.wrapping_add(w);
        h = h.wrapping_mul(m);
        h ^= h >> 16;
    }
    let new = &data[count..];

    let fallthrough = |value: &mut u32, d: usize| match d {
        3 => {
            *value = (*value).wrapping_add((new[2] as u32).wrapping_shl(16));
        }
        2 => {
            *value = (*value).wrapping_add((new[1] as u32).wrapping_shl(8));
        }
        1 => {
            *value = (*value).wrapping_add(new[0] as u32);
            *value = (*value).wrapping_mul(m);
            *value ^= *value >> r;
        }
        _ => {}
    };
    for i in (0..(n - count) + 1).rev() {
        fallthrough(&mut h, i);
    }
    h
}
