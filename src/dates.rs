use crate::input::{DAY, ingest_time, timestamp};
#[derive(Clone, Copy, Default)]
struct Entry {
    first: u64,
    last: u16,
    day: i64,
}
pub struct Dates {
    cache: [Entry; 256],
}
// This index can collide. A hit also compares all ten original date bytes.
fn key(s: &[u8]) -> (usize, u64, u16) {
    let slot = usize::from(s[6] & 3) * 64 + usize::from(s[8] & 3) * 16 + usize::from(s[9] & 15);
    (
        slot,
        u64::from_ne_bytes(s[..8].try_into().unwrap()),
        u16::from_ne_bytes(s[8..10].try_into().unwrap()),
    )
}
fn civil(day: i64) -> (i64, i64, i64) {
    let z = day + 719468;
    let era = z.div_euclid(146097);
    let d = z - era * 146097;
    let y = (d - d / 1460 + d / 36524 - d / 146096) / 365;
    let ordinal = d - (365 * y + y / 4 - y / 100);
    let m = (5 * ordinal + 2) / 153;
    let date = ordinal - (153 * m + 2) / 5 + 1;
    let month = if m < 10 { m + 3 } else { m - 9 };
    (era * 400 + y + i64::from(month <= 2), month, date)
}
#[inline]
fn two(s: &[u8], at: usize) -> i64 {
    i64::from(s[at].wrapping_sub(b'0')) * 10 + i64::from(s[at + 1].wrapping_sub(b'0'))
}
impl Dates {
    pub fn new(lo: i64, hi: i64) -> Self {
        let mut cache = [Entry::default(); 256];
        let first = lo.div_euclid(DAY) - 8;
        let last = hi.div_euclid(DAY) + 7;
        if last - first <= 120 {
            for day in first..=last {
                let (y, m, d) = civil(day);
                if !(0..=9999).contains(&y) {
                    continue;
                }
                let stamp = format!("{y:04}-{m:02}-{d:02}");
                let (k, a, b) = key(stamp.as_bytes());
                cache[k] = Entry {
                    first: a,
                    last: b,
                    day: day * DAY,
                };
            }
        }
        Self { cache }
    }
    #[inline]
    fn hit(&self, s: &[u8]) -> Option<i64> {
        let (k, a, b) = key(s);
        let entry = self.cache[k];
        if a != 0 && entry.first == a && entry.last == b {
            Some(entry.day)
        } else {
            None
        }
    }
    #[inline]
    pub fn ingest(&self, s: &[u8]) -> i64 {
        // The input contract guarantees a valid, fixed-width UTC ingest timestamp.
        if let Some(day) = self.hit(s) {
            day + ((two(s, 11) * 60 + two(s, 14)) * 60 + two(s, 17)) * 1000
        } else {
            ingest_time(s)
        }
    }
    #[inline]
    pub fn parse(&self, s: &[u8]) -> Option<i64> {
        if s.len() == 13 {
            // Validate ASCII digits, then reduce eight decimal digits in register.
            let w = u64::from_le_bytes(s[..8].try_into().unwrap());
            if ((w.wrapping_add(0x4646464646464646) | w.wrapping_sub(0x3030303030303030))
                & 0x8080808080808080)
                != 0
            {
                return None;
            }
            let mut v = w & 0x0f0f0f0f0f0f0f0f;
            v = (v.wrapping_mul(10).wrapping_add(v >> 8)) & 0x00ff00ff00ff00ff;
            v = (v.wrapping_mul(100).wrapping_add(v >> 16)) & 0x0000ffff0000ffff;
            let mut value = (v.wrapping_mul(10000).wrapping_add(v >> 32) & 0xffffffff) as i64;
            for &c in &s[8..] {
                let d = c.wrapping_sub(b'0');
                if d > 9 {
                    return None;
                }
                value = value * 10 + i64::from(d);
            }
            return Some(value);
        }
        if (s.len() == 20 || s.len() == 25) && s[10] == b'T' && s[13] == b':' && s[16] == b':' {
            if let Some(day) = self.hit(s) {
                let h = two(s, 11);
                let m = two(s, 14);
                let sec = two(s, 17);
                if [11, 12, 14, 15, 17, 18]
                    .iter()
                    .any(|&i| !s[i].is_ascii_digit())
                    || h > 23
                    || m > 59
                    || sec > 59
                {
                    return None;
                }
                let offset = if s.len() == 20 {
                    if s[19] != b'Z' {
                        return None;
                    }
                    0
                } else {
                    if s[22] != b':' || [20, 21, 23, 24].iter().any(|&i| !s[i].is_ascii_digit()) {
                        return None;
                    }
                    let sign = match s[19] {
                        b'+' => 1,
                        b'-' => -1,
                        _ => return None,
                    };
                    let oh = two(s, 20);
                    let om = two(s, 23);
                    if oh > 23 || om > 59 {
                        return None;
                    }
                    sign * (oh * 60 + om)
                };
                return Some(day + ((h * 60 + m - offset) * 60 + sec) * 1000);
            }
        }
        timestamp(s)
    }
}
