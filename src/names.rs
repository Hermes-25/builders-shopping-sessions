use crate::{Map, Text};
use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};
const EXTERNAL: u64 = 1 << 63;
const HIGH: u64 = 0x8080808080808080;
const ONES: u64 = 0x0101010101010101;
type Dictionary = (Map<Arc<[u8]>, u64>, Vec<Arc<[u8]>>);

#[derive(Default)]
pub struct Names {
    dictionaries: [Mutex<Dictionary>; 32],
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "bmi2")]
unsafe fn pack_eight(word: u64) -> u64 {
    std::arch::x86_64::_pext_u64(word, !HIGH)
}

impl Names {
    #[inline]
    pub fn encode(&self, name: Text<'_>) -> u64 {
        // Big-endian seven-bit digits retain lexicographic order. Zero padding
        // distinguishes lengths, so actual NUL bytes use the exact dictionary.
        let n = name.len();
        if n == 8 || n == 9 {
            let x = u64::from_be_bytes(name[..8].try_into().unwrap());
            let last = if n == 9 { name[8] } else { 0 };
            if x & HIGH == 0
                && (x.wrapping_sub(ONES) & !x & HIGH) == 0
                && (n == 8 || (last != 0 && last < 128))
            {
                #[cfg(target_arch = "x86_64")]
                if std::arch::is_x86_feature_detected!("bmi2") {
                    return (unsafe { pack_eight(x) } << 7) | u64::from(last);
                }
            }
        }
        if n <= 9 && name.iter().all(|c| *c > 0 && *c < 128) {
            let key = name
                .iter()
                .fold(0u64, |acc, &byte| (acc << 7) | u64::from(byte));
            return key << (7 * (9 - n));
        }
        self.intern_long(name)
    }

    #[cold]
    pub(crate) fn intern_long(&self, name: Text<'_>) -> u64 {
        let bucket = crate::input::signature(name) % 32;
        let mut guard = self.dictionaries[bucket].lock().unwrap();
        let (map, names) = &mut *guard;
        if let Some(&key) = map.get(name) {
            return key;
        }
        let key = EXTERNAL | ((names.len() as u64) << 5) | bucket as u64;
        let owned: Arc<[u8]> = Arc::from(name);
        names.push(owned.clone());
        map.insert(owned, key);
        key
    }
    pub fn append_plain(&self, key: u64, out: &mut Vec<u8>) {
        if key & EXTERNAL != 0 {
            let guard = self.dictionaries[(key & 31) as usize].lock().unwrap();
            let bytes = &guard.1[((key ^ EXTERNAL) >> 5) as usize];
            out.extend_from_slice(&u32::try_from(bytes.len()).unwrap().to_le_bytes());
            out.extend_from_slice(bytes);
        } else {
            let len = 9 - (key.trailing_zeros() / 7).min(9);
            out.extend_from_slice(&len.to_le_bytes());
            for i in (9 - len..9).rev() {
                out.push(((key >> (7 * i)) & 127) as u8);
            }
        }
    }
    pub fn decode(&self, key: u64) -> Vec<u8> {
        if key & EXTERNAL != 0 {
            return self.dictionaries[(key & 31) as usize].lock().unwrap().1
                [((key ^ EXTERNAL) >> 5) as usize]
                .to_vec();
        }
        let mut bytes = Vec::with_capacity(9);
        for i in (0..9).rev() {
            let b = ((key >> (i * 7)) & 127) as u8;
            if b == 0 {
                break;
            }
            bytes.push(b);
        }
        bytes
    }
    #[inline]
    pub fn cmp(&self, a: u64, b: u64) -> Ordering {
        if a == b || (a | b) & EXTERNAL == 0 {
            a.cmp(&b)
        } else {
            self.decode(a).cmp(&self.decode(b))
        }
    }
}

#[inline]
pub fn mixed(key: u64) -> usize {
    let x = (key ^ (key >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb) as usize
}
