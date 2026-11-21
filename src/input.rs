use memchr::{memchr, memchr_iter};
use memmap2::Mmap;

use rustc_hash::{FxHashMap as Map, FxHasher};
use std::{fs, fs::File, hash::Hasher, path::Path};

pub type Text<'a> = &'a [u8];
pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;

pub fn load(path: &Path) -> Vec<Mmap> {
    let paths = if path.is_dir() {
        fs::read_dir(path)
            .unwrap()
            .map(|p| p.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "csv"))
            .collect()
    } else {
        vec![path.to_path_buf()]
    };
    paths
        .into_iter()
        .filter_map(|p| {
            let f = File::open(p).unwrap();
            if f.metadata().unwrap().len() == 0 {
                return None;
            }
            // The organizer mounts the dataset read-only for the entire process lifetime.
            Some(unsafe { Mmap::map(&f).unwrap() })
        })
        .collect()
}

pub fn chunks(files: &[Mmap]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    for file in files {
        let Some(mut start) = memchr(b'\n', file).map(|i| i + 1) else {
            continue;
        };
        while start < file.len() {
            let target = (start + 1024 * 1024).min(file.len());
            let end = memchr(b'\n', &file[target..]).map_or(file.len(), |i| target + i + 1);
            chunks.push(&file[start..end]);
            start = end;
        }
    }
    chunks
}

pub fn rows<'a, const N: usize>(chunk: &'a [u8], mut process: impl FnMut([&'a [u8]; N])) {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        return unsafe { vector_rows(chunk, process) };
    }

    for line in memchr_iter(b'\n', chunk)
        .chain(std::iter::once(chunk.len()))
        .scan(0, |start, end| {
            let row = chunk.get(*start..end);
            *start = end + 1;
            Some(row)
        })
        .flatten()
    {
        let mut fields = [&[][..]; N];
        let mut count = 0;
        let mut start = 0;
        for end in memchr_iter(b',', line).chain(std::iter::once(line.len())) {
            if count == N {
                count = N + 1;
                break;
            }
            let mut value = &line[start..end];
            if value.first() == Some(&b'"') {
                if value.len() < 2 || value.last() != Some(&b'"') {
                    count = N + 1;
                    break;
                }
                value = &value[1..value.len() - 1];
            }
            if memchr(b'"', value).is_some() {
                count = N + 1;
                break;
            }
            fields[count] = value;
            count += 1;
            start = end + 1;
        }
        if count == N {
            process(fields)
        }
    }
}

pub fn decimal(s: &[u8], limit: i64) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.len() <= 18 {
        let mut v = 0;
        for &c in s {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + i64::from(d);
        }
        return (v <= limit).then_some(v);
    }
    let mut v = 0i64;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 || v > (limit - i64::from(d)) / 10 {
            return None;
        }
        v = v * 10 + d as i64;
    }
    (v <= limit).then_some(v)
}

#[inline]
fn pair(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        None
    } else {
        Some(i64::from(a) * 10 + i64::from(b))
    }
}

pub fn timestamp(s: &[u8]) -> Option<i64> {
    if (s.len() != 20 && s.len() != 25) || s[4] != b'-' {
        return decimal(s, i64::MAX);
    }
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = 100 * pair(s, 0)? + pair(s, 2)?;
    let month = pair(s, 5)?;
    let day = pair(s, 8)?;
    let hour = pair(s, 11)?;
    let minute = pair(s, 14)?;
    let sec = pair(s, 17)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let maxday = match month {
        2 => 28 + i64::from(leap),
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    if day < 1 || day > maxday || hour > 23 || minute > 59 || sec > 59 {
        return None;
    }
    let offset = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let h = pair(s, 20)?;
        let m = pair(s, 23)?;
        if s[22] != b':' || h > 23 || m > 59 {
            return None;
        }
        sign * (h * 60 + m)
    };
    let y = year - i64::from(month <= 2);
    let era = y.div_euclid(400);
    let r = y - era * 400;
    let march = if month > 2 { month - 3 } else { month + 9 };
    let days = era * 146097 + 365 * r + r / 4 - r / 100 + (153 * march + 2) / 5 + day - 1 - 719468;
    Some(days * DAY + ((hour * 60 + minute - offset) * 60 + sec) * 1000)
}

pub fn signature(key: &[u8]) -> usize {
    let mut hasher = FxHasher::default();
    hasher.write(key);
    hasher.finish() as usize
}

pub fn read_pairs(files: &[Mmap]) -> Map<Text<'_>, Text<'_>> {
    let mut m = Map::default();
    for c in chunks(files) {
        rows::<2>(c, |[a, b]| {
            m.insert(a, b);
        });
    }
    m
}

pub fn linked<'a>(files: &'a [Mmap], codec: &crate::Names<'a>) -> Map<u64, u64> {
    use rayon::prelude::*;
    let edges: Vec<_> = chunks(files)
        .into_par_iter()
        .map(|chunk| {
            let mut edges = Vec::new();
            rows::<2>(chunk, |[a, b]| {
                if a != b && !a.is_empty() && !b.is_empty() {
                    edges.push((codec.encode(a), codec.encode(b)));
                }
            });
            edges
        })
        .collect();
    let n = edges.iter().map(Vec::len).sum::<usize>();
    let mut id = Map::<u64, usize>::default();
    id.reserve(n);
    let mut parent = Vec::<usize>::with_capacity(n);
    let mut minima = Vec::<u64>::with_capacity(n);
    fn find(parent: &mut [usize], mut i: usize) -> usize {
        while i != parent[i] {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        i
    }
    for (a, b) in edges.into_iter().flatten() {
        let mut intern = |k| {
            *id.entry(k).or_insert_with(|| {
                let i = parent.len();
                parent.push(i);
                minima.push(k);
                i
            })
        };
        let (a, b) = (intern(a), intern(b));
        let (a, b) = (find(&mut parent, a), find(&mut parent, b));
        if a != b {
            let (lo, hi) = if codec.cmp(minima[a], minima[b]).is_lt() {
                (a, b)
            } else {
                (b, a)
            };
            parent[hi] = lo;
        }
    }
    let mut resolved = Map::default();
    resolved.reserve(id.len() / 2);
    for (key, i) in id {
        let name = minima[find(&mut parent, i)];
        if key != name {
            resolved.insert(key, name);
        }
    }
    resolved
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn vector_rows<'a, const N: usize>(chunk: &'a [u8], mut process: impl FnMut([&'a [u8]; N])) {
    use std::arch::x86_64::*;
    let mut fields = [&[][..]; N];
    let mut count = 0usize;
    let mut quotes = 0u32;
    let mut start = 0;
    let mut valid = true;

    let mut field = |end: usize, quoted: u32, line_end: bool| {
        if count >= N {
            valid = false;
        } else {
            let mut value = &chunk[start..end];
            if quoted != 0 {
                if quoted == 2
                    && value.len() >= 2
                    && value[0] == b'"'
                    && value[value.len() - 1] == b'"'
                {
                    value = &value[1..value.len() - 1];
                } else {
                    valid = false;
                }
            }
            fields[count] = value;
        }
        count += 1;
        start = end + 1;
        if line_end {
            if valid && count == N {
                process(fields);
            }
            valid = true;
            count = 0;
        }
    };
    let end = chunk.len() / 32 * 32;
    for pos in (0..end).step_by(32) {
        // Only complete blocks are loaded. The remainder is handled below.
        let bytes = unsafe { _mm256_loadu_si256(chunk.as_ptr().add(pos).cast()) };
        let comma = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8));
        let lf = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'\n' as i8));
        let quote = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8));
        let mut ends = _mm256_movemask_epi8(_mm256_or_si256(comma, lf)) as u32;
        let mut quote_mask = _mm256_movemask_epi8(quote) as u32;
        while ends != 0 {
            let bit = ends & ends.wrapping_neg();
            let idx = pos + ends.trailing_zeros() as usize;
            let prior = bit - 1;
            quotes += (quote_mask & prior).count_ones();
            field(idx, quotes, chunk[idx] == b'\n');
            quotes = 0;
            quote_mask &= !prior;
            ends -= bit;
        }
        quotes += quote_mask.count_ones();
    }
    for (i, &c) in chunk[end..].iter().enumerate() {
        match c {
            b'"' => quotes += 1,
            b',' | b'\n' => {
                field(end + i, quotes, c == b'\n');
                quotes = 0;
            }
            _ => {}
        }
    }
    if chunk.last() != Some(&b'\n') {
        field(chunk.len(), quotes, true);
    }
}

// The input contract guarantees ingest_ts is a valid F1 timestamp.
pub fn ingest_time(s: &[u8]) -> i64 {
    #[inline]
    fn two(s: &[u8], i: usize) -> i64 {
        i64::from(s[i] - b'0') * 10 + i64::from(s[i + 1] - b'0')
    }
    let y = two(s, 0) * 100 + two(s, 2);
    let m = two(s, 5);
    let d = two(s, 8);
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let r = y - era * 400;
    let march = if m > 2 { m - 3 } else { m + 9 };
    let days = era * 146097 + 365 * r + r / 4 - r / 100 + (153 * march + 2) / 5 + d - 1 - 719468;
    days * DAY + ((two(s, 11) * 60 + two(s, 14)) * 60 + two(s, 17)) * 1000
}
