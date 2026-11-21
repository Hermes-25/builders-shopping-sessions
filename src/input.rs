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

fn root(parent: &mut [usize], mut i: usize) -> usize {
    while parent[i] != i {
        parent[i] = parent[parent[i]];
        i = parent[i];
    }
    i
}

pub fn linked(files: &[Mmap]) -> Map<Text<'_>, Text<'_>> {
    let mut ids = Map::default();
    let mut names = Vec::new();
    let mut parent = Vec::new();
    for c in chunks(files) {
        rows::<2>(c, |[a, b]| {
            if a.is_empty() || b.is_empty() {
                return;
            }
            let mut intern = |s| {
                *ids.entry(s).or_insert_with(|| {
                    let i = parent.len();
                    parent.push(i);
                    names.push(s);
                    i
                })
            };
            let (x, y) = (intern(a), intern(b));
            let (rx, ry) = (root(&mut parent, x), root(&mut parent, y));
            if names[rx] < names[ry] {
                parent[ry] = rx;
            } else {
                parent[rx] = ry;
            }
        });
    }
    for (i, name) in names.clone().into_iter().enumerate() {
        ids.insert(name, root(&mut parent, i));
    }
    ids.into_iter().map(|(name, i)| (name, names[i])).collect()
}

pub fn shard(key: &[u8]) -> usize {
    signature(key) % 128
}
