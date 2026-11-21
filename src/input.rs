use memchr::{memchr, memchr_iter};
use memmap2::{Mmap, MmapOptions};

use rustc_hash::{FxHashMap as Map, FxHasher};
use std::{
    fs,
    fs::File,
    hash::Hasher,
    path::{Path, PathBuf},
    sync::Arc,
};

pub type Text<'a> = &'a [u8];
pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;

fn csv_files(path: &Path) -> Vec<PathBuf> {
    if path.is_dir() {
        fs::read_dir(path)
            .unwrap()
            .map(|p| p.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "csv"))
            .collect()
    } else {
        vec![path.to_path_buf()]
    }
}
pub fn load(path: &Path) -> Vec<Mmap> {
    csv_files(path)
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

pub struct FileWindow {
    file: Arc<File>,
    offset: usize,
    pub bytes: usize,
    size: usize,
}

pub struct WindowRows {
    map: Mmap,
    range: std::ops::Range<usize>,
}

impl std::ops::Deref for WindowRows {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.map[self.range.clone()]
    }
}

impl FileWindow {
    pub fn read(&self) -> WindowRows {
        // Each nominal window owns rows whose first byte falls inside it. A byte
        // before the window distinguishes a row boundary from a partial record.
        let prefix = usize::from(self.offset != 0);
        let offset = self.offset - prefix;
        let nominal_end = prefix + self.bytes;
        let remaining = self.size - offset;
        let mut length = nominal_end.saturating_add(4096).min(remaining);
        loop {
            // Input files are immutable during processing.
            let map = unsafe {
                MmapOptions::new()
                    .offset(offset as u64)
                    .len(length)
                    .map(&*self.file)
                    .unwrap()
            };
            let start = if prefix == 1 && map[0] == b'\n' {
                1
            } else {
                // The file's first row is its header. All other incomplete
                // first records belong to whichever window contains their start.
                memchr(b'\n', &map[..nominal_end]).map_or(nominal_end, |index| index + 1)
            };
            if start >= nominal_end {
                return WindowRows { map, range: 0..0 };
            }
            if let Some(index) = memchr(b'\n', &map[nominal_end - 1..]) {
                return WindowRows {
                    map,
                    range: start..nominal_end + index,
                };
            }
            if length == remaining {
                return WindowRows {
                    map,
                    range: start..length,
                };
            }
            // There is a row longer than the normal overlap. Only the window
            // that owns its first byte expands until its terminator or EOF.
            length = length.saturating_mul(2).min(remaining);
        }
    }
}
pub fn windows(path: &Path) -> Vec<FileWindow> {
    let mut windows = Vec::new();
    for path in csv_files(path) {
        let file = Arc::new(File::open(path).unwrap());
        let size = usize::try_from(file.metadata().unwrap().len()).unwrap();
        const BYTES: usize = 2 * 1024 * 1024;
        for offset in (0..size).step_by(BYTES) {
            windows.push(FileWindow {
                file: file.clone(),
                offset,
                bytes: (size - offset).min(BYTES),
                size,
            });
        }
    }
    windows
}

#[cfg(test)]
mod window_tests {
    use super::*;

    #[test]
    fn nominal_windows_own_whole_records_once() {
        let path =
            std::env::temp_dir().join(format!("shopping-window-test-{}", std::process::id()));
        let long_record = vec![b'x'; 12_000];
        for (header, rows) in [
            (b"h\n".as_slice(), b"a\n\nxyz\nlast".as_slice()),
            (b"header\n", b"a\n\nxyz\nlast\n"),
            (b"only header", b""),
            (b"\n", b"\n"),
            (b"header\n", long_record.as_slice()),
        ] {
            let contents = [header, rows].concat();
            std::fs::write(&path, &contents).unwrap();
            let file = Arc::new(File::open(&path).unwrap());
            for width in [1, 2, 3, 4, 5, 7, 127, 3500, 4096] {
                let mut actual = Vec::new();
                for offset in (0..contents.len()).step_by(width) {
                    let window = FileWindow {
                        file: file.clone(),
                        offset,
                        bytes: width.min(contents.len() - offset),
                        size: contents.len(),
                    };
                    actual.extend_from_slice(&window.read());
                }
                assert_eq!(actual.as_slice(), rows);
            }
        }
        std::fs::remove_file(path).unwrap();
    }
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
    if std::arch::is_x86_feature_detected!("avx2")
        && std::arch::is_x86_feature_detected!("bmi2")
        && std::arch::is_x86_feature_detected!("popcnt")
    {
        return unsafe { record_rows(chunk, process) };
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

pub fn link_edges(files: &[Mmap], codec: &crate::Names) -> Vec<Vec<(u64, u64)>> {
    use rayon::prelude::*;
    chunks(files)
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
        .collect()
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
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,bmi2,popcnt")]
unsafe fn record_rows<'a, const N: usize>(chunk: &'a [u8], mut process: impl FnMut([&'a [u8]; N])) {
    use std::arch::x86_64::*;
    // Caller guarantees a readable 32-byte block before loading.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn masks(p: *const u8) -> (u32, u32, u32) {
        let v = unsafe { _mm256_loadu_si256(p.cast()) };
        let mask = |ch| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(ch))) as u32;
        (mask(b',' as i8), mask(b'\n' as i8), mask(b'"' as i8))
    }
    let mut base = 0;
    while chunk.len() - base >= 128 {
        let p = unsafe { chunk.as_ptr().add(base) };
        let (a, b, c) = unsafe { masks(p) };
        let mut commas = u128::from(a);
        let mut lf = u128::from(b);
        let mut quotes = u128::from(c);
        let mut shift = 32;
        while lf == 0 && shift < 128 {
            let (a, b, c) = unsafe { masks(p.add(shift)) };
            commas |= u128::from(a) << shift;
            lf |= u128::from(b) << shift;
            quotes |= u128::from(c) << shift;
            shift += 32;
        }
        if lf == 0 {
            let beyond = base + 128;
            let end = memchr(b'\n', &chunk[beyond..]).map_or(chunk.len(), |i| beyond + i + 1);
            unsafe { vector_rows(&chunk[base..end], &mut process) };
            base = end;
            continue;
        }
        let length = lf.trailing_zeros() as usize;
        let end_bit = 1u128 << length;
        commas &= end_bit - 1;
        quotes &= end_bit - 1;
        let record = &chunk[base..base + length];
        base += length + 1;
        if commas.count_ones() as usize != N - 1 {
            continue;
        }
        let ends = commas | end_bit;
        let starts = (commas << 1) | 1;
        let stops = ends >> 1;
        if quotes & !(starts | stops) != 0 || quotes & starts & stops != 0 {
            continue;
        }
        // Pack quote presence at each field boundary and compare matching edges.
        let project = |mask: u128| {
            _pext_u64(quotes as u64, mask as u64)
                | (_pext_u64((quotes >> 64) as u64, (mask >> 64) as u64)
                    << (mask as u64).count_ones())
        };
        let mut opening = project(starts);
        let closing = project(stops) << ((ends & 1) as u32);
        if opening != closing {
            continue;
        }
        let mut fields = [&[][..]; N];
        let mut separators = ends;
        let mut start = 0;
        for f in &mut fields {
            let end = separators.trailing_zeros() as usize;
            separators &= separators - 1;
            let trim = (opening & 1) as usize;
            opening >>= 1;
            *f = &record[start + trim..end - trim];
            start = end + 1;
        }
        process(fields);
    }
    unsafe { vector_rows(&chunk[base..], process) };
}
