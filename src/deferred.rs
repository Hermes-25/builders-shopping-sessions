use crate::{
    DAY, Map, Names, Text,
    aliases::Aliases,
    buckets,
    dates::Dates,
    decimal,
    events::{Data, Placed},
    input::FileWindow,
    names, rows, transpose,
};
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

// A source owns one allocation. The range offsets partition its rows by event ID.
#[derive(Clone, Copy, Default)]
struct Row {
    id: u64,
    user: u64,
    payload: u64,
}

struct Source<V> {
    rows: Vec<Row>,
    ends: Vec<usize>,
    wide: Vec<(i64, V)>,
}

/// Move rows into counted partitions in small contiguous writes. One open
/// partial batch per partition bounds the temporary staging memory.
fn scatter_rows(input: Vec<Row>, ends: &[usize]) -> Vec<Row> {
    const BATCH: usize = 8;
    let partitions = ends.len() - 1;
    let mut next = ends[..partitions].to_vec();
    let mut staged = vec![Row::default(); partitions * BATCH];
    let mut held = vec![0u8; partitions];
    let count = input.len();
    let mut ordered = Vec::<Row>::with_capacity(count);
    let spare = &mut ordered.spare_capacity_mut()[..count];
    let mut flush = |partition: usize, length: usize, staging: &[Row]| {
        let destination = next[partition];
        let start = partition * BATCH;
        let batch = &staging[start..start + length];
        for (target, &row) in spare[destination..destination + length]
            .iter_mut()
            .zip(batch)
        {
            target.write(row);
        }
        next[partition] += length;
    };
    for row in input {
        let partition = row.id as usize & (partitions - 1);
        let index = usize::from(held[partition]);
        staged[partition * BATCH + index] = row;
        if index + 1 == BATCH {
            flush(partition, BATCH, &staged);
            held[partition] = 0;
        } else {
            held[partition] += 1;
        }
    }
    for (partition, used) in held.into_iter().enumerate() {
        if used != 0 {
            flush(partition, usize::from(used), &staged);
        }
    }
    assert_eq!(next, &ends[1..]);
    // Each cursor started at that partition’s prefix and advanced only after
    // initializing its destination slice. Together the completed ranges cover 0..count.
    unsafe { ordered.set_len(count) };
    ordered
}

impl<V> Source<V> {
    fn group(&self, index: usize) -> &[Row] {
        &self.rows[self.ends[index]..self.ends[index + 1]]
    }
}

#[derive(Clone, Copy)]
struct RawVisit<V> {
    user: u64,
    visit: V,
}

const WIDE: u64 = 1 << 63;
const BIAS: i64 = 1 << 30;

// A normal nonpurchase event stores an exact millisecond offset and a whole-second
// ingestion offset, biased so unsigned comparison preserves ingestion order.
// A wide reference carries its source number and its offset in that source's side table.
fn encode<V: Data>(
    source: usize,
    wide: &mut Vec<(i64, V)>,
    ingestion: i64,
    ts: i64,
    price: u32,
    category: u32,
    lo: i64,
) -> u64 {
    let difference = ingestion / 1000 - lo.div_euclid(1000) + BIAS;
    if price == 0
        && (0..1i64 << 31).contains(&difference)
        && ingestion % 1000 == 0
        && let Ok(offset) = u32::try_from(ts - lo)
    {
        return ((difference as u64) << 32) | u64::from(offset);
    }
    let index = u32::try_from(wide.len()).unwrap();
    wide.push((ingestion, V::pack(ts, price, category, lo)));
    WIDE | ((source as u64) << 32) | u64::from(index)
}

fn earlier<V>(candidate: u64, old: u64, sources: &[Source<V>], lo: i64) -> bool {
    if (candidate | old) & WIDE == 0 {
        return candidate >> 32 < old >> 32;
    }
    let millis = |value: u64| {
        if value & WIDE != 0 {
            sources[((value & !WIDE) >> 32) as usize].wide[value as u32 as usize].0
        } else {
            ((value >> 32) as i64 - BIAS + lo.div_euclid(1000)) * 1000
        }
    };
    millis(candidate) < millis(old)
}

fn visit<V: Data>(value: u64, sources: &[Source<V>], lo: i64) -> V {
    if value & WIDE != 0 {
        sources[((value & !WIDE) >> 32) as usize].wide[value as u32 as usize].1
    } else {
        V::pack(lo + i64::from(value as u32), 0, 0, lo)
    }
}

const ID_MASK: u64 = (1 << 40) - 1;
const MULTIPLIER: u64 = 0x9e37_79b9_7f4b;
const INVERSE_FIRST: u64 = 0x3d_ee21_9263;
const INVERSE_LAST: u64 = 0xa0_9058_81e9;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
unsafe fn eight_base36(digits: &[u8]) -> Option<u64> {
    use std::arch::x86_64::*;
    let input = unsafe { _mm_loadl_epi64(digits.as_ptr().cast()) };
    let decimal = _mm_and_si128(
        _mm_cmpgt_epi8(input, _mm_set1_epi8(47)),
        _mm_cmpgt_epi8(_mm_set1_epi8(58), input),
    );
    let letter = _mm_and_si128(
        _mm_cmpgt_epi8(input, _mm_set1_epi8(96)),
        _mm_cmpgt_epi8(_mm_set1_epi8(123), input),
    );
    if _mm_movemask_epi8(_mm_or_si128(decimal, letter)) & 255 != 255 {
        return None;
    }
    let values = _mm_sub_epi8(
        _mm_sub_epi8(input, _mm_set1_epi8(48)),
        _mm_and_si128(letter, _mm_set1_epi8(39)),
    );
    let pairs = _mm_maddubs_epi16(values, _mm_set1_epi16(0x0124));
    let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0510));
    let both = _mm_cvtsi128_si64(quads) as u64;
    Some((both as u32 as u64) * 1_679_616 + (both >> 32))
}

// Inverting two fixed permutations of the public base-36 encoding leaves an
// arbitrary XOR translation. Every possible translation remains a bijection.
// We choose one from the observed IDs solely to make the resulting table dense.
#[inline]
fn untranslated(text: &[u8]) -> Option<u64> {
    let (&prefix, digits) = text.split_first()?;
    if prefix != b'e'
        || digits.is_empty()
        || digits.len() > 8
        || (digits.len() != 1 && digits[0] == b'0')
    {
        return None;
    }
    let scalar = || {
        let mut number = 0u64;
        for &byte in digits {
            let digit = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'z' => byte - b'a' + 10,
                _ => return None,
            };
            number = number * 36 + u64::from(digit);
        }
        Some(number)
    };
    #[cfg(target_arch = "x86_64")]
    let number = if digits.len() == 8 && std::arch::is_x86_feature_detected!("ssse3") {
        unsafe { eight_base36(digits) }?
    } else {
        scalar()?
    };
    #[cfg(not(target_arch = "x86_64"))]
    let number = scalar()?;
    if number > ID_MASK {
        return None;
    }
    let y = number.wrapping_mul(INVERSE_LAST) & ID_MASK;
    Some(y ^ (y >> 17) ^ (y >> 34))
}

#[derive(Clone, Copy)]
struct Ordinals {
    translation: u64,
    capacity: usize,
}

impl Ordinals {
    #[inline]
    fn slot(self, key: u64) -> Option<usize> {
        let index = ((key ^ self.translation).wrapping_mul(INVERSE_FIRST) & ID_MASK) as usize;
        (index < self.capacity).then_some(index)
    }

    fn learn(windows: &[FileWindow], bytes: usize) -> Option<Self> {
        // The capacity is a performance budget derived from input size. Distinct
        // strings outside it take the ordinary exact sparse winner map.
        let capacity = (bytes / 64).clamp(256, 100_000_000);
        let mut anchors = Vec::new();
        let mut sample = Vec::new();
        let read_sample = |window: &FileWindow| -> Vec<u64> {
            let contents = window.read();
            contents
                .split(|&b| b == b'\n')
                .take(12)
                .filter_map(|line| {
                    let text = line.split(|&b| b == b',').next()?;
                    let text = text
                        .strip_prefix(b"\"")
                        .and_then(|s| s.strip_suffix(b"\""))
                        .unwrap_or(text);
                    untranslated(text)
                })
                .collect()
        };
        for window in windows.iter().filter(|w| w.starts_file()).take(192) {
            let ids = read_sample(window);
            anchors.extend(ids.iter().take(2).copied());
            sample.extend(ids.into_iter().take(2));
        }
        for window in windows
            .iter()
            .step_by(windows.len().div_ceil(20).max(1))
            .take(20)
        {
            sample.extend(read_sample(window).into_iter().take(8));
        }
        if sample.len() < 12 {
            return None;
        }
        for offset in 0u64..32 {
            for &anchor in &anchors {
                let layout = Self {
                    translation: anchor ^ (offset.wrapping_mul(MULTIPLIER) & ID_MASK),
                    capacity,
                };
                if sample.iter().take(8).all(|&k| layout.slot(k).is_some())
                    && sample.iter().filter(|&&k| layout.slot(k).is_some()).count() * 20
                        >= sample.len() * 19
                {
                    return Some(layout);
                }
            }
        }
        sample.sort_unstable();
        sample.dedup();
        if sample.len() < 10 {
            return None;
        }
        let anchors = [sample[sample.len() / 3], sample[2 * sample.len() / 3]];
        let probes = [sample[0], sample[sample.len() / 2], *sample.last().unwrap()];
        for anchor in anchors {
            // With disordered input the anchor could occupy any slot. Each
            // possible position determines a single translation. Three
            // independent key probes reject unlikely layouts before a full check.
            if let Some(found) = (0..capacity).into_par_iter().find_map_any(|offset| {
                let layout = Self {
                    translation: anchor ^ ((offset as u64).wrapping_mul(MULTIPLIER) & ID_MASK),
                    capacity,
                };
                if probes.iter().all(|&key| layout.slot(key).is_some())
                    && sample
                        .iter()
                        .filter(|&&key| layout.slot(key).is_some())
                        .count()
                        * 20
                        >= sample.len() * 19
                {
                    Some(layout)
                } else {
                    None
                }
            }) {
                return Some(found);
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
struct DirectRow<V> {
    slot: u32,
    person: u32,
    ingestion: u32,
    data: V,
}
#[derive(Clone, Copy)]
struct SparseRow<V> {
    id: u64,
    ingestion: i64,
    person: u32,
    data: V,
}

struct PendingPerson {
    key: u64,
    bucket: usize,
    index: usize,
    sparse: bool,
}

fn finish_people<V>(
    aliases: &Aliases,
    pending: &mut Vec<PendingPerson>,
    cache: &mut [(u64, u32)],
    dense: &mut [DirectRow<V>],
    sparse: &mut [SparseRow<V>],
    unsupported: &AtomicBool,
) {
    for row in pending.drain(..) {
        let resolved = aliases.resolve(row.key, row.bucket);
        let Ok(person) = u32::try_from(resolved) else {
            unsupported.store(true, Relaxed);
            continue;
        };
        if person == u32::MAX {
            unsupported.store(true, Relaxed);
            continue;
        }
        cache[row.bucket & (cache.len() - 1)] = (row.key, person);
        if row.sparse {
            sparse[row.index].person = person;
        } else {
            dense[row.index].person = person;
        }
    }
}

fn direct<'a, V: Data>(
    chunks: &[FileWindow],
    layout: Ordinals,
    lo: i64,
    hi: i64,
    products: &Map<Text<'a>, u32>,
    unknown: u32,
    names: &Names,
    aliases: &Aliases,
    dates: &Dates,
) -> Option<Vec<Vec<Vec<Placed<V>>>>> {
    let sources = rayon::current_num_threads().min(chunks.len()).max(1);
    let start = std::time::Instant::now();
    let width = chunks.len().div_ceil(sources);
    let unsupported = AtomicBool::new(false);
    let words = layout.capacity.div_ceil(64);
    let sparse_names = Names::default();
    let parts: Vec<_> = chunks
        .par_chunks(width)
        .map(|windows| {
            let mut dense = Vec::with_capacity(windows.iter().map(|w| w.bytes).sum::<usize>() / 96);
            let mut sparse = Vec::new();
            let mut seen = vec![0u64; words];
            let mut repeated = vec![0u64; words];
            let mut pending = Vec::with_capacity(48);
            let mut cache = vec![(0, u32::MAX); 4096];
            for window in windows {
                let bytes = window.read();
                rows::<7>(&bytes, |[id, user, raw, ingest, kind, product, cents]| {
                    if id.is_empty() {
                        return;
                    }
                    let ingestion = dates.ingest(ingest);
                    let ts = dates.parse(raw).unwrap_or(i64::MIN);
                    let valid = ts >= lo
                        && ts < hi
                        && ingestion - ts <= DAY
                        && !user.is_empty()
                        && !product.is_empty();
                    let (price, category) = if valid && kind == b"purchase" {
                        (
                            decimal(cents, 10_000_000).unwrap_or(0) as u32,
                            *products.get(product).unwrap_or(&unknown),
                        )
                    } else {
                        (0, 0)
                    };
                    let valid = valid && (kind == b"view" || kind == b"cart" || price > 0);
                    let mut person = u32::MAX;
                    let mut deferred = None;
                    if valid {
                        let key = names.encode(user);
                        let bucket = aliases.prefetch(key);
                        let found = cache[bucket & (cache.len() - 1)];
                        if found.0 == key && found.1 != u32::MAX {
                            person = found.1;
                        } else {
                            deferred = Some((key, bucket));
                        }
                    }
                    let data = V::pack(if valid { ts } else { lo }, price, category, lo);
                    let destination;
                    if let Some(slot) = untranslated(id).and_then(|key| layout.slot(key)) {
                        let seconds =
                            ingestion.div_euclid(1000) - lo.div_euclid(1000) + (1i64 << 31);
                        if !(0..=i64::from(u32::MAX)).contains(&seconds) {
                            unsupported.store(true, Relaxed);
                            return;
                        }
                        let bit = 1u64 << (slot & 63);
                        let word = slot >> 6;
                        let previous = seen[word];
                        if previous & bit != 0 {
                            repeated[word] |= bit;
                        }
                        seen[word] = previous | bit;
                        destination = (dense.len(), false);
                        dense.push(DirectRow {
                            slot: slot as u32,
                            person,
                            ingestion: seconds as u32,
                            data,
                        });
                    } else {
                        destination = (sparse.len(), true);
                        sparse.push(SparseRow {
                            id: sparse_names.encode(id),
                            ingestion,
                            person,
                            data,
                        });
                    }
                    if let Some((key, bucket)) = deferred {
                        pending.push(PendingPerson {
                            key,
                            bucket,
                            index: destination.0,
                            sparse: destination.1,
                        });
                        if pending.len() == 48 {
                            finish_people(
                                aliases,
                                &mut pending,
                                &mut cache,
                                &mut dense,
                                &mut sparse,
                                &unsupported,
                            );
                        }
                    }
                });
            }
            finish_people(
                aliases,
                &mut pending,
                &mut cache,
                &mut dense,
                &mut sparse,
                &unsupported,
            );
            ((dense, sparse), (seen, repeated))
        })
        .collect();
    if unsupported.load(Relaxed) {
        return None;
    }
    if std::env::var_os("TIMINGS").is_some() {
        eprintln!("event projection and source masks {:?}", start.elapsed());
    }
    let (records, maps): (Vec<_>, Vec<_>) = parts.into_iter().unzip();
    // A slot repeats if it repeats in a single source or belongs to two
    // sources. Only this small union of exact IDs needs a winner record.
    let repeated: Vec<u64> = (0..words)
        .into_par_iter()
        .map(|word| {
            let mut union = 0;
            let mut conflict = 0;
            for (seen, local) in &maps {
                conflict |= local[word] | (union & seen[word]);
                union |= seen[word];
            }
            conflict
        })
        .collect();
    drop(maps);
    let sparse_count: usize = records.iter().map(|(_, s)| s.len()).sum();
    if std::env::var_os("TIMINGS").is_some() {
        eprintln!(
            "direct event winners, slots {}, sparse rows {}",
            layout.capacity, sparse_count
        );
    }
    let (dense, sparse): (Vec<_>, Vec<_>) = records.into_iter().unzip();
    let (mut output, contests): (Vec<_>, Vec<_>) = dense
        .into_par_iter()
        .map(|records| {
            let mut out = buckets();
            let mut contests = buckets();
            for row in records {
                let slot = row.slot as usize;
                if repeated[slot >> 6] & (1u64 << (slot & 63)) != 0 {
                    contests[slot & 127].push(row);
                } else if row.person != u32::MAX {
                    out[(row.person & 127) as usize].push(Placed {
                        user: row.person >> 7,
                        data: row.data,
                    });
                }
            }
            (out, contests)
        })
        .unzip();
    let contested: Vec<_> = transpose(contests)
        .into_par_iter()
        .map(|blocks| {
            let mut exact = Map::<u32, DirectRow<V>>::default();
            for row in blocks.into_iter().flatten() {
                let old = exact.entry(row.slot).or_insert(row);
                if row.ingestion < old.ingestion {
                    *old = row;
                }
            }
            let mut out = buckets();
            for row in exact.into_values().filter(|r| r.person != u32::MAX) {
                out[(row.person & 127) as usize].push(Placed {
                    user: row.person >> 7,
                    data: row.data,
                });
            }
            out
        })
        .collect();
    output.extend(contested);
    let mut exact = Map::<u64, SparseRow<V>>::default();
    for row in sparse.into_iter().flatten() {
        let old = exact.entry(row.id).or_insert(row);
        if row.ingestion < old.ingestion {
            *old = row;
        }
    }
    let mut out = buckets();
    for row in exact.into_values().filter(|row| row.person != u32::MAX) {
        out[(row.person & 127) as usize].push(Placed {
            user: row.person >> 7,
            data: row.data,
        });
    }
    output.push(out);
    Some(transpose(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timelines::Visit;

    #[test]
    fn direct_spelling_is_canonical_and_bijective() {
        fn spelling(value: u64) -> Vec<u8> {
            let mut number = value;
            let mut out = Vec::new();
            loop {
                out.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(number % 36) as usize]);
                number /= 36;
                if number == 0 {
                    break;
                }
            }
            out.push(b'e');
            out.reverse();
            out
        }
        for translation in [0, 23, 0x98_76ab_cdef] {
            let layout = Ordinals {
                translation,
                capacity: 100_000,
            };
            for i in 0..100_000u64 {
                let k = (i.wrapping_mul(MULTIPLIER) ^ translation) & ID_MASK;
                let value = (k ^ (k >> 17)).wrapping_mul(0xd6e8_feb8_6659) & ID_MASK;
                assert_eq!(
                    untranslated(&spelling(value)).and_then(|x| layout.slot(x)),
                    Some(i as usize)
                );
            }
        }
        for id in [
            b"e00".as_slice(),
            b"eaA",
            b"E15",
            b"e",
            b"ezzzzzzzz",
            b"e1111111\xff",
            b"e1111111/",
        ] {
            assert!(untranslated(id).is_none());
        }
    }

    #[test]
    fn scatter_preserves_each_row_with_empty_and_partial_batches() {
        for partitions in [1, 4, 16] {
            for count in [0, 1, 7, 8, 9, 127, 1025] {
                let mut ends = vec![0; partitions + 1];
                let mut input = Vec::new();
                for sequence in 0..count {
                    let id = names::mixed(((sequence * 17) % 37 + 1) as u64) as u64;
                    ends[(id as usize & (partitions - 1)) + 1] += 1;
                    input.push(Row {
                        id,
                        user: sequence as u64,
                        payload: 0,
                    });
                }
                for part in 0..partitions {
                    ends[part + 1] += ends[part];
                }
                let output = scatter_rows(input, &ends);
                let mut sequences = Vec::new();
                for part in 0..partitions {
                    for record in &output[ends[part]..ends[part + 1]] {
                        assert_eq!(record.id as usize & (partitions - 1), part);
                        sequences.push(record.user);
                    }
                }
                sequences.sort_unstable();
                assert_eq!(sequences, (0..count as u64).collect::<Vec<_>>());
            }
        }
    }

    #[test]
    fn packed_rows_preserve_milliseconds_and_purchase_values() {
        let lo = 1_700_000_000_000;
        let mut wide = Vec::new();
        let a = encode::<Visit>(0, &mut wide, lo - 1000, lo + 1499, 0, 0, lo);
        let b = encode::<Visit>(0, &mut wide, lo + 1, lo + 999, 16777211, 124, lo);
        let c = encode::<Visit>(0, &mut wide, lo + 1000, lo + u32::MAX as i64 + 1, 0, 0, lo);
        assert_eq!(a & WIDE, 0);
        assert_ne!(b & WIDE, 0);
        assert_ne!(c & WIDE, 0);
        let sources = [Source {
            rows: Vec::new(),
            ends: vec![0],
            wide,
        }];
        assert!(earlier(a, b, &sources, lo));
        assert!(earlier(b, c, &sources, lo));
        assert!(!earlier(b, a, &sources, lo));
        assert_eq!(visit(a, &sources, lo).ts, lo + 1499);
        let purchase = visit(b, &sources, lo);
        assert_eq!(
            (purchase.ts, purchase.price, purchase.category),
            (lo + 999, 16777211, 124)
        );
        assert_eq!(visit(c, &sources, lo).ts, lo + u32::MAX as i64 + 1);
    }
}

pub fn route<'a, V: Data>(
    chunks: Vec<FileWindow>,
    lo: i64,
    hi: i64,
    products: &Map<Text<'a>, u32>,
    unknown: u32,
    people: &Names,
    aliases: &Aliases,
    dates: &Dates,
) -> Vec<Vec<Vec<Placed<V>>>> {
    let bytes = chunks.iter().map(|w| w.bytes).sum::<usize>();
    let started = std::time::Instant::now();
    if let Some(layout) = Ordinals::learn(&chunks, bytes) {
        if std::env::var_os("TIMINGS").is_some() {
            eprintln!("event index layout selection {:?}", started.elapsed());
        }
        if let Some(result) = direct::<V>(
            &chunks, layout, lo, hi, products, unknown, people, aliases, dates,
        ) {
            return result;
        }
    }
    let groups = (bytes / (90 * 8192)).clamp(1, 4096).next_power_of_two();
    let workers = rayon::current_num_threads();
    let chunk_width = chunks.len().div_ceil(workers * 4).max(1);
    let event_ids = Names::default();
    let mut previous = std::time::Instant::now();
    let mut phase = |label| {
        if std::env::var_os("TIMINGS").is_some() {
            eprintln!("{label} {:?}", previous.elapsed());
        }
        previous = std::time::Instant::now();
    };
    let sources: Vec<_> = chunks
        .par_chunks(chunk_width)
        .enumerate()
        .map(|(source_number, windows)| {
            let mut collected =
                Vec::with_capacity(windows.iter().map(|w| w.bytes).sum::<usize>() / 90);
            let mut ends = vec![0usize; groups + 1];
            let mut wide = Vec::new();
            for window in windows {
                let bytes = window.read();
                let mut cached = (b"".as_slice(), u64::MAX);
                rows::<7>(&bytes, |[id, user, raw, ingest, kind, product, price]| {
                    if id.is_empty() {
                        return;
                    }
                    let ingestion = dates.ingest(ingest);
                    let ts = dates.parse(raw).unwrap_or(i64::MIN);
                    let valid = ts >= lo
                        && ts < hi
                        && ingestion - ts <= DAY
                        && !user.is_empty()
                        && !product.is_empty();
                    let (price, category) = if valid && kind == b"purchase" {
                        (
                            decimal(price, 10_000_000).unwrap_or(0) as u32,
                            *products.get(product).unwrap_or(&unknown),
                        )
                    } else {
                        (0, 0)
                    };
                    let valid = valid && (kind == b"view" || kind == b"cart" || price > 0);
                    if valid && cached.0 != user {
                        cached = (user, people.encode(user));
                    }
                    // This reversible permutation serves as both an exact identity
                    // and a hash in every later pass.
                    let id = names::mixed(event_ids.encode(id)) as u64;
                    ends[(id as usize & (groups - 1)) + 1] += 1;
                    let payload = encode(
                        source_number,
                        &mut wide,
                        ingestion,
                        if valid { ts } else { lo },
                        price,
                        category,
                        lo,
                    );
                    collected.push(Row {
                        id,
                        user: if valid { cached.1 } else { u64::MAX },
                        payload,
                    });
                });
            }
            for group in 0..groups {
                ends[group + 1] += ends[group];
            }
            Source {
                rows: scatter_rows(collected, &ends),
                ends,
                wide,
            }
        })
        .collect();
    phase("event input and local partitioning");
    let group_shift = groups.trailing_zeros();
    let jobs = (workers * 4).min(groups);
    let joined_input: Vec<_> = (0..jobs)
        .into_par_iter()
        .map(|job| {
            let mut out = buckets();
            let mut slots = Vec::<u64>::new();
            let mut winners = Vec::<Row>::new();
            for group in (job..groups).step_by(jobs) {
                // Every duplicate shares an event-ID partition. Its table is owned by this job.
                let count = sources.iter().map(|s| s.group(group).len()).sum::<usize>();
                if count == 0 {
                    continue;
                }
                let mask = (count + count / 3 + 1).next_power_of_two() - 1;
                // The group generation makes slots reusable without clearing the table.
                // Only the dense winners own payloads. A fingerprint filters collisions.
                let generation = ((group + 1) as u64) << 48;
                if slots.len() < mask + 1 {
                    slots.resize(mask + 1, 0);
                }
                winners.clear();
                winners.reserve(count);
                for source in &sources {
                    for &row in source.group(group) {
                        let mut pos = (row.id as usize >> group_shift) & mask;
                        let signature = generation | ((row.id >> 48) << 32);
                        loop {
                            let slot = slots[pos];
                            if slot >> 48 != generation >> 48 {
                                let index = u32::try_from(winners.len()).unwrap();
                                slots[pos] = signature | u64::from(index);
                                winners.push(row);
                                break;
                            }
                            if slot >> 32 == signature >> 32 {
                                let old = &mut winners[slot as u32 as usize];
                                if old.id == row.id {
                                    if earlier(row.payload, old.payload, &sources, lo) {
                                        *old = row;
                                    }
                                    break;
                                }
                            }
                            pos = (pos + 1) & mask;
                        }
                    }
                }
                for &row in &winners {
                    if row.user != u64::MAX {
                        let shard = names::mixed(row.user) >> 57;
                        out[shard].push(RawVisit {
                            user: row.user,
                            visit: visit(row.payload, &sources, lo),
                        });
                    }
                }
            }
            out
        })
        .collect();
    drop(sources);
    phase("cache-local event deduplication");
    let routed: Vec<_> = transpose(joined_input)
        .into_par_iter()
        .map(|blocks| {
            let mut out = buckets();
            // The flat identity table keeps these raw keys in one contiguous probe range.
            for row in blocks.into_iter().flatten() {
                let person = aliases.resolve(row.user, aliases.prefetch(row.user));
                out[(person & 127) as usize].push(Placed {
                    user: (person >> 7) as u32,
                    data: row.visit,
                });
            }
            out
        })
        .collect();
    phase("cache-local user join");
    transpose(routed)
}
