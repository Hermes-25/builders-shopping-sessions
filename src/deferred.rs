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

#[derive(Clone, Copy, Default)]
struct Row {
    key: u64,
    payload: u64,
}

#[derive(Clone, Copy)]
struct Wide<V> {
    ingestion: i64,
    person: u64,
    visit: V,
}
struct Source<V> {
    rows: Vec<Row>,
    ends: Vec<usize>,
    wide: Vec<Wide<V>>,
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
        let partition = row.key as usize & (partitions - 1);
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

const WIDE: u64 = 1 << 63;
const TIME_BITS: u64 = (1 << 31) - 1;
const DELAY_MASK: u64 = 1023;

// An inline payload uses 32 bits for the canonical person, 21 for seconds
// since the window start and 10 for ingestion lag. Its high bit stays clear.
// A tagged payload identifies an exact full-width value in its source table.
fn encode<V: Data>(
    source: usize,
    extras: &mut Vec<Wide<V>>,
    person: u64,
    ts: i64,
    ingestion: i64,
    price: u32,
    category: u32,
    lo: i64,
) -> u64 {
    if price == 0 && person <= u64::from(u32::MAX) {
        let offset = ts - lo;
        let lag = ingestion - ts;
        if (0..(1i64 << 21) * 1000).contains(&offset)
            && (0..=1023_000).contains(&lag)
            && offset % 1000 == 0
            && lag % 1000 == 0
        {
            return (person << 31) | (((offset / 1000) as u64) << 10) | (lag / 1000) as u64;
        }
    }
    let idx = u32::try_from(extras.len()).unwrap();
    let source = u32::try_from(source).unwrap();
    assert!(source < 1 << 31);
    extras.push(Wide {
        ingestion,
        person,
        visit: V::pack(ts, price, category, lo),
    });
    WIDE | (u64::from(source) << 32) | u64::from(idx)
}

#[inline]
fn extra<V>(value: u64, sources: &[Source<V>]) -> &Wide<V> {
    &sources[((value & !WIDE) >> 32) as usize].wide[value as u32 as usize]
}
#[inline]
fn ingestion_seconds(value: u64) -> u64 {
    ((value & TIME_BITS) >> 10) + (value & DELAY_MASK)
}
#[inline]
fn earlier<V>(a: u64, b: u64, sources: &[Source<V>], lo: i64) -> bool {
    if (a | b) & WIDE == 0 {
        return ingestion_seconds(a) < ingestion_seconds(b);
    }
    let millis = |value| {
        if value & WIDE == 0 {
            lo + (ingestion_seconds(value) as i64) * 1000
        } else {
            extra(value, sources).ingestion
        }
    };
    millis(a) < millis(b)
}
#[inline]
fn unpack<V: Data>(value: u64, sources: &[Source<V>], lo: i64) -> (u64, V) {
    if value & WIDE != 0 {
        let row = extra(value, sources);
        return (row.person, row.visit);
    }
    (
        value >> 31,
        V::pack(lo + (((value & TIME_BITS) >> 10) as i64) * 1000, 0, 0, lo),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timelines::Visit;
    #[test]
    fn scattering_handles_empty_partitions_and_incomplete_batches() {
        for count in [0, 1, 7, 8, 9, 127, 1025] {
            let mut offsets = vec![0; 17];
            let mut records = Vec::new();
            for i in 0..count {
                let key = ((i * 13) % 29 + 1) as u64;
                offsets[(key as usize & 15) + 1] += 1;
                records.push(Row { key, payload: i as u64 });
            }
            for i in 0..16 {
                offsets[i + 1] += offsets[i];
            }
            let result = scatter_rows(records, &offsets);
            let mut ordinals = Vec::new();
            for i in 0..16 {
                for row in &result[offsets[i]..offsets[i + 1]] {
                    assert_eq!(row.key as usize & 15, i);
                    ordinals.push(row.payload as usize);
                }
            }
            ordinals.sort_unstable();
            assert_eq!(ordinals, (0..count).collect::<Vec<_>>());
        }
    }
    #[test]
    fn common_clock_and_exception_domains_compare_exactly() {
        let lo = 1_750_000_000_000;
        let mut side = Vec::new();
        let first = encode::<Visit>(0, &mut side, 1, lo + 1000, lo + 12000, 0, 0, lo);
        let later = encode::<Visit>(0, &mut side, 2, lo + 8000, lo + 15000, 0, 0, lo);
        let purchase = encode::<Visit>(0, &mut side, 8, lo + 1999, lo + 3000, 200, 3, lo);
        let invalid = encode::<Visit>(0, &mut side, u64::MAX, lo, lo - 5000, 0, 0, lo);
        let large = encode::<Visit>(
            0,
            &mut side,
            1u64 << 37,
            lo + 1000,
            lo + 23 * 1000,
            0,
            0,
            lo,
        );
        let delayed = encode::<Visit>(0, &mut side, 1, lo + 1000, lo + 1_025_000, 0, 0, lo);
        let far = encode::<Visit>(
            0,
            &mut side,
            1,
            lo + (1i64 << 21) * 1000,
            lo + (1i64 << 21) * 1000,
            0,
            0,
            lo,
        );
        assert_eq!(first & WIDE, 0);
        assert_eq!(later & WIDE, 0);
        assert_eq!(side.len(), 5);
        let groups = [Source {
            rows: vec![],
            ends: vec![0],
            wide: side,
        }];
        for pair in [invalid, purchase, first, later, large, delayed, far].windows(2) {
            assert!(earlier(pair[0], pair[1], &groups, lo));
            assert!(!earlier(pair[1], pair[0], &groups, lo));
        }
        assert_eq!(unpack(invalid, &groups, lo).0, u64::MAX);
        assert_eq!(unpack(large, &groups, lo).0, 1u64 << 37);
        let (user, visit) = unpack(purchase, &groups, lo);
        assert_eq!(
            (user, visit.ts, visit.price, visit.category),
            (8, lo + 1999, 200, 3)
        );
        let (user, visit) = unpack(first, &groups, lo);
        assert_eq!((user, visit.ts), (1, lo + 1000));
    }
}
struct Candidate {
    key: u64,
    user: u64,
    start: usize,
    ts: i64,
    ingestion: i64,
    price: u32,
    category: u32,
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

            let mut pending = Vec::with_capacity(48);
            let mut flush = |queue: &mut Vec<Candidate>| {
                for row in queue.drain(..) {
                    let person = if row.user == u64::MAX {
                        u64::MAX
                    } else {
                        aliases.resolve(row.user, row.start)
                    };
                    let payload = encode(
                        source_number,
                        &mut wide,
                        person,
                        row.ts,
                        row.ingestion,
                        row.price,
                        row.category,
                        lo,
                    );
                    ends[(row.key as usize & (groups - 1)) + 1] += 1;
                    collected.push(Row {
                        key: row.key,
                        payload,
                    });
                }
            };
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

                    // The two odd multiplications and xor-shifts are invertible
                    // on 64-bit words. This stored permutation acts as both the
                    // complete event identity and its hash in all later passes.
                    let key = names::mixed(event_ids.encode(id)) as u64;
                    let user = if valid { cached.1 } else { u64::MAX };
                    let start = if valid { aliases.prefetch(user) } else { 0 };
                    pending.push(Candidate {
                        key,
                        user,
                        start,
                        ts: if valid { ts } else { lo },
                        ingestion,
                        price,
                        category,
                    });
                    if pending.len() == 48 {
                        flush(&mut pending);
                    }
                });
            }
            flush(&mut pending);
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
    let routed: Vec<_> = (0..jobs)
        .into_par_iter()
        .map(|job| {
            let mut out = buckets();
            for group in (job..groups).step_by(jobs) {
                // Every duplicate shares an event-ID partition. Its table is owned by this job.
                let count = sources.iter().map(|s| s.group(group).len()).sum::<usize>();
                if count == 0 {
                    continue;
                }
                let mask = (count + count / 3 + 1).next_power_of_two() - 1;
                let mut winners = vec![Row::default(); mask + 1];
                for source in &sources {
                    for &row in source.group(group) {
                        let mut pos = (row.key as usize >> group_shift) & mask;
                        loop {
                            let old = &mut winners[pos];
                            // Empty event names are discarded. Every retained name has a nonzero encoding.
                            if old.key == 0 {
                                *old = row;
                                break;
                            }
                            if old.key == row.key {
                                if earlier(row.payload, old.payload, &sources, lo) {
                                    *old = row;
                                }
                                break;
                            }
                            pos = (pos + 1) & mask;
                        }
                    }
                }

                for row in winners {
                    if row.key != 0 {
                        let (person, visit) = unpack(row.payload, &sources, lo);
                        if person != u64::MAX {
                            out[(person & 127) as usize].push(Placed {
                                user: (person >> 7) as u32,
                                data: visit,
                            });
                        }
                    }
                }
            }
            out
        })
        .collect();
    phase("event deduplication and direct person placement");
    transpose(routed)
}
