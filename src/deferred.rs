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
        let partition = names::mixed(row.id) & (partitions - 1);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timelines::Visit;

    #[test]
    fn scatter_preserves_each_row_with_empty_and_partial_batches() {
        for partitions in [1, 4, 16] {
            for count in [0, 1, 7, 8, 9, 127, 1025] {
                let mut ends = vec![0; partitions + 1];
                let mut input = Vec::new();
                for sequence in 0..count {
                    let id = ((sequence * 17) % 37 + 1) as u64;
                    ends[(names::mixed(id) & (partitions - 1)) + 1] += 1;
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
                        assert_eq!(names::mixed(record.id) & (partitions - 1), part);
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
                    let id = event_ids.encode(id);
                    ends[(names::mixed(id) & (groups - 1)) + 1] += 1;
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
                        let mut pos = (names::mixed(row.id) >> group_shift) & mask;
                        loop {
                            let old = &mut winners[pos];
                            // Empty event names are discarded. Every retained name has a nonzero encoding.
                            if old.id == 0 {
                                *old = row;
                                break;
                            }
                            if old.id == row.id {
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
                    if row.id != 0 && row.user != u64::MAX {
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
