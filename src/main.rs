mod input;
mod output;
use input::{DAY, Text, WEEK, chunks, decimal, linked, load, read_pairs, rows, shard, timestamp};
use rayon::prelude::*;
use rustc_hash::FxHashMap as Map;
use std::{env, fs, path::PathBuf};

type Buckets<T> = Vec<Vec<T>>;
fn buckets<T>() -> Buckets<T> {
    (0..128).map(|_| Vec::new()).collect()
}

// Place locally produced columns into independently owned partitions.
fn transpose<T>(matrix: Vec<Buckets<T>>) -> Vec<Vec<Vec<T>>> {
    let mut out = buckets();
    for chunk in matrix {
        for (dst, items) in out.iter_mut().zip(chunk) {
            if !items.is_empty() {
                dst.push(items);
            }
        }
    }
    out
}

#[derive(Clone, Copy)]
struct Event<'a> {
    user: Text<'a>,
    ts: i64,
    price: u32,
    category: u32,
}
#[derive(Clone, Copy)]
struct Candidate<'a> {
    id: Text<'a>,
    ingest: i64,
    event: Event<'a>,
}
#[derive(Clone, Copy)]
struct Click<'a> {
    user: Text<'a>,
    ts: i64,
    id: Text<'a>,
    campaign: Text<'a>,
}

fn route<'a>(
    mut event: Event<'a>,
    alias: &Map<Text<'a>, Text<'a>>,
    grouped: &mut Buckets<Event<'a>>,
) {
    if !event.user.is_empty() {
        event.user = alias.get(event.user).copied().unwrap_or(event.user);
        grouped[shard(event.user)].push(event);
    }
}

pub(crate) struct UserDay<'a> {
    user: Text<'a>,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<Text<'a>>,
}
#[derive(Default)]
pub(crate) struct Totals {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assists: i64,
}
type Campaigns<'a> = Map<(Text<'a>, i32), Totals>;

fn sessions<'a>(
    events: &[Event<'a>],
    clicks: &[Click<'a>],
    users: &mut Vec<UserDay<'a>>,
    campaigns: &mut Campaigns<'a>,
    labels: &[Text<'a>],
) {
    let user = events[0].user;
    let (mut left, mut right) = (0, 0);
    let mut active: Map<Text<'a>, usize> = Map::default();
    let mut categories: Map<Text<'a>, i64> = Map::default();
    let mut i = 0;
    while i < events.len() {
        let ts = events[i].ts;
        let day = ts.div_euclid(DAY) as i32;
        if users.last().is_none_or(|r| r.user != user || r.day != day) {
            categories.clear();
            users.push(UserDay {
                user,
                day,
                sessions: 0,
                events: 0,
                revenue: 0,
                top: None,
            });
        }
        let row = users.last_mut().unwrap();
        while right < clicks.len() && clicks[right].ts <= ts {
            *active.entry(clicks[right].campaign).or_default() += 1;
            right += 1;
        }
        while left < right && clicks[left].ts < ts - WEEK {
            let campaign = clicks[left].campaign;
            let count = active.get_mut(campaign).unwrap();
            *count -= 1;
            if *count == 0 {
                active.remove(campaign);
            }
            left += 1;
        }
        let credit = if left < right {
            clicks[right - 1].campaign
        } else {
            b"organic"
        };
        for &campaign in active.keys() {
            campaigns.entry((campaign, day)).or_default().assists += 1;
        }
        let mut last = ts;
        let mut revenue = 0;
        let start = i;
        while i < events.len() && events[i].ts - last <= 1_800_000 {
            let e = events[i];
            last = e.ts;
            if e.price > 0 {
                let category = labels[e.category as usize];
                let category_revenue = categories.entry(category).or_default();
                *category_revenue += i64::from(e.price);
                let n = *category_revenue;
                if row
                    .top
                    .is_none_or(|t| n > categories[t] || (n == categories[t] && category < t))
                {
                    row.top = Some(category);
                }
                revenue += i64::from(e.price);
            }
            i += 1;
        }
        row.sessions += 1;
        row.events += (i - start) as i64;
        row.revenue += revenue;
        let c = campaigns.entry((credit, day)).or_default();
        c.sessions += 1;
        c.purchasing += i64::from(revenue > 0);
        c.revenue += revenue;
    }
}

fn main() {
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let output = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let threads = env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json")).unwrap()).unwrap();
    let window = |key: &str| timestamp(manifest[key].as_str().unwrap().as_bytes()).unwrap();
    let (lo, hi) = (window("window_start"), window("window_end"));
    let l = load(&data.join("links"));
    let alias = linked(&l);
    let p = load(&data.join("products.csv"));
    let all_products = read_pairs(&p);
    let mut labels: Vec<Text> = vec![b"unknown"];
    labels.extend(all_products.values().copied());
    labels.sort_unstable();
    labels.dedup();
    let category_id = |name: Text| labels.binary_search(&name).unwrap() as u32;
    let products: Map<Text, u32> = all_products
        .into_iter()
        .map(|(k, v)| (k, category_id(v)))
        .collect();
    let unknown = category_id(b"unknown");
    let c = load(&data.join("campaigns.csv"));
    let channels = read_pairs(&c);
    let es = load(&data.join("events"));
    let cs = load(&data.join("clicks"));
    let t = std::time::Instant::now();
    let measure = |msg| {
        if env::var_os("TIMINGS").is_some() {
            eprintln!("{msg} {:?}", t.elapsed());
        }
    };
    let event_chunks = chunks(&es);
    // First-field counts can overestimate duplicates on malformed rows; a false
    // positive simply takes the exact string-keyed path.
    let nbits = (es.iter().map(|f| f.len()).sum::<usize>() / 8)
        .max(64)
        .next_power_of_two();
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    let seen: Vec<_> = (0..nbits / 64).map(|_| AtomicU64::new(0)).collect();
    let repeated: Vec<_> = (0..nbits / 64).map(|_| AtomicU64::new(0)).collect();
    event_chunks.par_iter().for_each(|chunk| {
        let mut start = 0;
        for end in memchr::memchr_iter(b'\n', chunk).chain(std::iter::once(chunk.len())) {
            if end < start {
                continue;
            }
            let line = &chunk[start..end];
            start = end + 1;
            if let Some(comma) = memchr::memchr(b',', line) {
                let id = &line[..comma];
                let id = if id.len() >= 2 && id[0] == b'"' && id[id.len() - 1] == b'"' {
                    &id[1..id.len() - 1]
                } else {
                    id
                };
                let bit = input::signature(id) & (nbits - 1);
                let mask = 1u64 << (bit % 64);
                if seen[bit / 64].fetch_or(mask, Relaxed) & mask != 0 {
                    repeated[bit / 64].fetch_or(mask, Relaxed);
                }
            }
        }
    });
    drop(seen);
    measure("ID scan");
    let (parts, mut by_user): (Vec<_>, Vec<_>) = event_chunks
        .into_par_iter()
        .map(|chunk| {
            let mut out = buckets();
            let mut direct = buckets();
            rows::<7>(
                chunk,
                |[id, user, raw_ts, raw_ingest, kind, product, price]| {
                    if id.is_empty() {
                        return;
                    }
                    let ingest = timestamp(raw_ingest).unwrap();
                    let ts = timestamp(raw_ts).unwrap_or(i64::MIN);
                    let valid = ts >= lo
                        && ts < hi
                        && ingest - ts <= DAY
                        && !user.is_empty()
                        && !product.is_empty();
                    let (price, category) = if valid && kind == b"purchase" {
                        (
                            decimal(price, 10_000_000).unwrap_or(0) as u32,
                            *products.get(product).unwrap_or(&unknown),
                        )
                    } else {
                        (0, unknown)
                    };
                    let valid = valid && (kind == b"view" || kind == b"cart" || price > 0);
                    let signature = input::signature(id);
                    let event = Event {
                        user: if valid { user } else { b"" },
                        ts,
                        price,
                        category,
                    };
                    let bit = signature & (nbits - 1);
                    if repeated[bit / 64].load(Relaxed) & (1u64 << (bit % 64)) != 0 {
                        out[signature % 128].push(Candidate { id, ingest, event });
                    } else {
                        route(event, &alias, &mut direct);
                    }
                },
            );
            (out, direct)
        })
        .unzip();
    drop(repeated);
    measure("event decoding");
    let by_duplicate: Vec<_> = transpose(parts)
        .into_par_iter()
        .map(|parts| {
            let mut grouped = buckets();
            let mut winners: Map<Text, (i64, Event)> = Map::default();
            for candidate in parts.into_iter().flatten() {
                let slot = winners
                    .entry(candidate.id)
                    .or_insert((candidate.ingest, candidate.event));
                if candidate.ingest < slot.0 {
                    *slot = (candidate.ingest, candidate.event);
                }
            }
            for (_, event) in winners.into_values() {
                route(event, &alias, &mut grouped);
            }
            grouped
        })
        .collect();
    by_user.extend(by_duplicate);
    measure("event deduplication");
    let click_parts: Vec<_> = chunks(&cs)
        .into_par_iter()
        .map(|chunk| {
            let mut grouped = buckets();
            rows::<4>(chunk, |[id, user, campaign, raw]| {
                if id.is_empty() || user.is_empty() || !channels.contains_key(campaign) {
                    return;
                }
                if let Some(ts) = timestamp(raw).filter(|t| *t >= lo - WEEK && *t < hi) {
                    let user = alias.get(user).copied().unwrap_or(user);
                    grouped[shard(user)].push(Click {
                        user,
                        ts,
                        id,
                        campaign,
                    });
                }
            });
            grouped
        })
        .collect();
    measure("click decoding");
    let udir = output.join("user_daily");
    fs::create_dir_all(&udir).unwrap();
    let cdir = output.join("campaign_daily");
    fs::create_dir_all(&cdir).unwrap();
    let sums: Vec<_> = transpose(by_user)
        .into_par_iter()
        .zip(transpose(click_parts))
        .enumerate()
        .map(|(index, (ev, cl))| {
            let mut groups: Map<Text, (Vec<Event>, Vec<Click>)> = Map::default();
            for event in ev.into_iter().flatten() {
                groups.entry(event.user).or_default().0.push(event);
            }
            for click in cl.into_iter().flatten() {
                if let Some(group) = groups.get_mut(click.user) {
                    group.1.push(click);
                }
            }
            let mut rows = Vec::new();
            let mut totals = Map::default();
            for (mut events, mut clicks) in groups.into_values() {
                events.sort_unstable_by_key(|e| e.ts);
                clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| b.id.cmp(a.id)));
                sessions(&events, &clicks, &mut rows, &mut totals, &labels);
            }
            if index == 0 || !rows.is_empty() {
                output::users(&udir.join(format!("{index}.parquet")), &rows);
            }
            totals
        })
        .collect();
    measure("user results");
    let mut merged: Campaigns = Map::default();
    for totals in sums {
        for (k, t) in totals {
            let v = merged.entry(k).or_default();
            v.sessions += t.sessions;
            v.purchasing += t.purchasing;
            v.revenue += t.revenue;
            v.assists += t.assists;
        }
    }
    output::campaigns(&cdir.join("daily.parquet"), &merged, &channels);
}
