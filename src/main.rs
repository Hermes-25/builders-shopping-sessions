mod aliases;
mod campaigns;
use aliases::{Aliases, Router};
mod input;
mod output;
use campaigns::CampaignGrid;
mod names;
mod timelines;
use input::{DAY, Text, WEEK, chunks, decimal, linked, load, read_pairs, rows, timestamp};
use names::{Names, partition};
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
struct Event {
    user: u64,
    ts: i64,
    price: u32,
    category: u32,
}
#[derive(Clone, Copy)]
struct Candidate {
    id: u64,
    ingest: i64,
    event: Event,
}
#[derive(Clone, Copy)]
struct Click {
    user: u64,
    ts: i64,
    id: u64,
    campaign: u32,
}

pub(crate) struct UserDay {
    user: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u32>,
}
#[derive(Clone, Copy, Default)]
pub(crate) struct Totals {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assists: i64,
}
impl Totals {
    fn add(&mut self, x: Self) {
        self.sessions += x.sessions;
        self.assists += x.assists;
        self.purchasing += x.purchasing;
        self.revenue += x.revenue;
    }
}
type Campaigns = Map<(u32, i32), Totals>;

struct Window {
    counts: Vec<u32>,
    positions: Vec<usize>,
    present: Vec<u32>,
}
impl Window {
    fn new(n: usize) -> Self {
        Self {
            counts: vec![0; n],
            positions: vec![0; n],
            present: Vec::new(),
        }
    }
    fn add(&mut self, id: u32) {
        let i = id as usize;
        if self.counts[i] == 0 {
            self.positions[i] = self.present.len();
            self.present.push(id);
        }
        self.counts[i] += 1;
    }
    fn remove(&mut self, id: u32) {
        let i = id as usize;
        self.counts[i] -= 1;
        if self.counts[i] == 0 {
            let pos = self.positions[i];
            self.present.swap_remove(pos);
            if pos < self.present.len() {
                self.positions[self.present[pos] as usize] = pos;
            }
        }
    }
    fn clear(&mut self) {
        for id in self.present.drain(..) {
            self.counts[id as usize] = 0;
        }
    }
}

fn sessions(
    user: u64,
    events: &[timelines::Visit],
    clicks: &[timelines::Ad],
    users: &mut Vec<UserDay>,
    campaigns: &mut CampaignGrid,
    categories: &mut [i64],
    active: &mut Window,
) {
    let (mut left, mut right) = (0, 0);
    active.clear();
    let mut i = 0;
    while i < events.len() {
        let ts = events[i].ts;
        let day = ts.div_euclid(DAY) as i32;
        if users.last().is_none_or(|r| r.user != user || r.day != day) {
            categories.fill(0);
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
            active.add(clicks[right].campaign);
            right += 1;
        }
        while left < right && clicks[left].ts < ts - WEEK {
            let campaign = clicks[left].campaign;
            active.remove(campaign);
            left += 1;
        }
        let credit = if left < right {
            clicks[right - 1].campaign
        } else {
            0
        };
        for &campaign in &active.present {
            campaigns.cell(campaign, day).assists += 1;
        }
        let mut last = ts;
        let mut revenue = 0;
        let start = i;
        while i < events.len() && events[i].ts - last <= 1_800_000 {
            let e = events[i];
            last = e.ts;
            if e.price > 0 {
                let category = e.category;
                let category_revenue = &mut categories[category as usize];
                *category_revenue += i64::from(e.price);
                let n = *category_revenue;
                if row.top.is_none_or(|t| {
                    n > categories[t as usize] || (n == categories[t as usize] && category < t)
                }) {
                    row.top = Some(category);
                }
                revenue += i64::from(e.price);
            }
            i += 1;
        }
        row.sessions += 1;
        row.events += (i - start) as i64;
        row.revenue += revenue;
        let c = campaigns.cell(credit, day);
        c.sessions += 1;
        c.purchasing += i64::from(revenue > 0);
        c.revenue += revenue;
    }
}

fn main() {
    let t = std::time::Instant::now();
    let measure = |msg| {
        if env::var_os("TIMINGS").is_some() {
            eprintln!("{msg} {:?}", t.elapsed());
        }
    };

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
    let mut campaigns = vec![b"organic".as_slice()];
    campaigns.extend(channels.keys().copied());
    let campaign_ids: Map<Text, u32> = campaigns
        .iter()
        .enumerate()
        .skip(1)
        .map(|(i, &c)| (c, i as u32))
        .collect();
    let es = load(&data.join("events"));
    let cs = load(&data.join("clicks"));
    let names = Names::default();
    let aliases = Aliases::new(linked(&l, &names));
    measure("catalogs and identities");
    let event_chunks = chunks(&es);
    let event_names = Names::default();
    let parts: Vec<_> = event_chunks
        .into_par_iter()
        .map(|chunk| {
            let mut out = buckets();
            let mut cached: (Text, u64) = (b"", u64::MAX);
            rows::<7>(
                chunk,
                |[id, user, raw_ts, raw_ingest, kind, product, price]| {
                    if id.is_empty() {
                        return;
                    }
                    let ingest = input::ingest_time(raw_ingest);
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
                    let id = event_names.encode(id);
                    if valid && cached.0 != user {
                        let k = names.encode(user);
                        cached = (user, k);
                    }
                    let event = Event {
                        user: if valid { cached.1 } else { u64::MAX },
                        ts,
                        price,
                        category,
                    };
                    out[names::mixed(id) % 128].push(Candidate { id, ingest, event });
                },
            );
            out
        })
        .collect();
    measure("event decoding");
    let by_user: Vec<_> = transpose(parts)
        .into_par_iter()
        .map(|parts| {
            // A singleton fingerprint proves uniqueness. All possible collisions
            // and actual duplicates go through the exact identifier map.
            let bins = parts
                .iter()
                .map(Vec::len)
                .sum::<usize>()
                .max(1)
                .next_power_of_two()
                * 8;
            let mut counts = vec![0u8; bins];
            for c in parts.iter().flatten() {
                let slot = &mut counts[(names::mixed(c.id) >> 7) & (bins - 1)];
                *slot = (*slot + 1).min(2);
            }
            let mut grouped = Router::new(&aliases);
            let mut winners: Map<u64, (i64, Event)> = Map::default();
            for candidate in parts.into_iter().flatten() {
                if counts[(names::mixed(candidate.id) >> 7) & (bins - 1)] == 1 {
                    if candidate.event.user != u64::MAX {
                        grouped.push(candidate.event);
                    }
                    continue;
                }
                let slot = winners
                    .entry(candidate.id)
                    .or_insert((candidate.ingest, candidate.event));
                if candidate.ingest < slot.0 {
                    *slot = (candidate.ingest, candidate.event);
                }
            }
            for (_, event) in winners.into_values() {
                if event.user != u64::MAX {
                    grouped.push(event);
                }
            }
            grouped.finish()
        })
        .collect();

    measure("event deduplication");
    let click_names = Names::default();
    let click_parts: Vec<_> = chunks(&cs)
        .into_par_iter()
        .map(|chunk| {
            let mut grouped = Router::new(&aliases);
            rows::<4>(chunk, |[id, user, campaign, raw]| {
                let Some(&campaign) = campaign_ids.get(campaign) else {
                    return;
                };
                if id.is_empty() || user.is_empty() {
                    return;
                }
                if let Some(ts) = timestamp(raw).filter(|t| *t >= lo - WEEK && *t < hi) {
                    let user = names.encode(user);
                    grouped.push(Click {
                        user,
                        ts,
                        id: click_names.encode(id),
                        campaign,
                    });
                }
            });
            grouped.finish()
        })
        .collect();
    measure("click decoding");
    let udir = output.join("user_daily");
    fs::create_dir_all(&udir).unwrap();
    let cdir = output.join("campaign_daily");
    fs::create_dir_all(&cdir).unwrap();
    let slots: Vec<_> = (0..threads)
        .map(|_| {
            std::sync::Mutex::new(CampaignGrid::new(
                lo.div_euclid(DAY) as i32,
                (hi - 1).div_euclid(DAY) as i32,
                campaigns.len(),
            ))
        })
        .collect();
    let parcels: Vec<_> = transpose(by_user)
        .into_iter()
        .zip(transpose(click_parts))
        .collect();
    let job_count = threads.saturating_mul(3).clamp(1, parcels.len());
    let mut jobs: Vec<Vec<_>> = (0..job_count).map(|_| Vec::new()).collect();
    for (i, p) in parcels.into_iter().enumerate() {
        jobs[i % job_count].push(p);
    }
    jobs.into_par_iter().enumerate().for_each(|(index, parts)| {
        let worker = rayon::current_thread_index().unwrap();
        let mut totals = slots[worker].lock().unwrap();
        let mut rows = Vec::new();
        let mut active = Window::new(campaigns.len());
        let mut category_sums = vec![0; labels.len()];
        for (ev, cl) in parts {
            let timelines = timelines::Timelines::build(ev, cl);
            timelines.for_each(&click_names, |user, events, clicks| {
                sessions(
                    user,
                    events,
                    clicks,
                    &mut rows,
                    &mut totals,
                    &mut category_sums,
                    &mut active,
                );
            });
        }
        if index == 0 || !rows.is_empty() {
            output::users(
                &udir.join(format!("{index}.parquet")),
                &rows,
                &names,
                &labels,
            );
        }
    });
    measure("users with parquet");
    let mut merged = Map::default();
    for slot in slots {
        slot.into_inner().unwrap().merge(&mut merged);
    }
    output::campaigns(&cdir.join("daily.parquet"), &merged, &channels, &campaigns);
}
