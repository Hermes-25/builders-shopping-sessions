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
    category: Text<'a>,
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
                let category_revenue = categories.entry(e.category).or_default();
                *category_revenue += i64::from(e.price);
                let n = *category_revenue;
                if row
                    .top
                    .is_none_or(|t| n > categories[t] || (n == categories[t] && e.category < t))
                {
                    row.top = Some(e.category);
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
    let products = read_pairs(&p);
    let c = load(&data.join("campaigns.csv"));
    let channels = read_pairs(&c);
    let es = load(&data.join("events"));
    let cs = load(&data.join("clicks"));
    let parts: Vec<_> = chunks(&es)
        .into_par_iter()
        .map(|chunk| {
            let mut out = buckets();
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
                            *products.get(product).unwrap_or(&b"unknown".as_slice()),
                        )
                    } else {
                        (0, b"".as_slice())
                    };
                    let valid = valid && (kind == b"view" || kind == b"cart" || price > 0);
                    out[shard(id)].push(Candidate {
                        id,
                        ingest,
                        event: Event {
                            user: if valid { user } else { b"" },
                            ts,
                            price,
                            category,
                        },
                    });
                },
            );
            out
        })
        .collect();
    let by_user: Vec<_> = transpose(parts)
        .into_par_iter()
        .map(|parts| {
            let mut winners: Map<Text, (i64, Event)> = Map::default();
            winners.reserve(parts.iter().map(Vec::len).sum());
            for candidate in parts.into_iter().flatten() {
                let slot = winners
                    .entry(candidate.id)
                    .or_insert((candidate.ingest, candidate.event));
                if candidate.ingest < slot.0 {
                    *slot = (candidate.ingest, candidate.event);
                }
            }
            let mut grouped = buckets();
            for (_, mut event) in winners.into_values() {
                if !event.user.is_empty() {
                    event.user = alias.get(event.user).copied().unwrap_or(event.user);
                    grouped[shard(event.user)].push(event);
                }
            }
            grouped
        })
        .collect();
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
    let udir = output.join("user_daily");
    fs::create_dir_all(&udir).unwrap();
    let cdir = output.join("campaign_daily");
    fs::create_dir_all(&cdir).unwrap();
    let sums: Vec<_> = transpose(by_user)
        .into_par_iter()
        .zip(transpose(click_parts))
        .enumerate()
        .map(|(index, (ev, cl))| {
            let mut ev: Vec<_> = ev.into_iter().flatten().collect();
            let mut cl: Vec<_> = cl.into_iter().flatten().collect();
            ev.sort_unstable_by(|a, b| a.user.cmp(b.user).then_with(|| a.ts.cmp(&b.ts)));
            cl.sort_unstable_by(|a, b| {
                a.user
                    .cmp(b.user)
                    .then_with(|| a.ts.cmp(&b.ts))
                    .then_with(|| b.id.cmp(a.id))
            });
            // Identical click IDs have identical contents. Repeats cannot change either
            // the maximum (time, reverse ID) or the set of assisting campaigns.
            let mut rows = Vec::new();
            let mut totals = Map::default();
            let mut cursor = 0;
            for group in ev.chunk_by(|a, b| a.user == b.user) {
                let user = group[0].user;
                while cursor < cl.len() && cl[cursor].user < user {
                    cursor += 1;
                }
                let start = cursor;
                while cursor < cl.len() && cl[cursor].user == user {
                    cursor += 1;
                }
                sessions(group, &cl[start..cursor], &mut rows, &mut totals);
            }
            if index == 0 || !rows.is_empty() {
                output::users(&udir.join(format!("{index}.parquet")), &rows);
            }
            totals
        })
        .collect();
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
