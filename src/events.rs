use crate::aliases::{Aliases, Router};
use crate::{DAY, Map, Names, Text, Visit, buckets, decimal, names, rows, timelines, transpose};
use rayon::prelude::*;

pub trait Data: Copy + Send + Sync {
    fn pack(ts: i64, price: u32, category: u32, lo: i64) -> Self;
    fn visit(self, lo: i64) -> Visit;
}
#[derive(Clone, Copy)]
pub struct Short {
    offset: u32,
    purchase: u32,
}
impl Data for Short {
    fn pack(ts: i64, price: u32, category: u32, lo: i64) -> Self {
        Self {
            offset: (ts - lo) as u32,
            purchase: (category << 24) | price,
        }
    }
    fn visit(self, lo: i64) -> Visit {
        Visit {
            ts: lo + i64::from(self.offset),
            price: self.purchase & 0x00ffffff,
            category: self.purchase >> 24,
        }
    }
}
impl Data for Visit {
    fn pack(ts: i64, price: u32, category: u32, _: i64) -> Self {
        Self {
            ts,
            price,
            category,
        }
    }
    fn visit(self, _: i64) -> Visit {
        self
    }
}
#[derive(Clone, Copy)]
pub struct Event<V> {
    pub user: u64,
    pub data: V,
}
pub enum Parcel {
    Narrow(Vec<Vec<Event<Short>>>),
    Wide(Vec<Vec<Event<Visit>>>),
}
impl Parcel {
    pub fn timelines(self, clicks: Vec<Vec<crate::Click>>, lo: i64) -> timelines::Timelines {
        match self {
            Self::Narrow(v) => timelines::Timelines::build(v, clicks, lo),
            Self::Wide(v) => timelines::Timelines::build(v, clicks, lo),
        }
    }
}

const PAGE: usize = 1 << 14;
#[derive(Clone, Copy)]
struct Winner<V> {
    id: u64,
    ingest: i64,
    event: Event<V>,
}
struct Table<V> {
    tags: Vec<u8>,
    indices: Vec<u32>,
    pages: Vec<Vec<Winner<V>>>,
    len: usize,
}
impl<V: Data> Table<V> {
    fn new(estimated: usize) -> Self {
        let cap = (estimated.saturating_mul(4) / 3)
            .max(64)
            .next_power_of_two();
        Self {
            tags: vec![0; cap],
            indices: vec![0; cap],
            pages: Vec::new(),
            len: 0,
        }
    }
    #[inline]
    fn tag(hash: usize) -> u8 {
        ((hash >> 56) as u8) | 1
    }
    fn grow(&mut self) {
        let cap = self.tags.len() * 2;
        let mask = cap - 1;
        self.tags = vec![0; cap];
        self.indices = vec![0; cap];
        for (i, win) in self.pages.iter().flatten().enumerate() {
            let hash = names::mixed(win.id);
            let mut slot = (hash >> 7) & mask;
            while self.tags[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            self.tags[slot] = Self::tag(hash);
            self.indices[slot] = i as u32;
        }
    }
    #[inline]
    fn accept(&mut self, hash: usize, win: Winner<V>) {
        if self.len * 4 >= self.tags.len() * 3 {
            self.grow();
        }
        let mask = self.tags.len() - 1;
        let tag = Self::tag(hash);
        let mut slot = (hash >> 7) & mask;
        loop {
            let t = self.tags[slot];
            if t == 0 {
                if self.len % PAGE == 0 {
                    self.pages.push(Vec::with_capacity(PAGE));
                }
                self.tags[slot] = tag;
                self.indices[slot] = u32::try_from(self.len).unwrap();
                self.pages.last_mut().unwrap().push(win);
                self.len += 1;
                return;
            }
            if t == tag {
                let i = self.indices[slot] as usize;
                let old = &mut self.pages[i / PAGE][i % PAGE];
                if old.id == win.id {
                    if win.ingest < old.ingest {
                        *old = win;
                    }
                    return;
                }
            }
            slot = (slot + 1) & mask;
        }
    }
}
pub fn route<'a, V: Data>(
    chunks: Vec<&'a [u8]>,
    lo: i64,
    hi: i64,
    products: &Map<Text<'a>, u32>,
    unknown: u32,
    names: &Names<'a>,
    aliases: &Aliases,
    dates: &crate::dates::Dates,
) -> Vec<Vec<Vec<Event<V>>>> {
    let estimate = chunks.iter().map(|v| v.len()).sum::<usize>() / 90 / 128;
    let tables: Vec<_> = (0..128)
        .into_par_iter()
        .map(|_| std::sync::Mutex::new(Table::new(estimate)))
        .collect();
    let ids = Names::default();
    chunks
        .into_par_iter()
        .enumerate()
        .for_each(|(batch, chunk)| {
            let mut updates = buckets();
            let mut cached = (b"".as_slice(), u64::MAX);
            rows::<7>(chunk, |[id, user, raw, ingest, kind, product, price]| {
                if id.is_empty() {
                    return;
                }
                let entry = dates.ingest(ingest);
                let ts = dates.parse(raw).unwrap_or(i64::MIN);
                let valid = ts >= lo
                    && ts < hi
                    && entry - ts <= DAY
                    && !user.is_empty()
                    && !product.is_empty();
                let (price, cat) = if valid && kind == b"purchase" {
                    (
                        decimal(price, 10_000_000).unwrap_or(0) as u32,
                        *products.get(product).unwrap_or(&unknown),
                    )
                } else {
                    (0, 0)
                };
                let valid = valid && (kind == b"view" || kind == b"cart" || price > 0);
                if valid && cached.0 != user {
                    cached = (user, names.encode(user));
                }
                let event = Event {
                    user: if valid { cached.1 } else { u64::MAX },
                    data: V::pack(if valid { ts } else { lo }, price, cat, lo),
                };
                let id = ids.encode(id);
                let hash = names::mixed(id);
                updates[hash % 128].push((
                    hash,
                    Winner {
                        id,
                        ingest: entry,
                        event,
                    },
                ));
            });
            // Each lock covers an entire batch of rows. Offset the starting
            // partition across producers so they do not queue in the same order.
            for k in 0..128 {
                let i = (batch * 37 + k) % 128;
                if updates[i].is_empty() {
                    continue;
                }
                let mut tab = tables[i].lock().unwrap();
                for (hash, w) in updates[i].drain(..) {
                    tab.accept(hash, w);
                }
            }
        });
    let grouped: Vec<_> = tables
        .into_par_iter()
        .map(|t| {
            let mut out = Router::new(aliases);
            for page in t.into_inner().unwrap().pages {
                for win in page {
                    let e = win.event;
                    if e.user != u64::MAX {
                        out.push(e);
                    }
                }
            }
            out.finish()
        })
        .collect();
    transpose(grouped)
}
