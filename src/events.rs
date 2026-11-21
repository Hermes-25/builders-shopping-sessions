use crate::aliases::Aliases;
use crate::{DAY, Map, Names, Text, Visit, buckets, decimal, names, rows, timelines, transpose};
use rayon::prelude::*;

pub trait Data: Copy + Default + Send + Sync {
    type Ordered: timelines::OrderedVisit;
    fn pack(ts: i64, price: u32, category: u32, lo: i64) -> Self;
    fn ordered(self) -> Self::Ordered;
}
#[derive(Clone, Copy, Default)]
pub struct Short {
    offset: u32,
    purchase: u32,
}
impl Data for Short {
    type Ordered = u64;
    fn pack(ts: i64, price: u32, category: u32, lo: i64) -> Self {
        Self {
            offset: (ts - lo) as u32,
            purchase: (category << 24) | price,
        }
    }
    fn ordered(self) -> u64 {
        (u64::from(self.offset) << 32) | u64::from(self.purchase)
    }
}
impl Data for Visit {
    type Ordered = Visit;
    fn pack(ts: i64, price: u32, category: u32, _: i64) -> Self {
        Self {
            ts,
            price,
            category,
        }
    }
    fn ordered(self) -> Visit {
        self
    }
}
#[derive(Clone, Copy)]
pub struct Placed<V> {
    pub user: u32,
    pub data: V,
}
pub enum Parcel {
    Narrow(Vec<Vec<Placed<Short>>>),
    Wide(Vec<Vec<Placed<Visit>>>),
}
impl Parcel {
    pub fn reduce(
        self,
        part: usize,
        clicks: Vec<Vec<timelines::PlacedClick>>,
        lo: i64,
        hi: i64,
        aliases: &Aliases,
        names: &Names,
        output: &mut crate::Daily<'_>,
    ) {
        let short = hi - lo + crate::WEEK <= i64::from(u32::MAX);
        match (self, short) {
            (Self::Narrow(v), true) => timelines::reduce::<_, timelines::CompactAd>(
                part, v, clicks, lo, aliases, names, output,
            ),
            (Self::Narrow(v), false) => {
                timelines::reduce::<_, timelines::Ad>(part, v, clicks, lo, aliases, names, output)
            }
            (Self::Wide(v), true) => timelines::reduce::<_, timelines::CompactAd>(
                part, v, clicks, lo, aliases, names, output,
            ),
            (Self::Wide(v), false) => {
                timelines::reduce::<_, timelines::Ad>(part, v, clicks, lo, aliases, names, output)
            }
        }
    }
}

const PAGE: usize = 1 << 14;
#[derive(Clone, Copy)]
struct Winner<V> {
    id: u64,
    entry: u64,
    data: V,
}
const PERSON_MASK: u64 = (1 << 24) - 1;
const WIDE_BIT: u64 = 1 << 63;
// A four-digit UTC year fits in 39 bits of seconds from 0000-01-01.
const EPOCH_SHIFT: i64 = 62_167_219_200;
#[derive(Default)]
struct Stamps(std::sync::Mutex<Vec<(i64, u64)>>);
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packed_and_external_stamps_compare_exactly() {
        let stamps = Stamps::default();
        let invalid = stamps.encode(0, u64::MAX);
        let earlier = stamps.encode(-1000, 7);
        let wide_person = stamps.encode(1000, PERSON_MASK);
        let far_future = stamps.encode(i64::MAX, 28);
        assert_eq!(stamps.person(invalid), u64::MAX);
        assert_eq!(stamps.person(wide_person), PERSON_MASK);
        assert_eq!(stamps.person(far_future), 28);
        assert!(stamps.earlier(earlier, invalid));
        assert!(stamps.earlier(invalid, wide_person));
        assert!(stamps.earlier(wide_person, far_future));
        assert!(!stamps.earlier(far_future, invalid));
    }
}
impl Stamps {
    #[inline]
    fn encode(&self, millis: i64, person: u64) -> u64 {
        let seconds = millis / 1000 + EPOCH_SHIFT;
        if (person < PERSON_MASK || person == u64::MAX)
            && (0..1i64 << 39).contains(&seconds)
            && millis % 1000 == 0
        {
            return ((seconds as u64) << 24)
                | if person == u64::MAX {
                    PERSON_MASK
                } else {
                    person
                };
        }
        let mut wide = self.0.lock().unwrap();
        let id = wide.len();
        wide.push((millis, person));
        WIDE_BIT | id as u64
    }
    #[inline]
    fn earlier(&self, a: u64, b: u64) -> bool {
        if (a | b) & WIDE_BIT == 0 {
            return (a >> 24) < (b >> 24);
        }
        let wide = self.0.lock().unwrap();
        let millis = |v: u64| {
            if v & WIDE_BIT == 0 {
                ((v >> 24) as i64 - EPOCH_SHIFT) * 1000
            } else {
                wide[(v & !WIDE_BIT) as usize].0
            }
        };
        millis(a) < millis(b)
    }
    #[inline]
    fn person(&self, value: u64) -> u64 {
        if value & WIDE_BIT != 0 {
            return self.0.lock().unwrap()[(value & !WIDE_BIT) as usize].1;
        }
        let id = value & PERSON_MASK;
        if id == PERSON_MASK { u64::MAX } else { id }
    }
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
    fn accept(&mut self, hash: usize, win: Winner<V>, stamps: &Stamps) {
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
                    if stamps.earlier(win.entry, old.entry) {
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
    chunks: Vec<crate::input::FileWindow>,
    lo: i64,
    hi: i64,
    products: &Map<Text<'a>, u32>,
    unknown: u32,
    names: &Names,
    aliases: &Aliases,
    dates: &crate::dates::Dates,
) -> Vec<Vec<Vec<Placed<V>>>> {
    let estimate = chunks.iter().map(|v| v.bytes).sum::<usize>() / 90 / 128;
    let tables: Vec<_> = (0..128)
        .into_par_iter()
        .map(|_| std::sync::Mutex::new(Table::new(estimate)))
        .collect();
    let ids = Names::default();
    let stamps = Stamps::default();
    chunks.into_par_iter().enumerate().for_each_init(
        || (buckets(), Vec::with_capacity(48)),
        |(updates, pending), (batch, chunk)| {
            let mapped = chunk.read();
            let mut cached = (b"".as_slice(), u64::MAX);
            let mut flush = |batch: &mut Vec<(u64, usize, u64, i64, V)>| {
                for (key, start, id, ts, data) in batch.drain(..) {
                    let person = if key == u64::MAX {
                        u64::MAX
                    } else {
                        aliases.resolve(key, start)
                    };
                    let entry = stamps.encode(ts, person);
                    let hash = names::mixed(id);
                    updates[hash % 128].push((hash, Winner { id, entry, data }));
                }
            };
            rows::<7>(&mapped, |[id, user, raw, ingest, kind, product, price]| {
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
                let id = ids.encode(id);
                let (user, start) = if valid {
                    (cached.1, aliases.prefetch(cached.1))
                } else {
                    (u64::MAX, 0)
                };
                pending.push((
                    user,
                    start,
                    id,
                    entry,
                    V::pack(if valid { ts } else { lo }, price, cat, lo),
                ));
                if pending.len() == 48 {
                    flush(pending);
                }
            });
            flush(pending);
            // Each lock covers an entire batch of rows. Offset the starting
            // partition across producers so they do not queue in the same order.
            for k in 0..128 {
                let i = (batch * 37 + k) % 128;
                if updates[i].is_empty() {
                    continue;
                }
                let mut tab = tables[i].lock().unwrap();
                for (hash, w) in updates[i].drain(..) {
                    tab.accept(hash, w, &stamps);
                }
            }
        },
    );
    let grouped: Vec<_> = tables
        .into_par_iter()
        .map(|t| {
            let mut out = buckets();
            for page in t.into_inner().unwrap().pages {
                for win in page {
                    let person = stamps.person(win.entry);
                    if person != u64::MAX {
                        out[(person & 127) as usize].push(Placed {
                            user: (person >> 7) as u32,
                            data: win.data,
                        });
                    }
                }
            }
            out
        })
        .collect();
    transpose(grouped)
}
