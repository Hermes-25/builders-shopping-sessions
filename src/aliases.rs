use crate::{Click, Map, buckets, names};

use crate::timelines::PlacedClick;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering::Relaxed},
};

const BINS: usize = 128;
const EMPTY: u64 = u64::MAX;
const PROBES: usize = 24;
struct Slot {
    key: AtomicU64,
    representative: u64,
}
#[derive(Default)]
struct Overflow {
    keys: Vec<u64>,
    by_key: Map<u64, u32>,
}
struct Bin {
    slots: Vec<Slot>,
    overflow: Mutex<Overflow>,
    representatives: Map<u32, u64>,
}
pub struct Aliases {
    bins: Vec<Bin>,
    mask: usize,
}

impl Aliases {
    pub fn new(links: Map<u64, u64>, input_bytes: usize) -> Self {
        let estimated = (input_bytes / 750).max(links.len().saturating_mul(3));
        let capacity = estimated
            .div_ceil(BINS)
            .clamp(32, 1 << 19)
            .next_power_of_two();
        let bins = (0..BINS)
            .map(|_| Bin {
                slots: (0..capacity)
                    .map(|_| Slot {
                        key: AtomicU64::new(EMPTY),
                        representative: EMPTY,
                    })
                    .collect(),
                overflow: Mutex::default(),
                representatives: Map::default(),
            })
            .collect();
        let mut out = Self {
            bins,
            mask: capacity - 1,
        };
        // The link resolver supplies final component minima, including minima
        // which have no events. Install those redirects before parallel readers start.
        for (key, canonical) in links {
            let target = out.intern(canonical, out.bucket(canonical));
            let source = out.intern(key, out.bucket(key));
            let bin = &mut out.bins[(source & 127) as usize];
            let slot = (source >> 7) as usize;
            if slot < capacity {
                bin.slots[slot].representative = target;
            } else {
                bin.representatives.insert(slot as u32, target);
            }
        }
        out
    }
    #[inline]
    fn bucket(&self, key: u64) -> usize {
        let hash = names::mixed(key);
        ((hash & self.mask) << 7) | (hash >> 57)
    }
    #[inline]
    pub fn prefetch(&self, key: u64) -> usize {
        let start = self.bucket(key);
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                self.bins[start & 127].slots.as_ptr().add(start >> 7).cast(),
                std::arch::x86_64::_MM_HINT_T0,
            );
        }
        start
    }
    #[inline]
    fn intern(&self, key: u64, start: usize) -> u64 {
        let part = start & 127;
        let bin = &self.bins[part];
        let mut at = start >> 7;
        for _ in 0..PROBES {
            let entry = &bin.slots[at];
            let mut found = entry.key.load(Relaxed);
            if found == EMPTY {
                found = match entry.key.compare_exchange(EMPTY, key, Relaxed, Relaxed) {
                    Ok(_) => key,
                    Err(other) => other,
                };
            }
            if found == key {
                return ((at as u64) << 7) | part as u64;
            }
            at = (at + 1) & self.mask;
        }
        let mut extra = bin.overflow.lock().unwrap();
        let Overflow { keys, by_key } = &mut *extra;
        let local = *by_key.entry(key).or_insert_with(|| {
            let local = u32::try_from(bin.slots.len() + keys.len()).unwrap();
            keys.push(key);
            local
        });
        (u64::from(local) << 7) | part as u64
    }
    #[inline]
    pub fn resolve(&self, key: u64, start: usize) -> u64 {
        let id = self.intern(key, start);
        let bin = &self.bins[(id & 127) as usize];
        let slot = (id >> 7) as usize;
        if slot > self.mask {
            return bin
                .representatives
                .get(&(slot as u32))
                .copied()
                .unwrap_or(id);
        }
        let target = bin.slots[slot].representative;
        if target == EMPTY { id } else { target }
    }
    pub fn width(&self, part: usize) -> usize {
        let bin = &self.bins[part];
        bin.slots.len() + bin.overflow.lock().unwrap().keys.len()
    }
    pub fn key(&self, part: usize, slot: usize) -> u64 {
        let bin = &self.bins[part];
        if slot <= self.mask {
            bin.slots[slot].key.load(Relaxed)
        } else {
            bin.overflow.lock().unwrap().keys[slot - bin.slots.len()]
        }
    }
}
pub struct Router<'a> {
    aliases: &'a Aliases,
    pending: Vec<(usize, Click)>,
    out: Vec<Vec<PlacedClick>>,
}
impl<'a> Router<'a> {
    pub fn new(aliases: &'a Aliases) -> Self {
        Self {
            aliases,
            pending: Vec::with_capacity(48),
            out: buckets(),
        }
    }
    #[inline]
    pub fn push(&mut self, row: Click) {
        self.pending.push((self.aliases.prefetch(row.user), row));
        if self.pending.len() == 48 {
            self.flush();
        }
    }
    fn flush(&mut self) {
        for (start, row) in self.pending.drain(..) {
            let person = self.aliases.resolve(row.user, start);
            self.out[(person & 127) as usize].push(PlacedClick {
                user: (person >> 7) as u32,
                ts: row.ts,
                id: row.id,
                campaign: row.campaign,
            });
        }
    }
    pub fn finish(mut self) -> Vec<Vec<PlacedClick>> {
        self.flush();
        self.out
    }
}
