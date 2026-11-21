use crate::{Click, Map, buckets, names};

use crate::timelines::PlacedClick;
use rayon::prelude::*;
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
    overflow: Mutex<Overflow>,
    representatives: Map<u32, u64>,
}
pub struct Aliases {
    bins: Vec<Bin>,
    slots: Vec<Slot>,
    mask: usize,
    shift: u32,
}

impl Aliases {
    pub fn new(mut edges: Vec<Vec<(u64, u64)>>, names: &crate::Names, input_bytes: usize) -> Self {
        let link_rows: usize = edges.iter().map(Vec::len).sum();
        let estimated = (input_bytes / 750).max(link_rows.saturating_mul(2));
        let capacity = estimated
            .div_ceil(BINS)
            .clamp(32, 1 << 19)
            .next_power_of_two();
        let bins = (0..BINS)
            .into_par_iter()
            .map(|_| Bin {
                overflow: Mutex::default(),
                representatives: Map::default(),
            })
            .collect();
        let mut out = Self {
            bins,
            slots: (0..BINS * capacity)
                .into_par_iter()
                .map(|_| Slot {
                    key: AtomicU64::new(EMPTY),
                    representative: EMPTY,
                })
                .collect(),
            mask: capacity - 1,
            shift: capacity.trailing_zeros(),
        };
        if link_rows == 0 {
            return out;
        }
        edges.par_iter_mut().for_each(|part| {
            for (a, b) in part {
                *a = out.intern(*a, out.bucket(*a));
                *b = out.intern(*b, out.bucket(*b));
            }
        });
        let parents: Vec<Vec<AtomicU64>> = (0..BINS)
            .into_par_iter()
            .map(|part| {
                (0..out.width(part))
                    .map(|_| AtomicU64::new(EMPTY))
                    .collect()
            })
            .collect();
        let parent = |key: u64| &parents[(key & 127) as usize][(key >> 7) as usize];
        let root = |mut key: u64| {
            loop {
                let next = parent(key).load(Relaxed);
                if next == EMPTY {
                    return key;
                }
                let after = parent(next).load(Relaxed);
                if after == EMPTY {
                    return next;
                }
                let _ = parent(key).compare_exchange(next, after, Relaxed, Relaxed);
                key = after;
            }
        };
        let bytes_key = |v: u64| out.key((v & 127) as usize, (v >> 7) as usize);
        edges.into_par_iter().for_each(|part| {
            for (mut a, mut b) in part {
                loop {
                    a = root(a);
                    b = root(b);
                    if a == b {
                        break;
                    }
                    let (high, low) = if names.cmp(bytes_key(a), bytes_key(b)).is_lt() {
                        (b, a)
                    } else {
                        (a, b)
                    };
                    // Every edge points strictly toward a smaller byte string.
                    // A concurrent union can only shorten the path to the minimum.
                    if parent(high)
                        .compare_exchange(EMPTY, low, Relaxed, Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        });
        out.bins
            .par_iter_mut()
            .zip(out.slots.par_chunks_exact_mut(capacity))
            .enumerate()
            .for_each(|(part, (bin, slots))| {
                for (slot, edge) in parents[part].iter().enumerate() {
                    let p = edge.load(Relaxed);
                    if p == EMPTY {
                        continue;
                    }
                    let destination = root(p);
                    if slot < capacity {
                        slots[slot].representative = destination;
                    } else {
                        bin.representatives.insert(slot as u32, destination);
                    }
                }
            });
        out
    }
    #[inline]
    fn bucket(&self, key: u64) -> usize {
        let hash = names::mixed(key);
        ((hash >> 57) << self.shift) | (hash & self.mask)
    }
    #[inline]
    pub fn prefetch(&self, key: u64) -> usize {
        let start = self.bucket(key);
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                self.slots.as_ptr().add(start).cast(),
                std::arch::x86_64::_MM_HINT_T0,
            );
        }
        start
    }
    #[inline]
    fn intern(&self, key: u64, start: usize) -> u64 {
        self.locate(key, start).0
    }
    #[inline]
    fn locate(&self, key: u64, start: usize) -> (u64, usize) {
        let base = start & !self.mask;
        let part = start >> self.shift;
        let mut at = start & self.mask;
        for _ in 0..PROBES {
            let physical = base + at;
            let entry = &self.slots[physical];
            let mut found = entry.key.load(Relaxed);
            if found == EMPTY {
                found = match entry.key.compare_exchange(EMPTY, key, Relaxed, Relaxed) {
                    Ok(_) => key,
                    Err(other) => other,
                };
            }
            if found == key {
                return (((at as u64) << 7) | part as u64, physical);
            }
            at = (at + 1) & self.mask;
        }
        let mut extra = self.bins[part].overflow.lock().unwrap();
        let Overflow { keys, by_key } = &mut *extra;
        let local = *by_key.entry(key).or_insert_with(|| {
            let local = u32::try_from(self.mask + 1 + keys.len()).unwrap();
            keys.push(key);
            local
        });
        ((u64::from(local) << 7) | part as u64, usize::MAX)
    }
    #[inline]
    pub fn resolve(&self, key: u64, start: usize) -> u64 {
        let (id, physical) = self.locate(key, start);
        if physical == usize::MAX {
            let bin = &self.bins[(id & 127) as usize];
            let slot = (id >> 7) as usize;
            return bin
                .representatives
                .get(&(slot as u32))
                .copied()
                .unwrap_or(id);
        }
        let target = self.slots[physical].representative;
        if target == EMPTY { id } else { target }
    }
    pub fn width(&self, part: usize) -> usize {
        let bin = &self.bins[part];
        self.mask + 1 + bin.overflow.lock().unwrap().keys.len()
    }
    pub fn key(&self, part: usize, slot: usize) -> u64 {
        let bin = &self.bins[part];
        if slot <= self.mask {
            self.slots[(part << self.shift) + slot].key.load(Relaxed)
        } else {
            bin.overflow.lock().unwrap().keys[slot - (self.mask + 1)]
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
