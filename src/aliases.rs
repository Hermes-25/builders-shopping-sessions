use crate::{Click, Event, Map, buckets, names, partition};
#[derive(Clone, Copy)]
struct Slot {
    key: u64,
    person: u64,
}
pub struct Aliases {
    slots: Vec<Slot>,
    mask: usize,
}
impl Aliases {
    pub fn new(input: Map<u64, u64>) -> Self {
        let size = (input.len() * 2).max(64).next_power_of_two();
        let mask = size - 1;
        let mut slots = vec![
            Slot {
                key: u64::MAX,
                person: 0
            };
            size
        ];
        for (key, person) in input {
            let mut p = names::mixed(key) & mask;
            while slots[p].key != u64::MAX {
                p = (p + 1) & mask;
            }
            slots[p] = Slot { key, person };
        }
        Self { slots, mask }
    }
    #[inline]
    fn prefetch(&self, key: u64) -> usize {
        let p = names::mixed(key) & self.mask;
        #[cfg(target_arch = "x86_64")]
        // The address is always within the allocated slot array.
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                self.slots.as_ptr().add(p).cast(),
                std::arch::x86_64::_MM_HINT_T0,
            );
        }
        p
    }
    #[inline]
    fn resolve(&self, key: u64, mut p: usize) -> u64 {
        loop {
            let entry = self.slots[p];
            if entry.key == key {
                return entry.person;
            }
            if entry.key == u64::MAX {
                return key;
            }
            p = (p + 1) & self.mask;
        }
    }
}
pub trait Row: Copy {
    fn key(&self) -> u64;
    fn set(&mut self, k: u64);
}
impl Row for Event {
    fn key(&self) -> u64 {
        self.user
    }
    fn set(&mut self, k: u64) {
        self.user = k;
    }
}
impl Row for Click {
    fn key(&self) -> u64 {
        self.user
    }
    fn set(&mut self, k: u64) {
        self.user = k;
    }
}
pub struct Router<'a, T: Row> {
    aliases: &'a Aliases,
    pending: Vec<(usize, T)>,
    out: Vec<Vec<T>>,
}
impl<'a, T: Row> Router<'a, T> {
    pub fn new(aliases: &'a Aliases) -> Self {
        Self {
            aliases,
            pending: Vec::with_capacity(48),
            out: buckets(),
        }
    }
    #[inline]
    pub fn push(&mut self, row: T) {
        let p = self.aliases.prefetch(row.key());
        self.pending.push((p, row));
        if self.pending.len() == 48 {
            self.flush();
        }
    }
    fn flush(&mut self) {
        for (p, mut row) in self.pending.drain(..) {
            let k = self.aliases.resolve(row.key(), p);
            row.set(k);
            self.out[partition(k)].push(row);
        }
    }
    pub fn finish(mut self) -> Vec<Vec<T>> {
        self.flush();
        self.out
    }
}
