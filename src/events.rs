use crate::aliases::Aliases;
use crate::{Names, Visit, timelines};

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
