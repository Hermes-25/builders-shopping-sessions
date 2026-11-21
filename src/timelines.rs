use crate::events::{Data, Placed};
use crate::{Daily, Names, aliases::Aliases};

#[derive(Clone, Copy, Default)]
pub struct Visit {
    pub ts: i64,
    pub price: u32,
    pub category: u32,
}
pub trait OrderedVisit: Copy + Default {
    fn visit(self, lo: i64) -> Visit;
    fn sort(rows: &mut [Self]);
}
impl OrderedVisit for u64 {
    #[inline]
    fn visit(self, lo: i64) -> Visit {
        Visit {
            ts: lo + (self >> 32) as i64,
            price: self as u32 & 0x00ff_ffff,
            category: (self as u32) >> 24,
        }
    }
    fn sort(rows: &mut [Self]) {
        rows.sort_unstable();
    }
}
impl OrderedVisit for Visit {
    #[inline]
    fn visit(self, _: i64) -> Visit {
        self
    }
    fn sort(rows: &mut [Self]) {
        rows.sort_unstable_by_key(|r| r.ts);
    }
}
#[derive(Clone, Copy, Default)]
pub struct Ad {
    pub ts: i64,
    pub id: u64,
    pub campaign: u32,
}
#[derive(Clone, Copy)]
pub struct PlacedClick {
    pub user: u32,
    pub ts: i64,
    pub id: u64,
    pub campaign: u32,
}
#[derive(Clone, Copy, Default)]
pub struct CompactAd {
    id: u64,
    offset: u32,
    campaign: u32,
}
pub trait AdData: Copy + Default {
    fn from_row(row: PlacedClick, origin: i64) -> Self;
    fn time(self, origin: i64) -> i64;
    fn campaign(self) -> u32;
    fn id(self) -> u64;
}
impl AdData for CompactAd {
    #[inline]
    fn from_row(row: PlacedClick, origin: i64) -> Self {
        Self {
            offset: (row.ts - origin) as u32,
            campaign: row.campaign,
            id: row.id,
        }
    }
    #[inline]
    fn time(self, origin: i64) -> i64 {
        origin + i64::from(self.offset)
    }
    #[inline]
    fn id(self) -> u64 {
        self.id
    }
    #[inline]
    fn campaign(self) -> u32 {
        self.campaign
    }
}
impl AdData for Ad {
    #[inline]
    fn from_row(row: PlacedClick, _: i64) -> Self {
        Self {
            ts: row.ts,
            campaign: row.campaign,
            id: row.id,
        }
    }
    #[inline]
    fn time(self, _: i64) -> i64 {
        self.ts
    }
    #[inline]
    fn id(self) -> u64 {
        self.id
    }
    #[inline]
    fn campaign(self) -> u32 {
        self.campaign
    }
}

pub fn reduce<V: Data, A: AdData>(
    part: usize,
    events: Vec<Vec<Placed<V>>>,
    clicks: Vec<Vec<PlacedClick>>,
    lo: i64,
    aliases: &Aliases,
    names: &Names,
    output: &mut Daily<'_>,
) {
    let mut spans = vec![(0u32, 0u32); aliases.width(part)];
    for row in events.iter().flatten() {
        spans[row.user as usize].0 += 1;
    }
    for row in clicks.iter().flatten() {
        spans[row.user as usize].1 += 1;
    }
    let (mut ec, mut cc) = (0u32, 0u32);
    for span in &mut spans {
        let (en, cn) = *span;
        *span = (ec, cc);
        ec = ec.checked_add(en).unwrap();
        cc = cc.checked_add(cn).unwrap();
    }
    let mut visits = vec![V::Ordered::default(); ec as usize];
    let origin = lo - crate::WEEK;
    let mut ads = vec![A::default(); cc as usize];
    for row in events.into_iter().flatten() {
        let cursor = &mut spans[row.user as usize].0;
        visits[*cursor as usize] = row.data.ordered();
        *cursor += 1;
    }
    for row in clicks.into_iter().flatten() {
        let cursor = &mut spans[row.user as usize].1;
        ads[*cursor as usize] = A::from_row(row, origin);
        *cursor += 1;
    }
    let (mut e, mut c) = (0, 0);
    for (index, (event_end, click_end)) in spans.into_iter().enumerate() {
        let (en, cn) = (event_end as usize, click_end as usize);
        if en != e {
            let ev = &mut visits[e..en];
            let cl = &mut ads[c..cn];
            V::Ordered::sort(ev);
            cl.sort_unstable_by(|a, b| {
                a.time(origin)
                    .cmp(&b.time(origin))
                    .then_with(|| names.cmp(b.id(), a.id()))
            });
            crate::sessions(aliases.key(part, index), ev, cl, lo, output);
        }
        (e, c) = (en, cn);
    }
}
