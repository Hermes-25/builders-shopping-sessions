use crate::{Campaigns, Map, Totals};

pub struct CampaignGrid {
    first: i32,
    width: usize,
    dense: Vec<Totals>,
    sparse: Campaigns,
}
impl CampaignGrid {
    pub fn new(first: i32, last: i32, width: usize) -> Self {
        let days = (i64::from(last) - i64::from(first) + 1).max(0) as usize;
        let cells = days.saturating_mul(width);
        // If the manifest spans a large date range the sparse path stays bounded
        // by the number of actual campaign/day pairs.
        let dense = if cells <= 262_144 {
            vec![Totals::default(); cells]
        } else {
            Vec::new()
        };
        Self {
            first,
            width,
            dense,
            sparse: Map::default(),
        }
    }
    #[inline]
    pub fn cell(&mut self, campaign: u32, day: i32) -> &mut Totals {
        if self.dense.is_empty() {
            self.sparse.entry((campaign, day)).or_default()
        } else {
            let index = (day - self.first) as usize * self.width + campaign as usize;
            &mut self.dense[index]
        }
    }
    pub fn merge(self, out: &mut Campaigns) {
        for ((c, d), t) in self.sparse {
            out.entry((c, d)).or_default().add(t);
        }
        for (i, t) in self.dense.into_iter().enumerate() {
            if t.sessions > 0 || t.assists > 0 {
                out.entry((
                    (i % self.width) as u32,
                    self.first + (i / self.width) as i32,
                ))
                .or_default()
                .add(t);
            }
        }
    }
}
