use crate::events::{Data, Event};
use crate::{Click, Map, Names};
#[derive(Clone, Copy, Default)]
pub struct Visit {
    pub ts: i64,
    pub price: u32,
    pub category: u32,
}
#[derive(Clone, Copy, Default)]
pub struct Ad {
    pub ts: i64,
    pub id: u64,
    pub campaign: u32,
}

struct Span {
    person: u64,
    events: usize,
    clicks: usize,
}
pub struct Timelines {
    people: Vec<Span>,
    visits: Vec<Visit>,
    ads: Vec<Ad>,
}
impl Timelines {
    pub fn build<V: Data>(
        mut events: Vec<Vec<Event<V>>>,
        mut clicks: Vec<Vec<Click>>,
        lo: i64,
    ) -> Self {
        let mut index = Map::<u64, usize>::default();
        let mut people = Vec::<Span>::new();
        let mut event_count = 0;
        let mut click_count = 0;
        for e in events.iter_mut().flatten() {
            let person = e.user;
            let id = *index.entry(person).or_insert_with(|| {
                let n = people.len();
                people.push(Span {
                    person,
                    events: 0,
                    clicks: 0,
                });
                n
            });
            people[id].events += 1;
            event_count += 1;
            e.user = id as u64;
        }
        for click in clicks.iter_mut().flatten() {
            if let Some(&id) = index.get(&click.user) {
                people[id].clicks += 1;
                click_count += 1;
                click.user = id as u64;
            } else {
                click.user = u64::MAX;
            }
        }
        drop(index);
        // Convert counts to independent write cursors. After scatter they are
        // exclusive ends, so contiguous slices recover complete timelines.
        let (mut ec, mut cc) = (0, 0);
        for span in &mut people {
            let (ne, nc) = (span.events, span.clicks);
            span.events = ec;
            span.clicks = cc;
            ec += ne;
            cc += nc;
        }
        let mut visits = vec![Visit::default(); event_count];
        let mut ads = vec![Ad::default(); click_count];
        for e in events.into_iter().flatten() {
            let span = &mut people[e.user as usize];
            visits[span.events] = e.data.visit(lo);
            span.events += 1;
        }
        for c in clicks.into_iter().flatten().filter(|c| c.user != u64::MAX) {
            let span = &mut people[c.user as usize];
            ads[span.clicks] = Ad {
                ts: c.ts,
                id: c.id,
                campaign: c.campaign,
            };
            span.clicks += 1;
        }
        Self {
            people,
            visits,
            ads,
        }
    }
    pub fn for_each(mut self, names: &Names, mut f: impl FnMut(u64, &[Visit], &[Ad])) {
        let (mut ep, mut cp) = (0, 0);
        for s in self.people {
            let events = &mut self.visits[ep..s.events];
            let clicks = &mut self.ads[cp..s.clicks];
            events.sort_unstable_by_key(|e| e.ts);
            clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| names.cmp(b.id, a.id)));
            f(s.person, events, clicks);
            ep = s.events;
            cp = s.clicks;
        }
    }
}
