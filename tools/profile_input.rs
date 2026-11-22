//! Build-time sample inputs for compiler branch profiling. The solver reads only
//! the real mounted data at runtime. No sample output is retained in the image.
use std::{
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
};

const DAY_SECONDS: i64 = 86_400;
const START: i64 = 1_767_225_600; // 2026-01-01 00:00:00 UTC

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, limit: u64) -> u64 {
        self.next() % limit
    }
}

fn identifier(prefix: u8, index: u64) -> String {
    let mask = (1u64 << 40) - 1;
    let mut value = if prefix == b'e' {
        let translated = (index.wrapping_mul(0x9e37_79b9_7f4b) ^ 0x53b2_8421) & mask;
        (translated ^ (translated >> 17)).wrapping_mul(0xd6e8_feb8_6659) & mask
    } else {
        index.wrapping_mul(1_103_515_245).wrapping_add(12_345) & mask
    };
    let mut text = vec![prefix];
    let mut digits = Vec::new();
    loop {
        digits.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(value % 36) as usize]);
        value /= 36;
        if value == 0 {
            break;
        }
    }
    text.extend(digits.into_iter().rev());
    String::from_utf8(text).unwrap()
}

fn clock(seconds: i64, zone: &str) -> String {
    let day = (seconds - START).div_euclid(DAY_SECONDS);
    let (year, month, date) = if day < 0 {
        (2025, 12, 32 + day)
    } else {
        (2026, 1, 1 + day)
    };
    let time = seconds.rem_euclid(DAY_SECONDS);
    format!(
        "{year}-{month:02}-{date:02}T{:02}:{:02}:{:02}{zone}",
        time / 3600,
        time % 3600 / 60,
        time % 60
    )
}

fn timestamp(seconds: i64, rng: &mut Random) -> String {
    match rng.below(11) {
        0..=2 => (seconds * 1000 + rng.below(1000) as i64).to_string(),
        3 => clock(seconds + 18_000, "+05:00"),
        4 => clock(seconds - 25_200, "-07:00"),
        _ => clock(seconds, "Z"),
    }
}

fn row(file: &mut BufWriter<File>, fields: &[&str], rng: &mut Random) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            file.write_all(b",").unwrap();
        }
        if rng.below(5) == 0 {
            write!(file, "\"{}\"", field).unwrap();
        } else {
            file.write_all(field.as_bytes()).unwrap();
        }
    }
    file.write_all(b"\n").unwrap();
}

fn writer(folder: &Path, name: &str, header: &str) -> BufWriter<File> {
    let mut out = BufWriter::new(File::create(folder.join(name)).unwrap());
    writeln!(out, "{header}").unwrap();
    out
}

fn main() {
    let out = env::args().nth(1).unwrap();
    let root = Path::new(&out);
    for folder in ["events", "clicks", "links"] {
        fs::create_dir_all(root.join(folder)).unwrap();
    }
    fs::write(
        root.join("manifest.json"),
        r#"{"window_start":"2026-01-01T00:00:00Z","window_end":"2026-01-15T00:00:00Z"}"#,
    )
    .unwrap();
    let mut products = writer(root, "products.csv", "product_id,category");
    for i in 0..2048 {
        writeln!(products, "p{i},category{}", i % 23).unwrap();
    }
    let mut campaigns = writer(root, "campaigns.csv", "campaign_id,channel");
    for i in 0..64 {
        writeln!(campaigns, "c{i},channel{}", i % 5).unwrap();
    }
    let mut rng = Random(0x89df_5014_a7e8_43b1);
    let mut links = writer(root, "links/part.csv", "user_id_a,user_id_b");
    for i in (2..6500).step_by(3) {
        row(
            &mut links,
            &[&identifier(b'u', i), &identifier(b'u', i - 1)],
            &mut rng,
        );
    }
    let mut events = writer(
        root,
        "events/part.csv",
        "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
    );
    let mut clicks = writer(root, "clicks/part.csv", "click_id,user_id,campaign_id,ts");
    for index in 0..240_000 {
        let user_index = if rng.below(8) == 0 {
            rng.below(16)
        } else {
            rng.below(6500)
        };
        let user = identifier(b'u', user_index);
        let seconds = START + rng.below((14 * DAY_SECONDS) as u64) as i64;
        let event_id = identifier(b'e', index);
        let raw = if index % 161 == 0 {
            "invalid".into()
        } else {
            timestamp(seconds, &mut rng)
        };
        let ingest = clock(seconds + rng.below(1800) as i64, "Z");
        let kind = match rng.below(30) {
            0 => "purchase",
            1..=5 => "cart",
            _ => "view",
        };
        let product = format!("p{}", rng.below(2090));
        let price = if kind == "purchase" {
            format!("{}", 1 + rng.below(80_000))
        } else {
            String::new()
        };
        row(
            &mut events,
            &[&event_id, &user, &raw, &ingest, kind, &product, &price],
            &mut rng,
        );
        if index % 23 == 0 {
            row(
                &mut events,
                &[
                    &event_id,
                    &user,
                    &raw,
                    &clock(seconds + 3600, "Z"),
                    kind,
                    &product,
                    &price,
                ],
                &mut rng,
            );
        }
        if index % 4 == 0 {
            let click_time = timestamp(
                seconds - rng.below((7 * DAY_SECONDS) as u64) as i64,
                &mut rng,
            );
            let campaign = format!("c{}", rng.below(65));
            row(
                &mut clicks,
                &[&identifier(b'k', index), &user, &campaign, &click_time],
                &mut rng,
            );
        }
    }
}
