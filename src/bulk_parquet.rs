//! Emit the fixed user-day schema as uncompressed Parquet v1 pages. Apache
//! Parquet's generated Thrift types serialize each page header and the footer.
use crate::{Names, Text, UserDay};
use parquet::{
    format::{
        ColumnChunk, ColumnMetaData, CompressionCodec, DataPageHeader, DictionaryPageHeader,
        Encoding, FileMetaData, PageHeader, PageType, RowGroup, SchemaElement, Type,
    },
    schema::{parser::parse_message_type, types::to_thrift},
    thrift::{TCompactOutputProtocol, TSerializable},
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::OnceLock,
};

const ROWS_PER_GROUP: usize = 65_536;
const SCHEMA: &str = "message users { REQUIRED BINARY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (UTF8); }";

fn schema() -> &'static Vec<SchemaElement> {
    static SCHEMA_CACHE: OnceLock<Vec<SchemaElement>> = OnceLock::new();
    SCHEMA_CACHE.get_or_init(|| to_thrift(&parse_message_type(SCHEMA).unwrap()).unwrap())
}
fn write_string(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&u32::try_from(s.len()).unwrap().to_le_bytes());
    out.extend_from_slice(s);
}
fn varint(out: &mut Vec<u8>, mut value: usize) {
    while value >= 128 {
        out.push(value as u8 | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
fn width(cardinality: usize) -> u8 {
    (usize::BITS - (cardinality.saturating_sub(1)).leading_zeros()).max(1) as u8
}
struct Page {
    dictionary: Option<(Vec<u8>, usize)>,
    body: Vec<u8>,
    encoding: Encoding,
    optional: bool,
}
impl Page {
    fn plain(body: Vec<u8>) -> Self {
        Self {
            dictionary: None,
            body,
            encoding: Encoding::PLAIN,
            optional: false,
        }
    }
}
struct FileWriter {
    out: BufWriter<File>,
    pos: i64,
    groups: Vec<RowGroup>,
}
impl FileWriter {
    fn new(path: &Path) -> Self {
        let mut out = BufWriter::with_capacity(128 * 1024, File::create(path).unwrap());
        out.write_all(b"PAR1").unwrap();
        Self {
            out,
            pos: 4,
            groups: Vec::new(),
        }
    }
    fn append(&mut self, bytes: &[u8]) {
        self.out.write_all(bytes).unwrap();
        self.pos += bytes.len() as i64;
    }
    fn page(&mut self, head: PageHeader, bytes: &[u8]) {
        let mut header = Vec::with_capacity(64);
        head.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut header))
            .unwrap();
        self.append(&header);
        self.append(bytes);
    }
    fn column(&mut self, name: &str, physical: Type, rows: usize, page: Page) -> ColumnChunk {
        let start = self.pos;
        let mut encodings = vec![Encoding::PLAIN];
        let dict_offset = page.dictionary.map(|(bytes, count)| {
            let size = i32::try_from(bytes.len()).unwrap();
            let head = DictionaryPageHeader::new(count as i32, Encoding::PLAIN, false);
            self.page(
                PageHeader::new(
                    PageType::DICTIONARY_PAGE,
                    size,
                    size,
                    None,
                    None,
                    None,
                    head,
                    None,
                ),
                &bytes,
            );
            encodings.push(Encoding::RLE_DICTIONARY);
            start
        });
        if page.optional || dict_offset.is_some() {
            encodings.push(Encoding::RLE);
        }
        let data_offset = self.pos;
        let size = i32::try_from(page.body.len()).unwrap();
        let head = DataPageHeader::new(
            rows as i32,
            page.encoding,
            Encoding::RLE,
            Encoding::RLE,
            None,
        );
        self.page(
            PageHeader::new(
                PageType::DATA_PAGE,
                size,
                size,
                None,
                head,
                None,
                None,
                None,
            ),
            &page.body,
        );
        let bytes = self.pos - start;
        let meta = ColumnMetaData::new(
            physical,
            encodings,
            vec![name.into()],
            CompressionCodec::UNCOMPRESSED,
            rows as i64,
            bytes,
            bytes,
            None,
            data_offset,
            None,
            dict_offset,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        ColumnChunk::new(None, 0, meta, None, None, None, None, None, None)
    }
    fn row_group(&mut self, rows: &[UserDay], names: &Names, labels: &[Text]) {
        let start = self.pos;
        let n = rows.len();
        let mut cols = Vec::with_capacity(6);
        cols.push(self.column("user_id", Type::BYTE_ARRAY, n, user_names(rows, names)));
        let mut day = Vec::with_capacity(n * 4);
        for row in rows {
            day.extend_from_slice(&row.day.to_le_bytes());
        }
        cols.push(self.column("day", Type::INT32, n, Page::plain(day)));
        macro_rules! integer {
            ($name:literal,$field:ident) => {{
                let mut bytes = Vec::with_capacity(n * 8);
                for row in rows {
                    bytes.extend_from_slice(&row.$field.to_le_bytes());
                }
                cols.push(self.column($name, Type::INT64, n, Page::plain(bytes)));
            }};
        }
        integer!("sessions", sessions);
        integer!("events", events);
        integer!("revenue_cents", revenue);
        cols.push(self.column(
            "top_category",
            Type::BYTE_ARRAY,
            n,
            top_category(rows, labels),
        ));
        let bytes = self.pos - start;
        self.groups.push(RowGroup::new(
            cols, bytes, n as i64, None, start, bytes, None,
        ));
    }
    fn finish(mut self, rows: usize) {
        let metadata = FileMetaData::new(
            1,
            schema().clone(),
            rows as i64,
            self.groups,
            None,
            String::from("shopping-sessions"),
            None,
            None,
            None,
        );
        let mut footer = Vec::with_capacity(1024);
        metadata
            .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut footer))
            .unwrap();
        self.out.write_all(&footer).unwrap();
        self.out
            .write_all(&u32::try_from(footer.len()).unwrap().to_le_bytes())
            .unwrap();
        self.out.write_all(b"PAR1").unwrap();
        self.out.flush().unwrap();
    }
}
fn user_names(rows: &[UserDay], names: &Names) -> Page {
    // The session reducer emits every date for a given person contiguously.
    // Each run therefore needs only one dictionary entry and one index.
    let run_count = 1 + rows.windows(2).filter(|r| r[0].user != r[1].user).count();
    if run_count * 4 > rows.len() * 3 {
        let mut body = Vec::with_capacity(rows.len() * 14);
        for row in rows {
            names.append_plain(row.user, &mut body);
        }
        return Page::plain(body);
    }
    let bits = width(run_count);
    let index_bytes = usize::from(bits).div_ceil(8);
    let mut dictionary = Vec::with_capacity(run_count * 14);
    let mut data = Vec::with_capacity(1 + run_count * (1 + index_bytes));
    data.push(bits);
    for (i, run) in rows.chunk_by(|a, b| a.user == b.user).enumerate() {
        names.append_plain(run[0].user, &mut dictionary);
        varint(&mut data, run.len() << 1);
        data.extend_from_slice(&(i as u32).to_le_bytes()[..index_bytes]);
    }
    Page {
        dictionary: Some((dictionary, run_count)),
        body: data,
        encoding: Encoding::RLE_DICTIONARY,
        optional: false,
    }
}
fn top_category(rows: &[UserDay], labels: &[Text]) -> Page {
    let groups = rows.len().div_ceil(8);
    let mut body = Vec::with_capacity(16 + groups * 3);
    body.resize(4, 0);
    varint(&mut body, (groups << 1) | 1);
    let mut valid = 0;
    for block in rows.chunks(8) {
        let mut bits = 0u8;
        for (i, row) in block.iter().enumerate() {
            bits |= u8::from(row.top.is_some()) << i;
        }
        valid += bits.count_ones() as usize;
        body.push(bits);
    }
    let prefix = (body.len() - 4) as u32;
    body[..4].copy_from_slice(&prefix.to_le_bytes());
    if valid == 0 || labels.len() > 256 {
        for row in rows {
            if let Some(id) = row.top {
                write_string(&mut body, labels[id as usize]);
            }
        }
        return Page {
            dictionary: None,
            body,
            encoding: Encoding::PLAIN,
            optional: true,
        };
    }
    let bits = width(labels.len());
    let mut dictionary = Vec::new();
    for label in labels {
        write_string(&mut dictionary, label);
    }
    body.push(bits);
    varint(&mut body, (valid.div_ceil(8) << 1) | 1);
    let mut pack = 0u64;
    let mut used = 0;
    for id in rows.iter().filter_map(|r| r.top) {
        pack |= u64::from(id) << (used * u32::from(bits));
        used += 1;
        if used == 8 {
            body.extend_from_slice(&pack.to_le_bytes()[..usize::from(bits)]);
            pack = 0;
            used = 0;
        }
    }
    if used != 0 {
        body.extend_from_slice(&pack.to_le_bytes()[..usize::from(bits)]);
    }
    Page {
        dictionary: Some((dictionary, labels.len())),
        body,
        encoding: Encoding::RLE_DICTIONARY,
        optional: true,
    }
}
pub fn write(path: &Path, rows: &[UserDay], names: &Names, labels: &[Text]) {
    let mut writer = FileWriter::new(path);
    for group in rows.chunks(ROWS_PER_GROUP) {
        writer.row_group(group, names, labels);
    }
    writer.finish(rows.len());
}
