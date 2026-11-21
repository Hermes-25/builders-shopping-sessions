use crate::{Campaigns, Map, Text};
use parquet::{
    basic::Encoding,
    data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type},
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::parser::parse_message_type,
};
use std::{fs::File, io::BufWriter, path::Path, sync::Arc};

type Group<'a> = SerializedRowGroupWriter<'a, BufWriter<File>>;
fn write_table(path: &Path, schema: &str, fill: impl FnOnce(&mut Group)) {
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .set_offset_index_disabled(true)
        .set_encoding(Encoding::PLAIN)
        .build();
    let mut writer = SerializedFileWriter::new(
        BufWriter::new(File::create(path).unwrap()),
        schema,
        Arc::new(props),
    )
    .unwrap();
    let mut group = writer.next_row_group().unwrap();
    fill(&mut group);
    group.close().unwrap();
    writer.close().unwrap();
}
fn strings<'a>(group: &mut Group, items: impl Iterator<Item = &'a [u8]>) {
    let mut col = group.next_column().unwrap().unwrap();
    let values: Vec<_> = items.map(ByteArray::from).collect();
    col.typed::<ByteArrayType>()
        .write_batch(&values, None, None)
        .unwrap();
    col.close().unwrap();
}
fn numbers(group: &mut Group, items: impl Iterator<Item = i64>) {
    let mut col = group.next_column().unwrap().unwrap();
    let values: Vec<_> = items.collect();
    col.typed::<Int64Type>()
        .write_batch(&values, None, None)
        .unwrap();
    col.close().unwrap();
}
fn days(group: &mut Group, items: impl Iterator<Item = i32>) {
    let mut col = group.next_column().unwrap().unwrap();
    col.typed::<Int32Type>()
        .write_batch(&items.collect::<Vec<_>>(), None, None)
        .unwrap();
    col.close().unwrap();
}
pub fn campaigns(path: &Path, totals: &Campaigns, channels: &Map<Text, Text>, campaigns: &[Text]) {
    let rows: Vec<_> = totals.iter().collect();
    write_table(
        path,
        "message campaigns { REQUIRED BINARY campaign_id (UTF8); REQUIRED BINARY channel (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        |g| {
            strings(g, rows.iter().map(|((c, _), _)| campaigns[*c as usize]));
            strings(
                g,
                rows.iter().map(|((c, _), _)| {
                    channels
                        .get(campaigns[*c as usize])
                        .copied()
                        .unwrap_or(b"organic")
                }),
            );
            days(g, rows.iter().map(|((_, d), _)| *d));
            numbers(g, rows.iter().map(|(_, t)| t.sessions));
            numbers(g, rows.iter().map(|(_, t)| t.purchasing));
            numbers(g, rows.iter().map(|(_, t)| t.revenue));
            numbers(g, rows.iter().map(|(_, t)| t.assists));
        },
    );
}
