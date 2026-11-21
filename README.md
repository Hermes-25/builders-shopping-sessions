# Shopping Sessions

A native Rust implementation of the Databricks Shopping Sessions track. Build with `docker build --platform linux/amd64 -t shopping-sessions .`. The binary reads `DATA_DIR` (default `/data`), writes `OUTPUT_DIR` (default `/output`), and uses `NCPU` worker threads (default eight).

## Pipeline

1. Split event and click files at complete CSV records near two-MiB boundaries. Workers map and decode one window at a time. Short ASCII identifiers have a reversible 63-bit representation. The dictionary owns the bytes of longer, multibyte and NUL-containing identifiers, so each input mapping can be released immediately.
2. Parse delimiters and quote boundaries with AVX2 and BMI2 when available. Longer records and other processors use the general parsers. A calendar index verifies the complete date prefix before using a cached reporting-window date.
3. Resolve connected identity components to their byte-wise smallest name. An insert-only sharded table assigns stable integer slots for all users encountered. A bounded probe sequence and exact overflow dictionary handle colliding keys. Workers batch independent identity lookups, then resolve duplicate event IDs by earliest ingestion timestamp, including an invalid earliest copy. Compact winner records handle the common range, with an exact full-width fallback.
4. Count events and clicks by integer person slot and scatter them into contiguous timelines. Where the duration and category bounds permit, each visit occupies eight bytes and each click occupies sixteen. Wider values use full-width fields. Sort each person's visits and clicks, scan session gaps, and maintain the inclusive seven-day attribution window and its distinct campaigns.
5. Accumulate campaign/day totals per worker. A sparse representation handles unusually large date or campaign ranges. Write independent user Parquet partitions with row groups of up to 65,536 rows. Numeric columns use little-endian values. Repeated user names and small category dictionaries use Parquet’s hybrid RLE/bit-packing encoding. User columns with few repeated names and large category dictionaries use plain strings. A standard Parquet writer emits the merged campaign table.

Dependencies are pinned in Cargo.lock and include Apache Parquet, Rayon, memchr, memmap2, rustc-hash, and serde_json. All contest-specific processing code was written for this entry. The official generator, fixtures and expected tables are used for local verification.
