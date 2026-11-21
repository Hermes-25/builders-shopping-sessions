# Shopping Sessions

A native Rust implementation of the Databricks Shopping Sessions track. Build with `docker build --platform linux/amd64 -t shopping-sessions .`. The binary reads `DATA_DIR` (default `/data`), writes `OUTPUT_DIR` (default `/output`), and uses `NCPU` worker threads (default eight).

## Pipeline

1. Parse CSV records from read-only memory maps. On processors with AVX2 and BMI2, the parser extracts delimiter and quote masks and compares each field’s two quote boundaries. Longer records use an incremental parser. Scalar parsing is available on other processors. A calendar index precomputes dates around the reporting window, verifies the full date prefix on each hit, and uses the general timestamp parser on a miss.
2. Store short ASCII identifiers in lexicographic order in a 63-bit integer. NUL-containing, long and multibyte identifiers use an exact sharded dictionary. A disjoint-set index resolves linked IDs to their byte-wise smallest name.
3. Decode events in bounded batches. Each batch routes records to 128 exact event-ID tables. Compact tags and indices find candidates in paged storage. Keep the earliest ingestion time for each ID, including invalid earliest copies. When the window and category count fit, a 16-byte record carries the user, millisecond offset, price and category. Wider ranges use full-width records.
4. Batch independent lookup requests from surviving events and valid clicks, prefetch the alias table, then route each row to its canonical person's partition. Counts and prefix offsets allocate contiguous event and click ranges. Sort each person's ranges by time and scan the sessions. A sliding campaign window maintains exact distinct assist counts and resolves the last-click tie by click ID.
5. Accumulate campaign/day totals per worker. A sparse representation handles unusually large date or campaign ranges. Write independent user Parquet partitions with row groups of up to 65,536 rows. Numeric columns use little-endian values. Repeated user names and small category dictionaries use Parquet’s hybrid RLE/bit-packing encoding. User columns with few repeated names and large category dictionaries use plain strings. A standard Parquet writer emits the merged campaign table.

Dependencies are pinned in Cargo.lock and include Apache Parquet, Rayon, memchr, memmap2, rustc-hash, and serde_json. All contest-specific processing code was written for this entry. The official generator, fixtures and expected tables are used for local verification.
