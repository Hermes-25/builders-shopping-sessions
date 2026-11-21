# Shopping Sessions

A native Rust implementation of the Databricks Shopping Sessions track. Build with `docker build --platform linux/amd64 -t shopping-sessions .`. The binary reads `DATA_DIR` (default `/data`), writes `OUTPUT_DIR` (default `/output`), and uses `NCPU` worker threads (default eight).

## Pipeline

1. Parse CSV records directly from read-only memory maps. The AVX2 parser finds commas, line endings and quotes in the same scan. A scalar parser handles machines without AVX2.
2. Store short ASCII identifiers in lexicographic order in a 63-bit integer. NUL-containing, long and multibyte identifiers use an exact sharded dictionary. A disjoint-set index resolves linked IDs to their byte-wise smallest name.
3. Decode each event once and partition it by exact event ID. Singleton fingerprints can bypass the duplicate map. Possible collisions and duplicates use exact key comparisons and earliest ingestion time, including invalid earliest records.
4. Route surviving events and valid clicks to the same partition for each canonical person. Counts and prefix offsets allocate contiguous event and click ranges. Sort each person's ranges by time and scan the sessions. A sliding campaign window maintains exact distinct assist counts and resolves the last-click tie by click ID.
5. Accumulate campaign/day totals per worker. A sparse representation handles unusually large date or campaign ranges. Write independently partitioned user Parquet files and one combined campaign Parquet file.

Dependencies are pinned in Cargo.lock and include Apache Parquet, Rayon, memchr, memmap2, rustc-hash, and serde_json. All contest-specific processing code was written for this entry. The official generator, fixtures and expected tables are used for local verification.
