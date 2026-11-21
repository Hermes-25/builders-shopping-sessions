# Shopping Sessions

A native Rust pipeline for the Databricks Shopping Sessions track. Build the Linux image with `docker build --platform linux/amd64 -t shopping-sessions .`. It reads `DATA_DIR` (default `/data`) and writes `OUTPUT_DIR` (default `/output`). Set `NCPU` to control worker threads.

Each input CSV is mapped read-only and divided on record boundaries. A disjoint-set index resolves aliases to the smallest ID. The parser validates full CSV records before projecting fields. Earliest-ingested event copies compete within independent event-ID partitions, retaining invalid earliest records as tombstones. A second partitioning step puts every person's events and clicks together. Time-ordered scans build sessions, apply the week-long attribution window, and maintain the set of assisting campaigns. Apache Parquet writes the output columns directly.

Dependencies are pinned in Cargo.lock. The pipeline uses Apache Parquet, Rayon, memchr, memmap2, rustc-hash, and serde_json. All contest-specific pipeline code was written for this entry. The official generator, fixtures and expected output are used only for local verification.
