# Shopping Sessions

A native Rust pipeline for the Databricks Shopping Sessions track. Build the Linux image with `docker build --platform linux/amd64 -t shopping-sessions .`. It reads `DATA_DIR` (default `/data`) and writes `OUTPUT_DIR` (default `/output`). Set `NCPU` to control worker threads.

Each input CSV is mapped read-only and divided on record boundaries. A disjoint-set index resolves aliases to the smallest ID. The parser validates full CSV records before projecting fields. An initial scan marks possible repeated event-ID fingerprints. A fingerprint observed once proves that its event can be routed directly to its person. For all possible repeats, partitions compare full IDs and retain the earliest ingested copy, including invalid earliest records. Each person's events and clicks are then grouped together and sorted by time. Time-ordered scans build sessions, apply the week-long attribution window, and maintain the set of assisting campaigns. Apache Parquet writes the output columns directly.

Dependencies are pinned in Cargo.lock. The pipeline uses Apache Parquet, Rayon, memchr, memmap2, rustc-hash, and serde_json. All contest-specific pipeline code was written for this entry. The official generator, fixtures and expected output are used only for local verification.
