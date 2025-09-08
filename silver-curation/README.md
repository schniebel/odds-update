# Silver Curation Job

This folder contains the Spark job that curates raw Bronze-layer data (Parquet files written by Kafka Connect S3 Sink) into a Silver layer of normalized, deduplicated, and query-efficient Parquet files.

## Purpose

### Bronze Layer
Raw odds updates streamed from Core Kafka (odds.updates.internal.*) into S3 by the Kafka Connect S3 Sink. Data is partitioned by league and date, stored as Parquet.

### Silver Layer (this job)
This PySpark job processes the Bronze data to produce the Silver layer, applying:

Deduplication: Remove duplicate odds updates using unique identifiers (game_id, event_id, model_version).

Schema normalization: Ensure consistent schema across leagues and updates.

File compaction: Combine many small Parquet files into larger, query-efficient partitions.

### BigQuery BigLake
The Silver layer in S3 is registered with BigQuery BigLake, enabling analysts and quants to query the curated data directly in place using SQL.
