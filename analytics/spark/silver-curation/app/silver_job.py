#!/usr/bin/env python3
import argparse
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

from common import (
    load_yaml,
    build_spark,
    bronze_path_for_window,
    parse_since_arg,
    coalesce_cols,
)

REQUIRED_COLUMNS = [
    "event_uid",        # unique id for dedupe
    "event_ts",         # event timestamp in UTC (seconds precision ok)
    "ingest_ts",        # when we ingested it (producer side)
    "league",
    "game_id",
    "team_id",
    "player_id",
    "market",
    "odds",             # numeric (decimal/double)
    "model_version",
    "source_topic",
    "source_partition",
    "source_offset",
]

NORMALIZED_SCHEMA = T.StructType([
    T.StructField("event_uid", T.StringType(), False),
    T.StructField("event_ts", T.TimestampType(), False),
    T.StructField("ingest_ts", T.TimestampType(), False),
    T.StructField("league", T.StringType(), False),
    T.StructField("game_id", T.StringType(), False),
    T.StructField("team_id", T.StringType(), True),
    T.StructField("player_id", T.StringType(), True),
    T.StructField("market", T.StringType(), False),
    T.StructField("odds", T.DoubleType(), False),
    T.StructField("model_version", T.StringType(), True),
    T.StructField("source_topic", T.StringType(), True),
    T.StructField("source_partition", T.IntegerType(), True),
    T.StructField("source_offset", T.LongType(), True),
])


def normalize(df: DataFrame) -> DataFrame:
    """
    - Ensure required columns exist (add nulls if missing)
    - Cast to normalized schema / types
    - Standardize league/market casing (lowercase)
    - Trim IDs
    """
    # Add missing columns
    for c in REQUIRED_COLUMNS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))

    # Cast types safely
    df = df.select(
        F.col("event_uid").cast("string").alias("event_uid"),
        # event_ts and ingest_ts may arrive as long epoch or string
        F.when(F.col("event_ts").cast("timestamp").isNotNull(),
               F.col("event_ts").cast("timestamp"))
         .when(F.col("event_ts").cast("double").isNotNull(),
               F.to_timestamp(F.col("event_ts")/F.lit(1), "yyyy-MM-dd HH:mm:ss"))
         .when(F.col("event_ts").cast("long").isNotNull(),
               F.to_timestamp(F.col("event_ts")))
         .otherwise(F.to_timestamp(F.col("event_ts"))).alias("event_ts"),

        F.when(F.col("ingest_ts").cast("timestamp").isNotNull(),
               F.col("ingest_ts").cast("timestamp"))
         .when(F.col("ingest_ts").cast("double").isNotNull(),
               F.to_timestamp(F.col("ingest_ts")/F.lit(1)))
         .when(F.col("ingest_ts").cast("long").isNotNull(),
               F.to_timestamp(F.col("ingest_ts")))
         .otherwise(F.to_timestamp(F.col("ingest_ts"))).alias("ingest_ts"),

        F.lower(F.col("league").cast("string")).alias("league"),
        F.col("game_id").cast("string").alias("game_id"),
        F.col("team_id").cast("string").alias("team_id"),
        F.col("player_id").cast("string").alias("player_id"),
        F.lower(F.col("market").cast("string")).alias("market"),
        F.col("odds").cast("double").alias("odds"),
        F.col("model_version").cast("string").alias("model_version"),
        F.col("source_topic").cast("string").alias("source_topic"),
        F.col("source_partition").cast("int").alias("source_partition"),
        F.col("source_offset").cast("long").alias("source_offset"),
    )

    df = df.filter(F.col("event_uid").isNotNull() & F.col("event_ts").isNotNull() & F.col("odds").isNotNull())
    return df


def dedupe_latest(df: DataFrame) -> DataFrame:
    """
    Keep the latest record per event_uid.
    Order by:
      1) event_ts (desc)
      2) ingest_ts (desc) as tiebreaker
      3) source_offset (desc) final tiebreaker
    """
    w = F.window.partitionBy("event_uid").orderBy(
        F.col("event_ts").desc_nulls_last(),
        F.col("ingest_ts").desc_nulls_last(),
        F.col("source_offset").desc_nulls_last(),
    )
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def compact_write(df: DataFrame, out_path: str, repartition_by: list[str], target_files: Optional[int]) -> None:
    if repartition_by:
        df = df.repartition(*[F.col(c) for c in repartition_by])
    if target_files and target_files > 0:
        df = df.coalesce(target_files)

    (df.write
       .mode("overwrite")  # with dynamic partition overwrite set in spark conf
       .format("parquet")
       .option("compression", "zstd")
       .partitionBy(*repartition_by)
       .save(out_path))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Silver curation: Bronze Parquet → Silver Parquet")
    parser.add_argument("--bronze", required=False, help="Bronze root (s3://bucket/bronze/odds_updates)")
    parser.add_argument("--silver", required=False, help="Silver root (s3://bucket/silver/odds_updates)")
    parser.add_argument("--since", required=False, default="hour-1",
                        help="Window start (e.g., 'hour-1', 'day-1', or ISO8601 start)")
    parser.add_argument("--config", required=False, default="/opt/app/conf/spark_defaults.yaml",
                        help="YAML config with defaults/overrides")
    args = parser.parse_args(argv)

    conf = load_yaml(args.config)

    bronze_root = args.bronze or conf["paths"]["bronze_root"]
    silver_root = args.silver or conf["paths"]["silver_root"]
    since_from, since_to = parse_since_arg(args.since)  # [start, end)

    spark: SparkSession = build_spark("silver-curation", conf.get("spark", {}))

    # Build the list of Bronze paths to read for the window (by date/hour partitions)
    bronze_paths = bronze_path_for_window(
        bronze_root=bronze_root,
        start=since_from,
        end=since_to,
        partitioning=conf["bronze"]["partitioning"],  # e.g., ["league","dt","hour"]
    )

    if not bronze_paths:
        print("No bronze partitions found for window; exiting 0.")
        return 0

    df = spark.read.parquet(*bronze_paths)

    # Normalize and dedupe
    df_norm = normalize(df)
    df_dedup = dedupe_latest(df_norm)

    # Optional: clamp to window to be safe
    df_windowed = df_dedup.filter(
        (F.col("event_ts") >= F.lit(since_from)) & (F.col("event_ts") < F.lit(since_to))
    )

    # Write to Silver (partitioned)
    repartition_cols = conf["silver"]["partitioning"]            # e.g., ["league","dt"]
    # Add dt (yyyy-MM-dd) if not present
    if "dt" in repartition_cols and "dt" not in df_windowed.columns:
        df_windowed = df_windowed.withColumn("dt", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))

    target_files = conf["silver"].get("coalesce_files", 0)
    compact_write(df_windowed, silver_root, repartition_cols, target_files)

    print(f"Wrote Silver to: {silver_root}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
