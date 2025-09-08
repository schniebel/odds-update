from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Tuple

import yaml
from pyspark.sql import SparkSession


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_spark(app_name: str, overrides: Dict[str, Any]) -> SparkSession:
    """
    Build a SparkSession with sane defaults for S3A + dynamic partition overwrite.
    Values from `overrides` (spark_defaults.yaml) win.
    """
    builder = (SparkSession.builder
               .appName(app_name)
               .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
               .config("spark.sql.parquet.compression.codec", "zstd")
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
               # S3A creds via IRSA (WebIdentity)
               .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                       "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
               .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
               .config("spark.hadoop.fs.s3a.fast.upload", "true")
               .config("spark.hadoop.fs.s3a.path.style.access", "true"))

    for k, v in overrides.items():
        builder = builder.config(k, v)

    return builder.getOrCreate()


def parse_since_arg(since: str) -> Tuple[datetime, datetime]:
    """
    Translate a friendly window spec to [start, end) UTC datetimes.
    Examples: "hour-1", "day-1", "2025-09-07T12:00:00Z"
    """
    now = datetime.now(timezone.utc)
    if since.startswith("hour-"):
        hours = int(since.split("-")[1])
        start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours)
        end = now.replace(minute=0, second=0, microsecond=0)
        return start, end
    if since.startswith("day-"):
        days = int(since.split("-")[1])
        start = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, end
    # ISO8601 start → end = now hour-rounded
    try:
        start = datetime.fromisoformat(since.replace("Z", "+00:00"))
    except Exception:
        raise ValueError(f"Unsupported --since value: {since}")
    end = now
    return start, end


def bronze_path_for_window(bronze_root: str,
                           start: datetime,
                           end: datetime,
                           partitioning: List[str]) -> List[str]:
    """
    Given a Bronze root like s3://bucket/bronze/odds_updates and partitions
    like ["league","dt","hour"], return a list of partition prefixes to read.

    We conservatively read all leagues for dt/hour slices overlapping [start,end).
    """
    # Expect dt=YYYY-MM-DD and hour=HH in partitioning
    paths = []
    cursor = start
    while cursor < end:
        dt = cursor.strftime("%Y-%m-%d")
        hh = cursor.strftime("%H")
        # Pull all leagues — Spark will prune by partition if league exists; or downstream filters apply.
        if "hour" in partitioning:
            paths.append(f"{bronze_root}/dt={dt}/hour={hh}")
        else:
            paths.append(f"{bronze_root}/dt={dt}")
        # step by hour if "hour" partition exists, otherwise by day
        cursor += timedelta(hours=1 if "hour" in partitioning else 24)
    return list(sorted(set(paths)))


def coalesce_cols(*cols):
    """Helper for future: pick first non-null column."""
    from pyspark.sql import functions as F
    return F.coalesce(*cols)
