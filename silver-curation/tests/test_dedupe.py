import sys
from pathlib import Path
import pytest
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

# Make app/ importable
APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))

from silver_job import normalize, dedupe_latest  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-dedupe")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_dedupe_keeps_latest_by_event_ts_then_ingest_ts_then_offset(spark):
    # Two duplicates for same event_uid with different timestamps/offsets
    rows = [
        {
            "event_uid": "e1",
            "event_ts": "2025-09-08T12:00:00Z",
            "ingest_ts": "2025-09-08T12:00:10Z",
            "league": "NBA",
            "game_id": "G1",
            "team_id": "T1",
            "player_id": "P1",
            "market": "PTS_OVER_20_5",
            "odds":  -115.0,
            "model_version": "v1",
            "source_topic": "odds.updates.internal.nba",
            "source_partition": 0,
            "source_offset": 100,
        },
        {
            # newer event_ts should win
            "event_uid": "e1",
            "event_ts": "2025-09-08T12:00:02Z",
            "ingest_ts": "2025-09-08T12:00:05Z",
            "league": "NBA",
            "game_id": "G1",
            "team_id": "T1",
            "player_id": "P1",
            "market": "PTS_OVER_20_5",
            "odds":  -120.0,
            "model_version": "v2",
            "source_topic": "odds.updates.internal.nba",
            "source_partition": 0,
            "source_offset": 101,
        },
        {
            "event_uid": "e2",
            "event_ts": "2025-09-08T12:01:00Z",
            "ingest_ts": "2025-09-08T12:01:01Z",
            "league": "NBA",
            "game_id": "G1",
            "team_id": "T2",
            "player_id": "P2",
            "market": "AST_OVER_5_5",
            "odds":  +105.0,
            "model_version": "v1",
            "source_topic": "odds.updates.internal.nba",
            "source_partition": 0,
            "source_offset": 200,
        },
    ]
    df = spark.createDataFrame(rows)
    df_n = normalize(df)
    out = dedupe_latest(df_n)

    # Expect 2 rows (e1 deduped, e2 untouched)
    assert out.count() == 2

    latest_e1 = out.filter(F.col("event_uid") == "e1").collect()[0]
    # The winner should be the second row (odds -120.0, model_version v2, offset 101)
    assert latest_e1.odds == -120.0
    assert latest_e1.model_version == "v2"
    assert latest_e1.source_offset == 101


def test_tiebreakers_ingest_ts_then_offset(spark):
    # Same event_ts; newer ingest_ts should win, then higher offset as final tiebreaker
    rows = [
        {
            "event_uid": "e3",
            "event_ts": "2025-09-08T12:05:00Z",
            "ingest_ts": "2025-09-08T12:05:01Z",
            "league": "NBA",
            "game_id": "G2",
            "team_id": "T3",
            "player_id": "P3",
            "market": "REB_OVER_7_5",
            "odds": -110.0,
            "model_version": "v1",
            "source_topic": "odds.updates.internal.nba",
            "source_partition": 1,
            "source_offset": 10,
        },
        {
            "event_uid": "e3",
            "event_ts": "2025-09-08T12:05:00Z",
            "ingest_ts": "2025-09-08T12:05:03Z",  # newer -> should win over above
            "league": "NBA",
            "game_id": "G2",
            "team_id": "T3",
            "player_id": "P3",
            "market": "REB_OVER_7_5",
            "odds": -112.0,
            "model_version": "v2",
            "source_topic": "odds.updates.internal.nba",
            "source_partition": 1,
            "source_offset": 11,
        },
        {
            "event_uid": "e3",
            "event_ts": "2025-09-08T12:05:00Z",
            "ingest_ts": "2025-09-08T12:05:03Z",  # tie on ingest_ts
            "league": "NBA",
            "game_id": "G2",
            "team_id": "T3",
            "player_id": "P3",
            "market": "REB_OVER_7_5",
            "odds": -113.0,
            "model_version": "v3",
            "source_topic": "odds.updates.internal.nba",
            "source_partition": 1,
            "source_offset": 12,  # higher offset = final tiebreaker
        },
    ]
    df = spark.createDataFrame(rows)
    out = dedupe_latest(normalize(df))
    winner = out.filter(F.col("event_uid") == "e3").collect()[0]
    assert winner.odds == -113.0
    assert winner.model_version == "v3"
    assert winner.source_offset == 12
