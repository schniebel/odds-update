import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession, functions as F, types as T

# Make app/ importable
APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))

from silver_job import normalize, NORMALIZED_SCHEMA  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-schema")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _schema_to_simple_list(schema: T.StructType):
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in schema.fields]


def test_normalize_emits_expected_columns_and_types(spark):
    # Provide intentionally messy input: mixed cases, missing optional cols, numeric timestamps
    rows = [
        {
            "event_uid": "e100",
            "event_ts": 1725796800,   # epoch seconds
            "ingest_ts": 1725796815,  # epoch seconds
            "league": "NBA",
            "game_id": "G100",
            "market": "PTS_OVER_20_5",
            "odds": "-115.0",         # string -> should cast to double
        }
    ]
    df_in = spark.createDataFrame(rows)

    df_out = normalize(df_in)

    # Columns present
    out_cols = set(df_out.columns)
    expected_cols = set([f.name for f in NORMALIZED_SCHEMA.fields])
    assert expected_cols.issubset(out_cols)

    # Types match expected
    assert _schema_to_simple_list(df_out.schema) == _schema_to_simple_list(NORMALIZED_SCHEMA)

    # Check a few content-level expectations
    row = df_out.collect()[0]
    # league lower-cased
    assert row.league == "nba"
    # odds cast to double
    assert isinstance(row.odds, float)
    # timestamps parsed
    assert str(type(row.event_ts)).endswith("timestamp.Timestamp'>") or str(type(row.event_ts)).endswith("datetime.datetime'>")


def test_normalize_fills_missing_optionals_with_nulls(spark):
    df_in = spark.createDataFrame(
        [
            {
                "event_uid": "e200",
                "event_ts": "2025-09-08T13:00:00Z",
                "ingest_ts": "2025-09-08T13:00:05Z",
                "league": "nFl",
                "game_id": "G200",
                "market": "reb_over_7_5",
                "odds": 101.0,
            }
        ]
    )
    df_out = normalize(df_in)
    row = df_out.collect()[0]

    # Optional fields should exist and be None when not provided
    assert row.team_id is None
    assert row.player_id is None
    assert row.model_version is None
    assert row.source_topic is None
    assert row.source_partition is None or isinstance(row.source_partition, int)
    assert row.source_offset is None or isinstance(row.source_offset, int)

    # league/market normalized to lower-case
    assert row.league == "nfl"
    assert row.market == "reb_over_7_5"
