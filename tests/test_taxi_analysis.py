"""Integration tests against the real NYC TLC yellow-taxi Parquet.

These tests skip cleanly if the dataset hasn't been downloaded yet.
Run `make download-nyc-cab-data` (or `.\\make.ps1 download-nyc-cab-data` on Windows) first to fetch it.

Docker:  make test
Native:  uv run pytest tests/ -v
"""
import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_PATHS = [
    "/home/jovyan/work/data/yellow_tripdata_2024-01.parquet",  # Docker path
    os.path.join(                                              # native path
        os.path.dirname(__file__), "..", "work", "data", "yellow_tripdata_2024-01.parquet"
    ),
]
DATA_PATH = next((p for p in DATA_PATHS if os.path.exists(p)), DATA_PATHS[0])
DATA_PRESENT = any(os.path.exists(p) for p in DATA_PATHS)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .appName("cs675-taxi-tests")
        .master("local[2]")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()


@pytest.mark.skipif(not DATA_PRESENT, reason="taxi dataset not downloaded; run `make download-nyc-cab-data` first")
def test_taxi_parquet_loads(spark):
    """The Parquet file loads and has the expected shape."""
    df = spark.read.parquet(DATA_PATH)
    n = df.count()
    assert n > 1_000_000, f"Expected >1M rows in a monthly slice, got {n}"
    assert "tpep_pickup_datetime" in df.columns
    assert "fare_amount" in df.columns
    assert "trip_distance" in df.columns


@pytest.mark.skipif(not DATA_PRESENT, reason="taxi dataset not downloaded; run `make download-nyc-cab-data` first")
def test_taxi_groupby_by_hour(spark):
    """Group-by hour aggregates to ≤24 distinct hours and preserves total row count."""
    df = spark.read.parquet(DATA_PATH)
    by_hour = (
        df.withColumn("hour", F.hour("tpep_pickup_datetime"))
        .groupBy("hour")
        .count()
        .collect()
    )
    assert 0 < len(by_hour) <= 24
    assert sum(r["count"] for r in by_hour) == df.count()


@pytest.mark.skipif(not DATA_PRESENT, reason="taxi dataset not downloaded; run `make download-nyc-cab-data` first")
def test_taxi_fare_aggregation(spark):
    """Average fare per passenger count returns sensible values."""
    df = spark.read.parquet(DATA_PATH)
    rows = (
        df.filter(F.col("passenger_count").isNotNull())
        .groupBy("passenger_count")
        .agg(F.avg("fare_amount").alias("avg_fare"))
        .collect()
    )
    assert len(rows) > 0
    for r in rows:
        # NYC taxi fares aren't infinite; sanity-check the range.
        assert -10 < r["avg_fare"] < 1000
