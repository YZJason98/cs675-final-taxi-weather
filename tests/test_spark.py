"""Smoke tests for the CS-675 PySpark dev environment.

Docker:  make test
Native:  uv run pytest tests/ -v
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


@pytest.fixture(scope="module")
def spark():
    """One SparkSession shared across the tests in this module."""
    session = (
        SparkSession.builder
        .appName("cs675-tests")
        .master("local[2]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_session_starts(spark):
    """A SparkSession can be created in local mode."""
    assert spark is not None
    assert spark.sparkContext.master.startswith("local")


def test_create_dataframe(spark):
    """We can build a DataFrame from in-memory Python data."""
    data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark.createDataFrame(data, ["id", "name"])
    assert df.count() == 3
    assert df.columns == ["id", "name"]


def test_filter_works(spark):
    """DataFrame.filter() returns the expected number of rows."""
    data = [(i, i * 2) for i in range(10)]
    df = spark.createDataFrame(data, ["x", "y"])
    assert df.filter(col("x") >= 5).count() == 5


def test_groupby_sum(spark):
    """DataFrame.groupBy().sum() aggregates correctly."""
    data = [("a", 1), ("a", 2), ("b", 3), ("b", 4)]
    df = spark.createDataFrame(data, ["key", "val"])
    result = {row["key"]: row["sum(val)"] for row in df.groupBy("key").sum().collect()}
    assert result == {"a": 3, "b": 7}
