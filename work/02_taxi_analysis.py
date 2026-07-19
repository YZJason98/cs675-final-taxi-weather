"""Yellow-taxi headline analysis — three questions on the trip Parquet.

  Q1. Which hour of day has the most pickups?
  Q2. How does average fare vary with passenger count?
  Q3. What are the 10 longest trips?

Each question is one group/aggregate/sort pipeline. The bonus at the end runs
Q1 again as raw SQL to show that the DataFrame API and Spark SQL are two
front-ends to the same engine — same plan, same result.
"""
import time

from pyspark.sql import functions as F

from constants import TAXI_PARQUET
from spark_helper import get_spark, print_ui_urls, require_files, show_step


def main() -> None:
    require_files((TAXI_PARQUET, "make download-nyc-cab-data"))
    spark = get_spark("cs675-taxi-analysis")
    start = time.time()

    df = spark.read.parquet(TAXI_PARQUET)            # Parquet -> DataFrame (lazy until an action)
    print(f"Rows: {df.count():,}    Columns: {len(df.columns)}")

    # Q1. Busiest pickup hour: derive hour -> count per hour -> take the top 5.
    hourly = (
        df.withColumn("hour", F.hour("tpep_pickup_datetime"))   # hour-of-day 0-23
        .groupBy("hour").count()
    )
    busiest = hourly.orderBy(F.col("count").desc()).limit(5)

    # Q2. Average fare and trip count per passenger-count value.
    by_passengers = (
        df.groupBy("passenger_count")
        .agg(
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.count("*").alias("n_trips"),
        )
        .orderBy("passenger_count")
    )

    # Q3. Longest trips by distance.
    longest = (
        df.select("trip_distance", "fare_amount", "total_amount")
        .orderBy(F.col("trip_distance").desc()).limit(10)
    )

    # Bonus: Q1 as raw SQL — registers the DataFrame as a view, same engine.
    df.createOrReplaceTempView("trips")
    busiest_sql = spark.sql(
        """
        SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, COUNT(*) AS count
        FROM trips GROUP BY 1 ORDER BY 2 DESC LIMIT 5
        """
    )

    # --- Peek ---
    show_step("Q1. Busiest pickup hours", busiest)
    show_step("Q2. Avg fare by passenger count", by_passengers, n=12)
    show_step("Q3. Top 10 longest trips (top row is bad data — outliers in Lecture 3)", longest, n=10)
    show_step("Q1 again, via Spark SQL — same result", busiest_sql)

    # --- Verify ---
    assert df.count() > 0
    assert busiest.count() == 5
    assert busiest.first()["count"] > 0

    print(f"\nDone in {time.time() - start:.1f}s.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
