from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("find-missing-weather-hours")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    weather = spark.read.parquet(
        "/home/jovyan/work/data/weather_hourly_processed"
    )

    expected = spark.sql(
        """
        SELECT explode(
            sequence(
                timestamp '2024-01-01 00:00:00',
                timestamp '2024-12-31 23:00:00',
                interval 1 hour
            )
        ) AS weather_hour
        """
    )

    missing = (
        expected
        .join(
            weather.select("weather_hour"),
            on="weather_hour",
            how="left_anti",
        )
        .orderBy("weather_hour")
    )

    observed_rows = weather.count()
    duplicate_hours = (
        weather
        .groupBy("weather_hour")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    missing_count = missing.count()

    print("\n=== Hourly weather coverage validation ===")
    print(f"Expected hours: 8,784")
    print(f"Observed rows: {observed_rows:,}")
    print(f"Duplicate hours: {duplicate_hours:,}")
    print(f"Missing hours: {missing_count:,}")

    print("\n=== Missing hourly timestamps ===")
    missing.show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()