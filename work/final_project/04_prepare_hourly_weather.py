import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description="Prepare NOAA LCD hourly weather observations."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input NOAA LCD CSV path.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output Parquet directory.",
    )
    return parser.parse_args()


def numeric_value(column_name: str):
    """
    Extract the numeric portion of an LCD text field.

    This also tolerates blanks and possible NOAA quality markers.
    """
    text_value = F.trim(F.col(column_name))

    extracted = F.regexp_extract(
        text_value,
        r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)",
        0,
    )

    return F.when(
        (text_value == "") | (extracted == ""),
        F.lit(None).cast("double"),
    ).otherwise(extracted.cast("double"))


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("cs675-prepare-hourly-weather")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_weather = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(args.input)
    )

    raw_count = raw_weather.count()

    # FM-15 is the standard hourly observation.
    # FM-16 is retained as a fallback when an FM-15 observation is absent.
    hourly_candidates = (
        raw_weather
        .filter(
            F.trim(F.col("REPORT_TYPE")).isin("FM-15", "FM-16")
        )
        .withColumn(
            "observation_timestamp",
            F.to_timestamp(
                F.col("DATE"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .filter(F.col("observation_timestamp").isNotNull())
        .withColumn(
            "weather_hour",
            F.date_trunc("hour", F.col("observation_timestamp")),
        )
        .withColumn(
            "report_priority",
            F.when(
                F.trim(F.col("REPORT_TYPE")) == "FM-15",
                F.lit(1),
            ).otherwise(F.lit(2)),
        )
        .withColumn(
            "temperature_c",
            numeric_value("HourlyDryBulbTemperature"),
        )
        .withColumn(
            "precipitation_trace",
            F.upper(F.trim(F.col("HourlyPrecipitation"))) == "T",
        )
        .withColumn(
            "precipitation_mm",
            F.when(
                F.upper(F.trim(F.col("HourlyPrecipitation"))) == "T",
                F.lit(0.0),
            ).otherwise(numeric_value("HourlyPrecipitation")),
        )
        .withColumn(
            "relative_humidity_pct",
            numeric_value("HourlyRelativeHumidity"),
        )
        .withColumn(
            "wind_speed_mps",
            numeric_value("HourlyWindSpeed"),
        )
        .withColumn(
            "wind_direction_degrees",
            numeric_value("HourlyWindDirection"),
        )
    )

    candidate_count = hourly_candidates.count()

    # Select one record per clock hour.
    # Prefer FM-15; use FM-16 only when a standard hourly record is absent.
    hourly_window = (
        Window
        .partitionBy("weather_hour")
        .orderBy(
            F.col("report_priority").asc(),
            F.col("observation_timestamp").desc(),
        )
    )

    hourly_weather = (
        hourly_candidates
        .withColumn(
            "hourly_record_rank",
            F.row_number().over(hourly_window),
        )
        .filter(F.col("hourly_record_rank") == 1)
        .select(
            F.col("STATION").alias("station"),
            "weather_hour",
            "observation_timestamp",
            F.trim(F.col("REPORT_TYPE")).alias("report_type"),
            "temperature_c",
            "precipitation_mm",
            "precipitation_trace",
            "relative_humidity_pct",
            "wind_speed_mps",
            "wind_direction_degrees",
            F.col("HourlyPresentWeatherType").alias("present_weather"),
        )
        .withColumn("year", F.year("weather_hour"))
        .withColumn("month", F.month("weather_hour"))
        .orderBy("weather_hour")
        .cache()
    )

    hourly_count = hourly_weather.count()

    print("\n=== NOAA LCD hourly weather preparation ===")
    print(f"Raw LCD records: {raw_count:,}")
    print(f"FM-15/FM-16 candidate records: {candidate_count:,}")
    print(f"Unique hourly records: {hourly_count:,}")
    print(f"Expected hours in leap year 2024: 8,784")
    print(f"Difference from expected: {8_784 - hourly_count:,}")

    print("\n=== Missing values in prepared hourly data ===")

    hourly_weather.select(
        *[
            F.sum(
                F.when(F.col(column_name).isNull(), 1).otherwise(0)
            ).alias(column_name)
            for column_name in [
                "temperature_c",
                "precipitation_mm",
                "relative_humidity_pct",
                "wind_speed_mps",
                "wind_direction_degrees",
            ]
        ]
    ).show(truncate=False)

    print("\n=== Prepared hourly weather sample ===")

    hourly_weather.select(
        "weather_hour",
        "report_type",
        "temperature_c",
        "precipitation_mm",
        "precipitation_trace",
        "relative_humidity_pct",
        "wind_speed_mps",
    ).show(24, truncate=False)

    (
        hourly_weather
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    print(f"\nHourly weather Parquet written to: {args.output}")

    hourly_weather.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()