from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


INPUT_PATH = "/home/jovyan/work/data/weather_hourly_processed"
OUTPUT_PATH = "/home/jovyan/work/data/weather_hourly_complete"

STATION_ID = "USW00094728"


def interpolate_column(df, column_name, previous_window, next_window):
    """Fill a numeric field using neighboring non-null hourly values."""

    previous_name = f"_previous_{column_name}"
    next_name = f"_next_{column_name}"
    flag_name = f"{column_name}_imputed"

    return (
        df
        .withColumn(flag_name, F.col(column_name).isNull())
        .withColumn(
            previous_name,
            F.last(F.col(column_name), ignorenulls=True)
            .over(previous_window),
        )
        .withColumn(
            next_name,
            F.first(F.col(column_name), ignorenulls=True)
            .over(next_window),
        )
        .withColumn(
            column_name,
            F.when(
                F.col(column_name).isNotNull(),
                F.col(column_name),
            )
            .when(
                F.col(previous_name).isNotNull()
                & F.col(next_name).isNotNull(),
                (
                    F.col(previous_name)
                    + F.col(next_name)
                ) / F.lit(2.0),
            )
            .otherwise(
                F.coalesce(
                    F.col(previous_name),
                    F.col(next_name),
                )
            ),
        )
        .drop(previous_name, next_name)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("complete-hourly-weather")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    observed = (
        spark.read
        .parquet(INPUT_PATH)
        .select(
            "station",
            "weather_hour",
            "observation_timestamp",
            "report_type",
            "temperature_c",
            "precipitation_mm",
            "precipitation_trace",
            "relative_humidity_pct",
            "wind_speed_mps",
            "wind_direction_degrees",
            "present_weather",
        )
    )

    # Generate all nominal local clock hours for 2024.
    expected_hours = spark.sql(
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

    # This local clock hour did not exist because of the spring DST change.
    valid_hours = expected_hours.filter(
        F.col("weather_hour")
        != F.to_timestamp(F.lit("2024-03-10 02:00:00"))
    )

    complete = (
        valid_hours
        .join(
            observed,
            on="weather_hour",
            how="left",
        )
        .withColumn(
            "hour_imputed",
            F.col("station").isNull(),
        )
        .withColumn(
            "station",
            F.coalesce(
                F.col("station"),
                F.lit(STATION_ID),
            ),
        )
        .withColumn(
            "report_type",
            F.coalesce(
                F.col("report_type"),
                F.lit("IMPUTED"),
            ),
        )
        .withColumn(
            "precipitation_trace",
            F.coalesce(
                F.col("precipitation_trace"),
                F.lit(False),
            ),
        )
    )

    previous_window = (
        Window
        .orderBy("weather_hour")
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    next_window = (
        Window
        .orderBy("weather_hour")
        .rowsBetween(1, Window.unboundedFollowing)
    )

    # Fill variables used in the taxi-weather analysis.
    for column_name in [
        "temperature_c",
        "precipitation_mm",
        "relative_humidity_pct",
        "wind_speed_mps",
    ]:
        complete = interpolate_column(
            complete,
            column_name,
            previous_window,
            next_window,
        )

    # Wind direction is not meaningful during calm conditions.
    # Use the nearest available direction only when needed.
    complete = (
        complete
        .withColumn(
            "_previous_wind_direction",
            F.last(
                F.col("wind_direction_degrees"),
                ignorenulls=True,
            ).over(previous_window),
        )
        .withColumn(
            "_next_wind_direction",
            F.first(
                F.col("wind_direction_degrees"),
                ignorenulls=True,
            ).over(next_window),
        )
        .withColumn(
            "wind_direction_imputed",
            F.col("wind_direction_degrees").isNull()
            & (F.col("wind_speed_mps") > 0),
        )
        .withColumn(
            "wind_direction_degrees",
            F.when(
                F.col("wind_speed_mps") == 0,
                F.lit(None).cast("double"),
            ).otherwise(
                F.coalesce(
                    F.col("wind_direction_degrees"),
                    F.col("_previous_wind_direction"),
                    F.col("_next_wind_direction"),
                )
            ),
        )
        .drop(
            "_previous_wind_direction",
            "_next_wind_direction",
        )
        .withColumn("year", F.year("weather_hour"))
        .withColumn("month", F.month("weather_hour"))
        .orderBy("weather_hour")
        .cache()
    )

    row_count = complete.count()

    duplicate_count = (
        complete
        .groupBy("weather_hour")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    imputed_hour_count = complete.filter(
        F.col("hour_imputed")
    ).count()

    print("\n=== Completed hourly weather validation ===")
    print("Valid unique local clock hours expected: 8,783")
    print(f"Final rows: {row_count:,}")
    print(f"Duplicate hours: {duplicate_count:,}")
    print(f"Fully imputed hours: {imputed_hour_count:,}")

    print("\n=== Remaining missing values ===")

    complete.select(
        *[
            F.sum(
                F.when(F.col(column_name).isNull(), 1)
                .otherwise(0)
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

    print("\n=== Imputed hours ===")

    complete.filter(
        F.col("hour_imputed")
    ).select(
        "weather_hour",
        "report_type",
        "temperature_c",
        "precipitation_mm",
        "relative_humidity_pct",
        "wind_speed_mps",
    ).show(100, truncate=False)

    (
        complete
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(OUTPUT_PATH)
    )

    print(f"\nCompleted hourly weather written to: {OUTPUT_PATH}")

    complete.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()