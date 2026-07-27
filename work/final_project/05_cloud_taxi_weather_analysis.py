import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Join January 2024 NYC Yellow Taxi trips with "
            "taxi zones and NOAA hourly weather."
        )
    )
    parser.add_argument("--taxi-input", required=True)
    parser.add_argument("--zone-input", required=True)
    parser.add_argument("--weather-input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def require_columns(dataframe, required_columns, dataset_name):
    missing = sorted(set(required_columns) - set(dataframe.columns))

    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{', '.join(missing)}"
        )


def write_parquet(dataframe, output_root, dataset_name):
    output_path = (
        f"{output_root.rstrip('/')}/{dataset_name}"
    )

    (
        dataframe.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Wrote {dataset_name} to: {output_path}")


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("cs675-cloud-taxi-weather-analysis")
        # Both project datasets use matching local clock values.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Reading project datasets ===")

    taxi_raw = spark.read.parquet(args.taxi_input)

    zones_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.zone_input)
    )

    weather_raw = spark.read.parquet(
        args.weather_input
    )

    require_columns(
        taxi_raw,
        [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "passenger_count",
            "trip_distance",
            "payment_type",
            "fare_amount",
            "tip_amount",
            "total_amount",
        ],
        "Taxi dataset",
    )

    require_columns(
        zones_raw,
        [
            "LocationID",
            "Borough",
            "Zone",
            "service_zone",
        ],
        "Taxi zone lookup",
    )

    require_columns(
        weather_raw,
        [
            "weather_hour",
            "temperature_c",
            "precipitation_mm",
            "precipitation_trace",
            "relative_humidity_pct",
            "wind_speed_mps",
            "present_weather",
            "hour_imputed",
        ],
        "Hourly weather dataset",
    )

    taxi_source_count = taxi_raw.count()
    weather_source_count = weather_raw.count()

    print(f"Taxi source rows: {taxi_source_count:,}")
    print(
        f"Hourly weather rows: "
        f"{weather_source_count:,}"
    )

    # ---------------------------------------------------------
    # Clean the January 2024 taxi data
    # ---------------------------------------------------------

    taxi = (
        taxi_raw
        .select(
            F.col("tpep_pickup_datetime")
            .cast("timestamp")
            .alias("pickup_timestamp"),

            F.col("tpep_dropoff_datetime")
            .cast("timestamp")
            .alias("dropoff_timestamp"),

            F.col("PULocationID")
            .cast("int")
            .alias("pickup_location_id"),

            F.col("passenger_count")
            .cast("double")
            .alias("passenger_count"),

            F.col("trip_distance")
            .cast("double")
            .alias("trip_distance_miles"),

            F.col("payment_type")
            .cast("int")
            .alias("payment_type"),

            F.col("fare_amount")
            .cast("double")
            .alias("fare_amount"),

            F.col("tip_amount")
            .cast("double")
            .alias("tip_amount"),

            F.col("total_amount")
            .cast("double")
            .alias("total_amount"),
        )
        .filter(
            (
                F.col("pickup_timestamp")
                >= F.lit(
                    "2024-01-01 00:00:00"
                ).cast("timestamp")
            )
            & (
                F.col("pickup_timestamp")
                < F.lit(
                    "2024-02-01 00:00:00"
                ).cast("timestamp")
            )
        )
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp(
                    "dropoff_timestamp"
                )
                - F.unix_timestamp(
                    "pickup_timestamp"
                )
            ) / F.lit(60.0),
        )
        .filter(
            F.col("pickup_timestamp").isNotNull()
            & F.col("dropoff_timestamp").isNotNull()
            & F.col(
                "pickup_location_id"
            ).between(1, 265)
            & F.col(
                "trip_duration_minutes"
            ).between(1.0, 180.0)
            & F.col(
                "trip_distance_miles"
            ).between(0.1, 100.0)
            & F.col(
                "fare_amount"
            ).between(0.0, 1000.0)
            & F.col(
                "tip_amount"
            ).between(0.0, 1000.0)
            & F.col(
                "total_amount"
            ).between(0.0, 2000.0)
        )
        .withColumn(
            "pickup_hour",
            F.date_trunc(
                "hour",
                "pickup_timestamp",
            ),
        )
        .withColumn(
            "pickup_date",
            F.to_date("pickup_timestamp"),
        )
        .withColumn(
            "pickup_hour_of_day",
            F.hour("pickup_timestamp"),
        )
        .withColumn(
            "_card_tip_percentage",
            F.when(
                (F.col("payment_type") == 1)
                & (F.col("fare_amount") > 0),
                (
                    F.col("tip_amount")
                    / F.col("fare_amount")
                ) * F.lit(100.0),
            ),
        )
        .withColumn(
            "card_tip_percentage",
            F.when(
                F.col(
                    "_card_tip_percentage"
                ).between(0.0, 200.0),
                F.col("_card_tip_percentage"),
            ),
        )
        .drop("_card_tip_percentage")
    )

    # ---------------------------------------------------------
    # Prepare taxi zone lookup
    # ---------------------------------------------------------

    zones = (
        zones_raw
        .select(
            F.col("LocationID")
            .cast("int")
            .alias("pickup_location_id"),

            F.trim(
                F.col("Borough")
            ).alias("pickup_borough"),

            F.trim(
                F.col("Zone")
            ).alias("pickup_zone"),

            F.trim(
                F.col("service_zone")
            ).alias("pickup_service_zone"),
        )
        .dropDuplicates(
            ["pickup_location_id"]
        )
    )

    # ---------------------------------------------------------
    # Prepare hourly weather categories
    # ---------------------------------------------------------

    weather_text = F.lower(
        F.coalesce(
            F.col("present_weather"),
            F.lit(""),
        )
    )

    weather = (
        weather_raw
        .select(
            "weather_hour",
            "temperature_c",
            "precipitation_mm",
            "precipitation_trace",
            "relative_humidity_pct",
            "wind_speed_mps",
            "present_weather",
            "hour_imputed",
        )
        .withColumn(
            "weather_condition",
            F.when(
                weather_text.rlike("snow|sn"),
                F.lit("Snow"),
            )
            .when(
                (
                    F.col("temperature_c")
                    <= 1.0
                )
                & (
                    F.col("precipitation_mm")
                    > 0
                ),
                F.lit("Snow"),
            )
            .when(
                F.col("precipitation_mm")
                >= 2.5,
                F.lit("Heavy Rain"),
            )
            .when(
                (
                    F.col("precipitation_mm")
                    > 0
                )
                | F.col(
                    "precipitation_trace"
                ),
                F.lit("Rain"),
            )
            .otherwise(
                F.lit("Dry")
            ),
        )
        .dropDuplicates(["weather_hour"])
    )

    # ---------------------------------------------------------
    # Broadcast joins
    # ---------------------------------------------------------

    joined = (
        taxi
        .join(
            F.broadcast(zones),
            on="pickup_location_id",
            how="left",
        )
        .join(
            F.broadcast(weather),
            taxi.pickup_hour
            == weather.weather_hour,
            how="left",
        )
        .drop(weather.weather_hour)
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

    quality_counts = (
        joined
        .agg(
            F.count("*").alias(
                "cleaned_count"
            ),
            F.sum(
                F.when(
                    F.col(
                        "pickup_zone"
                    ).isNotNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "zone_match_count"
            ),
            F.sum(
                F.when(
                    F.col(
                        "temperature_c"
                    ).isNotNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "weather_match_count"
            ),
        )
        .first()
    )

    cleaned_count = int(
        quality_counts[
            "cleaned_count"
        ]
    )

    zone_match_count = int(
        quality_counts[
            "zone_match_count"
        ]
    )

    weather_match_count = int(
        quality_counts[
            "weather_match_count"
        ]
    )

    zone_match_rate = (
        zone_match_count
        / cleaned_count
        * 100.0
        if cleaned_count
        else 0.0
    )

    weather_match_rate = (
        weather_match_count
        / cleaned_count
        * 100.0
        if cleaned_count
        else 0.0
    )

    print(
        "\n=== Cloud taxi-weather "
        "join validation ==="
    )

    print(
        f"Cleaned and joined rows: "
        f"{cleaned_count:,}"
    )

    print(
        f"Zone match rate: "
        f"{zone_match_rate:.2f}%"
    )

    print(
        f"Weather match rate: "
        f"{weather_match_rate:.2f}%"
    )

    print("\n=== Spark physical plan ===")

    joined.select(
        "pickup_location_id",
        "pickup_hour",
        "pickup_borough",
        "weather_condition",
    ).explain(mode="formatted")

    # ---------------------------------------------------------
    # Analytical results
    # ---------------------------------------------------------

    weather_summary = (
        joined
        .filter(
            F.col(
                "weather_condition"
            ).isNotNull()
        )
        .groupBy("weather_condition")
        .agg(
            F.count("*").alias(
                "trip_count"
            ),

            F.round(
                F.avg("fare_amount"),
                2,
            ).alias("avg_fare_amount"),

            F.round(
                F.avg("total_amount"),
                2,
            ).alias("avg_total_amount"),

            F.round(
                F.avg(
                    "trip_duration_minutes"
                ),
                2,
            ).alias(
                "avg_duration_minutes"
            ),

            F.round(
                F.avg(
                    "trip_distance_miles"
                ),
                2,
            ).alias(
                "avg_distance_miles"
            ),

            F.round(
                F.avg(
                    "card_tip_percentage"
                ),
                2,
            ).alias(
                "avg_card_tip_percentage"
            ),

            F.round(
                F.avg("temperature_c"),
                2,
            ).alias(
                "avg_temperature_c"
            ),

            F.round(
                F.avg(
                    "precipitation_mm"
                ),
                3,
            ).alias(
                "avg_precipitation_mm"
            ),
        )
        .orderBy(
            F.desc("trip_count")
        )
    )

    borough_weather_summary = (
        joined
        .filter(
            F.col(
                "pickup_borough"
            ).isNotNull()
            & F.col(
                "weather_condition"
            ).isNotNull()
        )
        .groupBy(
            "pickup_borough",
            "weather_condition",
        )
        .agg(
            F.count("*").alias(
                "trip_count"
            ),

            F.round(
                F.avg("fare_amount"),
                2,
            ).alias(
                "avg_fare_amount"
            ),

            F.round(
                F.avg(
                    "trip_duration_minutes"
                ),
                2,
            ).alias(
                "avg_duration_minutes"
            ),

            F.round(
                F.avg(
                    "card_tip_percentage"
                ),
                2,
            ).alias(
                "avg_card_tip_percentage"
            ),
        )
        .orderBy(
            "pickup_borough",
            F.desc("trip_count"),
        )
    )

    hourly_demand = (
        joined
        .filter(
            F.col(
                "weather_condition"
            ).isNotNull()
        )
        .groupBy(
            "pickup_date",
            "pickup_hour_of_day",
            "weather_condition",
        )
        .agg(
            F.count("*").alias(
                "trip_count"
            ),

            F.round(
                F.avg("fare_amount"),
                2,
            ).alias(
                "avg_fare_amount"
            ),

            F.round(
                F.avg(
                    "trip_duration_minutes"
                ),
                2,
            ).alias(
                "avg_duration_minutes"
            ),

            F.round(
                F.avg(
                    "card_tip_percentage"
                ),
                2,
            ).alias(
                "avg_card_tip_percentage"
            ),
        )
        .orderBy(
            "pickup_date",
            "pickup_hour_of_day",
        )
    )

    zone_weather_summary = (
        joined
        .filter(
            F.col(
                "pickup_zone"
            ).isNotNull()
            & F.col(
                "weather_condition"
            ).isNotNull()
        )
        .groupBy(
            "pickup_borough",
            "pickup_zone",
            "weather_condition",
        )
        .agg(
            F.count("*").alias(
                "trip_count"
            ),

            F.round(
                F.avg("fare_amount"),
                2,
            ).alias(
                "avg_fare_amount"
            ),

            F.round(
                F.avg(
                    "trip_duration_minutes"
                ),
                2,
            ).alias(
                "avg_duration_minutes"
            ),
        )
        .orderBy(
            F.desc("trip_count")
        )
    )

    # Write joined dataset
    # Write joined dataset
    # ---------------------------------------------------------

    joined_output = joined.select(
        "pickup_timestamp",
        "dropoff_timestamp",
        "pickup_hour",
        "pickup_date",
        "pickup_hour_of_day",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "passenger_count",
        "trip_distance_miles",
        "trip_duration_minutes",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "card_tip_percentage",
        "temperature_c",
        "precipitation_mm",
        "precipitation_trace",
        "relative_humidity_pct",
        "wind_speed_mps",
        "present_weather",
        "hour_imputed",
        "weather_condition",
    )

    joined_output_path = (
        f"{args.output.rstrip('/')}"
        "/joined_trips"
    )

    (
        joined_output.write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(joined_output_path)
    )

    print(
        f"Wrote joined trips to: "
        f"{joined_output_path}"
    )

    write_parquet(
        weather_summary,
        args.output,
        "weather_summary",
    )

    write_parquet(
        borough_weather_summary,
        args.output,
        "borough_weather_summary",
    )

    write_parquet(
        hourly_demand,
        args.output,
        "hourly_demand",
    )

    write_parquet(
        zone_weather_summary,
        args.output,
        "zone_weather_summary",
    )

    joined.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()