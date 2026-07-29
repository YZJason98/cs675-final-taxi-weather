import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


VALID_BOROUGHS = [
    "Bronx",
    "Brooklyn",
    "Manhattan",
    "Queens",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Build a full-year hourly borough-level taxi demand "
            "machine-learning dataset."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input path containing the full-year joined_trips Parquet dataset.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output root path for the ML dataset and supporting summaries.",
    )
    parser.add_argument(
        "--start-date",
        default="2024-01-01",
        help="Inclusive analysis start date.",
    )
    parser.add_argument(
        "--end-date",
        default="2025-01-01",
        help="Exclusive analysis end date.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("BuildFullYearHourlyBoroughDemandMLDataset")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    print("Reading joined trip data from:", args.input)

    joined = spark.read.parquet(args.input)

    filtered = (
        joined
        .filter(
            (F.col("pickup_date") >= F.to_date(F.lit(args.start_date)))
            & (F.col("pickup_date") < F.to_date(F.lit(args.end_date)))
            & F.col("pickup_borough").isin(VALID_BOROUGHS)
        )
        .select(
            "pickup_hour",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour_of_day",
            "pickup_borough",
            "temperature_c",
            "precipitation_mm",
            "precipitation_trace",
            "relative_humidity_pct",
            "wind_speed_mps",
            "present_weather",
            "weather_condition",
        )
    )

    source_trip_count = filtered.count()
    print("Filtered source trip count:", source_trip_count)

    # One weather record per actual NYC local hour.
    hourly_weather = (
        filtered
        .groupBy("pickup_hour")
        .agg(
            F.first("pickup_date", ignorenulls=True).alias("pickup_date"),
            F.first("pickup_year", ignorenulls=True).alias("pickup_year"),
            F.first("pickup_month", ignorenulls=True).alias("pickup_month"),
            F.first(
                "pickup_day_of_week",
                ignorenulls=True,
            ).alias("pickup_day_of_week"),
            F.first(
                "pickup_hour_of_day",
                ignorenulls=True,
            ).alias("pickup_hour_of_day"),
            F.first(
                "temperature_c",
                ignorenulls=True,
            ).alias("temperature_c"),
            F.first(
                "precipitation_mm",
                ignorenulls=True,
            ).alias("precipitation_mm"),
            F.first(
                "precipitation_trace",
                ignorenulls=True,
            ).alias("precipitation_trace"),
            F.first(
                "relative_humidity_pct",
                ignorenulls=True,
            ).alias("relative_humidity_pct"),
            F.first(
                "wind_speed_mps",
                ignorenulls=True,
            ).alias("wind_speed_mps"),
            F.first(
                "present_weather",
                ignorenulls=True,
            ).alias("present_weather"),
            F.first(
                "weather_condition",
                ignorenulls=True,
            ).alias("weather_condition"),
        )
        .fillna(
            {
                "weather_condition": "Unknown",
                "present_weather": "",
                "precipitation_trace": False,
            }
        )
    )

    hourly_count = hourly_weather.count()
    print("Distinct hourly weather rows:", hourly_count)

    # Aggregate observed taxi demand by hour and borough.
    borough_hourly_demand = (
        filtered
        .groupBy(
            "pickup_hour",
            "pickup_borough",
        )
        .agg(
            F.count("*").alias("trip_count")
        )
    )

    # Construct every actual hour × five-borough combination.
    borough_dimension = spark.createDataFrame(
        [(borough,) for borough in VALID_BOROUGHS],
        ["pickup_borough"],
    )

    complete_hourly_grid = (
        hourly_weather
        .crossJoin(F.broadcast(borough_dimension))
    )

    ml_dataset = (
        complete_hourly_grid
        .join(
            borough_hourly_demand,
            on=["pickup_hour", "pickup_borough"],
            how="left",
        )
        .fillna({"trip_count": 0})
        .withColumn(
            "is_weekday",
            F.when(
                F.col("pickup_day_of_week").isin(
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                ),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "rush_hour_indicator",
            F.when(
                (
                    F.col("pickup_day_of_week").isin(
                        "Monday",
                        "Tuesday",
                        "Wednesday",
                        "Thursday",
                        "Friday",
                    )
                )
                & (
                    F.col("pickup_hour_of_day").between(7, 9)
                    | F.col("pickup_hour_of_day").between(16, 19)
                ),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "snow_indicator",
            F.when(
                F.col("weather_condition") == "Snow",
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "heavy_rain_indicator",
            F.when(
                F.col("weather_condition") == "Heavy Rain",
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "precipitation_trace_indicator",
            F.when(
                F.col("precipitation_trace") == F.lit(True),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "dataset_split",
            F.when(
                F.col("pickup_month") <= 9,
                F.lit("train"),
            ).otherwise(F.lit("test")),
        )
    )

    # Calculate borough-specific 75th-percentile thresholds from
    # January–September training data only.
    training_threshold_source = ml_dataset.filter(
        F.col("dataset_split") == "train"
    )

    demand_thresholds = (
        training_threshold_source
        .groupBy("pickup_borough")
        .agg(
            F.expr(
                "percentile_approx(trip_count, 0.75, 10000)"
            ).cast("double").alias("high_demand_threshold")
        )
    )

    labeled_dataset = (
        ml_dataset
        .join(
            F.broadcast(demand_thresholds),
            on="pickup_borough",
            how="inner",
        )
        .withColumn(
            "label",
            F.when(
                F.col("trip_count")
                > F.col("high_demand_threshold"),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "demand_level",
            F.when(
                F.col("label") == 1.0,
                F.lit("High Demand"),
            ).otherwise(F.lit("Normal Demand")),
        )
    )

    label_distribution = (
        labeled_dataset
        .groupBy(
            "dataset_split",
            "pickup_borough",
            "demand_level",
            "label",
        )
        .agg(
            F.count("*").alias("row_count"),
            F.round(
                F.avg("trip_count"),
                2,
            ).alias("avg_trip_count"),
            F.min("trip_count").alias("min_trip_count"),
            F.max("trip_count").alias("max_trip_count"),
        )
        .orderBy(
            "dataset_split",
            "pickup_borough",
            "label",
        )
    )

    dataset_summary = (
        labeled_dataset
        .groupBy("dataset_split")
        .agg(
            F.count("*").alias("dataset_rows"),
            F.countDistinct("pickup_hour").alias("distinct_hours"),
            F.sum("trip_count").alias("represented_trip_count"),
            F.sum(
                F.when(
                    F.col("label") == 1.0,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("high_demand_rows"),
        )
        .withColumn(
            "high_demand_row_pct",
            F.round(
                100.0
                * F.col("high_demand_rows")
                / F.col("dataset_rows"),
                2,
            ),
        )
    )

    output_root = args.output.rstrip("/")

    print("Writing ML dataset to:", output_root)

    (
        labeled_dataset
        .repartition(5, "dataset_split")
        .write
        .mode("overwrite")
        .partitionBy("dataset_split")
        .parquet(f"{output_root}/ml_dataset")
    )

    (
        demand_thresholds
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(f"{output_root}/demand_thresholds")
    )

    (
        label_distribution
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(f"{output_root}/label_distribution")
    )

    (
        dataset_summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(f"{output_root}/dataset_summary")
    )

    print("Demand thresholds:")
    demand_thresholds.orderBy("pickup_borough").show(
        truncate=False
    )

    print("Dataset summary:")
    dataset_summary.orderBy("dataset_split").show(
        truncate=False
    )

    print("Label distribution:")
    label_distribution.show(
        100,
        truncate=False,
    )

    print("Phase 9.1 ML dataset creation completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()

