"""CS-675 Final Project: NYC taxi, zone, and daily weather join.

This script:
1. Reads January 2024 NYC Yellow Taxi records.
2. Cleans invalid and extreme trip records.
3. Reads and preprocesses Central Park daily weather observations.
4. Joins taxi trips with taxi zones and weather data.
5. Analyzes taxi demand, fares, duration, distance, and tipping behavior.
"""

import os
import sys
import time
from pathlib import Path

# Allow this file, which is inside work/final_project/, to import
# constants.py and spark_helper.py from the parent work/ directory.
WORK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORK_DIR))

from pyspark.sql import functions as F

from constants import DATA_DIR, TAXI_PARQUET, ZONES_CSV
from spark_helper import (
    get_spark,
    print_ui_urls,
    require_files,
    show_step,
)

WEATHER_CSV = os.path.join(DATA_DIR, "nyc_weather_2024-01.csv")


def main() -> None:
    """Run the local taxi-weather analysis."""

    # Confirm that the required local datasets exist.
    require_files(
        (TAXI_PARQUET, "make download-nyc-cab-data"),
        (ZONES_CSV, "make download-nyc-cab-zones-data"),
    )

    if not os.path.exists(WEATHER_CSV):
        raise FileNotFoundError(
            f"Missing weather file: {WEATHER_CSV}\n"
            "Download nyc_weather_2024-01.csv into work/data/ first."
        )

    spark = get_spark("cs675-taxi-weather-join")
    start_time = time.time()

    # ------------------------------------------------------------------
    # 1. Read and preprocess the weather data
    # ------------------------------------------------------------------

    weather_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(WEATHER_CSV)
    )

    weather = weather_raw.select(
        F.to_date("DATE").alias("weather_date"),
        F.col("TAVG").cast("double").alias("avg_temp_f"),
        F.col("TMAX").cast("double").alias("max_temp_f"),
        F.col("TMIN").cast("double").alias("min_temp_f"),
        F.col("PRCP").cast("double").alias("precip_inches"),
        F.col("SNOW").cast("double").alias("snow_inches"),
        F.col("AWND").cast("double").alias("wind_mph"),
    )

    weather_nulls_before = weather.select(
        [
            F.sum(F.col(column).isNull().cast("int")).alias(column)
            for column in [
                "avg_temp_f",
                "max_temp_f",
                "min_temp_f",
                "precip_inches",
                "snow_inches",
                "wind_mph",
            ]
        ]
    )

    show_step(
        "Weather missing values before imputation",
        weather_nulls_before,
        n=1,
    )

    # First estimate missing average temperature from the daily maximum
    # and minimum temperatures.
    weather = weather.withColumn(
        "avg_temp_f",
        F.coalesce(
            F.col("avg_temp_f"),
            (F.col("max_temp_f") + F.col("min_temp_f")) / F.lit(2.0),
        ),
    )

    # Median imputation for any remaining missing weather values.
    weather_columns = [
        "avg_temp_f",
        "max_temp_f",
        "min_temp_f",
        "precip_inches",
        "snow_inches",
        "wind_mph",
    ]

    weather_fill_values = {}

    for column in weather_columns:
        median_result = weather.approxQuantile(column, [0.5], 0.01)

        if median_result:
            weather_fill_values[column] = median_result[0]

    weather = weather.fillna(weather_fill_values)

    print("\nWeather median imputation values:")
    for column, value in weather_fill_values.items():
        print(f"  {column}: {value}")

    weather = (
        weather
        .withColumn(
            "weather_condition",
            F.when(F.col("snow_inches") > 0, "Snow")
            .when(F.col("precip_inches") >= 0.50, "Heavy Rain")
            .when(F.col("precip_inches") > 0, "Rain")
            .otherwise("Dry"),
        )
        .withColumn(
            "temperature_bin",
            F.when(F.col("avg_temp_f") < 32, "Freezing")
            .when(F.col("avg_temp_f") < 45, "Cold")
            .when(F.col("avg_temp_f") < 60, "Mild")
            .otherwise("Warm"),
        )
    )

    weather_nulls_after = weather.select(
        [
            F.sum(F.col(column).isNull().cast("int")).alias(column)
            for column in weather_columns
        ]
    )

    show_step(
        "Weather missing values after imputation",
        weather_nulls_after,
        n=1,
    )

    show_step(
        "Processed daily weather",
        weather.orderBy("weather_date"),
        n=31,
    )

    # ------------------------------------------------------------------
    # 2. Read and clean the taxi data
    # ------------------------------------------------------------------

    trips_raw = (
        spark.read
        .parquet(TAXI_PARQUET)
        .select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "payment_type",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
        )
    )

    raw_trip_count = trips_raw.count()

    trips = (
    trips_raw
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime"),
    )
    .withColumn(
        "trip_duration_minutes",
        F.timestamp_diff(
            "SECOND",
            F.col("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime"),
        ).cast("double")
        / F.lit(60.0),
    )
    .withColumn(
        "tip_percentage",
        F.when(
            (F.col("payment_type") == 1)
            & (F.col("fare_amount") > 0),
            100.0 * F.col("tip_amount") / F.col("fare_amount"),
        ),
    )
)

    # Remove invalid dates and implausible trip values.
    trips = trips.filter(
        (F.col("pickup_date") >= F.lit("2024-01-01").cast("date"))
        & (F.col("pickup_date") <= F.lit("2024-01-31").cast("date"))
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= 100)
        & (F.col("fare_amount") >= 0)
        & (F.col("fare_amount") <= 500)
        & (F.col("total_amount") > 0)
        & (F.col("total_amount") <= 1000)
        & (F.col("trip_duration_minutes") >= 1)
        & (F.col("trip_duration_minutes") <= 240)
    )

    clean_trip_count = trips.count()
    removed_trip_count = raw_trip_count - clean_trip_count

    print("\nTaxi data-quality comparison:")
    print(f"  Raw trips:     {raw_trip_count:,}")
    print(f"  Clean trips:   {clean_trip_count:,}")
    print(f"  Removed trips: {removed_trip_count:,}")
    print(
        "  Removed rate:  "
        f"{100.0 * removed_trip_count / raw_trip_count:.2f}%"
    )

    # ------------------------------------------------------------------
    # 3. Read the taxi zone lookup
    # ------------------------------------------------------------------

    zones = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ZONES_CSV)
        .select(
            F.col("LocationID").alias("zone_location_id"),
            "Borough",
            "Zone",
        )
    )

    print(f"\nWeather rows: {weather.count()}")
    print(f"Zone rows:    {zones.count()}")

    # ------------------------------------------------------------------
    # 4. Perform the cross-source joins
    # ------------------------------------------------------------------

    # Both weather and zone datasets are small dimension tables, so they
    # are broadcast to avoid shuffling the large taxi fact table.
    joined = (
        trips
        .join(
            F.broadcast(zones),
            trips["PULocationID"] == zones["zone_location_id"],
            "left",
        )
        .join(
            F.broadcast(weather),
            trips["pickup_date"] == weather["weather_date"],
            "left",
        )
        .cache()
    )

    joined_count = joined.count()

    weather_matched_count = joined.filter(
        F.col("weather_date").isNotNull()
    ).count()

    zone_matched_count = joined.filter(
        F.col("zone_location_id").isNotNull()
    ).count()

    print("\nJoin-quality results:")
    print(f"  Joined rows:          {joined_count:,}")
    print(f"  Weather-matched rows: {weather_matched_count:,}")
    print(f"  Zone-matched rows:    {zone_matched_count:,}")
    print(
        "  Weather match rate:   "
        f"{100.0 * weather_matched_count / joined_count:.2f}%"
    )
    print(
        "  Zone match rate:      "
        f"{100.0 * zone_matched_count / joined_count:.2f}%"
    )

    show_step(
        "Joined taxi, zone, and weather sample",
        joined.select(
            "pickup_date",
            "Borough",
            "Zone",
            "weather_condition",
            "temperature_bin",
            "avg_temp_f",
            "precip_inches",
            "snow_inches",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "trip_duration_minutes",
        ),
        n=10,
    )

    # ------------------------------------------------------------------
    # 5. Research Question 1:
    #    How does weather affect taxi demand?
    # ------------------------------------------------------------------

    demand_by_weather = (
        joined
        .groupBy("weather_condition")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("avg_temp_f"), 2).alias("avg_temperature_f"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.round(
                F.avg("trip_duration_minutes"),
                2,
            ).alias("avg_duration_minutes"),
        )
        .orderBy(F.col("trip_count").desc())
    )

    show_step(
        "Taxi demand and trip metrics by weather condition",
        demand_by_weather,
        n=10,
    )

    # Daily aggregation avoids comparing only raw trip-level totals.
    daily_demand = (
        joined
        .groupBy(
            "pickup_date",
            "weather_condition",
            "temperature_bin",
            "avg_temp_f",
            "precip_inches",
            "snow_inches",
        )
        .agg(
            F.count("*").alias("daily_trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.round(
                F.avg("trip_duration_minutes"),
                2,
            ).alias("avg_duration_minutes"),
        )
        .orderBy("pickup_date")
    )

    show_step(
        "Daily taxi demand with weather",
        daily_demand,
        n=31,
    )

    # ------------------------------------------------------------------
    # 6. Research Question 2:
    #    Does tipping behavior change with weather?
    # ------------------------------------------------------------------

    credit_card_trips = joined.filter(F.col("payment_type") == 1)

    tipping_by_weather = (
        credit_card_trips
        .groupBy("weather_condition")
        .agg(
            F.count("*").alias("credit_card_trips"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_usd"),
            F.round(
                F.avg("tip_percentage"),
                2,
            ).alias("avg_tip_percentage"),
            F.round(
                100.0
                * F.avg(
                    F.when(
                        F.col("tip_amount") <= 0,
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ),
                2,
            ).alias("no_tip_rate_pct"),
        )
        .orderBy(F.col("credit_card_trips").desc())
    )

    show_step(
        "Credit-card tipping behavior by weather condition",
        tipping_by_weather,
        n=10,
    )

    # ------------------------------------------------------------------
    # 7. Research Question 3:
    #    Which boroughs change most under different weather conditions?
    # ------------------------------------------------------------------

    borough_weather = (
        joined
        .groupBy("Borough", "weather_condition")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
            F.round(
                F.avg("trip_duration_minutes"),
                2,
            ).alias("avg_duration_minutes"),
        )
        .orderBy(F.col("trip_count").desc())
    )

    show_step(
        "Borough-level taxi activity by weather condition",
        borough_weather,
        n=25,
    )

    # ------------------------------------------------------------------
    # 8. Validation and completion
    # ------------------------------------------------------------------

    assert weather.count() == 31
    assert joined_count > 0
    assert weather_matched_count > 0
    assert zone_matched_count > 0

    joined.unpersist()

    print(
        "\nTaxi-weather-zone analysis completed in "
        f"{time.time() - start_time:.1f} seconds."
    )

    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()