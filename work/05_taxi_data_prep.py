"""Data-preparation pipeline on the yellow-taxi Parquet — covers Lecture 3.

Demonstrates the core preprocessing steps every project hits before modelling:
missing-value inspection + imputation, IQR outlier detection, z-score
normalization, equal-frequency binning, and one-hot encoding. (Larose's claim
that data prep is ~60% of the effort — these are the operations it's made of.)

Each numbered section computes a result and peeks at it; the asserts at the end
verify the pipeline ran as expected.
"""
import time

from pyspark.sql import functions as F

from constants import TAXI_PARQUET
from spark_helper import get_spark, print_ui_urls, require_files, show_step


def main() -> None:
    require_files((TAXI_PARQUET, "make download-nyc-cab-data"))
    spark = get_spark("cs675-taxi-data-prep")
    start = time.time()

    df = spark.read.parquet(TAXI_PARQUET)
    total = df.count()
    print(f"Starting rows: {total:,}")

    # 1. Missing-value inspection. isNull().cast("int") is 0/1; summing gives the count.
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in ["passenger_count", "trip_distance", "fare_amount", "RatecodeID"]
    ])
    show_step("Missing-value counts per column", null_counts)

    # 2. Impute missing passenger_count with the median (approxQuantile -> [median]).
    median_passengers = df.approxQuantile("passenger_count", [0.5], 0.01)[0]
    df = df.fillna({"passenger_count": median_passengers})
    print(f"Imputed passenger_count nulls with median = {median_passengers}")

    # 3. Outlier detection via the IQR rule (more robust than z-score when outliers exist).
    q1, q3 = df.approxQuantile("trip_distance", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    low_cutoff, high_cutoff = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    df = df.withColumn(
        "is_distance_outlier",
        (F.col("trip_distance") < low_cutoff) | (F.col("trip_distance") > high_cutoff),
    )
    n_outliers = df.filter(F.col("is_distance_outlier")).count()
    print(f"\ntrip_distance: Q1={q1:.2f} Q3={q3:.2f} IQR={iqr:.2f} "
          f"fence=[{low_cutoff:.2f}, {high_cutoff:.2f}] "
          f"-> {n_outliers:,} outliers ({100 * n_outliers / total:.2f}%)")

    # 4. Z-score normalization of fare_amount: (X - mean) / sd -> mean 0, sd 1.
    stats = df.agg(
        F.avg("fare_amount").alias("mean"),
        F.stddev("fare_amount").alias("sd"),
    ).first()
    df = df.withColumn("fare_z", (F.col("fare_amount") - F.lit(stats["mean"])) / F.lit(stats["sd"]))
    print(f"\nfare_amount mean={stats['mean']:.2f} sd={stats['sd']:.2f}")
    show_step("Normalized fare (sample)", df.select("fare_amount", "fare_z"))

    # 5. Equal-frequency binning of trip_distance using quartile boundaries
    #    (each bin holds ~25% of rows, so outliers don't distort the widths).
    p25, p50, p75 = df.approxQuantile("trip_distance", [0.25, 0.5, 0.75], 0.01)
    df = df.withColumn(
        "distance_bin",
        F.when(F.col("trip_distance") < p25, "Q1_short")
         .when(F.col("trip_distance") < p50, "Q2_medium")
         .when(F.col("trip_distance") < p75, "Q3_long")
         .otherwise("Q4_very_long"),
    )
    show_step("Trip count per distance bin", df.groupBy("distance_bin").count().orderBy("distance_bin"))

    # 6. One-hot encoding of payment_type: k-1 flags, credit_card (1) as the reference.
    for code, name in [(2, "cash"), (3, "no_charge"), (4, "dispute"), (5, "unknown"), (6, "voided")]:
        df = df.withColumn(f"pay_{name}", (F.col("payment_type") == code).cast("int"))
    show_step("One-hot encoding (sample)",
              df.select("payment_type", "pay_cash", "pay_no_charge", "pay_dispute", "pay_unknown", "pay_voided"))

    # --- Verify ---
    assert df.filter(F.col("passenger_count").isNull()).count() == 0   # imputation filled them
    assert n_outliers > 0                                              # the 300K-mile trip and friends
    assert "fare_z" in df.columns and "distance_bin" in df.columns

    print(f"\nData-prep pipeline complete in {time.time() - start:.1f}s.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
