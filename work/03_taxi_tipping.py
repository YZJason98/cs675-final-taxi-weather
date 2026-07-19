"""Tipping analysis on the yellow-taxi data.

Pipeline: drop bad fares -> derive tip % -> compare payment types -> focus on
credit cards -> tip % by hour -> percentile distribution.

Key data note: TLC records cash tips as $0 (payment_type 2 always shows
tip_amount 0), so credit-card trips (payment_type 1) are the only honest tip
signal — most of the analysis filters to those.
"""
import time

from pyspark.sql import functions as F

from constants import TAXI_PARQUET
from spark_helper import get_spark, print_ui_urls, require_files, show_step


def main() -> None:
    require_files((TAXI_PARQUET, "make download-nyc-cab-data"))
    spark = get_spark("cs675-taxi-tipping")
    start = time.time()

    # Build: load, drop non-positive fares (avoids div-by-zero), derive tip %.
    df = (
        spark.read.parquet(TAXI_PARQUET)
        .filter(F.col("fare_amount") > 0)
        .withColumn("tip_pct", F.col("tip_amount") / F.col("fare_amount") * 100)
    )

    # Tip behavior by payment type (cash will show $0 — not recorded).
    by_payment = (
        df.groupBy("payment_type")
        .agg(
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_usd"),
            F.count("*").alias("n_trips"),
        )
        .orderBy("payment_type")
    )

    # Credit cards only, broken down by hour of day.
    cc = df.filter(F.col("payment_type") == 1)
    by_hour = (
        cc.withColumn("hour", F.hour("tpep_pickup_datetime"))
        .groupBy("hour")
        .agg(
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.count("*").alias("n_trips"),
        )
        .orderBy("hour")
    )

    # --- Peek ---
    show_step("Avg tip by payment type (1=credit 2=cash 3=no-charge 4=dispute 5=unknown 6=voided)", by_payment)
    show_step("Credit-card tip % by hour", by_hour, n=24)

    # Percentile distribution of credit-card tip % (approxQuantile = cheap sketch estimate).
    labels = ["p10", "p25", "p50", "p75", "p90", "p99"]
    pcts = cc.approxQuantile("tip_pct", [0.10, 0.25, 0.50, 0.75, 0.90, 0.99], 0.01)
    print("\n--- Credit-card tip % distribution ---")
    for label, value in zip(labels, pcts):
        print(f"  {label}: {value:.2f}%")
    # The median (p50) sits well below the table's mean: a few huge tip% values on
    # tiny fares drag the mean up — the classic case where the median is honest.

    # --- Verify ---
    assert df.count() > 0
    assert cc.count() > 0
    cash = by_payment.filter(F.col("payment_type") == 2).first()
    assert cash["avg_tip_usd"] == 0.0          # cash tips are never recorded

    print(f"\nDone in {time.time() - start:.1f}s.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
