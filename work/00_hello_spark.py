"""Smoke test and a first tour of the core DataFrame operations.

Run with `make hello` (Docker) or `python 00_hello_spark.py`. It builds a tiny
DataFrame in memory, derives a column, filters it, and checks the result.

The idea to take away: in Spark, transformations like withColumn() and filter()
are *lazy* — they only describe a new DataFrame. Nothing runs until an *action*
like show() or count() asks for a result.

The body is grouped into three parts: build the data, peek at it (the only
prints), then verify it (asserts — silent unless something is wrong).
"""
import pyspark
from pyspark.sql import functions as F

from spark_helper import get_spark, print_ui_urls, show_step


def main() -> None:
    spark = get_spark("cs675-hello")
    sc = spark.sparkContext
    print(f"PySpark {pyspark.__version__} on {sc.master} ({sc.defaultParallelism} cores)")

    # --- Build: a tiny DataFrame, then two lazy transformations ---
    rows = [(i, i * i) for i in range(10)]               # plain Python: (x, x squared)
    df = spark.createDataFrame(rows, ["x", "x_squared"])  # Python list -> DataFrame
    cubed = df.withColumn("x_cubed", F.col("x") * F.col("x") * F.col("x"))  # add a derived column
    big = cubed.filter(F.col("x") > 5)                    # keep only rows where x > 5

    # --- Peek: actions that trigger the work and print each table ---
    show_step("Base data", df)
    show_step("After withColumn(x_cubed)", cubed)
    show_step("After filter(x > 5)", big)

    # --- Verify: asserts confirm behavior, printing nothing unless they fail ---
    assert df.count() == 10
    assert cubed.columns == ["x", "x_squared", "x_cubed"]
    assert big.count() == 4                               # x in {6, 7, 8, 9}

    print("\nSmoke test passed.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
