"""Practice: run Spark on your own computer.

Self-contained — it generates its own data, so there is nothing to download.
After installing PySpark and a Java runtime (see run-on-your-own.md), run:

    python practice.py

Read each block, run it, then change a number or a column and run it again.
The exercises at the bottom are the point — the analyses above are just examples.
"""

from pyspark.sql import SparkSession, functions as F

# Start a local Spark session that uses every CPU core on your machine.
spark = SparkSession.builder.appName("practice").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # quiet the startup logs

# --- Make some data (no download needed) ---
# spark.range generates 5,000 rows; each withColumn adds a random column.
trips = (
    spark.range(0, 5000)                                          # 5,000 rows, column "id"
    .withColumn("passengers", (F.rand() * 4 + 1).cast("int"))     # 1-4 passengers
    .withColumn("distance_km", F.round(F.rand() * 20, 2))         # 0-20 km
    .withColumn("fare", F.round(F.rand() * 40 + 5, 2))            # $5-$45
    .withColumn("payment", F.when(F.rand() < 0.7, "card").otherwise("cash"))  # ~70% card
)

print("Sample of the data:")
trips.show(5)                            # ACTION: runs the plan, prints 5 rows
print("Total trips:", trips.count())     # ACTION: counts every row

# 1. Average fare by payment type — groupBy then aggregate.
print("Average fare by payment type:")
trips.groupBy("payment").agg(
    F.round(F.avg("fare"), 2).alias("avg_fare"),
    F.count("*").alias("trips"),
).show()

# 2. The 5 longest trips — order by distance, descending.
print("5 longest trips:")
trips.orderBy(F.col("distance_km").desc()).select("distance_km", "fare").show(5)

# 3. Trips with more than 2 passengers — filter, then count.
big_groups = trips.filter(F.col("passengers") > 2)
print("Trips with 3+ passengers:", big_groups.count())

spark.stop()

# --- Your turn (edit this file and re-run) ---
# a. Add a tip column worth 15% of the fare:  .withColumn("tip", F.round(F.col("fare") * 0.15, 2))
# b. Count the trips for each passenger count (groupBy "passengers").
# c. Keep only card trips over $30, and show how many there are.
# d. Find the average distance per payment type.
