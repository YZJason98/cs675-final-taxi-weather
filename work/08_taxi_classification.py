"""Binary classification with Spark MLlib — covers Lecture 2b (Data Mining tasks).

Question: can we predict whether a credit-card trip received any tip, from
features known at the start of the trip (distance, fare, hour, passenger count)?

This is the canonical *Classification* task from Lecture 2b §4.4: the model sees
labeled training rows, learns the pattern, then assigns labels to unseen rows.
Steps: build features + target -> train/test split -> fit LogisticRegression ->
evaluate with AUC.
"""
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from constants import TAXI_PARQUET
from spark_helper import get_spark, print_ui_urls, require_files, show_step

FEATURE_COLS = ["trip_distance", "fare_amount", "passenger_count", "hour"]


def main() -> None:
    require_files((TAXI_PARQUET, "make download-nyc-cab-data"))
    spark = get_spark("cs675-taxi-classification")
    start = time.time()

    # Build the modelling table: credit-card trips only, with features and a binary target.
    df = (
        spark.read.parquet(TAXI_PARQUET)
        .filter(F.col("payment_type") == 1)                          # cash tips aren't recorded -> would leak
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("trip_distance") > 0)
        .withColumn("hour", F.hour("tpep_pickup_datetime"))          # feature: hour-of-day
        .withColumn("tipped", (F.col("tip_amount") > 0).cast("int")) # target: 1 if any tip
        .na.drop(subset=FEATURE_COLS + ["tipped"])
    )
    print(f"Modelling rows: {df.count():,}")
    show_step("Class distribution (check the balance before trusting accuracy)",
              df.groupBy("tipped").count().orderBy("tipped"))

    # MLlib wants all features in one vector column; the Pipeline chains the stages.
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="tipped", maxIter=10)
    pipeline = Pipeline(stages=[assembler, lr])

    train, test = df.randomSplit([0.8, 0.2], seed=42)                # seed -> reproducible split
    print(f"Train: {train.count():,}    Test: {test.count():,}")
    print("\nFitting LogisticRegression...")
    model = pipeline.fit(train)

    predictions = model.transform(test)
    show_step("Sample predictions",
              predictions.select("trip_distance", "fare_amount", "hour", "tipped", "prediction", "probability"))

    # AUC: area under the ROC curve. 1.0 = perfect, 0.5 = random guessing.
    auc = BinaryClassificationEvaluator(
        labelCol="tipped", rawPredictionCol="rawPrediction", metricName="areaUnderROC",
    ).evaluate(predictions)
    print(f"\nTest AUC: {auc:.4f}")

    # Logistic-regression coefficients are log-odds: positive -> pushes toward "tipped".
    lr_model = model.stages[-1]
    print("--- Feature coefficients (log-odds) ---")
    for col, coef in zip(FEATURE_COLS, lr_model.coefficients):
        print(f"  {col:>18}: {coef:+.4f}")
    print(f"  {'intercept':>18}: {lr_model.intercept:+.4f}")

    # --- Verify ---
    assert df.count() > 0
    assert 0.0 <= auc <= 1.0
    assert {row["tipped"] for row in df.select("tipped").distinct().collect()} == {0, 1}

    print(f"\nDone in {time.time() - start:.1f}s.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
