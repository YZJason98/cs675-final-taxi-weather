import argparse
import time

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CATEGORICAL = [
    "pickup_day_of_week",
    "pickup_borough",
    "weather_condition",
]

NUMERIC = [
    "temperature_c",
    "precipitation_mm",
    "relative_humidity_pct",
    "wind_speed_mps",
    "pickup_hour_of_day",
    "pickup_month",
    "is_weekday",
    "rush_hour_indicator",
    "snow_indicator",
    "heavy_rain_indicator",
    "precipitation_trace_indicator",
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def preprocessing():
    indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=f"{column}_index",
            handleInvalid="keep",
        )
        for column in CATEGORICAL
    ]

    encoder = OneHotEncoder(
        inputCols=[
            f"{column}_index"
            for column in CATEGORICAL
        ],
        outputCols=[
            f"{column}_ohe"
            for column in CATEGORICAL
        ],
        handleInvalid="keep",
        dropLast=True,
    )

    imputed = [
        f"{column}_imputed"
        for column in NUMERIC
    ]

    imputer = Imputer(
        inputCols=NUMERIC,
        outputCols=imputed,
        strategy="median",
    )

    assembler = VectorAssembler(
        inputCols=(
            imputed
            + [
                f"{column}_ohe"
                for column in CATEGORICAL
            ]
        ),
        outputCol="features",
        handleInvalid="keep",
    )

    return indexers + [
        encoder,
        imputer,
        assembler,
    ]


def feature_names(dataset):
    attrs = (
        dataset
        .schema["features"]
        .metadata["ml_attr"]["attrs"]
    )

    indexed = []

    for group in attrs.values():
        for attr in group:
            indexed.append(
                (
                    int(attr["idx"]),
                    attr["name"],
                )
            )

    return [
        name
        for _, name in sorted(indexed)
    ]


def evaluate(
    predictions,
    model_name,
    runtime_seconds,
):
    matrix = {
        (
            int(row["label"]),
            int(row["prediction"]),
        ): int(row["count"])
        for row in (
            predictions
            .groupBy("label", "prediction")
            .count()
            .collect()
        )
    }

    tn = matrix.get((0, 0), 0)
    fp = matrix.get((0, 1), 0)
    fn = matrix.get((1, 0), 0)
    tp = matrix.get((1, 1), 0)

    total = tn + fp + fn + tp

    accuracy = (
        (tp + tn) / total
        if total
        else 0.0
    )

    precision = (
        tp / (tp + fp)
        if tp + fp
        else 0.0
    )

    recall = (
        tp / (tp + fn)
        if tp + fn
        else 0.0
    )

    f1 = (
        2 * precision * recall
        / (precision + recall)
        if precision + recall
        else 0.0
    )

    roc_auc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    ).evaluate(predictions)

    return {
        "model": model_name,
        "test_rows": total,
        "true_positive": tp,
        "true_negative": tn,
        "false_positive": fp,
        "false_negative": fn,
        "accuracy": round(accuracy, 6),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1_score": round(f1, 6),
        "roc_auc": round(float(roc_auc), 6),
        "runtime_seconds": round(
            runtime_seconds,
            3,
        ),
    }


def ranked_features(
    model_name,
    names,
    values,
    coefficient_mode,
):
    rows = []

    for name, value in zip(names, values):
        value = float(value)

        rows.append(
            {
                "model": model_name,
                "feature": name,
                "importance": round(
                    abs(value),
                    8,
                ),
                "signed_coefficient": (
                    round(value, 8)
                    if coefficient_mode
                    else None
                ),
            }
        )

    rows.sort(
        key=lambda row: row["importance"],
        reverse=True,
    )

    for rank, row in enumerate(
        rows,
        start=1,
    ):
        row["rank"] = rank

    return rows


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("Phase9TaxiDemandModels")
        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.session.timeZone",
        "America/New_York",
    )

    data = spark.read.parquet(
        args.input.rstrip("/")
    )

    train = (
        data
        .filter(
            F.col("dataset_split") == "train"
        )
        .cache()
    )

    test = (
        data
        .filter(
            F.col("dataset_split") == "test"
        )
        .cache()
    )

    print(
        "Training rows:",
        train.count(),
    )

    print(
        "Testing rows:",
        test.count(),
    )

    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withMean=False,
        withStd=True,
    )

    logistic = LogisticRegression(
        labelCol="label",
        featuresCol="scaled_features",
        maxIter=100,
        regParam=0.05,
        elasticNetParam=0.0,
        standardization=False,
    )

    forest = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=150,
        maxDepth=12,
        maxBins=64,
        featureSubsetStrategy="sqrt",
        seed=42,
    )

    logistic_pipeline = Pipeline(
        stages=(
            preprocessing()
            + [
                scaler,
                logistic,
            ]
        )
    )

    forest_pipeline = Pipeline(
        stages=(
            preprocessing()
            + [forest]
        )
    )

    started = time.perf_counter()

    logistic_model = (
        logistic_pipeline.fit(train)
    )

    logistic_predictions = (
        logistic_model
        .transform(test)
        .cache()
    )

    logistic_predictions.count()

    logistic_runtime = (
        time.perf_counter()
        - started
    )

    started = time.perf_counter()

    forest_model = (
        forest_pipeline.fit(train)
    )

    forest_predictions = (
        forest_model
        .transform(test)
        .cache()
    )

    forest_predictions.count()

    forest_runtime = (
        time.perf_counter()
        - started
    )

    metrics = [
        evaluate(
            logistic_predictions,
            "Logistic Regression",
            logistic_runtime,
        ),
        evaluate(
            forest_predictions,
            "Random Forest",
            forest_runtime,
        ),
    ]

    names = feature_names(
        forest_predictions
    )

    importance_rows = (
        ranked_features(
            "Logistic Regression",
            names,
            list(
                logistic_model
                .stages[-1]
                .coefficients
            ),
            True,
        )
        + ranked_features(
            "Random Forest",
            names,
            list(
                forest_model
                .stages[-1]
                .featureImportances
            ),
            False,
        )
    )

    confusion_rows = []

    for result in metrics:
        confusion_rows.extend(
            [
                {
                    "model": result["model"],
                    "actual": "High Demand",
                    "predicted": "High Demand",
                    "count": result[
                        "true_positive"
                    ],
                },
                {
                    "model": result["model"],
                    "actual": "High Demand",
                    "predicted": "Normal Demand",
                    "count": result[
                        "false_negative"
                    ],
                },
                {
                    "model": result["model"],
                    "actual": "Normal Demand",
                    "predicted": "High Demand",
                    "count": result[
                        "false_positive"
                    ],
                },
                {
                    "model": result["model"],
                    "actual": "Normal Demand",
                    "predicted": "Normal Demand",
                    "count": result[
                        "true_negative"
                    ],
                },
            ]
        )

    output = args.output.rstrip("/")

    metrics_df = spark.createDataFrame(
        metrics
    )

    importance_df = spark.createDataFrame(
        importance_rows
    )

    confusion_df = spark.createDataFrame(
        confusion_rows
    )

    (
        metrics_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(
            f"{output}/model_metrics"
        )
    )

    (
        importance_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(
            f"{output}/feature_importance"
        )
    )

    (
        confusion_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(
            f"{output}/confusion_matrices"
        )
    )

    (
        logistic_model
        .write()
        .overwrite()
        .save(
            f"{output}/models/"
            "logistic_regression"
        )
    )

    (
        forest_model
        .write()
        .overwrite()
        .save(
            f"{output}/models/"
            "random_forest"
        )
    )

    print("Model comparison:")

    (
        metrics_df
        .orderBy(
            F.desc("f1_score")
        )
        .show(
            truncate=False
        )
    )

    print(
        "Top 15 features per model:"
    )

    (
        importance_df
        .filter(
            F.col("rank") <= 15
        )
        .orderBy(
            "model",
            "rank",
        )
        .show(
            40,
            truncate=False,
        )
    )

    print(
        "Phase 9.2 completed successfully."
    )

    logistic_predictions.unpersist()
    forest_predictions.unpersist()
    train.unpersist()
    test.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
