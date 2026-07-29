import argparse

from pyspark.sql import SparkSession


REQUIRED_TIMESTAMP_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

REQUIRED_NUMERIC_COLUMNS = [
    "PULocationID",
    "passenger_count",
    "trip_distance",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

NUMERIC_TYPE_PREFIXES = (
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "decimal",
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Validate the required columns and compatible data types "
            "for all monthly NYC Yellow Taxi Parquet files."
        )
    )

    parser.add_argument(
        "--taxi-root",
        required=True,
        help="S3 root such as s3://bucket/raw/taxi",
    )

    parser.add_argument(
        "--year",
        required=True,
        type=int,
    )

    return parser.parse_args()


def is_numeric_type(type_name):
    return type_name.startswith(NUMERIC_TYPE_PREFIXES)


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("cs675-full-year-taxi-schema-validation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    validation_failures = []

    for month_number in range(1, 13):
        month = f"{month_number:02d}"

        path = (
            f"{args.taxi_root.rstrip('/')}"
            f"/year={args.year}"
            f"/month={month}"
            f"/yellow_tripdata_{args.year}-{month}.parquet"
        )

        print()
        print("=" * 70)
        print(f"Checking {args.year}-{month}")
        print(f"Path: {path}")
        print("=" * 70)

        try:
            dataframe = spark.read.parquet(path)
        except Exception as error:
            validation_failures.append(
                f"{args.year}-{month}: unable to read file: {error}"
            )
            print(f"[FAIL] Unable to read Parquet file: {error}")
            continue

        schema_types = {
            field.name: field.dataType.simpleString().lower()
            for field in dataframe.schema.fields
        }

        missing_columns = [
            column
            for column in (
                REQUIRED_TIMESTAMP_COLUMNS
                + REQUIRED_NUMERIC_COLUMNS
            )
            if column not in schema_types
        ]

        incompatible_columns = []

        for column in REQUIRED_TIMESTAMP_COLUMNS:
            if (
                column in schema_types
                and not schema_types[column].startswith("timestamp")
            ):
                incompatible_columns.append(
                    f"{column}={schema_types[column]}"
                )

        for column in REQUIRED_NUMERIC_COLUMNS:
            if (
                column in schema_types
                and not is_numeric_type(schema_types[column])
            ):
                incompatible_columns.append(
                    f"{column}={schema_types[column]}"
                )

        print(f"Column count: {len(schema_types)}")

        for column in (
            REQUIRED_TIMESTAMP_COLUMNS
            + REQUIRED_NUMERIC_COLUMNS
        ):
            print(
                f"  {column}: "
                f"{schema_types.get(column, 'MISSING')}"
            )

        if missing_columns:
            print(
                "[FAIL] Missing columns: "
                + ", ".join(missing_columns)
            )

        if incompatible_columns:
            print(
                "[FAIL] Incompatible types: "
                + ", ".join(incompatible_columns)
            )

        if not missing_columns and not incompatible_columns:
            print(
                f"[PASS] {args.year}-{month} schema is compatible."
            )
        else:
            validation_failures.append(
                f"{args.year}-{month}: "
                f"missing={missing_columns}; "
                f"incompatible={incompatible_columns}"
            )

    spark.stop()

    print()
    print("=" * 70)

    if validation_failures:
        print("FULL-YEAR SCHEMA VALIDATION FAILED")

        for failure in validation_failures:
            print(f"  - {failure}")

        raise RuntimeError(
            f"{len(validation_failures)} monthly files failed "
            "schema validation."
        )

    print("ALL 12 MONTHLY TAXI SCHEMAS PASSED.")
    print("=" * 70)


if __name__ == "__main__":
    main()
