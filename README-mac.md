# Native macOS path

Run PySpark directly on your Mac, no Docker. Uses [`uv`](https://docs.astral.sh/uv/) to manage Python + dependencies and [Homebrew](https://brew.sh/) for the JDK.

> **Heads-up — no History Server on the native path.** The Docker path runs a Spark History Server on http://localhost:18080 that keeps every completed run visible. Native runs only have the live Spark UI on http://localhost:4040 while a SparkSession is alive. If you want persistent run history, use the Docker path.

## 1. Install Homebrew

If you don't have it:

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Verify:

```
brew --version
```

## 2. Install OpenJDK 17

Spark needs a Java runtime.

```
brew install openjdk@17
```

Then add Java to your environment. Open `~/.zshrc` (or `~/.bash_profile` if you use bash) and add:

```
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH=$JAVA_HOME/bin:$PATH
```

Reload your shell:

```
source ~/.zshrc
```

Verify:

```
java -version
```

You should see something with `17` in the output.

## 3. Install uv

```
brew install uv
```

Verify:

```
uv --version
```

## 4. Sync the environment

From inside `code-starter/`:

```
uv sync
```

This downloads Python 3.12 (if you don't have it), creates a virtualenv in `.venv`, and installs PySpark, pandas, pyarrow, pytest, and Jupyter Lab.

## 5. Run the smoke test

```
uv run python work/hello_spark.py
```

Expected output: PySpark version, default parallelism, the live Spark UI URL (http://localhost:4040), a small DataFrame, and `Smoke test passed.`

## 6. Download datasets and run the analyses (optional)

The starter ships with three datasets covering the typical Big Data shapes:

```
# Primary Parquet (~48 MB, ~3 M rows)
curl -L -o work/data/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# Small CSV (~12 KB, 265 rows) — taxi zone lookup
curl -L -o work/data/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# Large CSV (~10 MB unzipped, ~50 K rows) — JC Citi Bike trips
curl -L -o /tmp/citibike.zip \
  https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip
unzip -p /tmp/citibike.zip "JC-*.csv" > work/data/JC-202401-citibike-tripdata.csv && rm /tmp/citibike.zip
```

Then run any of the analyses — they're numbered `00` → `08` in rising order of complexity:

```
uv run python work/00_hello_spark.py            # smoke test
uv run python work/01_word_count.py             # word count on Shakespeare text
uv run python work/02_taxi_analysis.py          # cab trip overview
uv run python work/03_taxi_tipping.py           # cab tipping behavior
uv run python work/04_taxi_payments.py          # cab payment methods
uv run python work/05_taxi_data_prep.py         # cab data preparation (Lecture 3)
uv run python work/06_zones_analysis.py         # cab × zones broadcast join
uv run python work/07_citibike_analysis.py      # CSV → Parquet on Citi Bike
uv run python work/08_taxi_classification.py    # cab tip classifier (Lecture 2b)
```

If this is your first time with PySpark, run them in order. Scripts `02`–`05` read the same Parquet but ask different questions — a small worked example of building an analysis suite around one dataset.

## 7. Run the test suite

```
uv run pytest tests/ -v
```

Expected: 4 smoke tests pass; the 3 `test_taxi_analysis.py` integration tests pass if you downloaded the dataset, or skip cleanly otherwise.

## 8. Open Jupyter Lab

```
uv run jupyter lab
```

Browser opens to Jupyter Lab. Open `work/hello_spark.py` and run it interactively — the SparkSession stays alive while the kernel is running, so you can hit http://localhost:4040 in another tab and explore the Spark UI.

## Troubleshooting

- **`JAVA_HOME` not set or Java not found** — re-check Step 2. Run `echo $JAVA_HOME` to confirm.
- **`uv: command not found`** — re-open your terminal after `brew install uv`, or run `source ~/.zshrc`.
- **PySpark complains about `java.lang.UnsupportedClassVersionError`** — you have the wrong Java version. Verify with `java -version` that it says `17`.
- **Port 4040 already in use** — another Spark session is running. Stop it (kill the Python process) and retry.
