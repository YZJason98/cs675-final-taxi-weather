# Native Windows path

Run PySpark directly on Windows, no Docker. Uses [`uv`](https://docs.astral.sh/uv/) to manage Python + dependencies. Commands below assume PowerShell.

> **Heads-up — no History Server on the native path.** The Docker path runs a Spark History Server on http://localhost:18080 that keeps every completed run visible. Native runs only have the live Spark UI on http://localhost:4040 while a SparkSession is alive. If you want persistent run history, use the Docker path.

## 1. Install Python 3.12

```
winget install -e --id Python.Python.3.12
```

Or download the installer from [python.org](https://www.python.org/downloads/windows/). During install, check **Add Python to PATH**.

Verify (open a fresh PowerShell window):

```
python --version
```

## 2. Install OpenJDK 17

Spark needs a Java runtime.

1. Download the **JDK 17 Windows x64 MSI installer** from [Adoptium](https://adoptium.net/temurin/releases/?version=17).
2. Run the installer. On the *Custom Setup* screen, **enable**:
   - "Set JAVA_HOME variable"
   - "Add to PATH"
3. Finish the install and **open a fresh PowerShell window** so the new environment variables take effect.

Verify:

```
java -version
```

You should see something with `17` in the output.

## 3. Install uv

```
winget install --id=astral-sh.uv -e
```

Or, in PowerShell:

```
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Open a fresh PowerShell window and verify:

```
uv --version
```

## 4. Sync the environment

From inside `code-starter\`:

```
uv sync
```

This downloads Python 3.12 (if needed), creates a virtualenv in `.venv`, and installs PySpark, pandas, pyarrow, pytest, and Jupyter Lab.

## 5. Run the smoke test

```
uv run python work\hello_spark.py
```

Expected output: PySpark version, default parallelism, the live Spark UI URL (http://localhost:4040), a small DataFrame, and `Smoke test passed.`

## 6. Download datasets and run the analyses (optional)

The starter ships with three datasets covering the typical Big Data shapes:

```
# Primary Parquet (~48 MB, ~3 M rows)
Invoke-WebRequest `
  -Uri "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" `
  -OutFile work\data\yellow_tripdata_2024-01.parquet

# Small CSV (~12 KB, 265 rows) — taxi zone lookup
Invoke-WebRequest `
  -Uri "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" `
  -OutFile work\data\taxi_zone_lookup.csv

# Large CSV (~10 MB unzipped, ~50 K rows) — JC Citi Bike trips
Invoke-WebRequest `
  -Uri "https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip" `
  -OutFile $env:TEMP\citibike.zip
Expand-Archive $env:TEMP\citibike.zip -DestinationPath work\data\
Remove-Item $env:TEMP\citibike.zip
```

Then run any of the analyses — they're numbered `00` → `08` in rising order of complexity:

```
uv run python work\00_hello_spark.py            # smoke test
uv run python work\01_word_count.py             # word count on Shakespeare text
uv run python work\02_taxi_analysis.py          # cab trip overview
uv run python work\03_taxi_tipping.py           # cab tipping behavior
uv run python work\04_taxi_payments.py          # cab payment methods
uv run python work\05_taxi_data_prep.py         # cab data preparation (Lecture 3)
uv run python work\06_zones_analysis.py         # cab × zones broadcast join
uv run python work\07_citibike_analysis.py      # CSV → Parquet on Citi Bike
uv run python work\08_taxi_classification.py    # cab tip classifier (Lecture 2b)
```

If this is your first time with PySpark, run them in order. Scripts `02`–`05` read the same Parquet but ask different questions — a small worked example of building an analysis suite around one dataset.

## 7. Run the test suite

```
uv run pytest tests\ -v
```

Expected: 4 smoke tests pass; the 3 `test_taxi_analysis.py` integration tests pass if you downloaded the dataset, or skip cleanly otherwise.

## 8. Open Jupyter Lab

```
uv run jupyter lab
```

Browser opens to Jupyter Lab. Open `work\hello_spark.py` and run it interactively — the SparkSession stays alive while the kernel is running, so you can hit http://localhost:4040 in another tab and explore the Spark UI.

## Troubleshooting

- **`JAVA_HOME` not set or Java not found** — Reopen PowerShell to pick up the new env vars. Verify with `echo $env:JAVA_HOME` and `java -version`.
- **`uv: command not found`** after install — reopen PowerShell; the installer modifies your PATH but the current shell won't see it.
- **`winutils.exe` errors** — Spark on Windows sometimes complains about missing Hadoop helpers. PySpark 3.5+ usually ships its own; if you hit this, see [the Spark Windows guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#installation-using-pypi) or fall back to the Docker path.
- **Port 4040 already in use** — another Spark session is running. Kill the Python process and retry.
- **`uv sync` fails on Python 3.12 download** — install Python 3.12 yourself via Step 1, then re-run `uv sync`.
