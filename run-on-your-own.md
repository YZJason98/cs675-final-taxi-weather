# Run Spark on Your Own Computer

The quickest way to practice PySpark: install it on your own laptop and run a script. No Docker, no accounts. This page gets you from nothing to a running Spark job.

## What you need

- **Python 3.9 or newer.** Check with `python --version` (or `python3 --version`).
- **A Java runtime (JDK 17).** Spark runs on the Java Virtual Machine, so this is required even though you write Python. This is the one step people miss.
- **PySpark**, installed with pip.

### Install Java

- **macOS:** `brew install openjdk@17` (or download Temurin 17 from [adoptium.net](https://adoptium.net)).
- **Windows:** download the Temurin 17 installer from [adoptium.net](https://adoptium.net) and let it set `JAVA_HOME` for you.
- **Linux:** `sudo apt install openjdk-17-jdk` (Debian/Ubuntu) or your distro's equivalent.

Check it works:

```bash
java -version
```

### Install PySpark

A virtual environment keeps this separate from your other Python projects:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install pyspark
```

Check it works:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

## Run your first job

Grab `work/practice.py` from this folder and run it:

```bash
python practice.py
```

It starts Spark, makes 5,000 rows of fake taxi-trip data (so there is nothing to download), and prints a few analyses. You should see a sample table, a total count, average fare by payment type, the longest trips, and a passenger-count filter. The first run is slow while Spark starts up — that is normal.

## Now practice

The bottom of `practice.py` lists a few exercises: add a tip column, count trips per passenger count, filter card trips over a fare threshold, average distance per payment type. Edit the file, re-run it, and watch the output change. That loop — change one thing, run, read the result — is the whole point.

When you are comfortable, the numbered scripts (`00_hello_spark.py` through `08_taxi_classification.py`) walk through real analyses on the NYC taxi data in rising order of difficulty.

## If something breaks

- **`JAVA_HOME is not set` or `Java gateway process exited`** — Java is missing or not on your path. Re-check `java -version`. On Windows, restart your terminal after installing Java so the new `JAVA_HOME` takes effect.
- **`No module named pyspark`** — your virtual environment is not active, or you installed into a different Python. Re-run the activate line, then `pip install pyspark` again.
- **`python` opens the wrong version** — try `python3` instead, and `pip3` for installing.
- Nothing here is destructive. Fix the obvious thing and run again.

> For a fuller setup managed by `uv`, with the real NYC taxi datasets and the test suite, see [README-mac.md](README-mac.md) or [README-windows.md](README-windows.md). This page is the no-frills path for getting Spark running fast.
