"""SparkSession + dataset-presence + UI-URL helpers shared by the work/ scripts."""
import os
import sys

from pyspark.sql import DataFrame, SparkSession

from constants import EVENT_LOG_DIR, HISTORY_URL, LIVE_UI_URL


def show_step(caption: str, df: DataFrame, n: int = 5) -> None:
    """Print a labeled snapshot of a DataFrame (first n rows). Presentation only.

    Shared by the work/ scripts so the analysis code stays separate from the
    print boilerplate.
    """
    print(f"\n--- {caption} ---")
    df.show(n, truncate=False)


def get_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    """Return a SparkSession. Enables event logging when the History Server
    volume is mounted (Docker); silently skips it on native runs."""
    builder = SparkSession.builder.appName(app_name).master(master)
    if os.path.isdir(EVENT_LOG_DIR):
        builder = (
            builder
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", f"file:{EVENT_LOG_DIR}")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def print_ui_urls() -> None:
    print(f"Live Spark UI:  {LIVE_UI_URL}  (active while this script runs)")
    if os.path.isdir(EVENT_LOG_DIR):
        print(f"History Server: {HISTORY_URL} (run shows up after exit)")


def require_files(*paths_with_hints: tuple[str, str]) -> None:
    """Exit with a clear hint if any required dataset is missing.

    Each arg is `(path, command-to-fetch-it)`, e.g.
    `(TAXI_PARQUET, "make download-nyc-cab-data")`.
    """
    missing = [(p, h) for p, h in paths_with_hints if not os.path.exists(p)]
    if not missing:
        return
    for path, hint in missing:
        print(f"ERROR: missing {path}")
        print(f"       Run '{hint}' (or '.\\make.ps1 {hint.split()[1]}' on Windows) to fetch it.")
    sys.exit(1)
