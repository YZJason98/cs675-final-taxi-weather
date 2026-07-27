import argparse
import glob
import json
import os
from typing import Any


def numeric_value(mapping: dict[str, Any], key: str) -> int:
    value = mapping.get(key, 0)

    if isinstance(value, bool):
        return 0

    if isinstance(value, (int, float)):
        return int(value)

    return 0


def task_succeeded(event: dict[str, Any]) -> bool:
    reason = event.get("Task End Reason", {})

    if isinstance(reason, dict):
        reason_text = str(reason.get("Reason", ""))
    else:
        reason_text = str(reason)

    return reason_text.lower() == "success"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract Spark performance metrics from event logs."
    )
    parser.add_argument("--event-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    event_files = sorted(
        path
        for path in glob.glob(
            os.path.join(args.event_dir, "events_*")
        )
        if os.path.isfile(path)
    )

    if not event_files:
        raise FileNotFoundError(
            f"No Spark event files found in {args.event_dir}"
        )

    job_ids: set[int] = set()
    completed_stages: set[tuple[int, int]] = set()
    completed_task_attempts: set[
        tuple[int, int, int, int]
    ] = set()

    sql_execution_count = 0
    successful_tasks = 0
    failed_tasks = 0
    scheduled_stage_tasks = 0

    input_bytes = 0
    input_records = 0

    output_bytes = 0
    output_records = 0

    shuffle_read_bytes = 0
    shuffle_read_records = 0

    shuffle_write_bytes = 0
    shuffle_write_records = 0

    executor_run_time_ms = 0
    executor_cpu_time_ns = 0

    memory_bytes_spilled = 0
    disk_bytes_spilled = 0
    peak_execution_memory = 0

    malformed_lines = 0

    for event_file in event_files:
        with open(
            event_file,
            "r",
            encoding="utf-8",
            errors="replace",
        ) as handle:
            for line in handle:
                line = line.strip()

                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    malformed_lines += 1
                    continue

                event_type = event.get("Event")

                if event_type == "SparkListenerJobStart":
                    job_id = event.get("Job ID")

                    if isinstance(job_id, int):
                        job_ids.add(job_id)

                elif (
                    event_type
                    ==
                    "org.apache.spark.sql.execution.ui."
                    "SparkListenerSQLExecutionStart"
                ):
                    sql_execution_count += 1

                elif event_type == "SparkListenerStageCompleted":
                    stage_info = event.get("Stage Info", {})

                    stage_id = stage_info.get("Stage ID")
                    stage_attempt = stage_info.get(
                        "Stage Attempt ID",
                        0,
                    )

                    if not isinstance(stage_id, int):
                        continue

                    stage_key = (
                        stage_id,
                        int(stage_attempt),
                    )

                    if stage_key in completed_stages:
                        continue

                    completed_stages.add(stage_key)

                    scheduled_stage_tasks += numeric_value(
                        stage_info,
                        "Number of Tasks",
                    )

                elif event_type == "SparkListenerTaskEnd":
                    stage_id = event.get("Stage ID")
                    stage_attempt = event.get(
                        "Stage Attempt ID",
                        0,
                    )

                    task_info = event.get("Task Info", {})
                    task_id = task_info.get("Task ID")
                    task_attempt = task_info.get("Attempt", 0)

                    if not isinstance(stage_id, int):
                        continue

                    if not isinstance(task_id, int):
                        continue

                    task_key = (
                        stage_id,
                        int(stage_attempt),
                        task_id,
                        int(task_attempt),
                    )

                    if task_key in completed_task_attempts:
                        continue

                    completed_task_attempts.add(task_key)

                    if not task_succeeded(event):
                        failed_tasks += 1
                        continue

                    successful_tasks += 1

                    metrics = event.get("Task Metrics", {})

                    executor_run_time_ms += numeric_value(
                        metrics,
                        "Executor Run Time",
                    )

                    executor_cpu_time_ns += numeric_value(
                        metrics,
                        "Executor CPU Time",
                    )

                    memory_bytes_spilled += numeric_value(
                        metrics,
                        "Memory Bytes Spilled",
                    )

                    disk_bytes_spilled += numeric_value(
                        metrics,
                        "Disk Bytes Spilled",
                    )

                    peak_execution_memory = max(
                        peak_execution_memory,
                        numeric_value(
                            metrics,
                            "Peak Execution Memory",
                        ),
                    )

                    input_metrics = metrics.get(
                        "Input Metrics",
                        {},
                    )

                    input_bytes += numeric_value(
                        input_metrics,
                        "Bytes Read",
                    )

                    input_records += numeric_value(
                        input_metrics,
                        "Records Read",
                    )

                    output_metrics = metrics.get(
                        "Output Metrics",
                        {},
                    )

                    output_bytes += numeric_value(
                        output_metrics,
                        "Bytes Written",
                    )

                    output_records += numeric_value(
                        output_metrics,
                        "Records Written",
                    )

                    shuffle_read = metrics.get(
                        "Shuffle Read Metrics",
                        {},
                    )

                    shuffle_read_bytes += (
                        numeric_value(
                            shuffle_read,
                            "Remote Bytes Read",
                        )
                        + numeric_value(
                            shuffle_read,
                            "Local Bytes Read",
                        )
                    )

                    shuffle_read_records += max(
    numeric_value(
        shuffle_read,
        "Total Records Read",
    ),
    numeric_value(
        shuffle_read,
        "Records Read",
    ),
)

                    shuffle_write = metrics.get(
                        "Shuffle Write Metrics",
                        {},
                    )

                    shuffle_write_bytes += numeric_value(
                        shuffle_write,
                        "Shuffle Bytes Written",
                    )

                    shuffle_write_records += numeric_value(
                        shuffle_write,
                        "Shuffle Records Written",
                    )

    mib = 1024 ** 2

    result = {
        "event_files": len(event_files),
        "spark_jobs": len(job_ids),
        "completed_stage_attempts": len(completed_stages),
        "scheduled_stage_tasks": scheduled_stage_tasks,
        "successful_task_attempts": successful_tasks,
        "failed_task_attempts": failed_tasks,
        "sql_executions": sql_execution_count,

        "input_bytes_read": input_bytes,
        "input_megabytes_read": round(
            input_bytes / mib,
            3,
        ),
        "input_records_read": input_records,

        "output_bytes_written": output_bytes,
        "output_megabytes_written": round(
            output_bytes / mib,
            3,
        ),
        "output_records_written": output_records,

        "shuffle_read_bytes": shuffle_read_bytes,
        "shuffle_read_megabytes": round(
            shuffle_read_bytes / mib,
            3,
        ),
        "shuffle_read_records": shuffle_read_records,

        "shuffle_write_bytes": shuffle_write_bytes,
        "shuffle_write_megabytes": round(
            shuffle_write_bytes / mib,
            3,
        ),
        "shuffle_write_records": shuffle_write_records,

        "memory_bytes_spilled": memory_bytes_spilled,
        "memory_megabytes_spilled": round(
            memory_bytes_spilled / mib,
            3,
        ),

        "disk_bytes_spilled": disk_bytes_spilled,
        "disk_megabytes_spilled": round(
            disk_bytes_spilled / mib,
            3,
        ),

        "peak_execution_memory_bytes": (
            peak_execution_memory
        ),
        "peak_execution_memory_megabytes": round(
            peak_execution_memory / mib,
            3,
        ),

        "executor_run_time_seconds": round(
            executor_run_time_ms / 1000,
            3,
        ),

        "executor_cpu_time_seconds": round(
            executor_cpu_time_ns / 1_000_000_000,
            3,
        ),

        "malformed_event_lines": malformed_lines,
    }

    with open(
        args.output,
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            result,
            output_file,
            indent=2,
        )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
