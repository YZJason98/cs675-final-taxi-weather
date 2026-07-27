from pathlib import Path


SCRIPT_PATH = Path(
    "/home/jovyan/work/final_project/"
    "05b_daily_weather_join_analysis.py"
)


def replace_between(
    text: str,
    start_marker: str,
    end_marker: str,
    replacement: str,
) -> str:
    start_index = text.find(start_marker)

    if start_index == -1:
        raise RuntimeError(
            f"Start marker not found: {start_marker}"
        )

    end_index = text.find(
        end_marker,
        start_index + len(start_marker),
    )

    if end_index == -1:
        raise RuntimeError(
            f"End marker not found: {end_marker}"
        )

    return (
        text[:start_index]
        + replacement
        + text[end_index:]
    )


text = SCRIPT_PATH.read_text(
    encoding="utf-8"
).replace(
    "\r\n",
    "\n",
)


# Give the Daily Join job a separate Spark application name.
text = text.replace(
    '.appName("cs675-cloud-taxi-weather-analysis")',
    '.appName("cs675-daily-weather-join-analysis")',
)


# Rename the section comment.
text = text.replace(
    "# Prepare hourly weather categories",
    "# Prepare daily weather aggregation",
)


weather_start = (
    "    weather_text = F.lower(\n"
)

weather_end = (
    "    joined = (\n"
)

daily_weather_code = '''    weather_text = F.lower(
        F.coalesce(
            F.col("present_weather"),
            F.lit(""),
        )
    )

    # First classify every hourly weather observation.
    hourly_weather = (
        weather_raw
        .select(
            "weather_hour",
            "temperature_c",
            "precipitation_mm",
            "precipitation_trace",
            "relative_humidity_pct",
            "wind_speed_mps",
            "present_weather",
            "hour_imputed",
        )
        .withColumn(
            "weather_condition",
            F.when(
                weather_text.rlike("snow|sn"),
                F.lit("Snow"),
            )
            .when(
                (
                    F.col("temperature_c")
                    <= 1.0
                )
                & (
                    F.col("precipitation_mm")
                    > 0
                ),
                F.lit("Snow"),
            )
            .when(
                F.col("precipitation_mm")
                >= 2.5,
                F.lit("Heavy Rain"),
            )
            .when(
                (
                    F.col("precipitation_mm")
                    > 0
                )
                | F.col(
                    "precipitation_trace"
                ),
                F.lit("Rain"),
            )
            .otherwise(
                F.lit("Dry")
            ),
        )
        .dropDuplicates(
            ["weather_hour"]
        )
    )

    # Aggregate 744 January hourly records into 31 daily records.
    weather = (
        hourly_weather
        .withColumn(
            "weather_date",
            F.to_date("weather_hour"),
        )
        .withColumn(
            "is_snow",
            (
                F.col("weather_condition")
                == "Snow"
            ).cast("int"),
        )
        .withColumn(
            "is_heavy_rain",
            (
                F.col("weather_condition")
                == "Heavy Rain"
            ).cast("int"),
        )
        .withColumn(
            "is_rain",
            (
                F.col("weather_condition")
                == "Rain"
            ).cast("int"),
        )
        .groupBy(
            "weather_date"
        )
        .agg(
            F.avg(
                "temperature_c"
            ).alias(
                "temperature_c"
            ),
            F.sum(
                "precipitation_mm"
            ).alias(
                "precipitation_mm"
            ),
            F.max(
                F.col(
                    "precipitation_trace"
                ).cast("int")
            )
            .cast("boolean")
            .alias(
                "precipitation_trace"
            ),
            F.avg(
                "relative_humidity_pct"
            ).alias(
                "relative_humidity_pct"
            ),
            F.avg(
                "wind_speed_mps"
            ).alias(
                "wind_speed_mps"
            ),
            F.concat_ws(
                " | ",
                F.sort_array(
                    F.collect_set(
                        "present_weather"
                    )
                ),
            ).alias(
                "present_weather"
            ),
            F.max(
                F.col(
                    "hour_imputed"
                ).cast("int")
            )
            .cast("boolean")
            .alias(
                "hour_imputed"
            ),
            F.max(
                "is_snow"
            ).alias(
                "has_snow"
            ),
            F.max(
                "is_heavy_rain"
            ).alias(
                "has_heavy_rain"
            ),
            F.max(
                "is_rain"
            ).alias(
                "has_rain"
            ),
        )
        .withColumn(
            "weather_condition",
            F.when(
                F.col("has_snow") == 1,
                F.lit("Snow"),
            )
            .when(
                F.col(
                    "has_heavy_rain"
                ) == 1,
                F.lit("Heavy Rain"),
            )
            .when(
                F.col("has_rain") == 1,
                F.lit("Rain"),
            )
            .otherwise(
                F.lit("Dry")
            ),
        )
        .drop(
            "has_snow",
            "has_heavy_rain",
            "has_rain",
        )
    )

    daily_weather_count = weather.count()

    print(
        f"Daily weather rows: "
        f"{daily_weather_count:,}"
    )

'''

text = replace_between(
    text,
    weather_start,
    weather_end,
    daily_weather_code,
)


join_start = (
    "    joined = (\n"
)

join_end = (
    "    quality_counts = (\n"
)

daily_join_code = '''    joined = (
        taxi
        .join(
            F.broadcast(zones),
            on="pickup_location_id",
            how="left",
        )
        .join(
            F.broadcast(weather),
            taxi.pickup_date
            == weather.weather_date,
            how="left",
        )
        .drop(
            weather.weather_date
        )
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

'''

text = replace_between(
    text,
    join_start,
    join_end,
    daily_join_code,
)


required_fragments = [
    "daily_weather_count = weather.count()",
    "taxi.pickup_date",
    "weather.weather_date",
    "F.broadcast(weather)",
    ".persist(",
    "joined.unpersist()",
    "groupBy(",
]

for fragment in required_fragments:
    if fragment not in text:
        raise RuntimeError(
            "Required Daily Join fragment "
            f"is missing: {fragment}"
        )


forbidden_fragments = [
    "taxi.pickup_hour\n            == weather.weather_hour",
    ".drop(weather.weather_hour)",
]

for fragment in forbidden_fragments:
    if fragment in text:
        raise RuntimeError(
            "Old Hourly Join fragment remains: "
            f"{fragment}"
        )


SCRIPT_PATH.write_text(
    text,
    encoding="utf-8",
)

print(
    "Daily weather join script patched successfully."
)
