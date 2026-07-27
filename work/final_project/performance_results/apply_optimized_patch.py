from pathlib import Path


SCRIPT_PATH = Path(
    "/home/jovyan/work/final_project/"
    "05_cloud_taxi_weather_analysis.py"
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
    encoding="utf-8",
)

text = text.replace(
    "\r\n",
    "\n",
)


# Replace three separate count actions with one aggregation.
quality_start = (
    "    cleaned_count = joined.count()\n"
)

quality_end = (
    "    zone_match_rate = (\n"
)

quality_replacement = '''    quality_counts = (
        joined
        .agg(
            F.count("*").alias(
                "cleaned_count"
            ),
            F.sum(
                F.when(
                    F.col(
                        "pickup_zone"
                    ).isNotNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "zone_match_count"
            ),
            F.sum(
                F.when(
                    F.col(
                        "temperature_c"
                    ).isNotNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "weather_match_count"
            ),
        )
        .first()
    )

    cleaned_count = int(
        quality_counts[
            "cleaned_count"
        ]
    )

    zone_match_count = int(
        quality_counts[
            "zone_match_count"
        ]
    )

    weather_match_count = int(
        quality_counts[
            "weather_match_count"
        ]
    )

'''

text = replace_between(
    text,
    quality_start,
    quality_end,
    quality_replacement,
)


# Remove the two unnecessary show() actions.
show_start = (
    '    print("\\n=== Weather summary ===")\n'
)

show_end = (
    "    # Write joined dataset\n"
)

text = replace_between(
    text,
    show_start,
    show_end,
    show_end,
)


required_fragments = [
    "quality_counts = (",
    "F.broadcast(zones)",
    "F.broadcast(weather)",
    ".persist(",
    "joined.unpersist()",
    '"spark.sql.adaptive.enabled", "true"',
    (
        '"spark.sql.adaptive.'
        'coalescePartitions.enabled",'
    ),
]

for fragment in required_fragments:
    if fragment not in text:
        raise RuntimeError(
            "Required fragment is missing: "
            f"{fragment}"
        )


forbidden_fragments = [
    "cleaned_count = joined.count()",
    "weather_summary.show(",
    "borough_weather_summary.show(",
]

for fragment in forbidden_fragments:
    if fragment in text:
        raise RuntimeError(
            "Unwanted fragment remains: "
            f"{fragment}"
        )


SCRIPT_PATH.write_text(
    text,
    encoding="utf-8",
)

print(
    "Optimized script patched successfully."
)
