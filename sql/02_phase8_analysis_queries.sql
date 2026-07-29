-- Phase 8 Athena Analysis Queries
-- NYC Taxi and Hourly Weather Project
-- Exported from Athena saved queries

-- ==================================================
-- 08_01_january_vs_full_year_weather
-- Compares January and full-year 2024 taxi demand, fares, duration, distance, tipping, and weather conditions.
-- ==================================================
WITH comparison AS (
    SELECT
        'January 2024' AS analysis_period,
        weather_condition,
        trip_count,
        avg_fare_amount,
        avg_total_amount,
        avg_duration_minutes,
        avg_distance_miles,
        avg_card_tip_percentage,
        avg_temperature_c,
        avg_precipitation_mm
    FROM cs675_taxi_weather_project.weather_summary

    UNION ALL

    SELECT
        'Full Year 2024' AS analysis_period,
        weather_condition,
        trip_count,
        avg_fare_amount,
        avg_total_amount,
        avg_duration_minutes,
        avg_distance_miles,
        avg_card_tip_percentage,
        avg_temperature_c,
        avg_precipitation_mm
    FROM cs675_taxi_weather_project.full_year_weather_summary_2024
)
SELECT
    analysis_period,
    weather_condition,
    trip_count,
    ROUND(
        100.0 * trip_count
        / SUM(trip_count) OVER (
            PARTITION BY analysis_period
        ),
        2
    ) AS trip_share_pct,
    avg_fare_amount,
    avg_total_amount,
    avg_duration_minutes,
    avg_distance_miles,
    avg_card_tip_percentage,
    avg_temperature_c,
    avg_precipitation_mm
FROM comparison
ORDER BY
    CASE analysis_period
        WHEN 'January 2024' THEN 1
        WHEN 'Full Year 2024' THEN 2
        ELSE 3
    END,
    CASE weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_02_monthly_weather_trends
-- Shows monthly taxi demand shares, fares, duration, distance, tipping, temperature, and precipitation by hourly weather condition in 2024.
-- ==================================================
WITH monthly_totals AS (
    SELECT
        pickup_month,
        SUM(trip_count) AS monthly_total_trips
    FROM cs675_taxi_weather_project.full_year_monthly_weather_summary_2024
    GROUP BY pickup_month
)
SELECT
    m.pickup_month,
    m.weather_condition,
    m.trip_count,
    ROUND(
        100.0 * m.trip_count
        / NULLIF(t.monthly_total_trips, 0),
        2
    ) AS monthly_trip_share_pct,
    m.avg_fare_amount,
    m.avg_total_amount,
    m.avg_duration_minutes,
    m.avg_distance_miles,
    m.avg_card_tip_percentage,
    m.avg_temperature_c,
    m.avg_precipitation_mm
FROM cs675_taxi_weather_project.full_year_monthly_weather_summary_2024 m
JOIN monthly_totals t
    ON m.pickup_month = t.pickup_month
ORDER BY
    m.pickup_month,
    CASE m.weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_03_seasonal_weather_comparison
-- Compares seasonal taxi demand shares, fares, duration, distance, tipping, temperature, and precipitation across hourly weather conditions in 2024.
-- ==================================================
WITH seasonal_totals AS (
    SELECT
        season,
        SUM(trip_count) AS seasonal_total_trips
    FROM cs675_taxi_weather_project.full_year_seasonal_weather_summary_2024
    GROUP BY season
)
SELECT
    s.season,
    s.weather_condition,
    s.trip_count,
    ROUND(
        100.0 * s.trip_count
        / NULLIF(t.seasonal_total_trips, 0),
        2
    ) AS seasonal_trip_share_pct,
    s.avg_fare_amount,
    s.avg_total_amount,
    s.avg_duration_minutes,
    s.avg_distance_miles,
    s.avg_card_tip_percentage,
    s.avg_temperature_c,
    s.avg_precipitation_mm
FROM cs675_taxi_weather_project.full_year_seasonal_weather_summary_2024 s
JOIN seasonal_totals t
    ON s.season = t.season
ORDER BY
    CASE s.season
        WHEN 'Winter' THEN 1
        WHEN 'Spring' THEN 2
        WHEN 'Summer' THEN 3
        WHEN 'Fall' THEN 4
        ELSE 5
    END,
    CASE s.weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_04_hourly_demand_by_weather
-- Compares hourly taxi demand, fares, trip duration, and tipping across hourly weather conditions in 2024.
-- ==================================================
WITH hourly_weather AS (
    SELECT
        pickup_hour_of_day,
        weather_condition,
        SUM(trip_count) AS total_trips,
        COUNT(*) AS observed_date_hours,
        ROUND(
            AVG(trip_count),
            2
        ) AS avg_trips_per_observed_hour,
        ROUND(
            SUM(avg_fare_amount * trip_count)
            / NULLIF(SUM(trip_count), 0),
            2
        ) AS weighted_avg_fare_amount,
        ROUND(
            SUM(avg_duration_minutes * trip_count)
            / NULLIF(SUM(trip_count), 0),
            2
        ) AS weighted_avg_duration_minutes,
        ROUND(
            AVG(avg_card_tip_percentage),
            2
        ) AS avg_hourly_card_tip_percentage
    FROM cs675_taxi_weather_project.full_year_hourly_demand_2024
    GROUP BY
        pickup_hour_of_day,
        weather_condition
),
hour_totals AS (
    SELECT
        pickup_hour_of_day,
        SUM(total_trips) AS hour_total_trips
    FROM hourly_weather
    GROUP BY pickup_hour_of_day
)
SELECT
    h.pickup_hour_of_day,
    h.weather_condition,
    h.total_trips,
    h.observed_date_hours,
    h.avg_trips_per_observed_hour,
    ROUND(
        100.0 * h.total_trips
        / NULLIF(t.hour_total_trips, 0),
        2
    ) AS hour_weather_share_pct,
    h.weighted_avg_fare_amount,
    h.weighted_avg_duration_minutes,
    h.avg_hourly_card_tip_percentage
FROM hourly_weather h
JOIN hour_totals t
    ON h.pickup_hour_of_day = t.pickup_hour_of_day
ORDER BY
    h.pickup_hour_of_day,
    CASE h.weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_05_borough_weather_impact
-- Compares taxi demand share, fares, trip duration, and credit-card tipping by weather condition across NYC’s five boroughs, using dry weather as the baseline.
-- ==================================================
WITH five_boroughs AS (
    SELECT
        pickup_borough,
        weather_condition,
        trip_count,
        avg_fare_amount,
        avg_duration_minutes,
        avg_card_tip_percentage
    FROM cs675_taxi_weather_project.full_year_borough_weather_summary_2024
    WHERE pickup_borough IN (
        'Bronx',
        'Brooklyn',
        'Manhattan',
        'Queens',
        'Staten Island'
    )
),
borough_totals AS (
    SELECT
        pickup_borough,
        SUM(trip_count) AS borough_total_trips
    FROM five_boroughs
    GROUP BY pickup_borough
),
dry_baseline AS (
    SELECT
        pickup_borough,
        avg_fare_amount AS dry_avg_fare,
        avg_duration_minutes AS dry_avg_duration,
        avg_card_tip_percentage AS dry_avg_tip_percentage
    FROM five_boroughs
    WHERE weather_condition = 'Dry'
)
SELECT
    b.pickup_borough,
    b.weather_condition,
    b.trip_count,
    ROUND(
        100.0 * b.trip_count
        / NULLIF(t.borough_total_trips, 0),
        2
    ) AS borough_trip_share_pct,
    b.avg_fare_amount,
    ROUND(
        b.avg_fare_amount - d.dry_avg_fare,
        2
    ) AS fare_change_vs_dry,
    b.avg_duration_minutes,
    ROUND(
        b.avg_duration_minutes - d.dry_avg_duration,
        2
    ) AS duration_change_vs_dry,
    b.avg_card_tip_percentage,
    ROUND(
        b.avg_card_tip_percentage
        - d.dry_avg_tip_percentage,
        2
    ) AS tip_pct_change_vs_dry
FROM five_boroughs b
JOIN borough_totals t
    ON b.pickup_borough = t.pickup_borough
JOIN dry_baseline d
    ON b.pickup_borough = d.pickup_borough
ORDER BY
    CASE b.pickup_borough
        WHEN 'Bronx' THEN 1
        WHEN 'Brooklyn' THEN 2
        WHEN 'Manhattan' THEN 3
        WHEN 'Queens' THEN 4
        WHEN 'Staten Island' THEN 5
        ELSE 6
    END,
    CASE b.weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_06_daily_vs_hourly_weather_classification
-- Compares January 2024 daily and hourly weather joins, including weather-based trip classification, trip shares, fares, duration, tipping, temperature, and precipitation.
-- ==================================================
WITH hourly_results AS (
    SELECT
        weather_condition,
        trip_count,
        avg_fare_amount,
        avg_total_amount,
        avg_duration_minutes,
        avg_distance_miles,
        avg_card_tip_percentage,
        avg_temperature_c,
        avg_precipitation_mm
    FROM cs675_taxi_weather_project.weather_summary
),

daily_results AS (
    SELECT
        weather_condition,
        trip_count,
        avg_fare_amount,
        avg_total_amount,
        avg_duration_minutes,
        avg_distance_miles,
        avg_card_tip_percentage,
        avg_temperature_c,
        avg_precipitation_mm
    FROM cs675_taxi_weather_project.daily_join_weather_summary_january_2024
),

hourly_total AS (
    SELECT
        SUM(trip_count) AS total_trips
    FROM hourly_results
),

daily_total AS (
    SELECT
        SUM(trip_count) AS total_trips
    FROM daily_results
)

SELECT
    COALESCE(
        h.weather_condition,
        d.weather_condition
    ) AS weather_condition,

    COALESCE(
        h.trip_count,
        0
    ) AS hourly_trip_count,

    COALESCE(
        d.trip_count,
        0
    ) AS daily_trip_count,

    COALESCE(
        d.trip_count,
        0
    ) - COALESCE(
        h.trip_count,
        0
    ) AS trip_count_difference,

    ROUND(
        100.0 * COALESCE(h.trip_count, 0)
        / NULLIF(ht.total_trips, 0),
        2
    ) AS hourly_trip_share_pct,

    ROUND(
        100.0 * COALESCE(d.trip_count, 0)
        / NULLIF(dt.total_trips, 0),
        2
    ) AS daily_trip_share_pct,

    ROUND(
        (
            100.0 * COALESCE(d.trip_count, 0)
            / NULLIF(dt.total_trips, 0)
        )
        -
        (
            100.0 * COALESCE(h.trip_count, 0)
            / NULLIF(ht.total_trips, 0)
        ),
        2
    ) AS trip_share_change_pct_points,

    h.avg_fare_amount AS hourly_avg_fare,
    d.avg_fare_amount AS daily_avg_fare,

    ROUND(
        d.avg_fare_amount
        - h.avg_fare_amount,
        2
    ) AS fare_difference,

    h.avg_duration_minutes AS hourly_avg_duration,
    d.avg_duration_minutes AS daily_avg_duration,

    ROUND(
        d.avg_duration_minutes
        - h.avg_duration_minutes,
        2
    ) AS duration_difference,

    h.avg_card_tip_percentage AS hourly_avg_tip_pct,
    d.avg_card_tip_percentage AS daily_avg_tip_pct,

    ROUND(
        d.avg_card_tip_percentage
        - h.avg_card_tip_percentage,
        2
    ) AS tip_pct_difference,

    h.avg_temperature_c AS hourly_avg_temperature_c,
    d.avg_temperature_c AS daily_avg_temperature_c,

    h.avg_precipitation_mm AS hourly_avg_precipitation_mm,
    d.avg_precipitation_mm AS daily_avg_precipitation_mm

FROM hourly_results h
FULL OUTER JOIN daily_results d
    ON h.weather_condition = d.weather_condition
CROSS JOIN hourly_total ht
CROSS JOIN daily_total dt

ORDER BY
    CASE COALESCE(
        h.weather_condition,
        d.weather_condition
    )
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_07_intraday_weather_changes
-- Identifies 2024 dates with multiple hourly weather conditions and measures how frequently daily weather aggregation would hide intraday weather changes.
-- ==================================================
WITH daily_conditions AS (
    SELECT
        pickup_date,
        COUNT(
            DISTINCT weather_condition
        ) AS weather_condition_count,
        ARRAY_JOIN(
            ARRAY_SORT(
                ARRAY_AGG(
                    DISTINCT weather_condition
                )
            ),
            ', '
        ) AS weather_conditions,
        SUM(trip_count) AS total_trips
    FROM cs675_taxi_weather_project.full_year_hourly_demand_2024
    GROUP BY pickup_date
),

annual_summary AS (
    SELECT
        COUNT(*) AS total_days,
        SUM(
            CASE
                WHEN weather_condition_count > 1
                    THEN 1
                ELSE 0
            END
        ) AS multi_weather_days
    FROM daily_conditions
)

SELECT
    d.pickup_date,
    d.weather_condition_count,
    d.weather_conditions,
    d.total_trips,
    s.total_days,
    s.multi_weather_days,
    ROUND(
        100.0 * s.multi_weather_days
        / NULLIF(s.total_days, 0),
        2
    ) AS multi_weather_day_pct
FROM daily_conditions d
CROSS JOIN annual_summary s
WHERE d.weather_condition_count > 1
ORDER BY d.pickup_date;

-- ==================================================
-- 08_08_rush_hour_weather_impact
-- Compares rush-hour and non-rush-hour taxi demand, fares, trip duration, and tipping across hourly weather conditions in 2024, using dry weather as the baseline.
-- ==================================================
WITH classified_hours AS (
    SELECT
        pickup_date,
        pickup_hour_of_day,
        weather_condition,
        trip_count,
        avg_fare_amount,
        avg_duration_minutes,
        avg_card_tip_percentage,
        CASE
            WHEN day_of_week(pickup_date) BETWEEN 1 AND 5
                 AND (
                     pickup_hour_of_day BETWEEN 7 AND 9
                     OR pickup_hour_of_day BETWEEN 16 AND 19
                 )
                THEN 'Rush Hour'
            ELSE 'Non-Rush Hour'
        END AS time_period
    FROM cs675_taxi_weather_project.full_year_hourly_demand_2024
),

period_weather_summary AS (
    SELECT
        time_period,
        weather_condition,
        SUM(trip_count) AS total_trips,
        COUNT(*) AS observed_hour_rows,

        ROUND(
            1.0 * SUM(trip_count)
            / NULLIF(COUNT(*), 0),
            2
        ) AS avg_trips_per_observed_hour,

        ROUND(
            SUM(avg_fare_amount * trip_count)
            / NULLIF(SUM(trip_count), 0),
            2
        ) AS weighted_avg_fare_amount,

        ROUND(
            SUM(avg_duration_minutes * trip_count)
            / NULLIF(SUM(trip_count), 0),
            2
        ) AS weighted_avg_duration_minutes,

        ROUND(
            AVG(avg_card_tip_percentage),
            2
        ) AS avg_hourly_card_tip_percentage
    FROM classified_hours
    GROUP BY
        time_period,
        weather_condition
),

dry_baseline AS (
    SELECT
        time_period,
        avg_trips_per_observed_hour AS dry_avg_trips_per_hour,
        weighted_avg_fare_amount AS dry_avg_fare,
        weighted_avg_duration_minutes AS dry_avg_duration,
        avg_hourly_card_tip_percentage AS dry_avg_tip_percentage
    FROM period_weather_summary
    WHERE weather_condition = 'Dry'
)

SELECT
    p.time_period,
    p.weather_condition,
    p.total_trips,
    p.observed_hour_rows,
    p.avg_trips_per_observed_hour,

    ROUND(
        p.avg_trips_per_observed_hour
        - d.dry_avg_trips_per_hour,
        2
    ) AS trips_per_hour_change_vs_dry,

    p.weighted_avg_fare_amount,

    ROUND(
        p.weighted_avg_fare_amount
        - d.dry_avg_fare,
        2
    ) AS fare_change_vs_dry,

    p.weighted_avg_duration_minutes,

    ROUND(
        p.weighted_avg_duration_minutes
        - d.dry_avg_duration,
        2
    ) AS duration_change_vs_dry,

    p.avg_hourly_card_tip_percentage,

    ROUND(
        p.avg_hourly_card_tip_percentage
        - d.dry_avg_tip_percentage,
        2
    ) AS tip_pct_change_vs_dry

FROM period_weather_summary p
JOIN dry_baseline d
    ON p.time_period = d.time_period

ORDER BY
    CASE p.time_period
        WHEN 'Rush Hour' THEN 1
        WHEN 'Non-Rush Hour' THEN 2
        ELSE 3
    END,
    CASE p.weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_09_monthly_tipping_by_weather
-- Compares monthly credit-card tipping behavior across hourly weather conditions in 2024, including tip amount, tip percentage, and no-tip rate relative to dry-weather baselines.
-- ==================================================
WITH monthly_totals AS (
    SELECT
        pickup_year,
        pickup_month,
        SUM(credit_card_trip_count) AS monthly_credit_card_trips
    FROM cs675_taxi_weather_project.full_year_monthly_tipping_summary_2024
    GROUP BY
        pickup_year,
        pickup_month
),

monthly_dry_baseline AS (
    SELECT
        pickup_year,
        pickup_month,
        avg_tip_amount AS dry_avg_tip_amount,
        avg_tip_percentage AS dry_avg_tip_percentage,
        no_tip_rate_pct AS dry_no_tip_rate_pct
    FROM cs675_taxi_weather_project.full_year_monthly_tipping_summary_2024
    WHERE weather_condition = 'Dry'
)

SELECT
    t.pickup_year,
    t.pickup_month,
    t.weather_condition,
    t.credit_card_trip_count,

    ROUND(
        100.0 * t.credit_card_trip_count
        / NULLIF(m.monthly_credit_card_trips, 0),
        2
    ) AS monthly_credit_card_trip_share_pct,

    t.avg_tip_amount,

    ROUND(
        t.avg_tip_amount
        - d.dry_avg_tip_amount,
        2
    ) AS tip_amount_change_vs_dry,

    t.avg_tip_percentage,

    ROUND(
        t.avg_tip_percentage
        - d.dry_avg_tip_percentage,
        2
    ) AS tip_percentage_change_vs_dry,

    t.no_tip_rate_pct,

    ROUND(
        t.no_tip_rate_pct
        - d.dry_no_tip_rate_pct,
        2
    ) AS no_tip_rate_change_vs_dry

FROM cs675_taxi_weather_project.full_year_monthly_tipping_summary_2024 t

JOIN monthly_totals m
    ON t.pickup_year = m.pickup_year
    AND t.pickup_month = m.pickup_month

JOIN monthly_dry_baseline d
    ON t.pickup_year = d.pickup_year
    AND t.pickup_month = d.pickup_month

ORDER BY
    t.pickup_month,
    CASE t.weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;

-- ==================================================
-- 08_10_top_zone_weather_sensitivity
-- Analyzes weather-related demand, fare, and trip-duration changes for the ten highest-demand NYC taxi zones in 2024, using dry weather as the baseline.
-- ==================================================
WITH zone_totals AS (
    SELECT
        pickup_borough,
        pickup_zone,
        SUM(trip_count) AS zone_total_trips
    FROM cs675_taxi_weather_project.full_year_zone_weather_summary_2024
    WHERE pickup_borough NOT IN ('Unknown', 'N/A', 'EWR')
      AND pickup_zone NOT IN ('Unknown', 'N/A')
    GROUP BY
        pickup_borough,
        pickup_zone
),

top_zones AS (
    SELECT
        pickup_borough,
        pickup_zone,
        zone_total_trips
    FROM zone_totals
    ORDER BY zone_total_trips DESC
    LIMIT 10
),

dry_baseline AS (
    SELECT
        pickup_borough,
        pickup_zone,
        avg_fare_amount AS dry_avg_fare_amount,
        avg_duration_minutes AS dry_avg_duration_minutes
    FROM cs675_taxi_weather_project.full_year_zone_weather_summary_2024
    WHERE weather_condition = 'Dry'
)

SELECT
    z.pickup_borough,
    z.pickup_zone,
    z.weather_condition,
    t.zone_total_trips,
    z.trip_count,

    ROUND(
        100.0 * z.trip_count
        / NULLIF(t.zone_total_trips, 0),
        2
    ) AS zone_weather_trip_share_pct,

    z.avg_fare_amount,

    ROUND(
        z.avg_fare_amount
        - d.dry_avg_fare_amount,
        2
    ) AS fare_change_vs_dry,

    z.avg_duration_minutes,

    ROUND(
        z.avg_duration_minutes
        - d.dry_avg_duration_minutes,
        2
    ) AS duration_change_vs_dry

FROM cs675_taxi_weather_project.full_year_zone_weather_summary_2024 z

JOIN top_zones t
    ON z.pickup_borough = t.pickup_borough
    AND z.pickup_zone = t.pickup_zone

JOIN dry_baseline d
    ON z.pickup_borough = d.pickup_borough
    AND z.pickup_zone = d.pickup_zone

ORDER BY
    t.zone_total_trips DESC,
    CASE z.weather_condition
        WHEN 'Dry' THEN 1
        WHEN 'Rain' THEN 2
        WHEN 'Snow' THEN 3
        WHEN 'Heavy Rain' THEN 4
        ELSE 5
    END;
