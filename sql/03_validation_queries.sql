-- Validation Queries
-- NYC Taxi and Hourly Weather Project
-- Expected values reflect the completed 2024 pipeline.

-- ==================================================
-- 1. January joined-data validation
-- Expected summarized_trip_rows: 2,857,438
-- ==================================================
SELECT
    SUM(trip_count) AS summarized_trip_rows
FROM cs675_taxi_weather_project.weather_summary;


-- ==================================================
-- 2. Full-year joined-trip coverage
-- Expected:
--   first_date = 2024-01-01
--   last_date = 2024-12-31
--   month_count = 12
--   joined_trip_rows = 39,503,323
-- ==================================================
SELECT
    MIN(pickup_date) AS first_date,
    MAX(pickup_date) AS last_date,
    COUNT(DISTINCT pickup_month) AS month_count,
    COUNT(*) AS joined_trip_rows
FROM cs675_taxi_weather_project.full_year_joined_trips_2024;


-- ==================================================
-- 3. Full-year weather summary
-- Expected:
--   weather_categories = 4
--   summarized_trip_rows = 39,503,323
-- ==================================================
SELECT
    COUNT(*) AS weather_categories,
    SUM(trip_count) AS summarized_trip_rows
FROM cs675_taxi_weather_project.full_year_weather_summary_2024;


-- ==================================================
-- 4. Monthly weather summary
-- Expected:
--   month_count = 12
--   summarized_trip_rows = 39,503,323
-- ==================================================
SELECT
    COUNT(DISTINCT pickup_month) AS month_count,
    COUNT(*) AS month_weather_combinations,
    SUM(trip_count) AS summarized_trip_rows
FROM cs675_taxi_weather_project.full_year_monthly_weather_summary_2024;


-- ==================================================
-- 5. Seasonal weather summary
-- Expected:
--   season_count = 4
--   summarized_trip_rows = 39,503,323
-- ==================================================
SELECT
    COUNT(DISTINCT season) AS season_count,
    COUNT(*) AS season_weather_combinations,
    SUM(trip_count) AS summarized_trip_rows
FROM cs675_taxi_weather_project.full_year_seasonal_weather_summary_2024;


-- ==================================================
-- 6. Hourly demand validation
-- Expected:
--   first_date = 2024-01-01
--   last_date = 2024-12-31
--   date_count = 366
--   hourly_weather_rows = 8,783
--   summarized_trip_rows = 39,503,323
-- ==================================================
SELECT
    MIN(pickup_date) AS first_date,
    MAX(pickup_date) AS last_date,
    COUNT(DISTINCT pickup_date) AS date_count,
    COUNT(*) AS hourly_weather_rows,
    SUM(trip_count) AS summarized_trip_rows
FROM cs675_taxi_weather_project.full_year_hourly_demand_2024;


-- ==================================================
-- 7. Borough-weather summary validation
-- Expected summarized_trip_rows: 39,503,323
-- ==================================================
SELECT
    COUNT(DISTINCT pickup_borough) AS borough_count,
    COUNT(*) AS borough_weather_combinations,
    SUM(trip_count) AS summarized_trip_rows
FROM cs675_taxi_weather_project.full_year_borough_weather_summary_2024;


-- ==================================================
-- 8. Zone-weather summary validation
-- Expected summarized_trip_rows: 39,503,323
-- ==================================================
SELECT
    COUNT(DISTINCT pickup_zone) AS zone_count,
    COUNT(*) AS zone_weather_combinations,
    SUM(trip_count) AS summarized_trip_rows
FROM cs675_taxi_weather_project.full_year_zone_weather_summary_2024;


-- ==================================================
-- 9. Monthly tipping summary validation
-- Only payment_type = 1 credit-card trips are included.
-- Expected:
--   month_count = 12
--   month_weather_combinations = 38
--   summarized_credit_card_trips = 30,107,385
-- ==================================================
SELECT
    COUNT(DISTINCT pickup_month) AS month_count,
    COUNT(*) AS month_weather_combinations,
    SUM(credit_card_trip_count) AS summarized_credit_card_trips
FROM cs675_taxi_weather_project.full_year_monthly_tipping_summary_2024;


-- ==================================================
-- 10. Cross-table trip-count consistency
-- All five totals should equal 39,503,323.
-- ==================================================
SELECT
    (
        SELECT COUNT(*)
        FROM cs675_taxi_weather_project.full_year_joined_trips_2024
    ) AS joined_trip_rows,

    (
        SELECT SUM(trip_count)
        FROM cs675_taxi_weather_project.full_year_weather_summary_2024
    ) AS weather_summary_rows,

    (
        SELECT SUM(trip_count)
        FROM cs675_taxi_weather_project.full_year_monthly_weather_summary_2024
    ) AS monthly_summary_rows,

    (
        SELECT SUM(trip_count)
        FROM cs675_taxi_weather_project.full_year_seasonal_weather_summary_2024
    ) AS seasonal_summary_rows,

    (
        SELECT SUM(trip_count)
        FROM cs675_taxi_weather_project.full_year_hourly_demand_2024
    ) AS hourly_summary_rows;
