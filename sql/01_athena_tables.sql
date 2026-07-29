-- Athena External Table Definitions
-- NYC Taxi and Hourly Weather Project
-- Generated from AWS Glue Data Catalog

CREATE DATABASE IF NOT EXISTS cs675_taxi_weather_project;

-- ==================================================
-- Table: borough_weather_summary
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/borough_weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.borough_weather_summary (
    pickup_borough string,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_duration_minutes double,
    avg_card_tip_percentage double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/borough_weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: daily_join_weather_summary_january_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/performance_comparison/daily_join/january_2024/weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.daily_join_weather_summary_january_2024 (
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_total_amount double,
    avg_duration_minutes double,
    avg_distance_miles double,
    avg_card_tip_percentage double,
    avg_temperature_c double,
    avg_precipitation_mm double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/performance_comparison/daily_join/january_2024/weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_borough_weather_summary_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/borough_weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_borough_weather_summary_2024 (
    pickup_borough string,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_duration_minutes double,
    avg_card_tip_percentage double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/borough_weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_hourly_demand_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/hourly_demand
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_hourly_demand_2024 (
    pickup_date date,
    pickup_hour_of_day int,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_duration_minutes double,
    avg_card_tip_percentage double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/hourly_demand'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_joined_trips_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/joined_trips
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_joined_trips_2024 (
    pickup_timestamp timestamp,
    dropoff_timestamp timestamp,
    pickup_hour timestamp,
    pickup_date date,
    pickup_day int,
    pickup_day_of_week string,
    season string,
    pickup_hour_of_day int,
    pickup_location_id int,
    pickup_borough string,
    pickup_zone string,
    pickup_service_zone string,
    passenger_count double,
    trip_distance_miles double,
    trip_duration_minutes double,
    payment_type int,
    fare_amount double,
    tip_amount double,
    total_amount double,
    card_tip_percentage double,
    temperature_c double,
    precipitation_mm double,
    precipitation_trace boolean,
    relative_humidity_pct double,
    wind_speed_mps double,
    present_weather string,
    hour_imputed boolean,
    weather_condition string
)
PARTITIONED BY (
    pickup_year int,
    pickup_month int
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/joined_trips'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE cs675_taxi_weather_project.full_year_joined_trips_2024;

-- ==================================================
-- Table: full_year_monthly_borough_weather_summary_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/monthly_borough_weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_monthly_borough_weather_summary_2024 (
    pickup_year int,
    pickup_month int,
    pickup_borough string,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_duration_minutes double,
    avg_card_tip_percentage double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/monthly_borough_weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_monthly_tipping_summary_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/monthly_tipping_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_monthly_tipping_summary_2024 (
    pickup_year int,
    pickup_month int,
    weather_condition string,
    credit_card_trip_count bigint,
    avg_tip_amount double,
    avg_tip_percentage double,
    no_tip_rate_pct double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/monthly_tipping_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_monthly_weather_summary_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/monthly_weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_monthly_weather_summary_2024 (
    pickup_year int,
    pickup_month int,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_total_amount double,
    avg_duration_minutes double,
    avg_distance_miles double,
    avg_card_tip_percentage double,
    avg_temperature_c double,
    avg_precipitation_mm double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/monthly_weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_seasonal_weather_summary_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/seasonal_weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_seasonal_weather_summary_2024 (
    season string,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_total_amount double,
    avg_duration_minutes double,
    avg_distance_miles double,
    avg_card_tip_percentage double,
    avg_temperature_c double,
    avg_precipitation_mm double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/seasonal_weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_weather_summary_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_weather_summary_2024 (
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_total_amount double,
    avg_duration_minutes double,
    avg_distance_miles double,
    avg_card_tip_percentage double,
    avg_temperature_c double,
    avg_precipitation_mm double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: full_year_zone_weather_summary_2024
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/zone_weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.full_year_zone_weather_summary_2024 (
    pickup_borough string,
    pickup_zone string,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_duration_minutes double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/full_year_optimized/hourly/2024/zone_weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: hourly_demand
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/hourly_demand
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.hourly_demand (
    pickup_date date,
    pickup_hour_of_day int,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_duration_minutes double,
    avg_card_tip_percentage double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/hourly_demand'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: joined_trips
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/joined_trips
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.joined_trips (
    pickup_timestamp timestamp,
    dropoff_timestamp timestamp,
    pickup_hour timestamp,
    pickup_hour_of_day int,
    pickup_location_id int,
    pickup_borough string,
    pickup_zone string,
    pickup_service_zone string,
    passenger_count double,
    trip_distance_miles double,
    trip_duration_minutes double,
    payment_type int,
    fare_amount double,
    tip_amount double,
    total_amount double,
    card_tip_percentage double,
    temperature_c double,
    precipitation_mm double,
    precipitation_trace boolean,
    relative_humidity_pct double,
    wind_speed_mps double,
    present_weather string,
    hour_imputed boolean,
    weather_condition string
)
PARTITIONED BY (
    pickup_date date
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/joined_trips'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE cs675_taxi_weather_project.joined_trips;

-- ==================================================
-- Table: weather_summary
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.weather_summary (
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_total_amount double,
    avg_duration_minutes double,
    avg_distance_miles double,
    avg_card_tip_percentage double,
    avg_temperature_c double,
    avg_precipitation_mm double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- ==================================================
-- Table: zone_weather_summary
-- S3 location: s3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/zone_weather_summary
-- ==================================================
CREATE EXTERNAL TABLE IF NOT EXISTS cs675_taxi_weather_project.zone_weather_summary (
    pickup_borough string,
    pickup_zone string,
    weather_condition string,
    trip_count bigint,
    avg_fare_amount double,
    avg_duration_minutes double
)
STORED AS PARQUET
LOCATION 's3://cs675-taxi-weather-project-066849627846-data/processed/cloud_analysis/january_2024/zone_weather_summary'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);
