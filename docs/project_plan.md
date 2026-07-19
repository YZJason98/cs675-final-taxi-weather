# CS-675 Final Project Scope and Plan



## Project Title



Impact of Weather Conditions on NYC Taxi Demand, Fares, and Tipping Behavior



## Project Objective



This project will use Apache Spark to examine how weather conditions affect taxi demand, fares, trip duration, and passenger tipping behavior in New York City.



## Datasets



1. NYC TLC Yellow Taxi Trip Records

2. NYC Taxi Zone Lookup

3. NOAA Weather Data



## Join Strategy



- Taxi trips will be joined with the Taxi Zone Lookup using PULocationID = LocationID.

- Taxi trips will be joined with weather observations using pickup date and weather date.

- Hourly weather data will be used if available; otherwise, daily weather observations will be used.



## Research Questions



1. How do rain, snow, and temperature affect taxi trip demand?

2. Does passenger tipping behavior change under different weather conditions?

3. How does weather affect average fare, duration, and trip distance?

4. Which boroughs or taxi zones experience the largest demand changes during bad weather?



## Preprocessing Plan



- Missing-value imputation

- Invalid and extreme-value treatment

- Numerical normalization

- Categorical encoding

- Temperature, precipitation, and trip-distance binning

- Before-and-after data-quality comparisons



## Performance Plan



- Parquet storage

- Column and predicate pruning

- Broadcast joins for small lookup tables

- Partitioning by year and month

- Baseline versus optimized execution-time comparison

- Spark execution-plan evaluation



## Local Environment



- Docker Desktop

- Apache Spark / PySpark

- Spark History Server

- Git and GitHub



## Cloud Plan



- Amazon S3 for storage

- AWS Glue Data Catalog

- Amazon Athena for SQL analysis

- EMR Serverless for PySpark

- Terraform for reproducible infrastructure



## Optional Extension



A Spark ML classification or regression model will be attempted after all required project components are complete.

