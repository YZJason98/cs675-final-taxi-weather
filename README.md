# CS-675 Final Project

## Impact of Weather Conditions on NYC Taxi Analytics

This repository contains my individual CS-675 final project. The project uses Apache Spark locally and AWS cloud services at full scale to analyze relationships among NYC taxi trips, taxi zones, and weather conditions.

## Project Objective

The objective is to examine how temperature, rain, and snow affect taxi demand, fares, trip duration, distance, and passenger tipping behavior in New York City.

## Research Questions

1. How do rain, snow, and temperature affect taxi trip demand?
2. Does passenger tipping behavior change under different weather conditions?
3. How does weather affect average fare, duration, and trip distance?
4. Which boroughs or taxi zones experience the largest demand changes during bad weather?

## Datasets

- NYC TLC Yellow Taxi Trip Records
- NYC Taxi Zone Lookup
- NOAA Weather Data

## Technologies

- Apache Spark and PySpark
- Docker Desktop
- Spark History Server
- Git and GitHub
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- EMR Serverless
- Terraform

## Current Status

- Public GitHub repository created
- Dockerized Spark environment validated
- January 2024 Yellow Taxi sample loaded
- NOAA Central Park weather data loaded
- Taxi Zone Lookup loaded
- Missing weather values imputed
- Invalid and extreme taxi records removed
- Taxi, zone, and weather datasets successfully joined
- Two broadcast hash joins validated in Spark History Server
- Local analytical queries completed
- Cloud deployment planning in progress

## Repository Structure

- `work/final_project/` — PySpark analysis programs
- `docs/` — project plan and documentation
- `docs/screenshots/starter/` — initial Spark validation screenshots
- `results/` — analytical outputs and performance comparisons
- `sql/` — Athena SQL queries
- `infrastructure/` — Terraform configuration
- `slides/` — presentation materials
- `docs/screenshots/local/` — local Spark analysis and execution-plan evidence

## Data and Security

Large datasets, AWS credentials, environment files, Terraform state files, and other sensitive local files are excluded through `.gitignore`.

## Local Run Instructions

Start the Docker environment:

```powershell
.\make.ps1 up
```

Run the taxi-weather-zone analysis:

```powershell
docker compose exec pyspark python /home/jovyan/work/final_project/03_taxi_weather_join.py
```

Open the Spark History Server:

```text
http://localhost:18080
```

Stop the environment after use:

```powershell
.\make.ps1 down
```