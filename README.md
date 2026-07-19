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

- Dockerized Spark environment tested
- NYC Yellow Taxi sample downloaded
- Taxi Zone Lookup downloaded
- Spark preprocessing starter validated
- Broadcast join starter validated
- Spark execution-plan evidence collected
- Custom project development in progress

## Repository Structure

- `work/final_project/` — PySpark analysis programs
- `docs/` — project plan and documentation
- `docs/screenshots/starter/` — initial Spark validation screenshots
- `results/` — analytical outputs and performance comparisons
- `sql/` — Athena SQL queries
- `infrastructure/` — Terraform configuration
- `slides/` — presentation materials

## Data and Security

Large datasets, AWS credentials, environment files, Terraform state files, and other sensitive local files are excluded through `.gitignore`.