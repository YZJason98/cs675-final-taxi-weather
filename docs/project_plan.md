# CS-675 Final Project Scope and Plan



## Project Title



Impact of Weather Conditions on NYC Taxi Demand, Fares, and Tipping Behavior



## Project Objective



This project will use Apache Spark to examine how weather conditions affect taxi demand, fares, trip duration, and passenger tipping behavior in New York City.



## Datasets



1. NYC TLC Yellow Taxi Trip Records
2. NYC Taxi Zone Lookup
3. NOAA Weather Data



\## Local Validation Results



The local Spark analysis uses January 2024 NYC Yellow Taxi data.



\- Raw taxi records: 2,964,624

\- Clean taxi records after preprocessing: 2,859,224

\- Removed invalid or extreme records: 105,400 (3.56%)

\- Taxi Zone Lookup records: 265

\- Central Park daily weather records: 31

\- Weather join match rate: 100%

\- Zone join match rate: 100%

\- Local execution time: approximately 18–22 seconds



The local validation confirms that the preprocessing pipeline, cross-source joins, analytical queries, and broadcast-join strategy work before the project is expanded to hundreds of millions of cloud-hosted taxi records.



## Join Strategy



* Taxi trips will be joined with the Taxi Zone Lookup using PULocationID = LocationID.
* Taxi trips will be joined with weather observations using pickup date and weather date.
* Hourly weather data will be used if available; otherwise, daily weather observations will be used.



## Research Questions



1. How do rain, snow, and temperature affect taxi trip demand?
2. Does passenger tipping behavior change under different weather conditions?
3. How does weather affect average fare, duration, and trip distance?
4. Which boroughs or taxi zones experience the largest demand changes during bad weather?



## Preprocessing Plan



* Missing-value imputation
* Invalid and extreme-value treatment
* Numerical normalization
* Categorical encoding
* Temperature, precipitation, and trip-distance binning
* Before-and-after data-quality comparisons



## Performance Plan



* Parquet storage
* Column and predicate pruning
* Broadcast joins for small lookup tables
* Partitioning by year and month
* Baseline versus optimized execution-time comparison
* Spark execution-plan evaluation



## Local Environment



* Docker Desktop
* Apache Spark / PySpark
* Spark History Server
* Git and GitHub



## \## Cloud Architecture and Setup Plan

## 

## The cloud version will reproduce the local Spark analysis at a much larger scale.

## 

## 1\. Terraform will provision the required AWS infrastructure.

## 2\. Amazon S3 will store raw taxi, weather, and zone data, as well as processed Parquet outputs.

## 3\. AWS Glue Data Catalog will define metadata and external tables.

## 4\. Amazon Athena will be used for validation queries and result inspection.

## 5\. Amazon EMR Serverless will execute the PySpark preprocessing, joins, and analytical queries.

## 6\. Taxi data will be converted to Parquet and partitioned by pickup year and month.

## 7\. Taxi Zone Lookup and weather data will be broadcast during Spark joins.

## 8\. Multiple years of taxi records will be used to reach hundreds of millions of rows.

## 9\. Cloud runtime, rows processed, and execution plans will be compared with the local version.

## 10\. All cloud resources will be destroyed after the project is completed to avoid unnecessary cost.



## \## Planned Optional Extension

## 

## After completing all required components, I will attempt a Spark ML classification model that predicts whether a date has high taxi demand.

## 

## Possible features include:

## 

## \- Average temperature

## \- Precipitation

## \- Snowfall

## \- Day of week

## \- Month

## \- Borough

## \- Weather condition

## 

## The target variable will classify daily or borough-level demand as high or normal. Logistic Regression and Random Forest will be compared if time permits.

