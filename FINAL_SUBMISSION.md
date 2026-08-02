# Final Project Submission



## Project Information



**Project Title:** NYC Taxi and Hourly Weather Analytics  

**Student:** Yirui Zhang  

**Course:** CS675-152W - Big Data Management & Analytics  



## Submission Artifacts



\- Final Presentation PowerPoint

\- Final Presentation PDF

\- GitHub Repository:

&#x20; https://github.com/YZJason98/cs675-final-taxi-weather



## Important Project Components



1\. Local PySpark development environment using Docker

2\. January 2024 NYC Yellow Taxi baseline analysis

3\. NOAA hourly weather preprocessing and missing-hour handling

4\. NYC Taxi Zone Lookup enrichment

5\. Daily weather join versus hourly weather join comparison

6\. Full-year 2024 Spark processing using AWS EMR Serverless

7\. Amazon S3 storage using compressed Parquet files

8\. Year and month partitioning of the joined taxi dataset

9\. AWS Glue Data Catalog external tables

10\. Amazon Athena analytical and validation queries

11\. Terraform infrastructure-as-code

12\. Local and cloud Spark performance comparison

13\. Monthly, seasonal, hourly, borough, zone, and tipping analysis

14\. Spark ML Logistic Regression model

15\. Spark ML Random Forest Classifier

16\. Accuracy, precision, recall, F1, ROC-AUC, and confusion-matrix evaluation

17\. Feature-importance analysis

18\. Complete local and cloud execution instructions

19\. Project limitations and resource teardown instructions



## Repository Organization



\- `work/final\_project/` 鈥?PySpark processing and Spark ML scripts

\- `infrastructure/` 鈥?Terraform infrastructure configuration

\- `sql/` 鈥?Athena table definitions, analysis queries, and validation queries

\- `results/` 鈥?Performance metrics and Spark ML results

\- `docs/` 鈥?Project documentation and Spark screenshots

\- `slides/` 鈥?Presentation materials

\- `README.md` 鈥?Complete project overview and execution instructions



## Main Results



\- Original full-year taxi records: 41,169,720

\- Cleaned and joined full-year records: 39,503,323

\- January cleaned and joined records: 2,857,438

\- Prepared hourly weather rows: 8,783

\- Weather match rate: 100%

\- Full-year monthly partitions: 12

\- Random Forest accuracy: 77.69%

\- Random Forest precision: 79.82%

\- Random Forest F1 score: 59.65%

\- Random Forest ROC-AUC: 87.68%



## Large Files



Large source datasets, processed Parquet files, Spark model directories, and cloud logs are not stored directly in GitHub because of their size.



Official input-data links and instructions for reproducing the project are provided in the main README.

