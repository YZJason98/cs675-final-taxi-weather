# NYC Taxi and Hourly Weather Analytics

A scalable data-engineering and analytics project that examines how hourly weather conditions affect New York City Yellow Taxi demand, fares, trip duration, distance, and tipping behavior.

The project combines local Apache Spark development with an AWS cloud pipeline built on Amazon S3, AWS Glue, Athena, EMR Serverless, and Terraform. It also includes an optional Spark ML stage that predicts whether hourly taxi demand will be classified as High Demand or Normal Demand.

## Project Objectives

The project was designed to:

- Build a reproducible local PySpark development environment
- Clean and validate NYC Yellow Taxi trip data
- Prepare complete hourly NOAA weather observations
- Join taxi records with hourly weather conditions
- Compare daily and hourly weather-join strategies
- Scale the pipeline from January data to the full 2024 dataset
- Store optimized Parquet outputs in Amazon S3
- Query processed data through AWS Glue and Athena
- Compare local and cloud Spark performance
- Analyze monthly, seasonal, hourly, borough, and zone-level patterns
- Train and compare Logistic Regression and Random Forest classifiers

## Datasets

### NYC Yellow Taxi Trip Records

The project uses all twelve monthly NYC Yellow Taxi Parquet files for 2024.

- Original full-year records: `41,169,720`
- Cleaned and successfully joined records: `39,503,323`
- January cleaned and joined records: `2,857,438`

### NYC Taxi Zone Lookup

The NYC Taxi Zone Lookup dataset is used to map pickup location IDs to:

- Borough
- Taxi zone
- Service zone

### NOAA Hourly Weather Data

Hourly weather observations were prepared for the full 2024 calendar year.

- Complete hourly weather rows: `8,783`
- Valid NYC local calendar hours represented: `8,783`
- Weather-match rate after the taxi join: `100%`
- Weather categories:
  - Dry
  - Rain
  - Snow
  - Heavy Rain

The hourly dataset includes variables such as:

- Temperature
- Precipitation
- Relative humidity
- Wind speed
- Present weather
- Derived weather condition

## Architecture

```text
NYC Yellow Taxi Data ──────┐
                           │
NYC Taxi Zone Lookup ──────┼──> Amazon S3
                           │         │
NOAA Hourly Weather ───────┘         ▼
                               EMR Serverless
                                  PySpark
                                     │
                                     ▼
                           Partitioned Parquet
                                     │
                                     ▼
                          AWS Glue Data Catalog
                                     │
                                     ▼
                               Amazon Athena
                                     │
                      ┌──────────────┴──────────────┐
                      ▼                             ▼
              Analytical Queries              Spark ML
                                            Classification
```

## Technologies

- Apache Spark
- PySpark
- Spark ML
- Docker Desktop
- Spark History Server
- Python
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- Amazon EMR Serverless
- AWS IAM
- AWS CLI
- Terraform
- Git
- GitHub

## Repository Structure

```text
cs675-final-taxi-weather/
├── work/
│   └── final_project/
│       ├── 05c_full_year_hourly_taxi_weather_analysis.py
│       ├── 05d_build_full_year_hourly_borough_ml_dataset.py
│       ├── 05e_train_phase9_models.py
│       └── 06_validate_full_year_taxi_schema.py
├── infrastructure/
│   ├── main.tf
│   ├── emr_serverless.tf
│   ├── providers.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── versions.tf
│   ├── terraform.tfvars.example
│   └── README.md
├── sql/
│   ├── 01_athena_tables.sql
│   ├── 02_phase8_analysis_queries.sql
│   └── 03_validation_queries.sql
├── results/
│   ├── performance_comparison/
│   └── phase9_ml/
├── docs/
├── slides/
├── docker-compose.yml
├── .gitignore
└── README.md
```

Large source datasets, Parquet outputs, Terraform state files, downloaded EMR logs, Spark models, and AWS credentials are intentionally excluded from GitHub.

## Local Run Instructions

### Prerequisites

Install the following software before running the project locally:

- Docker Desktop
- Git
- PowerShell
- At least 8 GB of available memory for Docker

Clone the repository:

```powershell
git clone https://github.com/YZJason98/cs675-final-taxi-weather.git
Set-Location .\cs675-final-taxi-weather
```

### Start the Docker Environment

Start the PySpark and Spark History Server containers:

```powershell
docker compose up -d
```

Confirm that the containers are running:

```powershell
docker compose ps
```

The main services are:

- `pyspark` — PySpark execution environment
- `spark-history` — Spark History Server

Open the Spark History Server in a browser:

```text
http://localhost:18080
```

### Local Data

Place local source datasets under:

```text
work/data/
```

This directory is excluded from Git because the source datasets are too large for the repository.

The local pipeline uses:

- January 2024 NYC Yellow Taxi data
- NYC Taxi Zone Lookup data
- Prepared NOAA weather data

### Run the January Taxi-Weather Analysis

Run the January taxi-weather join inside the PySpark container:

```powershell
docker compose exec pyspark `
    python /home/jovyan/work/final_project/03_taxi_weather_join.py
```

The local analysis performs:

1. Taxi schema validation
2. Invalid-record filtering
3. Taxi-zone enrichment
4. Hourly weather matching
5. Fare, duration, distance, and tipping analysis
6. Aggregated Parquet output generation

### Monitor Local Execution

View the PySpark container logs:

```powershell
docker compose logs -f pyspark
```

Press `Ctrl + C` to stop viewing the logs without stopping the container.

Use the Spark History Server to inspect:

- Spark jobs and stages
- Shuffle activity
- SQL execution plans
- Broadcast joins
- Task duration
- Input and output metrics

### Stop the Local Environment

Stop and remove the running containers:

```powershell
docker compose down
```

Local data and generated files stored in mounted project directories are not deleted by this command.

## Cloud Run Instructions

### AWS Prerequisites

The cloud pipeline requires:

- AWS CLI
- AWS SSO access
- Terraform
- Permission to use Amazon S3, AWS Glue, Athena, IAM, and EMR Serverless
- AWS profile: `cs675-admin`
- AWS Region: `us-east-1`

Authenticate with AWS:

```powershell
aws sso login --profile cs675-admin
```

Verify the active AWS identity:

```powershell
aws sts get-caller-identity `
    --profile cs675-admin `
    --region us-east-1 `
    --output table
```

### Project Cloud Resources

The completed project uses the following resources:

```text
Data bucket:
cs675-taxi-weather-project-066849627846-data

Glue database:
cs675_taxi_weather_project

Athena workgroup:
cs675-taxi-weather-project-workgroup

EMR Serverless application:
cs675-taxi-weather-project-spark

EMR execution role:
cs675-taxi-weather-project-emr-job-role
```

### Set PowerShell Variables

Run the following commands from the repository root:

```powershell
$profile = "cs675-admin"
$region = "us-east-1"
$bucket = "cs675-taxi-weather-project-066849627846-data"
```

Retrieve the EMR Serverless application ID:

```powershell
$appId = aws emr-serverless list-applications `
    --profile $profile `
    --region $region `
    --query "applications[?name=='cs675-taxi-weather-project-spark'].id | [0]" `
    --output text

$appId
```

### Upload a Spark Script

Example using the full-year analysis script:

```powershell
$localScript = ".\work\final_project\05c_full_year_hourly_taxi_weather_analysis.py"
$s3Script = "s3://$bucket/scripts/05c_full_year_hourly_taxi_weather_analysis.py"

aws s3 cp `
    $localScript `
    $s3Script `
    --profile $profile `
    --region $region
```

### Prepare EMR Serverless Job Configuration

Use JSON files when passing complex parameters from PowerShell to the AWS CLI.

```powershell
$jobDriver = @{
    sparkSubmit = @{
        entryPoint = $s3Script
        entryPointArguments = @(
            "--taxi-input",
            "s3://$bucket/raw/taxi/yellow/2024/",
            "--zone-input",
            "s3://$bucket/reference/taxi_zone_lookup/",
            "--weather-input",
            "s3://$bucket/processed/weather/hourly/2024/",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2025-01-01",
            "--output",
            "s3://$bucket/processed/full_year_optimized/hourly/2024/"
        )
        sparkSubmitParameters = (
            "--conf spark.driver.cores=2 " +
            "--conf spark.driver.memory=4g " +
            "--conf spark.driver.memoryOverhead=1g " +
            "--conf spark.executor.cores=2 " +
            "--conf spark.executor.memory=4g " +
            "--conf spark.executor.memoryOverhead=1g " +
            "--conf spark.dynamicAllocation.enabled=true " +
            "--conf spark.dynamicAllocation.minExecutors=1 " +
            "--conf spark.dynamicAllocation.initialExecutors=2 " +
            "--conf spark.dynamicAllocation.maxExecutors=3"
        )
    }
} | ConvertTo-Json -Depth 6 -Compress
```

Save the job configuration:

```powershell
$jobDriver |
    Set-Content `
        ".\work\final_project\emr_job_driver.json" `
        -Encoding ASCII
```

Create the monitoring configuration:

```powershell
$configOverrides = @{
    monitoringConfiguration = @{
        s3MonitoringConfiguration = @{
            logUri = "s3://$bucket/logs/emr-serverless/"
        }
    }
} | ConvertTo-Json -Depth 6 -Compress

$configOverrides |
    Set-Content `
        ".\work\final_project\emr_config_overrides.json" `
        -Encoding ASCII
```

These temporary JSON configuration files are excluded through `.gitignore`.

### Submit the EMR Serverless Job

```powershell
$jobRunId = aws emr-serverless start-job-run `
    --application-id $appId `
    --execution-role-arn "arn:aws:iam::066849627846:role/cs675-taxi-weather-project-emr-job-role" `
    --name "full-year-hourly-taxi-weather-analysis" `
    --job-driver "file://work/final_project/emr_job_driver.json" `
    --configuration-overrides "file://work/final_project/emr_config_overrides.json" `
    --profile $profile `
    --region $region `
    --query "jobRunId" `
    --output text

$jobRunId
```

### Monitor Job Status

```powershell
do {
    $jobStatus = aws emr-serverless get-job-run `
        --application-id $appId `
        --job-run-id $jobRunId `
        --profile $profile `
        --region $region `
        --query "jobRun.{State:state,StateDetails:stateDetails}" `
        --output json |
        ConvertFrom-Json

    Write-Host "$(Get-Date -Format 'HH:mm:ss') State: $($jobStatus.State)"
    Write-Host "Details: $($jobStatus.StateDetails)"

    if ($jobStatus.State -notin @(
        "SUCCESS",
        "FAILED",
        "CANCELLED",
        "CANCELLING"
    )) {
        Start-Sleep -Seconds 20
    }
}
while ($jobStatus.State -notin @(
    "SUCCESS",
    "FAILED",
    "CANCELLED",
    "CANCELLING"
))
```

The expected successful sequence is:

```text
PENDING → SCHEDULED → RUNNING → SUCCESS
```

### Verify S3 Output

```powershell
aws s3 ls `
    "s3://$bucket/processed/full_year_optimized/hourly/2024/" `
    --recursive `
    --profile $profile `
    --region $region
```

The full-year pipeline produces:

- Joined trips
- Weather summary
- Borough-weather summary
- Hourly demand
- Zone-weather summary
- Monthly weather summary
- Seasonal weather summary
- Monthly borough-weather summary
- Monthly tipping summary

### Query Data with Athena

Athena table definitions are stored in:

```text
sql/01_athena_tables.sql
```

Phase 8 analytical queries are stored in:

```text
sql/02_phase8_analysis_queries.sql
```

Validation queries are stored in:

```text
sql/03_validation_queries.sql
```

Use the Athena workgroup:

```text
cs675-taxi-weather-project-workgroup
```

Processed datasets are registered in the Glue database:

```text
cs675_taxi_weather_project
```

## Terraform Instructions

Terraform configuration is stored in:

```text
infrastructure/
```

Detailed infrastructure documentation is available in:

```text
infrastructure/README.md
```

### Initialize the Local Configuration

Move into the Terraform directory:

```powershell
Set-Location .\infrastructure
```

For a new local environment, create `terraform.tfvars` from the example file:

```powershell
Copy-Item `
    .\terraform.tfvars.example `
    .\terraform.tfvars
```

Review the local variables before deployment. The actual `terraform.tfvars` file is excluded from GitHub because it may contain account-specific values.

### Authenticate with AWS

```powershell
aws sso login --profile cs675-admin
```

Verify the active identity:

```powershell
aws sts get-caller-identity `
    --profile cs675-admin `
    --region us-east-1 `
    --output table
```

### Initialize Terraform

```powershell
terraform init
```

This downloads the required providers and initializes the local Terraform working directory.

### Validate the Configuration

```powershell
terraform fmt -check
terraform validate
```

To automatically format Terraform files:

```powershell
terraform fmt
```

### Review the Deployment Plan

```powershell
terraform plan
```

The plan should be reviewed before applying changes, especially when existing cloud resources are already in use.

### Apply the Infrastructure

```powershell
terraform apply
```

Confirm the deployment when prompted.

Review the generated resource information:

```powershell
terraform output
```

### Important Terraform Files

- `main.tf` — S3, Glue, Athena, IAM, and supporting resources
- `emr_serverless.tf` — EMR Serverless Spark application
- `providers.tf` — AWS provider configuration
- `variables.tf` — input variable definitions
- `outputs.tf` — Terraform output values
- `versions.tf` — Terraform and AWS provider version requirements
- `terraform.tfvars.example` — example local variable configuration

### Files Excluded from GitHub

The following local files are intentionally excluded:

```text
infrastructure/.terraform/
infrastructure/terraform.tfstate
infrastructure/terraform.tfstate.backup
infrastructure/terraform.tfvars
*.tfplan
```

Terraform state files and local variables may contain account-specific or sensitive infrastructure information.

### Destroy the Infrastructure

Before destroying resources, download any required Athena results, model metrics, Spark logs, and other project outputs.

Review the destruction plan:

```powershell
terraform plan -destroy
```

Destroy the Terraform-managed resources:

```powershell
terraform destroy
```

Amazon S3 buckets may need to be emptied manually before deletion when they still contain project data.

## Spark Scripts and Commands

The primary PySpark scripts are stored in:

```text
work/final_project/
```

### January Taxi-Weather Analysis

```text
03_taxi_weather_join.py
```

Purpose:

- Reads January 2024 NYC Yellow Taxi records
- Cleans invalid taxi trips
- Adds taxi-zone and borough information
- Joins taxi records with prepared weather data
- Produces January analytical summaries
- Supports local daily-versus-hourly join comparison

Example local execution:

```powershell
docker compose exec pyspark `
    python /home/jovyan/work/final_project/03_taxi_weather_join.py
```

### Full-Year Hourly Taxi-Weather Analysis

```text
05c_full_year_hourly_taxi_weather_analysis.py
```

Purpose:

- Reads all twelve 2024 Yellow Taxi monthly datasets
- Filters the analysis period from January 1 through December 31, 2024
- Joins taxi trips with taxi-zone and hourly weather data
- Creates calendar, monthly, seasonal, and hourly fields
- Writes the joined dataset partitioned by year and month
- Produces nine analytical output datasets

Main arguments:

```text
--taxi-input
--zone-input
--weather-input
--start-date
--end-date
--output
```

Example syntax:

```powershell
spark-submit `
    05c_full_year_hourly_taxi_weather_analysis.py `
    --taxi-input "<taxi-s3-path>" `
    --zone-input "<zone-s3-path>" `
    --weather-input "<weather-s3-path>" `
    --start-date "2024-01-01" `
    --end-date "2025-01-01" `
    --output "<output-s3-path>"
```

The main full-year output includes:

```text
joined_trips/
weather_summary/
borough_weather_summary/
hourly_demand/
zone_weather_summary/
monthly_weather_summary/
seasonal_weather_summary/
monthly_borough_weather_summary/
monthly_tipping_summary/
```

### Full-Year Taxi Schema Validation

```text
06_validate_full_year_taxi_schema.py
```

Purpose:

- Reads each monthly 2024 Yellow Taxi Parquet file
- Confirms that all required fields exist
- Detects schema inconsistencies before the full-year Spark job
- Produces validation logs and job metrics

This validation was completed successfully for all twelve monthly datasets.

### Machine-Learning Dataset Construction

```text
05d_build_full_year_hourly_borough_ml_dataset.py
```

Purpose:

- Aggregates taxi demand by local hour and pickup borough
- Uses Bronx, Brooklyn, Manhattan, and Queens
- Excludes Staten Island from modeling because most hours contain zero Yellow Taxi pickups
- Creates a complete hour-by-borough grid
- Uses January through September as training data
- Uses October through December as testing data
- Calculates borough-specific 75th-percentile demand thresholds
- Creates High Demand and Normal Demand labels

Main arguments:

```text
--input
--output
--start-date
--end-date
```

The completed ML dataset contains:

```text
Training rows: 26,300
Testing rows:   8,832
Total rows:    35,132
```

### Spark ML Model Training

```text
05e_train_phase9_models.py
```

Purpose:

- Loads the prepared hourly borough-level ML dataset
- Imputes missing numerical values
- Indexes and one-hot encodes categorical variables
- Assembles the model feature vector
- Trains Logistic Regression
- Trains Random Forest Classifier
- Evaluates both models on the time-based test set
- Saves model metrics, confusion matrices, feature importance, and trained Spark models

Main arguments:

```text
--input
--output
```

Example syntax:

```powershell
spark-submit `
    05e_train_phase9_models.py `
    --input "<ml-dataset-s3-path>" `
    --output "<model-output-s3-path>"
```

The model evaluation includes:

- Accuracy
- Precision
- Recall
- F1 score
- ROC-AUC
- Confusion matrix
- Runtime
- Feature importance

### Python Syntax Validation

Before uploading a Python script to Amazon S3, validate its syntax locally:

```powershell
py -m py_compile `
    .\work\final_project\<script-name>.py
```

No output means that the Python syntax check passed.

## Analytical Results

### Data Processing Results

The full-year Spark pipeline processed all twelve months of 2024 NYC Yellow Taxi data.

| Measure | Result |
|---|---:|
| Original full-year taxi records | 41,169,720 |
| Cleaned and joined full-year records | 39,503,323 |
| January cleaned and joined records | 2,857,438 |
| Complete hourly weather records | 8,783 |
| Full-year weather-match rate | 100% |
| Full-year calendar days | 366 |
| Full-year months | 12 |

The main full-year EMR Serverless Spark job completed in approximately five minutes and thirty-nine seconds.

### January versus Full-Year Weather Distribution

January alone was not representative of the full-year weather distribution.

| Weather Measure | January 2024 | Full Year 2024 |
|---|---:|---:|
| Snow trip share | 4.93% | 0.64% |

This difference demonstrates why conclusions based on one winter month may overstate the overall importance of snow conditions.

### Hourly versus Daily Weather Join

The project compared two weather-join strategies:

- Daily weather join
- Hourly weather join

Both approaches used the same `2,857,438` cleaned January taxi records. However, the hourly join retained changes in weather conditions within the same day.

A daily join assigns one weather classification to every trip on the same date. This may hide short rain, snow, or heavy-rain periods and may incorrectly classify trips that occurred during different conditions.

The hourly join provides better analytical granularity because it:

- Preserves intraday weather changes
- Supports hour-level demand analysis
- Improves rush-hour weather comparisons
- Reduces weather-classification aggregation error
- Provides more precise fare, duration, and tipping comparisons

### Temporal and Geographic Findings

The Phase 8 Athena analysis examined:

- Monthly weather trends
- Seasonal weather patterns
- Hourly taxi demand
- Rush-hour versus non-rush-hour demand
- Borough-level differences
- High-demand Taxi Zone sensitivity
- Monthly credit-card tipping behavior
- Daily-versus-hourly weather classification

The results show that taxi demand is strongly associated with:

- Pickup hour
- Month
- Pickup borough
- Weekday and rush-hour status

Weather variables contributed useful additional information, but time and location were generally stronger demand predictors.

## Spark ML Results

### Prediction Target

The Spark ML stage predicts:

```text
High Demand
Normal Demand
```

Each observation represents one local hour and one pickup borough.

The model includes:

- Bronx
- Brooklyn
- Manhattan
- Queens

Staten Island was excluded because most hourly Yellow Taxi pickup counts were zero, causing its 75th-percentile demand threshold to equal zero.

### Training and Testing Split

A time-based split was used to reduce future-data leakage:

| Dataset | Period | Rows |
|---|---|---:|
| Training | January–September 2024 | 26,300 |
| Testing | October–December 2024 | 8,832 |
| Total | Full year | 35,132 |

Each borough received its own High Demand threshold based only on the training period.

| Borough | High Demand Threshold |
|---|---:|
| Bronx | 18 trips per hour |
| Brooklyn | 78 trips per hour |
| Manhattan | 5,581 trips per hour |
| Queens | 586 trips per hour |

A row was labeled High Demand when its hourly trip count exceeded the corresponding borough threshold.

### Model Comparison

| Metric | Logistic Regression | Random Forest |
|---|---:|---:|
| Accuracy | 64.98% | **77.69%** |
| Precision | 44.84% | **79.82%** |
| Recall | 4.97% | **47.61%** |
| F1 Score | 8.95% | **59.65%** |
| ROC-AUC | 66.58% | **87.68%** |
| Runtime | **14.88 seconds** | 48.37 seconds |

### Confusion Matrix Results

#### Logistic Regression

| Actual / Predicted | High Demand | Normal Demand |
|---|---:|---:|
| High Demand | 152 | 2,906 |
| Normal Demand | 187 | 5,587 |

Logistic Regression predicted most observations as Normal Demand. Its low recall indicates that it failed to identify most actual High Demand periods.

#### Random Forest

| Actual / Predicted | High Demand | Normal Demand |
|---|---:|---:|
| High Demand | 1,456 | 1,602 |
| Normal Demand | 368 | 5,406 |

Random Forest detected substantially more High Demand observations while maintaining strong precision.

### Best Model

Random Forest was selected as the best-performing model.

It achieved:

- 77.69% accuracy
- 79.82% precision
- 47.61% recall
- 59.65% F1 score
- 87.68% ROC-AUC

Although Random Forest required more runtime than Logistic Regression, its improvement in High Demand detection and overall classification quality justified the additional computational cost.

### Feature Importance

The most influential predictors included:

- Pickup hour
- Pickup month
- Pickup borough
- Temperature
- Relative humidity
- Rush-hour indicator

The feature-importance results suggest that taxi demand is driven primarily by temporal and geographic patterns. Weather conditions add predictive information, but they are not the sole cause of demand changes.

Detailed model outputs are stored in:

```text
results/phase9_ml/
```

This directory includes:

```text
model_metrics.json
confusion_matrices.json
feature_importance.json
dataset_summary.json
demand_thresholds.json
label_distribution.json
```

## Limitations

This project has several analytical and technical limitations.

### Dataset Scope

- The analysis includes only NYC Yellow Taxi trips.
- Green Taxi, For-Hire Vehicle, subway, bus, bicycle, pedestrian, and private-vehicle demand are not represented.
- Yellow Taxi activity is heavily concentrated in Manhattan and airport-related areas, so the results should not be interpreted as total transportation demand across New York City.

### Geographic Coverage

- Bronx, Brooklyn, Manhattan, and Queens were included in the Spark ML model.
- Staten Island was excluded because most hourly Yellow Taxi pickup counts were zero.
- Taxi Zone results may be affected by low-volume zones and unusual airport travel patterns.

### Weather Variables

The joined dataset includes:

- Temperature
- Precipitation
- Relative humidity
- Wind speed
- Present weather
- Derived weather condition

Independent visibility and snowfall measurements were not available in the final joined dataset. Snow was represented through the derived weather condition and snow indicator.

### Weather Classification

Weather conditions were grouped into four categories:

```text
Dry
Rain
Snow
Heavy Rain
```

This simplified classification improves interpretability but does not preserve every possible weather event or severity level.

### Association versus Causation

The project identifies statistical and operational relationships between weather conditions and taxi behavior. It does not prove that weather alone caused changes in:

- Taxi demand
- Fares
- Trip duration
- Distance
- Tipping behavior

Other possible influences include:

- Holidays
- Special events
- Traffic incidents
- Tourism
- Work schedules
- Public-transit disruptions
- Seasonal travel patterns
- Economic conditions

### Machine-Learning Label Definition

High Demand was defined using each borough's training-period 75th-percentile hourly trip threshold.

This definition is useful for classification, but it is a project-specific analytical choice rather than an official NYC demand standard.

A row was labeled High Demand only when:

```text
trip_count > borough-specific training threshold
```

### Training and Testing Distribution

The training period covered January through September, while the testing period covered October through December.

The High Demand share was higher in the test set than in the training set. This indicates possible seasonal demand drift and helps explain why the models did not achieve perfect recall.

### Model Performance

Random Forest was the strongest model, but its recall was 47.61%. It still failed to identify some actual High Demand periods.

The model should therefore be treated as an analytical demonstration rather than a production dispatch or forecasting system.

### Cloud Cost and Reproducibility

AWS costs depend on:

- EMR Serverless worker resources
- Job duration
- S3 storage
- Athena data scanned
- Glue resources
- Logging volume

Resource names, AWS account IDs, S3 paths, IAM permissions, and local AWS profiles may need to be changed before another user can reproduce the cloud deployment.

## Resource Teardown

Cloud resources should be reviewed and removed after the project is complete to avoid unnecessary charges.

### Preserve Required Results

Before deleting cloud resources, download or confirm the local availability of:

- Athena query results
- Model metrics
- Confusion matrices
- Feature-importance results
- Demand thresholds
- Dataset summaries
- EMR Serverless logs
- Spark performance metrics
- Required screenshots
- Final report and presentation files

### Review Terraform Resources

Move to the infrastructure directory:

```powershell
Set-Location .\infrastructure
```

Authenticate with AWS:

```powershell
aws sso login --profile cs675-admin
```

Review the destruction plan:

```powershell
terraform plan -destroy
```

Destroy Terraform-managed resources:

```powershell
terraform destroy
```

Review the Terraform output carefully before confirming deletion.

### S3 Cleanup

Terraform may be unable to delete an S3 bucket that still contains objects.

Review the project bucket:

```powershell
aws s3 ls `
    s3://cs675-taxi-weather-project-066849627846-data `
    --recursive `
    --profile cs675-admin `
    --region us-east-1
```

Only after confirming that required files have been preserved, the bucket contents can be removed:

```powershell
aws s3 rm `
    s3://cs675-taxi-weather-project-066849627846-data `
    --recursive `
    --profile cs675-admin `
    --region us-east-1
```

This command permanently deletes the objects in the project bucket and should be used with caution.

### Additional Resource Checks

After Terraform teardown, verify that the following resources are no longer active when they are no longer needed:

- EMR Serverless application
- Glue database and tables
- Athena workgroup
- IAM execution role
- S3 buckets and stored objects
- Cloud logs and query-result files

### Local Cleanup

Local Docker containers can be stopped with:

```powershell
docker compose down
```

Terraform provider downloads can be removed by deleting:

```text
infrastructure/.terraform/
```

Do not delete the following project materials before final submission:

```text
README.md
work/final_project/
infrastructure/
sql/
results/
docs/
slides/
```
