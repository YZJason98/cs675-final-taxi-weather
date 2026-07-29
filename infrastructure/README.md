# AWS Infrastructure



This directory contains the Terraform configuration used to provision the AWS resources for the NYC Taxi and Hourly Weather Analytics project.



## Resources



The Terraform configuration creates or manages the following resources:



- Amazon S3 data bucket

- Amazon S3 Athena query-results location

- AWS Glue Data Catalog database

- Amazon Athena workgroup

- Amazon EMR Serverless Spark application

- IAM execution role and related permissions



## Prerequisites



Before running Terraform, install and configure:



- Terraform

- AWS CLI

- AWS SSO profile named `cs675-admin`

- AWS Region `us-east-1`



Authenticate with AWS:



```powershell

aws sso login --profile cs675-admin

```



Verify the active identity:



```powershell

aws sts get-caller-identity `

&#x20;   --profile cs675-admin `

&#x20;   --region us-east-1

```



## Configuration



Run the following commands from the `infrastructure` directory:



```powershell

Set-Location .\\infrastructure

```



Copy the example variables file when creating a new local environment:



```powershell

Copy-Item `

&#x20;   .\\terraform.tfvars.example `

&#x20;   .\\terraform.tfvars

```



Review `terraform.tfvars` before deployment. Do not commit credentials, account secrets, or sensitive values.



## Terraform Commands



Initialize Terraform:



```powershell

terraform init

```



Review the planned infrastructure changes:



```powershell

terraform plan

```



Create or update the infrastructure:



```powershell

terraform apply

```



Review Terraform outputs:



```powershell

terraform output

```



Destroy the managed infrastructure after the project is complete:



```powershell

terraform destroy

```



## Important Files



- `providers.tf` 鈥?AWS provider configuration

- `versions.tf` 鈥?Terraform and provider version requirements

- `variables.tf` 鈥?input variable definitions

- `main.tf` 鈥?S3, Glue, Athena, IAM, and supporting resources

- `emr\_serverless.tf` 鈥?EMR Serverless Spark application

- `outputs.tf` 鈥?resource identifiers and output values

- `terraform.tfvars.example` 鈥?example local configuration



## Local Files Excluded from Git



The following files are intentionally excluded through `.gitignore`:



- `.terraform/`

- `terraform.tfstate`

- `terraform.tfstate.backup`

- `terraform.tfvars`

- `\*.tfplan`

- Terraform crash logs



These files may contain local state, account-specific values, or downloaded provider binaries.



## Resource Teardown



Before destroying resources:



1\. Download required Athena results, model metrics, and EMR logs.

2\. Confirm that important project results are stored locally or in GitHub.

3\. Check whether the S3 bucket contains files that must be retained.

4\. Run `terraform plan -destroy`.

5\. Run `terraform destroy`.



Some S3 resources may require manual cleanup if the bucket is not empty.
