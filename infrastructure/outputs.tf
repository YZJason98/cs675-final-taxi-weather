output "aws_region" {
  description = "AWS Region used by the project."
  value       = var.aws_region
}

output "data_bucket_name" {
  description = "S3 bucket used for raw and processed project data."
  value       = aws_s3_bucket.data.bucket
}

output "athena_results_bucket_name" {
  description = "S3 bucket used for Athena query results."
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_database_name" {
  description = "AWS Glue Data Catalog database name."
  value       = aws_glue_catalog_database.project.name
}

output "athena_workgroup_name" {
  description = "Amazon Athena workgroup name."
  value       = aws_athena_workgroup.project.name
}
output "emr_serverless_application_id" {
  description = "EMR Serverless Spark application ID."
  value       = aws_emrserverless_application.spark.id
}

output "emr_serverless_application_arn" {
  description = "EMR Serverless Spark application ARN."
  value       = aws_emrserverless_application.spark.arn
}

output "emr_serverless_job_role_arn" {
  description = "IAM runtime role used by EMR Serverless jobs."
  value       = aws_iam_role.emr_serverless_job.arn
}