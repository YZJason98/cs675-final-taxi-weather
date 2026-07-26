data "aws_caller_identity" "current" {}

locals {
  resource_prefix = "${var.project_name}-${var.environment}"
  account_id      = data.aws_caller_identity.current.account_id

  data_bucket_name = lower(
    "${local.resource_prefix}-${local.account_id}-data"
  )

  athena_results_bucket_name = lower(
    "${local.resource_prefix}-${local.account_id}-athena-results"
  )

  glue_database_name = replace(
    "${var.project_name}_${var.environment}",
    "-",
    "_"
  )
}

# -------------------------------------------------------------------
# Project data bucket
# -------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket        = local.data_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# -------------------------------------------------------------------
# Athena query-results bucket
# -------------------------------------------------------------------

resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_results_bucket_name
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# -------------------------------------------------------------------
# AWS Glue Data Catalog
# -------------------------------------------------------------------

resource "aws_glue_catalog_database" "project" {
  name        = local.glue_database_name
  description = "Glue database for the CS-675 taxi and hourly weather project."
}

# -------------------------------------------------------------------
# Amazon Athena
# -------------------------------------------------------------------

resource "aws_athena_workgroup" "project" {
  name        = "${local.resource_prefix}-workgroup"
  description = "Athena workgroup for the CS-675 taxi and weather project."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}