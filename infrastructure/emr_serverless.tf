# -------------------------------------------------------------------
# EMR Serverless job runtime role
# -------------------------------------------------------------------

data "aws_iam_policy_document" "emr_serverless_assume_role" {
  statement {
    sid     = "EMRServerlessTrustPolicy"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "emr_serverless_job" {
  name               = "${local.resource_prefix}-emr-job-role"
  assume_role_policy = data.aws_iam_policy_document.emr_serverless_assume_role.json
}

# -------------------------------------------------------------------
# Permissions used by EMR Serverless Spark jobs
# -------------------------------------------------------------------

data "aws_iam_policy_document" "emr_serverless_job" {
  statement {
    sid    = "ListProjectBuckets"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      aws_s3_bucket.data.arn,
      aws_s3_bucket.athena_results.arn
    ]
  }

  statement {
    sid    = "ReadWriteProjectObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]

    resources = [
      "${aws_s3_bucket.data.arn}/*",
      "${aws_s3_bucket.athena_results.arn}/*"
    ]
  }

  statement {
    sid    = "GlueDataCatalogAccess"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:GetUserDefinedFunction",
      "glue:GetUserDefinedFunctions"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_serverless_job" {
  name   = "${local.resource_prefix}-emr-job-policy"
  role   = aws_iam_role.emr_serverless_job.id
  policy = data.aws_iam_policy_document.emr_serverless_job.json
}

# -------------------------------------------------------------------
# EMR Serverless Spark application
# -------------------------------------------------------------------

resource "aws_emrserverless_application" "spark" {
  name          = "${local.resource_prefix}-spark"
  release_label = "emr-spark-8.0.0"
  type          = "spark"
  architecture  = "X86_64"

  scheduler_configuration {
    max_concurrent_runs   = 15
    queue_timeout_minutes = 360
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }

  maximum_capacity {
    cpu    = "8 vCPU"
    memory = "32 GB"
  }
}