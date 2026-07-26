variable "aws_region" {
  description = "AWS Region used for the CS-675 project."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = var.aws_region == "us-east-1"
    error_message = "This project is configured to use us-east-1."
  }
}

variable "project_name" {
  description = "Name used to identify and tag project resources."
  type        = string
  default     = "cs675-taxi-weather"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "project"
}

variable "force_destroy_buckets" {
  description = "Allow Terraform to delete S3 buckets that still contain project objects."
  type        = bool
  default     = false
}