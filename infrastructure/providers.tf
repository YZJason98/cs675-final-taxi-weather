provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      Course    = "CS-675"
      ManagedBy = "Terraform"
    }
  }
}