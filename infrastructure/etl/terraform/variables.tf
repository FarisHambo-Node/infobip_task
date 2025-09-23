# ETL Terraform Variables

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-north-1"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

# Database variables for ETL
variable "db_host" {
  description = "RDS database host"
  type        = string
}

variable "db_name" {
  description = "RDS database name"
  type        = string
}

variable "db_user" {
  description = "RDS database username"
  type        = string
}

variable "db_password" {
  description = "RDS database password"
  type        = string
  sensitive   = true
}

# Existing resources from base infrastructure
variable "s3_bucket_name" {
  description = "S3 bucket name from base infrastructure"
  type        = string
}

variable "data_processing_role_arn" {
  description = "Data processing role ARN from base infrastructure"
  type        = string
}

variable "s3_data_access_policy_arn" {
  description = "S3 data access policy ARN from base infrastructure"
  type        = string
}
