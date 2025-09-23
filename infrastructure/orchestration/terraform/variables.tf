# General configuration
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-north-1"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "data-engineering-task"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

# ETL Resources (from ETL infrastructure)
variable "glue_job_arn" {
  description = "ARN of the Glue job to execute"
  type        = string
}

variable "glue_job_name" {
  description = "Name of the Glue job to execute"
  type        = string
}

variable "lambda_function_arn" {
  description = "ARN of the Lambda function to execute"
  type        = string
}
