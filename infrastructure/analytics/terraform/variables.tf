variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "infobip-task"
}

variable "data_bucket_arn" {
  description = "ARN of the S3 bucket containing the data files"
  type        = string
}

variable "data_bucket_name" {
  description = "Name of the S3 bucket containing the data files"
  type        = string
}

variable "rds_endpoint" {
  description = "RDS endpoint"
  type        = string
}

variable "rds_database_name" {
  description = "RDS database name"
  type        = string
}

variable "rds_username" {
  description = "RDS username"
  type        = string
}

variable "rds_password" {
  description = "RDS password"
  type        = string
  sensitive   = true
}

variable "glue_max_capacity" {
  description = "Maximum capacity for Glue job"
  type        = number
  default     = 2
}

variable "glue_timeout" {
  description = "Timeout for Glue job in minutes"
  type        = number
  default     = 60
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default = {
    Project     = "infobip-task"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}
