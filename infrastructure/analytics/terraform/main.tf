terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 bucket for Glue job scripts
resource "aws_s3_bucket" "analytics_scripts" {
  bucket = "${var.project_name}-analytics-scripts-${random_id.bucket_suffix.hex}"
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Upload Glue job script
resource "aws_s3_object" "business_analytics_job" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "glue_jobs/business_analytics_job.py"
  source = "../../../analytics/glue_jobs/business_analytics_job.py"
}

# Upload SQL files
resource "aws_s3_object" "analytics_schema_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/analytics_schema.sql"
  source = "../../../analytics/sql/analytics_schema.sql"
}

resource "aws_s3_object" "question_1_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_1_industry_exposure.sql"
  source = "../../../analytics/sql/question_1_industry_exposure.sql"
}

resource "aws_s3_object" "question_2_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_2_segment_analysis.sql"
  source = "../../../analytics/sql/question_2_segment_analysis.sql"
}

resource "aws_s3_object" "question_3_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_3_recent_customers.sql"
  source = "../../../analytics/sql/question_3_recent_customers.sql"
}

resource "aws_s3_object" "question_4_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_4_top_customers.sql"
  source = "../../../analytics/sql/question_4_top_customers.sql"
}

resource "aws_s3_object" "question_5_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_5_monthly_active_customers.sql"
  source = "../../../analytics/sql/question_5_monthly_active_customers.sql"
}

# IAM role for Glue job
resource "aws_iam_role" "glue_analytics_role" {
  name = "${var.project_name}-glue-analytics-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# IAM policy for Glue analytics job
resource "aws_iam_policy" "glue_analytics_policy" {
  name        = "${var.project_name}-glue-analytics-policy"
  description = "Policy for Glue analytics job"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.analytics_scripts.arn,
          "${aws_s3_bucket.analytics_scripts.arn}/*",
          var.data_bucket_arn,
          "${var.data_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:StartJobRun"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "rds:DescribeDBInstances",
          "rds:DescribeDBClusters"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_analytics_policy_attachment" {
  role       = aws_iam_role.glue_analytics_role.name
  policy_arn = aws_iam_policy.glue_analytics_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_analytics_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Glue job for business analytics
resource "aws_glue_job" "business_analytics" {
  name         = "${var.project_name}-business-analytics"
  role_arn     = aws_iam_role.glue_analytics_role.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.analytics_scripts.bucket}/glue_jobs/business_analytics_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--job-bookmark-option"             = "job-bookmark-disable"
    "--additional-python-modules"       = "psycopg2-binary==2.9.7"
    "--S3_BUCKET"                       = var.data_bucket_name
    "--DB_HOST"                         = var.rds_endpoint
    "--DB_NAME"                         = var.rds_database_name
    "--DB_USER"                         = var.rds_username
    "--DB_PASSWORD"                     = var.rds_password
  }

  max_capacity = var.glue_max_capacity
  timeout      = var.glue_timeout
}