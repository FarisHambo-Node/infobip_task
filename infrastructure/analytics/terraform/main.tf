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

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# S3 bucket for Glue job scripts
resource "aws_s3_bucket" "analytics_scripts" {
  bucket = "${var.project_name}-analytics-scripts-${random_id.bucket_suffix.hex}"
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket_versioning" "analytics_scripts" {
  bucket = aws_s3_bucket.analytics_scripts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "analytics_scripts" {
  bucket = aws_s3_bucket.analytics_scripts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Upload Glue job script
resource "aws_s3_object" "business_analytics_job" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "glue_jobs/business_analytics_job.py"
  source = "../../../analytics/glue_jobs/business_analytics_job.py"
  etag   = filemd5("../../../analytics/glue_jobs/business_analytics_job.py")
}

# Upload SQL files
resource "aws_s3_object" "analytics_schema_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/analytics_schema.sql"
  source = "../../../analytics/sql/analytics_schema.sql"
  etag   = filemd5("../../../analytics/sql/analytics_schema.sql")
}

resource "aws_s3_object" "question_1_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_1_industry_exposure.sql"
  source = "../../../analytics/sql/question_1_industry_exposure.sql"
  etag   = filemd5("../../../analytics/sql/question_1_industry_exposure.sql")
}

resource "aws_s3_object" "question_2_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_2_segment_analysis.sql"
  source = "../../../analytics/sql/question_2_segment_analysis.sql"
  etag   = filemd5("../../../analytics/sql/question_2_segment_analysis.sql")
}

resource "aws_s3_object" "question_3_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_3_recent_customers.sql"
  source = "../../../analytics/sql/question_3_recent_customers.sql"
  etag   = filemd5("../../../analytics/sql/question_3_recent_customers.sql")
}

resource "aws_s3_object" "question_4_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_4_top_customers.sql"
  source = "../../../analytics/sql/question_4_top_customers.sql"
  etag   = filemd5("../../../analytics/sql/question_4_top_customers.sql")
}

resource "aws_s3_object" "question_5_sql" {
  bucket = aws_s3_bucket.analytics_scripts.id
  key    = "sql/question_5_monthly_active_customers.sql"
  source = "../../../analytics/sql/question_5_monthly_active_customers.sql"
  etag   = filemd5("../../../analytics/sql/question_5_monthly_active_customers.sql")
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
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.analytics_scripts.bucket}/spark-logs/"
    "--TempDir"                         = "s3://${aws_s3_bucket.analytics_scripts.bucket}/temp/"
    "--extra-py-files"                  = "s3://${aws_s3_bucket.analytics_scripts.bucket}/sql/analytics_schema.sql,s3://${aws_s3_bucket.analytics_scripts.bucket}/sql/question_1_industry_exposure.sql,s3://${aws_s3_bucket.analytics_scripts.bucket}/sql/question_2_segment_analysis.sql,s3://${aws_s3_bucket.analytics_scripts.bucket}/sql/question_3_recent_customers.sql,s3://${aws_s3_bucket.analytics_scripts.bucket}/sql/question_4_top_customers.sql,s3://${aws_s3_bucket.analytics_scripts.bucket}/sql/question_5_monthly_active_customers.sql"
    "--S3_BUCKET"                       = var.data_bucket_name
    "--DB_HOST"                         = var.rds_endpoint
    "--DB_NAME"                         = var.rds_database_name
    "--DB_USER"                         = var.rds_username
    "--DB_PASSWORD"                     = var.rds_password
  }

  max_capacity = var.glue_max_capacity
  timeout      = var.glue_timeout

  tags = var.tags
}

# CloudWatch Log Group for Glue job
resource "aws_cloudwatch_log_group" "glue_analytics_logs" {
  name              = "/aws-glue/jobs/${aws_glue_job.business_analytics.name}"
  retention_in_days = 14
}

# EventBridge rule to trigger Glue job monthly
resource "aws_cloudwatch_event_rule" "analytics_schedule" {
  name                = "${var.project_name}-analytics-schedule"
  description         = "Trigger business analytics job monthly"
  schedule_expression = "cron(0 9 1 * ? *)"  # First day of every month at 9 AM UTC
}

resource "aws_cloudwatch_event_target" "glue_job_target" {
  rule      = aws_cloudwatch_event_rule.analytics_schedule.name
  target_id = "GlueJobTarget"
  arn       = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.business_analytics.name}"
  role_arn  = aws_iam_role.eventbridge_glue_role.arn
}

# IAM role for EventBridge to trigger Glue job
resource "aws_iam_role" "eventbridge_glue_role" {
  name = "${var.project_name}-eventbridge-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_glue_policy" {
  name = "${var.project_name}-eventbridge-glue-policy"
  role = aws_iam_role.eventbridge_glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun"
        ]
        Resource = aws_glue_job.business_analytics.arn
      }
    ]
  })
}

# SNS topic for job notifications
resource "aws_sns_topic" "analytics_notifications" {
  name = "${var.project_name}-analytics-notifications"
}

resource "aws_sns_topic_policy" "analytics_notifications_policy" {
  arn = aws_sns_topic.analytics_notifications.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.analytics_notifications.arn
      }
    ]
  })
}
