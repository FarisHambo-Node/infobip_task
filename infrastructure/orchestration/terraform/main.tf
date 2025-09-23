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

# ===== ORCHESTRATION RESOURCES =====

# IAM Role for Step Functions
resource "aws_iam_role" "step_functions_role" {
  name = "${var.project_name}-${var.environment}-step-functions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "Step Functions Role"
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Policy for Step Functions to invoke Glue and Lambda
resource "aws_iam_policy" "step_functions_policy" {
  name        = "${var.project_name}-${var.environment}-step-functions-policy"
  description = "Policy for Step Functions to invoke Glue and Lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = var.glue_job_arn
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = var.lambda_function_arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:*"
      }
    ]
  })
}

# Attach policy to Step Functions role
resource "aws_iam_role_policy_attachment" "step_functions_policy_attachment" {
  role       = aws_iam_role.step_functions_role.name
  policy_arn = aws_iam_policy.step_functions_policy.arn
}

# Step Functions State Machine
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${var.project_name}-${var.environment}-etl-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Simple ETL Pipeline: Glue Job -> Lambda Check"
    StartAt = "RunGlueJob"
    States = {
      RunGlueJob = {
        Type = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = var.glue_job_name
        }
        TimeoutSeconds = 1800
        Retry = [
          {
            ErrorEquals = ["States.ALL"]
            IntervalSeconds = 30
            MaxAttempts = 2
            BackoffRate = 2.0
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next = "ETLFailed"
            ResultPath = "$.error"
          }
        ]
        Next = "CheckTable"
      }
      CheckTable = {
        Type = "Task"
        Resource = var.lambda_function_arn
        TimeoutSeconds = 60
        Retry = [
          {
            ErrorEquals = ["States.ALL"]
            IntervalSeconds = 5
            MaxAttempts = 2
            BackoffRate = 2.0
          }
        ]
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next = "ETLFailed"
            ResultPath = "$.error"
          }
        ]
        Next = "ETLSuccess"
      }
      ETLSuccess = {
        Type = "Succeed"
        Comment = "ETL pipeline completed successfully"
      }
      ETLFailed = {
        Type = "Fail"
        Comment = "ETL pipeline failed"
      }
    }
  })

  tags = {
    Name        = "ETL Pipeline State Machine"
    Environment = var.environment
    Project     = var.project_name
  }
}

# EventBridge Rule for monthly trigger (1st of month at 9:00 AM UTC)
resource "aws_cloudwatch_event_rule" "monthly_etl_trigger" {
  name                = "${var.project_name}-${var.environment}-monthly-etl-trigger"
  description         = "Trigger ETL pipeline on the 1st of every month at 9:00 AM UTC"
  schedule_expression = "cron(0 9 1 * ? *)"

  tags = {
    Name        = "Monthly ETL Trigger"
    Environment = var.environment
    Project     = var.project_name
  }
}

# EventBridge Target to trigger Step Functions
resource "aws_cloudwatch_event_target" "step_functions_target" {
  rule      = aws_cloudwatch_event_rule.monthly_etl_trigger.name
  target_id = "ETLPipelineTarget"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    "comment": "Monthly ETL pipeline execution triggered by EventBridge"
  })
}

# IAM Role for EventBridge to invoke Step Functions
resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project_name}-${var.environment}-eventbridge-role"

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

  tags = {
    Name        = "EventBridge Role"
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Policy for EventBridge to invoke Step Functions
resource "aws_iam_policy" "eventbridge_policy" {
  name        = "${var.project_name}-${var.environment}-eventbridge-policy"
  description = "Policy for EventBridge to invoke Step Functions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.etl_pipeline.arn
      }
    ]
  })
}

# Attach policy to EventBridge role
resource "aws_iam_role_policy_attachment" "eventbridge_policy_attachment" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_policy.arn
}

# Data source for current AWS account ID
data "aws_caller_identity" "current" {}
