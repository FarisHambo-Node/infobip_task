terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ===== ETL PIPELINE RESOURCES =====

# Upload Glue script to S3
resource "aws_s3_object" "glue_script" {
  bucket = var.s3_bucket_name
  key    = "scripts/glue_job.py"
  source = "${path.module}/../../../etl/glue_job.py"
  etag   = filemd5("${path.module}/../../../etl/glue_job.py")
  
  tags = {
    Name        = "Glue ETL Script"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Build Lambda package with dependencies
resource "null_resource" "build_lambda_package" {
  triggers = {
    lambda_code = filemd5("${path.module}/../../../etl/check_lambda.py")
    requirements = filemd5("${path.module}/../../../etl/lambda_requirements.txt")
  }

  provisioner "local-exec" {
    command = <<-EOT
      cd ${path.module}
      rm -rf lambda_package
      mkdir lambda_package
      cp ${path.module}/../../../etl/check_lambda.py lambda_package/
      pip install -r ${path.module}/../../../etl/lambda_requirements.txt -t lambda_package/
      cd lambda_package
      zip -r ../check_lambda.zip .
      cd ..
      rm -rf lambda_package
    EOT
  }
}

# Lambda function for table check
resource "aws_lambda_function" "check_table" {
  filename         = "${path.module}/check_lambda.zip"
  function_name    = "${var.project_name}-${var.environment}-check-table"
  role            = var.data_processing_role_arn
  handler         = "check_lambda.lambda_handler"
  runtime         = "python3.9"
  timeout         = 60
  
  environment {
    variables = {
      DB_HOST     = var.db_host
      DB_NAME     = var.db_name
      DB_USER     = var.db_user
      DB_PASSWORD = var.db_password
    }
  }

  tags = {
    Name        = "ETL Check Table Lambda"
    Environment = var.environment
    Project     = var.project_name
  }

  depends_on = [null_resource.build_lambda_package]
}

# Glue Job
resource "aws_glue_job" "etl_job" {
  name     = "${var.project_name}-${var.environment}-etl-glue-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/glue_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                = "python"
    "--S3_BUCKET"                  = var.s3_bucket_name
    "--DB_HOST"                    = var.db_host
    "--DB_NAME"                    = var.db_name
    "--DB_USER"                    = var.db_user
    "--DB_PASSWORD"                = var.db_password
    "--additional-python-modules"  = "psycopg2-binary==2.9.7"
  }

  max_capacity = 10
  timeout      = 30

  tags = {
    Name        = "ETL Glue Job"
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-${var.environment}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })

  tags = {
    Name        = "Glue Role"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Attach Glue service role policy
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Attach S3 access policy to Glue role
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = var.s3_data_access_policy_arn
}

# Data source for current AWS account ID
data "aws_caller_identity" "current" {}
