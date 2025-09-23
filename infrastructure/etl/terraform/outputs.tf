# ETL Pipeline Outputs

output "glue_job_name" {
  description = "Name of the ETL Glue job"
  value       = aws_glue_job.etl_job.name
}

output "lambda_function_name" {
  description = "Name of the check table Lambda function"
  value       = aws_lambda_function.check_table.function_name
}

output "glue_job_arn" {
  description = "ARN of the ETL Glue job"
  value       = aws_glue_job.etl_job.arn
}

output "lambda_function_arn" {
  description = "ARN of the check table Lambda function"
  value       = aws_lambda_function.check_table.arn
}
