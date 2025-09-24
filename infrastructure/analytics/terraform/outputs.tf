output "analytics_scripts_bucket_name" {
  description = "Name of the S3 bucket containing analytics scripts"
  value       = aws_s3_bucket.analytics_scripts.bucket
}

output "analytics_scripts_bucket_arn" {
  description = "ARN of the S3 bucket containing analytics scripts"
  value       = aws_s3_bucket.analytics_scripts.arn
}

output "glue_analytics_job_name" {
  description = "Name of the Glue analytics job"
  value       = aws_glue_job.business_analytics.name
}

output "glue_analytics_job_arn" {
  description = "ARN of the Glue analytics job"
  value       = aws_glue_job.business_analytics.arn
}

output "glue_analytics_role_arn" {
  description = "ARN of the IAM role for Glue analytics job"
  value       = aws_iam_role.glue_analytics_role.arn
}

