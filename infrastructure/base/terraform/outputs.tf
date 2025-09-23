output "s3_bucket_name" {
  description = "Name of the S3 bucket for data storage"
  value       = aws_s3_bucket.data_bucket.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.data_bucket.arn
}

output "data_processing_role_arn" {
  description = "ARN of the IAM role for data processing"
  value       = aws_iam_role.data_processing_role.arn
}

output "data_processing_role_name" {
  description = "Name of the IAM role for data processing"
  value       = aws_iam_role.data_processing_role.name
}
