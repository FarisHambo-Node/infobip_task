# Step Functions State Machine
output "step_functions_state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = aws_sfn_state_machine.etl_pipeline.arn
}

output "step_functions_state_machine_name" {
  description = "Name of the Step Functions state machine"
  value       = aws_sfn_state_machine.etl_pipeline.name
}

# EventBridge Rule
output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.monthly_etl_trigger.arn
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.monthly_etl_trigger.name
}

# IAM Roles
output "step_functions_role_arn" {
  description = "ARN of the Step Functions IAM role"
  value       = aws_iam_role.step_functions_role.arn
}

output "eventbridge_role_arn" {
  description = "ARN of the EventBridge IAM role"
  value       = aws_iam_role.eventbridge_role.arn
}

# Analytics Pipeline Step Functions State Machine
output "analytics_pipeline_state_machine_arn" {
  description = "ARN of the Analytics Pipeline Step Functions state machine"
  value       = aws_sfn_state_machine.analytics_pipeline.arn
}

output "analytics_pipeline_state_machine_name" {
  description = "Name of the Analytics Pipeline Step Functions state machine"
  value       = aws_sfn_state_machine.analytics_pipeline.name
}
