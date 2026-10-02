output "base_url" {
  description = "Public URL of the service (ALB DNS name unless you front it with your own domain)."
  value       = local.base_url
}

output "alb_dns_name" {
  value = aws_lb.this.dns_name
}

output "click_queue_url" {
  value = aws_sqs_queue.clicks.url
}

output "click_dlq_url" {
  value = aws_sqs_queue.clicks_dlq.url
}

output "db_endpoint" {
  value = aws_db_instance.this.address
}

output "seed_api_key_secret_arn" {
  description = "Fetch the demo API key: aws secretsmanager get-secret-value --secret-id <arn> --query SecretString --output text"
  value       = aws_secretsmanager_secret.seed_api_key.arn
}

output "alarm_topic_arn" {
  value = aws_sns_topic.alarms.arn
}
