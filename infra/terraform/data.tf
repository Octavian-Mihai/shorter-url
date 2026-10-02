# ---------- PostgreSQL (source of truth) ----------
resource "random_password" "db" {
  length  = 32
  special = false # keeps the connection URL free of characters needing escaping
}

resource "aws_db_subnet_group" "this" {
  name       = var.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_instance" "this" {
  identifier     = var.name
  engine         = "postgres"
  engine_version = "16"
  instance_class = var.db_instance_class

  allocated_storage     = 20
  max_allocated_storage = 100
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "shortener"
  username = "shortener"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.db.id]
  publicly_accessible    = false
  multi_az               = var.db_multi_az

  backup_retention_period      = 7
  performance_insights_enabled = true
  auto_minor_version_upgrade   = true

  deletion_protection       = var.deletion_protection
  skip_final_snapshot       = !var.deletion_protection
  final_snapshot_identifier = var.deletion_protection ? "${var.name}-final" : null
}

# ---------- Redis (cache + rate limiter) ----------
resource "aws_elasticache_subnet_group" "this" {
  name       = var.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_elasticache_replication_group" "this" {
  replication_group_id = var.name
  description          = "Slug cache and token buckets"
  engine               = "redis"
  engine_version       = "7.1"
  node_type            = var.redis_node_type
  port                 = 6379

  num_cache_clusters         = var.redis_replicas
  automatic_failover_enabled = var.redis_replicas > 1
  multi_az_enabled           = var.redis_replicas > 1

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.redis.id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true # the API enables TLS via REDIS_TLS=true

  # Pure cache + rate-limit state: losing it is harmless, so no snapshots.
  snapshot_retention_limit = 0
}

# ---------- SQS (click events), replacing Kafka in the cloud ----------
resource "aws_sqs_queue" "clicks_dlq" {
  name                      = "${var.name}-click-events-dlq"
  message_retention_seconds = 1209600 # 14 days to investigate poison messages
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "clicks" {
  name = "${var.name}-click-events"

  # Must exceed consumer flush interval + DB insert time, otherwise in-flight
  # messages reappear while still being processed (harmless: sink is idempotent).
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600 # 4 days of buffer if the consumer is down
  receive_wait_time_seconds  = 20     # long polling
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.clicks_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.clicks_dlq.id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.clicks.arn]
  })
}
