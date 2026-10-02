locals {
  base_url = "${local.https ? "https" : "http"}://${aws_lb.this.dns_name}"

  common_env = {
    EVENT_BACKEND = "sqs"
    SQS_QUEUE_URL = aws_sqs_queue.clicks.url
    AWS_REGION    = var.region
    REDIS_ADDR    = "${aws_elasticache_replication_group.this.primary_endpoint_address}:6379"
    REDIS_TLS     = "true"
    METRICS_ADDR  = ":9100"
  }

  common_secrets = {
    DATABASE_URL    = aws_secretsmanager_secret.database_url.arn
    SCRAMBLE_SECRET = aws_secretsmanager_secret.scramble_secret.arn
  }

  env_list   = [for k, v in local.common_env : { name = k, value = v }]
  secretlist = [for k, arn in local.common_secrets : { name = k, valueFrom = arn }]
}

resource "aws_ecs_cluster" "this" {
  name = var.name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${var.name}/api"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/ecs/${var.name}/consumer"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "migrate" {
  name              = "/ecs/${var.name}/migrate"
  retention_in_days = 14
}

# ---------------- API ----------------
resource "aws_ecs_task_definition" "api" {
  family                   = "${var.name}-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.api_cpu
  memory                   = var.api_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.api.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "api"
    image     = var.image_app
    essential = true

    portMappings = [{ containerPort = 8080, protocol = "tcp" }]

    environment = concat(local.env_list, [
      { name = "HTTP_ADDR", value = ":8080" },
      { name = "BASE_URL", value = local.base_url },
      # The ALB appends the real client address to X-Forwarded-For; the API
      # reads the rightmost entry, so client-forged values are ignored.
      { name = "TRUST_PROXY", value = "true" },
      { name = "RATE_LIMIT_BURST", value = tostring(var.rate_limit_burst) },
      { name = "RATE_LIMIT_PER_MINUTE", value = tostring(var.rate_limit_per_minute) },
    ])
    secrets = concat(local.secretlist, [
      { name = "SEED_API_KEY", valueFrom = aws_secretsmanager_secret.seed_api_key.arn },
    ])

    readonlyRootFilesystem = true
    stopTimeout            = 30 # matches the API's graceful shutdown window

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.api.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "api"
      }
    }
  }])
}

resource "aws_ecs_service" "api" {
  name            = "api"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_min_count
  launch_type     = "FARGATE"

  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  health_check_grace_period_seconds  = 30

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.app.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8080
  }

  # Autoscaling owns the count after creation.
  lifecycle {
    ignore_changes = [desired_count]
  }

  depends_on = [aws_lb_listener.http, terraform_data.migrate]
}

# ---------------- Consumer ----------------
resource "aws_ecs_task_definition" "consumer" {
  family                   = "${var.name}-consumer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.consumer.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name       = "consumer"
    image      = var.image_app
    essential  = true
    entryPoint = ["/consumer"]

    environment            = local.env_list
    secrets                = local.secretlist
    readonlyRootFilesystem = true
    stopTimeout            = 30 # final batch flush on SIGTERM

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.consumer.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "consumer"
      }
    }
  }])
}

resource "aws_ecs_service" "consumer" {
  name            = "consumer"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.consumer.arn
  desired_count   = var.consumer_count
  launch_type     = "FARGATE"

  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 200

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.app.id]
    assign_public_ip = false
  }

  depends_on = [terraform_data.migrate]
}

# ---------------- Migrations (one-off task) ----------------
resource "aws_ecs_task_definition" "migrate" {
  family                   = "${var.name}-migrate"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.execution.arn

  container_definitions = jsonencode([{
    name       = "migrate"
    image      = var.image_migrate
    essential  = true
    entryPoint = ["sh", "-c"]
    command    = ["migrate -path /migrations -database \"$DATABASE_URL\" up"]
    secrets    = [{ name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.migrate.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "migrate"
      }
    }
  }])
}

# Runs the schema migration to completion BEFORE the services are created, and
# again whenever the migration image changes. Requires the AWS CLI + credentials
# on the machine running `terraform apply`.
resource "terraform_data" "migrate" {
  triggers_replace = [var.image_migrate, aws_ecs_task_definition.migrate.arn]

  provisioner "local-exec" {
    command = "${path.module}/scripts/run-migrate.sh"
    environment = {
      AWS_REGION      = var.region
      CLUSTER         = aws_ecs_cluster.this.name
      TASK_DEFINITION = aws_ecs_task_definition.migrate.arn
      SUBNETS         = join(",", aws_subnet.private[*].id)
      SECURITY_GROUP  = aws_security_group.app.id
    }
  }

  depends_on = [aws_db_instance.this, aws_secretsmanager_secret_version.database_url]
}

# ---------------- Autoscaling (API) ----------------
resource "aws_appautoscaling_target" "api" {
  service_namespace  = "ecs"
  scalable_dimension = "ecs:service:DesiredCount"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${aws_ecs_service.api.name}"
  min_capacity       = var.api_min_count
  max_capacity       = var.api_max_count
}

resource "aws_appautoscaling_policy" "api_cpu" {
  name               = "${var.name}-api-cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.api.service_namespace
  scalable_dimension = aws_appautoscaling_target.api.scalable_dimension
  resource_id        = aws_appautoscaling_target.api.resource_id

  target_tracking_scaling_policy_configuration {
    target_value       = 60
    scale_in_cooldown  = 120
    scale_out_cooldown = 30
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}

resource "aws_appautoscaling_policy" "api_requests" {
  name               = "${var.name}-api-requests"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.api.service_namespace
  scalable_dimension = aws_appautoscaling_target.api.scalable_dimension
  resource_id        = aws_appautoscaling_target.api.resource_id

  target_tracking_scaling_policy_configuration {
    target_value       = 6000 # requests per target per minute
    scale_in_cooldown  = 120
    scale_out_cooldown = 30
    predefined_metric_specification {
      predefined_metric_type = "ALBRequestCountPerTarget"
      resource_label         = "${aws_lb.this.arn_suffix}/${aws_lb_target_group.api.arn_suffix}"
    }
  }
}
