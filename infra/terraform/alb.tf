# The service is public by design: redirects must be reachable from anywhere.
#trivy:ignore:AWS-0053
resource "aws_lb" "this" {
  name_prefix        = "short-"
  load_balancer_type = "application"
  internal           = false
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]

  idle_timeout               = 30
  drop_invalid_header_fields = true
  enable_deletion_protection = var.deletion_protection
}

resource "aws_lb_target_group" "api" {
  name_prefix = "api-"
  vpc_id      = aws_vpc.this.id
  protocol    = "HTTP"
  port        = 8080
  target_type = "ip" # required for Fargate (awsvpc)

  deregistration_delay = 30 # the API drains in-flight requests on SIGTERM

  health_check {
    path                = "/readyz" # checks Postgres and Redis, so a broken task leaves rotation
    matcher             = "200"
    interval            = 10
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  lifecycle {
    create_before_destroy = true
  }
}

locals {
  https = var.certificate_arn != ""
}

# Without a custom domain there is no certificate, so the demo default serves
# HTTP. Set var.certificate_arn to serve HTTPS and redirect all HTTP to it.
#trivy:ignore:AWS-0054
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = local.https ? "redirect" : "forward"
    target_group_arn = local.https ? null : aws_lb_target_group.api.arn

    dynamic "redirect" {
      for_each = local.https ? [1] : []
      content {
        port        = "443"
        protocol    = "HTTPS"
        status_code = "HTTP_301"
      }
    }
  }
}

resource "aws_lb_listener" "https" {
  count             = local.https ? 1 : 0
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }
}
