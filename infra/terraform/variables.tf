variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name prefix for all resources."
  type        = string
  default     = "shortener"
}

variable "image_app" {
  description = "Container image for the API and consumer (must be pullable by Fargate: ECR, or a public GHCR package)."
  type        = string
  default     = "ghcr.io/octavian-mihai/shorter-url:latest"
}

variable "image_migrate" {
  description = "Container image that applies database migrations."
  type        = string
  default     = "ghcr.io/octavian-mihai/shorter-url-migrate:latest"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "az_count" {
  description = "Number of availability zones to spread across."
  type        = number
  default     = 2
  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "Use 2 or 3 AZs."
  }
}

variable "single_nat_gateway" {
  description = "One shared NAT gateway (cheap, single AZ dependency) vs one per AZ (resilient)."
  type        = bool
  default     = true
}

# ---- sizing ----
variable "api_cpu" {
  type    = number
  default = 256
}

variable "api_memory" {
  type    = number
  default = 512
}

variable "api_min_count" {
  type    = number
  default = 2
}

variable "api_max_count" {
  type    = number
  default = 10
}

variable "consumer_count" {
  description = "Consumer tasks. SQS lets any number of consumers share the queue."
  type        = number
  default     = 2
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}

variable "db_multi_az" {
  type    = bool
  default = false
}

variable "redis_node_type" {
  type    = string
  default = "cache.t4g.micro"
}

variable "redis_replicas" {
  description = "Cache nodes (>=2 enables automatic failover)."
  type        = number
  default     = 1
}

# ---- safety rails ----
variable "deletion_protection" {
  description = "Protect the database and ALB from accidental deletion. Set false to tear down a demo."
  type        = bool
  default     = false
}

variable "certificate_arn" {
  description = "ACM certificate ARN. If set, the ALB serves HTTPS and redirects HTTP to it."
  type        = string
  default     = ""
}

variable "alarm_email" {
  description = "Optional e-mail subscribed to the alarm SNS topic."
  type        = string
  default     = ""
}

variable "rate_limit_burst" {
  type    = number
  default = 10
}

variable "rate_limit_per_minute" {
  type    = number
  default = 60
}
