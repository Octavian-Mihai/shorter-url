resource "random_password" "seed_api_key" {
  length  = 40
  special = false
}

# Must be identical on every task, forever: it determines slug scrambling.
resource "random_integer" "scramble_secret" {
  min = 1000000
  max = 2147483647

  lifecycle {
    ignore_changes = [min, max]
  }
}

resource "aws_secretsmanager_secret" "database_url" {
  name_prefix             = "${var.name}/database-url-"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "database_url" {
  secret_id = aws_secretsmanager_secret.database_url.id
  # sslmode=require: RDS enforces TLS on Postgres 15+.
  secret_string = "postgres://${aws_db_instance.this.username}:${random_password.db.result}@${aws_db_instance.this.address}:${aws_db_instance.this.port}/${aws_db_instance.this.db_name}?sslmode=require"
}

resource "aws_secretsmanager_secret" "seed_api_key" {
  name_prefix             = "${var.name}/seed-api-key-"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "seed_api_key" {
  secret_id     = aws_secretsmanager_secret.seed_api_key.id
  secret_string = random_password.seed_api_key.result
}

resource "aws_secretsmanager_secret" "scramble_secret" {
  name_prefix             = "${var.name}/scramble-secret-"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "scramble_secret" {
  secret_id     = aws_secretsmanager_secret.scramble_secret.id
  secret_string = tostring(random_integer.scramble_secret.result)
}
