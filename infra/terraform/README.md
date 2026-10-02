# AWS deployment (Terraform)

Deploys the same two binaries to **ECS Fargate**, with **SQS instead of Kafka**
(`EVENT_BACKEND=sqs`: the interface swap described in the main README).

```
Internet → ALB ──► ECS Fargate: api  (2–10 tasks, autoscaled) ──► ElastiCache Redis (TLS)
                                 │                           └──► RDS PostgreSQL 16 (TLS)
                                 └── SendMessage ──► SQS click-events ──► (DLQ after 5 receives)
                   ECS Fargate: consumer (N tasks) ◄── ReceiveMessage / DeleteMessage ┘
                                 └── batched idempotent INSERT ──► RDS
```

| File | What |
|---|---|
| `network.tf` | VPC, 2–3 AZs, public/private subnets, NAT (single by default to save cost) |
| `security.tf` | Security groups: ALB → app:8080 → {RDS:5432, Redis:6379}; nothing else inbound |
| `data.tf` | RDS Postgres (encrypted, private, 7-day backups), ElastiCache Redis (TLS + at-rest), SQS queue + DLQ (SSE, redrive) |
| `secrets.tf` | DB URL, seed API key and scramble secret in Secrets Manager (never in task definitions) |
| `iam.tf` | Execution role + **separate least-privilege task roles**: API can only `SendMessage`; consumer can only receive/delete |
| `alb.tf` | ALB, `/readyz` health checks, optional HTTPS + HTTP→HTTPS redirect |
| `ecs.tf` | Cluster, API/consumer services, one-off migration task, autoscaling (CPU and requests/target) |
| `monitoring.tf` | CloudWatch alarms → SNS (5xx, p99 latency, unhealthy targets, DLQ not empty, consumer behind, RDS CPU) |

## Use

```bash
cd infra/terraform
terraform init
terraform plan  -out tfplan
terraform apply tfplan          # needs AWS credentials + AWS CLI (runs the migration task)
terraform output base_url
```

First apply order is enforced in Terraform: database → **migration task runs to completion**
(`scripts/run-migrate.sh`, fails the apply if the exit code is non-zero) → services start.

Get the generated API key:

```bash
aws secretsmanager get-secret-value --secret-id "$(terraform output -raw seed_api_key_secret_arn)" \
  --query SecretString --output text
```

Container images default to `ghcr.io/octavian-mihai/shorter-url{,-migrate}:latest`, which CI publishes.
Fargate can only pull **public** GHCR packages; make them public, or push to ECR and set
`-var image_app=... -var image_migrate=...`.

Tear down a demo: `terraform destroy` (set `-var deletion_protection=false`, the default, first).

## What was verified, and what was not

Verified in this repo (CI runs the first three on every change under `infra/`):
`terraform fmt -check`, `terraform init` + `terraform validate` against the real AWS provider schema,
and a Trivy misconfiguration scan with no HIGH/CRITICAL findings (three deliberate, justified ignores:
public ALB, HTTP listener when no certificate is configured, HTTPS egress to AWS APIs via NAT).

**Not verified: `terraform plan`/`apply` against a real AWS account.** That needs credentials and creates
billable resources, so it has not been run. Expect to iterate on account-specific details
(quotas, AZ availability, image pull access) on a first real apply.

## Rough cost (us-east-1, defaults, running 24/7)

NAT gateway ≈ $33/mo + ALB ≈ $17/mo + 2×api & 2×consumer Fargate (0.25 vCPU / 0.5 GB) ≈ $36/mo +
RDS `db.t4g.micro` ≈ $13/mo + ElastiCache `cache.t4g.micro` ≈ $12/mo + small SQS/CloudWatch/Secrets
charges. Roughly **$110–130/month**; destroy it when you are not demoing.

## Known hardening gaps (Trivy LOW/MEDIUM)

Deliberately left out to keep the demo affordable and simple; each is a small addition:
customer-managed KMS keys for CloudWatch logs, Secrets Manager and Performance Insights; RDS IAM database
authentication; VPC flow logs; deletion protection defaults to `false` so a demo can be destroyed;
VPC endpoints instead of NAT for SQS/Secrets/Logs; remote state backend (S3 + DynamoDB lock);
WAF in front of the ALB; an OIDC-authenticated `plan` job in CI.
