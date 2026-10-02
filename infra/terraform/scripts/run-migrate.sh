#!/usr/bin/env bash
# Runs the one-off migration ECS task and fails if it does not exit 0.
# Invoked by terraform_data.migrate; inputs arrive as environment variables.
set -euo pipefail

: "${AWS_REGION:?}" "${CLUSTER:?}" "${TASK_DEFINITION:?}" "${SUBNETS:?}" "${SECURITY_GROUP:?}"

task_arn=$(aws ecs run-task \
  --region "$AWS_REGION" --cluster "$CLUSTER" --launch-type FARGATE \
  --task-definition "$TASK_DEFINITION" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SECURITY_GROUP],assignPublicIp=DISABLED}" \
  --query 'tasks[0].taskArn' --output text)

if [ -z "$task_arn" ] || [ "$task_arn" = "None" ]; then
  echo "failed to start migration task" >&2
  exit 1
fi
echo "migration task: $task_arn"

aws ecs wait tasks-stopped --region "$AWS_REGION" --cluster "$CLUSTER" --tasks "$task_arn"

exit_code=$(aws ecs describe-tasks --region "$AWS_REGION" --cluster "$CLUSTER" --tasks "$task_arn" \
  --query 'tasks[0].containers[0].exitCode' --output text)

if [ "$exit_code" != "0" ]; then
  echo "migration failed (exit code: $exit_code). See CloudWatch log group /ecs/*/migrate" >&2
  exit 1
fi
echo "migrations applied"
