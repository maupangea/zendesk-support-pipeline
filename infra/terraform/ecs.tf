resource "aws_cloudwatch_log_group" "app" {
  name              = "/zendesk-ingestion/${var.env}"
  retention_in_days = 30
}

resource "aws_ecs_cluster" "this" {
  name = "zendesk-ingestion-${var.env}"
}

resource "aws_ecs_task_definition" "sync" {
  family                   = "zendesk-ingestion-${var.env}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024" # 1 vCPU
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = "zendesk-ingestion"
      image     = var.ecr_image_uri
      essential = true
      command   = ["sync"]
      environment = [
        { name = "CONNECTOR_ID", value = var.connector_id },
        { name = "AWS_REGION", value = var.aws_region },
        { name = "DYNAMODB_TABLE", value = aws_dynamodb_table.state.name },
        { name = "S3_BUCKET", value = var.s3_bucket },
        { name = "S3_PREFIX", value = "zendesk_support" },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.app.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "sync"
        }
      }
    }
  ])
}
