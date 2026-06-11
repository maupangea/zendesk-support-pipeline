# ----- Task role: least-privilege permissions for the running container -----

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task" {
  name               = "zendesk-ingestion-task-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    sid       = "S3ReadWrite"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:GetObject"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.s3_bucket}/zendesk_support/*"]
  }

  statement {
    sid       = "S3List"
    actions   = ["s3:ListBucket"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.s3_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["zendesk_support/*"]
    }
  }

  statement {
    sid       = "DynamoDBState"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.state.arn]
  }

  statement {
    sid       = "SecretsManager"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:${data.aws_partition.current.partition}:secretsmanager:*:*:secret:zendesk/*"]
  }

  statement {
    sid       = "SSM"
    actions   = ["ssm:GetParameter"]
    resources = ["arn:${data.aws_partition.current.partition}:ssm:*:*:parameter/zendesk/*"]
  }

  statement {
    sid       = "CloudWatchMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # CloudWatch does not support resource-level restrictions
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["ZendeskIngestion"]
    }
  }

  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.app.arn}:*"]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "zendesk-ingestion-task-${var.env}"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

# ----- Execution role: lets Fargate pull the image and ship logs ------------

resource "aws_iam_role" "execution" {
  name               = "zendesk-ingestion-execution-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ----- EventBridge role: lets the schedules launch the task -----------------

data "aws_iam_policy_document" "events_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events" {
  name               = "zendesk-ingestion-events-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.events_assume.json
}

data "aws_iam_policy_document" "events" {
  statement {
    sid     = "RunTask"
    actions = ["ecs:RunTask"]
    resources = [
      "arn:${data.aws_partition.current.partition}:ecs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:task-definition/zendesk-ingestion-${var.env}:*"
    ]
    condition {
      test     = "ArnLike"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.this.arn]
    }
  }

  statement {
    sid       = "PassRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.task.arn, aws_iam_role.execution.arn]
  }
}

resource "aws_iam_role_policy" "events" {
  name   = "zendesk-ingestion-events-${var.env}"
  role   = aws_iam_role.events.id
  policy = data.aws_iam_policy_document.events.json
}
