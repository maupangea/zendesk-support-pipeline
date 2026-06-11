locals {
  # High-frequency (every 15 min) streams.
  high_frequency_streams = ["ticket", "user", "organization", "ticket_comment"]

  # Every other stream (every 6 hours). Keep in sync with config/connector.yaml.
  # Derived streams whose parent is high-frequency (e.g. ticket_tag → ticket) are
  # safe here: the orchestrator cache-only-fetches the missing parent automatically.
  config_streams = [
    "automation", "automation_action", "automation_condition",
    "brand",
    "group", "group_membership",
    "macro", "macro_attachment",
    "organization_field", "organization_field_option", "organization_member", "organization_tag",
    "satisfaction_rating",
    "schedule", "schedule_holiday",
    "sla_policy", "sla_policy_condition", "sla_policy_filter",
    "tag",
    "ticket_audit", "ticket_comment_attachment", "ticket_email_cc",
    "ticket_field", "ticket_field_option", "ticket_follower",
    "ticket_form", "ticket_form_condition",
    "ticket_metric", "ticket_metric_event", "ticket_tag",
    "trigger", "trigger_action", "trigger_condition",
    "user_field", "user_field_option", "user_identity", "user_tag",
    "view",
  ]
}

resource "aws_sqs_queue" "dlq" {
  name                      = "zendesk-ingestion-dlq-${var.env}"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_cloudwatch_event_rule" "high_frequency" {
  name                = "zendesk-high-frequency-${var.env}"
  description         = "High-frequency Zendesk stream sync."
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_rule" "config" {
  name                = "zendesk-config-${var.env}"
  description         = "Lower-frequency Zendesk config/metadata stream sync."
  schedule_expression = "rate(6 hours)"
}

resource "aws_cloudwatch_event_target" "high_frequency" {
  rule     = aws_cloudwatch_event_rule.high_frequency.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.events.arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.sync.arn
    task_count          = 1
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = var.subnet_ids
      security_groups  = var.security_group_ids
      assign_public_ip = var.assign_public_ip
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "zendesk-ingestion"
        command = ["sync", "--streams", join(",", local.high_frequency_streams)]
      }
    ]
  })

  dead_letter_config {
    arn = aws_sqs_queue.dlq.arn
  }
}

resource "aws_cloudwatch_event_target" "config" {
  rule     = aws_cloudwatch_event_rule.config.name
  arn      = aws_ecs_cluster.this.arn
  role_arn = aws_iam_role.events.arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.sync.arn
    task_count          = 1
    launch_type         = "FARGATE"

    network_configuration {
      subnets          = var.subnet_ids
      security_groups  = var.security_group_ids
      assign_public_ip = var.assign_public_ip
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "zendesk-ingestion"
        command = ["sync", "--streams", join(",", local.config_streams)]
      }
    ]
  })

  dead_letter_config {
    arn = aws_sqs_queue.dlq.arn
  }
}

data "aws_iam_policy_document" "dlq" {
  statement {
    sid       = "AllowEventBridge"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [
        aws_cloudwatch_event_rule.high_frequency.arn,
        aws_cloudwatch_event_rule.config.arn,
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  policy    = data.aws_iam_policy_document.dlq.json
}
