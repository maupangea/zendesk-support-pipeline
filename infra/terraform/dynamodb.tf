resource "aws_dynamodb_table" "state" {
  name         = "zendesk_ingestion_state_${var.env}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "connector_id"
  range_key    = "stream_name"

  attribute {
    name = "connector_id"
    type = "S"
  }

  attribute {
    name = "stream_name"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }
}
