# The data-lake bucket may already be managed elsewhere; set create_bucket=false to skip.
resource "aws_s3_bucket" "raw" {
  count  = var.create_bucket ? 1 : 0
  bucket = var.s3_bucket
}

resource "aws_s3_bucket_versioning" "raw" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.raw[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.raw[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.raw[0].id

  rule {
    id     = "zendesk-intelligent-tiering"
    status = "Enabled"

    filter {
      prefix = "zendesk_support/"
    }

    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  count                   = var.create_bucket ? 1 : 0
  bucket                  = aws_s3_bucket.raw[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
