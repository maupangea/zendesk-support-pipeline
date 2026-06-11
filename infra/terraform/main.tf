terraform {
  required_version = "~> 1.8"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {} # configure via -backend-config in CI
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project = "zendesk-ingestion"
      Env     = var.env
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}
