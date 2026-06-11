variable "env" {
  type        = string
  description = "Deployment environment (dev, staging, prod)."
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "s3_bucket" {
  type        = string
  description = "Target data-lake bucket for raw Zendesk Parquet."
}

variable "ecr_image_uri" {
  type        = string
  description = "Full ECR image URI for the sync container (set by CI)."
}

variable "connector_id" {
  type    = string
  default = "zendesk_support"
}

variable "create_bucket" {
  type        = bool
  default     = true
  description = "Create and manage the S3 bucket here. Set false if it already exists elsewhere."
}

variable "subnet_ids" {
  type        = list(string)
  default     = []
  description = "Subnets for the Fargate task ENI. Must be set per environment at apply time."
}

variable "security_group_ids" {
  type        = list(string)
  default     = []
  description = "Security groups for the Fargate task ENI."
}

variable "assign_public_ip" {
  type        = bool
  default     = false
  description = "Assign a public IP to the task ENI (true for public subnets without a NAT)."
}
