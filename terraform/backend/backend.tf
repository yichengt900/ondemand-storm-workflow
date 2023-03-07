provider "aws" { 
  region = "us-east-1"
}


###################
resource "aws_s3_bucket" "odssm-s3-backend" {
  bucket = "tacc-nos-icogs-backend"

  tags = {
    Name = "On-Demand Storm Surge Modeling"
    Phase = "Development"
    POCName = "saeed.moghimi@noaa.gov"
    Project = "NOAA ICOGS-C"
    LineOffice = "NOS"
    DivisionBranch = "CSDL-CMMB"
    Reason = "terraform"
  }
}


###################
resource "aws_s3_bucket_acl" "odssm-s3-backend-acl" {
  bucket = aws_s3_bucket.odssm-s3-backend.id
  acl = "private"
}


###################
resource "aws_s3_bucket_public_access_block" "odssm-s3-backend-accessblock" {
  bucket = aws_s3_bucket.odssm-s3-backend.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


###################
resource "aws_kms_key" "odssm-kms-s3-backend" {
  description             = "This key is used to encrypt bucket objects"
  deletion_window_in_days = 10
}


###################
resource "aws_s3_bucket_server_side_encryption_configuration" "odssm-s3-backend-encrypt" {
  bucket = aws_s3_bucket.odssm-s3-backend.bucket

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.odssm-kms-s3-backend.arn
      sse_algorithm     = "aws:kms"
    }
  }
}


###################
resource "aws_s3_bucket_versioning" "odssm-s3-backend-versioning" {
  bucket = aws_s3_bucket.odssm-s3-backend.id
  versioning_configuration {
    status = "Enabled"
  }
}


###################
resource "aws_s3_bucket_lifecycle_configuration" "odssm-s3-backend-lifecycle" {
  # Must have bucket versioning enabled first
  depends_on = [aws_s3_bucket_versioning.odssm-s3-backend-versioning]

  bucket = aws_s3_bucket.odssm-s3-backend.id

  rule {
    id = "statefile"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 60
    }

    status = "Enabled"
  }
}
