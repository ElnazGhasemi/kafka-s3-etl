# S3 bucket for storing application data
provider "aws" {
  region = "us-east-1"
}

terraform {
  backend "s3" {
    bucket = "data-engineering-terraform-state-bucket"
    key    = "statefile/terraform.tfstate"
    region = "us-east-1"
  }
}

resource "aws_s3_bucket" "product_bucket" {
  bucket = "data-engineering-product-bucket"
  tags = {
    Name        = "ProductBucket"
    Environment = "Production"
  }

}
resource "aws_s3_bucket" "terraform_state_bucket" {
  bucket = "data-engineering-terraform-state-bucket"
  tags = {
    Name        = "TerraformStateBucket"
    Environment = "Production"
  }

}


