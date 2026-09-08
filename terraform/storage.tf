data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "iceberg" {
  bucket        = "${var.project_name}-iceberg-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_glue_catalog_database" "iceberg" {
  name = "${replace(var.project_name, "-", "_")}_db"
}
