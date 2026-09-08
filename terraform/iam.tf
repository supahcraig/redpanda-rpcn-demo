data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "demo" {
  name               = "${var.project_name}-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

data "aws_iam_policy_document" "demo_permissions" {
  statement {
    sid = "S3IcebergBucket"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.iceberg.arn,
      "${aws_s3_bucket.iceberg.arn}/*",
    ]
  }

  statement {
    sid       = "GlueCatalog"
    actions   = ["glue:*"]
    resources = ["*"]
  }

  statement {
    sid       = "AthenaQuery"
    actions   = ["athena:*"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "demo" {
  name   = "${var.project_name}-permissions"
  role   = aws_iam_role.demo.id
  policy = data.aws_iam_policy_document.demo_permissions.json
}

resource "aws_iam_instance_profile" "demo" {
  name = "${var.project_name}-profile"
  role = aws_iam_role.demo.name
}
