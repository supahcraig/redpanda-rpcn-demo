output "instance_public_ip" {
  value = aws_instance.demo.public_ip
}

output "iceberg_bucket" {
  value = aws_s3_bucket.iceberg.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.iceberg.name
}

output "aws_account_id" {
  value = data.aws_caller_identity.current.account_id
}
