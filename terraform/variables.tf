variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "vpc_id" {
  type    = string
  default = "vpc-0342476ccc1ef05f4"
}

variable "subnet_id" {
  type    = string
  default = "subnet-043c307ad7cec520a"
}

variable "key_pair_name" {
  type    = string
  default = "cnelson-prodse-01-15-2025"
}

variable "my_ip_cidr" {
  description = "Your current public IP in CIDR form, e.g. 1.2.3.4/32. Restricts SSH/Console access to just you."
  type        = string
}

variable "instance_type" {
  type    = string
  default = "t3.xlarge"
}

variable "project_name" {
  type    = string
  default = "rp-demo"
}
