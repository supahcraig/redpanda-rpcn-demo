# Redpanda + RPCN Prospect Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a self-contained Redpanda + Redpanda Connect (RPCN) demo — 3-broker cluster, Postgres, and four pipelines (MQ generator, alerting, enrichment, FTP-to-Iceberg) — on a single EC2 host, with the Iceberg output queryable live from Athena.

**Architecture:** Terraform provisions a minimal AWS footprint (EC2 host with an IAM instance role, S3 bucket, Glue catalog database) inside an existing VPC. All application containers (Redpanda x3, Console, Postgres, one Redpanda Connect container per pipeline, an SFTP test server) run via a single `docker-compose.yml` deployed to that EC2 host. Three pipelines talk only to the local Redpanda cluster and Postgres; the fourth (FTP/Iceberg) uses the EC2 instance's IAM role to write Iceberg tables straight to S3 via AWS Glue's REST catalog endpoint, queried afterward from Athena.

**Tech Stack:** Terraform (AWS provider ~> 5.0), Docker + Docker Compose, Redpanda v26.2.2, Redpanda Connect (`docker.redpanda.com/redpandadata/connect`), Postgres 16, `atmoz/sftp`, AWS Glue Data Catalog + S3 + Athena.

**Spec:** None — no separate spec file was written. The design was agreed live in the brainstorming conversation on 2026-09-08 and is captured in full in this plan's Architecture section and per-task detail below.

## Global Constraints

- AWS region: `us-east-2`.
- Use the existing VPC `vpc-0342476ccc1ef05f4` and its public subnet `subnet-043c307ad7cec520a` — do not create new networking.
- Use the existing EC2 key pair `cnelson-prodse-01-15-2025` — do not create a new key pair.
- All AWS resources are named/tagged with the prefix `rp-demo` and must be safe to `terraform destroy` in full after the demo (no resources created by hand outside Terraform).
- This is an infra/streaming demo, not an application with unit tests. Every task's "testable deliverable" is a concrete verification command (terraform plan/apply output, `docker compose ps`, `rpk topic consume`, a SQL query, an Athena query) whose expected output is stated in the step — treat these the same way a unit test is treated elsewhere: run them, confirm the exact expected output, before moving on.
- Enrichment pipeline baseline is a Postgres lookup (not an LLM call) — this was explicitly deprioritized in favor of the Iceberg/Athena piece. Do not implement an LLM swap unless all 8 tasks below are done with time still remaining.
- Error handling is intentionally minimal throughout (log-and-drop on bad records) — do not add retry/DLQ machinery.

---

### Task 1: Terraform foundation — networking, IAM, storage

**Files:**
- Create: `terraform/versions.tf`
- Create: `terraform/variables.tf`
- Create: `terraform/terraform.tfvars.example`
- Create: `terraform/security.tf`
- Create: `terraform/iam.tf`
- Create: `terraform/storage.tf`
- Create: `.gitignore`

**Interfaces:**
- Consumes: nothing (first task).
- Produces (referenced by Task 2 and later Terraform files in the same `terraform/` module, since Terraform resources in the same directory share scope automatically): `aws_security_group.demo`, `aws_iam_instance_profile.demo`, `aws_s3_bucket.iceberg`, `aws_glue_catalog_database.iceberg`, `data.aws_caller_identity.current`, variables `var.aws_region`, `var.vpc_id`, `var.subnet_id`, `var.key_pair_name`, `var.instance_type`, `var.project_name`, `var.my_ip_cidr`.

- [ ] **Step 1: Initialize the repo**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git init
```

- [ ] **Step 2: Write `.gitignore`**

```gitignore
.terraform/
.terraform.lock.hcl
terraform.tfstate
terraform.tfstate.backup
terraform.tfvars
```

- [ ] **Step 3: Write `terraform/versions.tf`**

```hcl
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}
```

- [ ] **Step 4: Write `terraform/variables.tf`**

```hcl
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
```

- [ ] **Step 5: Write `terraform/terraform.tfvars.example`**

```hcl
my_ip_cidr = "REPLACE.WITH.YOUR.IP/32"
```

- [ ] **Step 6: Create the real tfvars file with your actual IP**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
MY_IP=$(curl -s https://checkip.amazonaws.com)
echo "my_ip_cidr = \"${MY_IP}/32\"" > terraform.tfvars
cat terraform.tfvars
```

Expected: prints a line like `my_ip_cidr = "203.0.113.7/32"`.

- [ ] **Step 7: Write `terraform/security.tf`**

```hcl
resource "aws_security_group" "demo" {
  name        = "${var.project_name}-sg"
  description = "SSH and Redpanda Console access for the RPCN demo"
  vpc_id      = var.vpc_id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }

  ingress {
    description = "Redpanda Console"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-sg"
  }
}
```

- [ ] **Step 8: Write `terraform/storage.tf`**

```hcl
data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "iceberg" {
  bucket        = "${var.project_name}-iceberg-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_glue_catalog_database" "iceberg" {
  name = "${replace(var.project_name, "-", "_")}_db"
}
```

- [ ] **Step 9: Write `terraform/iam.tf`**

The demo role gets broad-but-scoped permissions (full S3 access on just this bucket, full Glue/Athena on the account) — acceptable because this role is attached to nothing but this one throwaway demo host.

```hcl
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
```

- [ ] **Step 10: Validate**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
terraform init
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 11: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add .gitignore terraform/versions.tf terraform/variables.tf terraform/terraform.tfvars.example terraform/security.tf terraform/storage.tf terraform/iam.tf
git commit -m "Add Terraform foundation: security group, IAM role, S3 bucket, Glue database"
```

---

### Task 2: Terraform compute — EC2 host

**Files:**
- Create: `terraform/user_data.sh`
- Create: `terraform/compute.tf`
- Create: `terraform/outputs.tf`

**Interfaces:**
- Consumes: `aws_security_group.demo.id`, `aws_iam_instance_profile.demo.name`, `var.subnet_id`, `var.key_pair_name`, `var.instance_type` (Task 1).
- Produces: Terraform outputs `instance_public_ip`, `iceberg_bucket`, `glue_database`, `aws_account_id` — used by every later task to reach the host and to fill in pipeline configs.

- [ ] **Step 1: Write `terraform/user_data.sh`**

```bash
#!/bin/bash
set -eux
apt-get update -y
apt-get install -y ca-certificates curl gnupg
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
usermod -aG docker ubuntu
mkdir -p /home/ubuntu/stack
chown -R ubuntu:ubuntu /home/ubuntu/stack
```

- [ ] **Step 2: Write `terraform/compute.tf`**

The `metadata_options.http_put_response_hop_limit = 2` line is required — without it, containers on this host (which are one network hop further from the metadata service than the host itself) cannot reach IMDS to pick up the IAM role credentials, and the Iceberg pipeline in Task 8 will fail to authenticate to AWS.

```hcl
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "demo" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type                = var.instance_type
  subnet_id                    = var.subnet_id
  key_name                     = var.key_pair_name
  vpc_security_group_ids       = [aws_security_group.demo.id]
  iam_instance_profile         = aws_iam_instance_profile.demo.name
  associate_public_ip_address  = true

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 40
    volume_type = "gp3"
  }

  user_data = file("${path.module}/user_data.sh")

  tags = {
    Name = "${var.project_name}-host"
  }
}
```

- [ ] **Step 3: Write `terraform/outputs.tf`**

```hcl
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
```

- [ ] **Step 4: Apply**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
terraform plan -out=tfplan
terraform apply tfplan
```

Expected: apply completes with `Apply complete! Resources: 7 added, 0 changed, 0 destroyed.` (exact count may vary slightly) and prints the four outputs.

- [ ] **Step 5: Verify SSH access and Docker install**

Wait ~90s after apply for user-data to finish, then:

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
IP=$(terraform output -raw instance_public_ip)
ssh -o StrictHostKeyChecking=accept-new ubuntu@$IP "docker --version && docker compose version"
```

Expected: prints a Docker version and a Docker Compose version with no errors.

- [ ] **Step 6: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add terraform/user_data.sh terraform/compute.tf terraform/outputs.tf
git commit -m "Add Terraform EC2 host with instance-role IMDS hop limit fix"
```

---

### Task 3: Redpanda cluster + Console

**Files:**
- Create: `stack/docker-compose.yml`

**Interfaces:**
- Consumes: `instance_public_ip` (Task 2, to deploy to).
- Produces: a running 3-broker cluster reachable inside the compose network at `redpanda-0:9092`/`redpanda-1:9092`/`redpanda-2:9092`, and Console reachable at `http://<instance_public_ip>:8080`. Later tasks add services to this same file rather than replacing it.

- [ ] **Step 1: Write `stack/docker-compose.yml`**

```yaml
name: rp-demo
networks:
  demo_net:
    driver: bridge

volumes:
  redpanda-0: null
  redpanda-1: null
  redpanda-2: null

services:
  redpanda-0:
    image: docker.redpanda.com/redpandadata/redpanda:v26.2.2
    container_name: redpanda-0
    command:
      - redpanda
      - start
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr=internal://redpanda-0:9092,external://localhost:19092
      - --rpc-addr=redpanda-0:33145
      - --advertise-rpc-addr=redpanda-0:33145
      - --smp=1
      - --memory=1G
      - --mode=dev-container
      - --default-log-level=info
    volumes:
      - redpanda-0:/var/lib/redpanda/data
    networks:
      - demo_net
    ports:
      - "19092:19092"
      - "9644:9644"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 5s
      timeout: 3s
      retries: 30

  redpanda-1:
    image: docker.redpanda.com/redpandadata/redpanda:v26.2.2
    container_name: redpanda-1
    command:
      - redpanda
      - start
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:29092
      - --advertise-kafka-addr=internal://redpanda-1:9092,external://localhost:29092
      - --rpc-addr=redpanda-1:33145
      - --advertise-rpc-addr=redpanda-1:33145
      - --smp=1
      - --memory=1G
      - --mode=dev-container
      - --default-log-level=info
      - --seeds=redpanda-0:33145
    volumes:
      - redpanda-1:/var/lib/redpanda/data
    networks:
      - demo_net
    depends_on:
      - redpanda-0

  redpanda-2:
    image: docker.redpanda.com/redpandadata/redpanda:v26.2.2
    container_name: redpanda-2
    command:
      - redpanda
      - start
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:39092
      - --advertise-kafka-addr=internal://redpanda-2:9092,external://localhost:39092
      - --rpc-addr=redpanda-2:33145
      - --advertise-rpc-addr=redpanda-2:33145
      - --smp=1
      - --memory=1G
      - --mode=dev-container
      - --default-log-level=info
      - --seeds=redpanda-0:33145
    volumes:
      - redpanda-2:/var/lib/redpanda/data
    networks:
      - demo_net
    depends_on:
      - redpanda-0

  console:
    image: docker.redpanda.com/redpandadata/console:latest
    container_name: redpanda-console
    entrypoint: /bin/sh
    command: -c 'echo "$$CONSOLE_CONFIG_FILE" > /tmp/config.yml; /app/console -config.filepath=/tmp/config.yml'
    environment:
      CONSOLE_CONFIG_FILE: |
        kafka:
          brokers: ["redpanda-0:9092","redpanda-1:9092","redpanda-2:9092"]
        redpanda:
          adminApi:
            enabled: true
            urls: ["http://redpanda-0:9644"]
    ports:
      - "8080:8080"
    networks:
      - demo_net
    depends_on:
      - redpanda-0
      - redpanda-1
      - redpanda-2
```

- [ ] **Step 2: Deploy to the EC2 host and start the cluster**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
IP=$(terraform output -raw instance_public_ip)
scp -r ../stack ubuntu@$IP:~/stack
ssh ubuntu@$IP "cd ~/stack && docker compose up -d redpanda-0 redpanda-1 redpanda-2 console"
```

- [ ] **Step 3: Verify cluster health and Console reachability**

```bash
ssh ubuntu@$IP "docker exec redpanda-0 rpk cluster health"
curl -s -o /dev/null -w "%{http_code}\n" http://$IP:8080
```

Expected: `rpk cluster health` shows `Healthy: true` and 3 brokers; the curl prints `200`.

- [ ] **Step 4: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add stack/docker-compose.yml
git commit -m "Add 3-broker Redpanda cluster and Console to compose stack"
```

---

### Task 4: Postgres name-lookup table

**Files:**
- Create: `stack/postgres/init.sql`
- Modify: `stack/docker-compose.yml` (add `postgres` service)

**Interfaces:**
- Consumes: `demo_net` network (Task 3).
- Produces: a Postgres instance reachable inside the compose network at `postgres:5432`, database `demo`, table `name_lookup(nickname TEXT PRIMARY KEY, formal_name TEXT NOT NULL)` — consumed by the enrichment pipeline in Task 7 via DSN `postgres://demo:demo@postgres:5432/demo?sslmode=disable`.

- [ ] **Step 1: Write `stack/postgres/init.sql`**

```sql
CREATE TABLE name_lookup (
  nickname TEXT PRIMARY KEY,
  formal_name TEXT NOT NULL
);

INSERT INTO name_lookup (nickname, formal_name) VALUES
  ('bill', 'William'),
  ('billy', 'William'),
  ('will', 'William'),
  ('liam', 'William'),
  ('bob', 'Robert'),
  ('rob', 'Robert'),
  ('bobby', 'Robert'),
  ('jim', 'James'),
  ('jimmy', 'James'),
  ('jamie', 'James'),
  ('mike', 'Michael'),
  ('mikey', 'Michael'),
  ('tony', 'Anthony'),
  ('beth', 'Elizabeth'),
  ('liz', 'Elizabeth'),
  ('betty', 'Elizabeth'),
  ('dave', 'David'),
  ('davey', 'David'),
  ('ken', 'Kenneth'),
  ('kathy', 'Katherine'),
  ('kate', 'Katherine');
```

- [ ] **Step 2: Add the `postgres` service to `stack/docker-compose.yml`**

Add under `services:`, alongside the existing services:

```yaml
  postgres:
    image: postgres:16-alpine
    container_name: postgres
    environment:
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: demo
      POSTGRES_DB: demo
    volumes:
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    networks:
      - demo_net
```

- [ ] **Step 3: Deploy and verify**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
IP=$(terraform output -raw instance_public_ip)
scp -r ../stack ubuntu@$IP:~/stack
ssh ubuntu@$IP "cd ~/stack && docker compose up -d postgres"
ssh ubuntu@$IP "docker exec postgres psql -U demo -d demo -c \"SELECT * FROM name_lookup WHERE nickname = 'billy';\""
```

Expected: a table row `billy | William`.

- [ ] **Step 4: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add stack/postgres/init.sql stack/docker-compose.yml
git commit -m "Add Postgres with nickname-to-formal-name lookup table"
```

---

### Task 5: MQ pipeline (sensor generator)

**Files:**
- Create: `stack/pipelines/mq.yaml`
- Modify: `stack/docker-compose.yml` (add `connect-mq` service)

**Interfaces:**
- Consumes: `redpanda-0:9092` (Task 3).
- Produces: topic `sensors.raw`, with documents of shape `{sensor_id, machine, temperature_c, vibration_mm_s, timestamp}` — consumed by the alerting pipeline in Task 6.

- [ ] **Step 1: Write `stack/pipelines/mq.yaml`**

Range `40-115` for `temperature_c` is deliberately chosen so that the alerting pipeline's `> 105` threshold (Task 6) trips on roughly 5% of readings (`110-105=5` of `115-40=75` possible integer values ≈ 6-7%; tune the bounds during Task 6's verification if the observed rate drifts far from the target).

```yaml
input:
  generate:
    interval: 500ms
    mapping: |
      let machines = ["stamping-press", "cnc-mill", "conveyor-belt", "welder-robot", "injection-molder"]
      root.sensor_id = "sensor-" + (random_int(min: 1, max: 20) | string())
      root.machine = $machines.index(random_int(min: 0, max: 4))
      root.temperature_c = random_int(min: 40, max: 115)
      root.vibration_mm_s = random_int(min: 0, max: 30)
      root.timestamp = now()

pipeline:
  processors: []

output:
  kafka_franz:
    seed_brokers: ["redpanda-0:9092"]
    topic: sensors.raw
```

- [ ] **Step 2: Add the `connect-mq` service to `stack/docker-compose.yml`**

```yaml
  connect-mq:
    image: docker.redpanda.com/redpandadata/connect:latest
    container_name: connect-mq
    volumes:
      - ./pipelines/mq.yaml:/connect.yaml
    command: ["run"]
    networks:
      - demo_net
    depends_on:
      - redpanda-0
```

- [ ] **Step 3: Deploy and verify**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
IP=$(terraform output -raw instance_public_ip)
scp -r ../stack ubuntu@$IP:~/stack
ssh ubuntu@$IP "cd ~/stack && docker compose up -d connect-mq"
sleep 3
ssh ubuntu@$IP "docker exec redpanda-0 rpk topic consume sensors.raw -n 3"
```

Expected: 3 JSON messages printed, each with `sensor_id`, `machine`, `temperature_c`, `vibration_mm_s`, `timestamp` fields.

- [ ] **Step 4: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add stack/pipelines/mq.yaml stack/docker-compose.yml
git commit -m "Add MQ pipeline generating synthetic sensor readings"
```

---

### Task 6: Alerting pipeline

**Files:**
- Create: `stack/pipelines/alerting.yaml`
- Modify: `stack/docker-compose.yml` (add `connect-alerting` service)

**Interfaces:**
- Consumes: topic `sensors.raw` (Task 5).
- Produces: topic `alerts`, containing only the subset of `sensors.raw` documents where `temperature_c > 105` or `vibration_mm_s > 27`, with an added `alert: true` field.

- [ ] **Step 1: Write `stack/pipelines/alerting.yaml`**

```yaml
input:
  kafka_franz:
    seed_brokers: ["redpanda-0:9092"]
    topics: ["sensors.raw"]
    consumer_group: alerting-pipeline

pipeline:
  processors:
    - mapping: |
        root = this
        root.alert = this.temperature_c > 105 || this.vibration_mm_s > 27

output:
  switch:
    cases:
      - check: 'this.alert == true'
        output:
          kafka_franz:
            seed_brokers: ["redpanda-0:9092"]
            topic: alerts
      - output:
          drop: {}
```

- [ ] **Step 2: Add the `connect-alerting` service to `stack/docker-compose.yml`**

```yaml
  connect-alerting:
    image: docker.redpanda.com/redpandadata/connect:latest
    container_name: connect-alerting
    volumes:
      - ./pipelines/alerting.yaml:/connect.yaml
    command: ["run"]
    networks:
      - demo_net
    depends_on:
      - redpanda-0
      - connect-mq
```

- [ ] **Step 3: Deploy and verify the ~5% alert rate**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
IP=$(terraform output -raw instance_public_ip)
scp -r ../stack ubuntu@$IP:~/stack
ssh ubuntu@$IP "cd ~/stack && docker compose up -d connect-alerting"
sleep 20
ssh ubuntu@$IP "docker exec redpanda-0 rpk topic describe sensors.raw -p | grep -i 'high watermark'"
ssh ubuntu@$IP "docker exec redpanda-0 rpk topic describe alerts -p | grep -i 'high watermark'"
```

Expected: the `alerts` high watermark is roughly 5% (give or take a few points given randomness — 3-10% is fine for a demo) of the `sensors.raw` high watermark. If it's far off, adjust the `temperature_c`/`vibration_mm_s` bounds in `mq.yaml` or the thresholds in `alerting.yaml` and redeploy `connect-mq`.

- [ ] **Step 4: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add stack/pipelines/alerting.yaml stack/docker-compose.yml
git commit -m "Add alerting pipeline flagging ~5% of sensor readings"
```

---

### Task 7: Enrichment pipeline (Postgres name lookup)

**Files:**
- Create: `stack/pipelines/enrichment.yaml`
- Modify: `stack/docker-compose.yml` (add `connect-enrichment` service)

**Interfaces:**
- Consumes: Postgres `name_lookup` table via DSN `postgres://demo:demo@postgres:5432/demo?sslmode=disable` (Task 4).
- Produces: topic `customers.enriched`, containing the original customer record plus `formal_first_name`, `match_status` (`success`/`no_match`), and `match_confidence` (`1.0`/`0.0`).

- [ ] **Step 1: Write `stack/pipelines/enrichment.yaml`**

The `branch` processor preserves the original customer document while the nested `sql_select` looks up just the first name — without `branch`, `sql_select` would replace the whole message with the query result.

```yaml
input:
  generate:
    interval: 1s
    mapping: |
      let first_names = ["bill","billy","will","bob","rob","jim","jimmy","mike","tony","beth","liz","dave","ken","kathy","susan","mark"]
      let last_names = ["Smith","Johnson","Garcia","Brown","Miller","Davis"]
      let bands = ["Rush","Metallica","Fleetwood Mac","Queen","Radiohead"]
      root.first_name = $first_names.index(random_int(min: 0, max: $first_names.length() - 1))
      root.last_name = $last_names.index(random_int(min: 0, max: $last_names.length() - 1))
      root.address = (random_int(min: 100, max: 9999) | string()) + " Main St"
      root.favorite_band = $bands.index(random_int(min: 0, max: $bands.length() - 1))

pipeline:
  processors:
    - branch:
        request_map: 'root = this.first_name.lowercase()'
        processors:
          - sql_select:
              driver: postgres
              dsn: postgres://demo:demo@postgres:5432/demo?sslmode=disable
              table: name_lookup
              columns: ["formal_name"]
              where: nickname = ?
              args_mapping: 'root = [ content().string() ]'
        result_map: |
          root.formal_first_name = if this.length() > 0 { this.index(0).formal_name } else { null }
          root.match_status = if this.length() > 0 { "success" } else { "no_match" }
          root.match_confidence = if this.length() > 0 { 1.0 } else { 0.0 }

output:
  kafka_franz:
    seed_brokers: ["redpanda-0:9092"]
    topic: customers.enriched
```

- [ ] **Step 2: Add the `connect-enrichment` service to `stack/docker-compose.yml`**

```yaml
  connect-enrichment:
    image: docker.redpanda.com/redpandadata/connect:latest
    container_name: connect-enrichment
    volumes:
      - ./pipelines/enrichment.yaml:/connect.yaml
    command: ["run"]
    networks:
      - demo_net
    depends_on:
      - redpanda-0
      - postgres
```

- [ ] **Step 3: Deploy and verify**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
IP=$(terraform output -raw instance_public_ip)
scp -r ../stack ubuntu@$IP:~/stack
ssh ubuntu@$IP "cd ~/stack && docker compose up -d connect-enrichment"
sleep 3
ssh ubuntu@$IP "docker exec redpanda-0 rpk topic consume customers.enriched -n 3"
```

Expected: 3 JSON messages, each with `first_name`, `formal_first_name` (e.g. `"billy"` → `"William"`), `match_status: "success"`, `match_confidence: 1.0`.

- [ ] **Step 4: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add stack/pipelines/enrichment.yaml stack/docker-compose.yml
git commit -m "Add enrichment pipeline resolving nicknames via Postgres lookup"
```

---

### Task 8: FTP (SFTP watch) → Iceberg pipeline

**Files:**
- Create: `stack/pipelines/ftp_iceberg.yaml`
- Modify: `stack/docker-compose.yml` (add `sftp` and `connect-ftp-iceberg` services)

**Interfaces:**
- Consumes: `iceberg_bucket`, `glue_database`, `aws_account_id` (Task 2 outputs); the EC2 instance role's credentials via IMDS (works because of the hop-limit fix in Task 2).
- Produces: an Iceberg table `sensor_files` in the Glue database, queryable from Athena — the final demo payoff.

- [ ] **Step 1: Add the `sftp` service to `stack/docker-compose.yml`**

`atmoz/sftp` is a minimal test SFTP server; `SFTP_USERS` creates user `demo`/password `demopass`. Files placed on the host at `./sftp-data/incoming` appear to the container (and thus to RPCN's `sftp` input) at `/incoming`.

```yaml
  sftp:
    image: atmoz/sftp:latest
    container_name: sftp
    command: demo:demopass:1001
    volumes:
      - ./sftp-data/incoming:/home/demo/incoming
    networks:
      - demo_net
```

- [ ] **Step 2: Write `stack/pipelines/ftp_iceberg.yaml`**

Fill in `<GLUE_DATABASE>`, `<ICEBERG_BUCKET>`, `<AWS_ACCOUNT_ID>` from `terraform output` before deploying (Step 4 below does this substitution). The `auth.aws_sigv4.credentials` block is deliberately left empty so the AWS SDK falls back to its default credential chain, which picks up the EC2 instance role automatically.

```yaml
input:
  sftp:
    address: sftp:22
    credentials:
      username: demo
      password: demopass
    paths: ["/incoming/*.json"]
    watcher:
      enabled: true
      minimum_age: 2s
      poll_interval: 2s
      cache: sftp_seen

cache_resources:
  - label: sftp_seen
    memory: {}

pipeline:
  processors:
    - mapping: 'root = content().parse_json()'

output:
  iceberg:
    catalog:
      url: "https://glue.us-east-2.amazonaws.com/iceberg"
      warehouse: "<AWS_ACCOUNT_ID>"
      auth:
        aws_sigv4:
          region: us-east-2
          service: glue
    namespace: "<GLUE_DATABASE>"
    table: sensor_files
    schema_evolution:
      enabled: true
      table_location: "s3://<ICEBERG_BUCKET>/sensor_files/"
    storage:
      aws_s3:
        bucket: "<ICEBERG_BUCKET>"
        region: us-east-2
```

- [ ] **Step 3: Add the `connect-ftp-iceberg` service to `stack/docker-compose.yml`**

```yaml
  connect-ftp-iceberg:
    image: docker.redpanda.com/redpandadata/connect:latest
    container_name: connect-ftp-iceberg
    volumes:
      - ./pipelines/ftp_iceberg.yaml:/connect.yaml
    command: ["run"]
    networks:
      - demo_net
    depends_on:
      - sftp
```

- [ ] **Step 4: Fill in the placeholders and deploy**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
IP=$(terraform output -raw instance_public_ip)
BUCKET=$(terraform output -raw iceberg_bucket)
DB=$(terraform output -raw glue_database)
ACCOUNT=$(terraform output -raw aws_account_id)

sed -i.bak \
  -e "s#<AWS_ACCOUNT_ID>#${ACCOUNT}#g" \
  -e "s#<GLUE_DATABASE>#${DB}#g" \
  -e "s#<ICEBERG_BUCKET>#${BUCKET}#g" \
  ../stack/pipelines/ftp_iceberg.yaml
rm ../stack/pipelines/ftp_iceberg.yaml.bak

mkdir -p ../stack/sftp-data/incoming
scp -r ../stack ubuntu@$IP:~/stack
ssh ubuntu@$IP "cd ~/stack && docker compose up -d sftp connect-ftp-iceberg"
```

- [ ] **Step 5: Drop a test file and verify the Iceberg table**

```bash
ssh ubuntu@$IP "echo '{\"file_id\":\"f1\",\"machine\":\"cnc-mill\",\"reading\":42.5}' > ~/stack/sftp-data/incoming/test1.json"
sleep 10
ssh ubuntu@$IP "docker logs connect-ftp-iceberg --tail 50"
```

Expected: no error lines in the log tail. Then, in the AWS console or CLI:

```bash
aws athena start-query-execution \
  --query-string "SELECT * FROM ${DB}.sensor_files LIMIT 10" \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --region us-east-2
```

Poll the returned `QueryExecutionId` with `aws athena get-query-results --query-execution-id <id> --region us-east-2` until `Status.State` is `SUCCEEDED`. Expected: one row containing `file_id: f1`, `machine: cnc-mill`, `reading: 42.5`.

- [ ] **Step 6: Commit**

```bash
cd /Users/cnelson/sandbox/demos/ST_eng
git add stack/pipelines/ftp_iceberg.yaml stack/docker-compose.yml
git commit -m "Add SFTP-watch to Iceberg pipeline, queryable from Athena"
```

---

## Teardown (after the demo)

```bash
cd /Users/cnelson/sandbox/demos/ST_eng/terraform
terraform destroy
```

Expected: all AWS resources removed. The S3 bucket has `force_destroy = true` so it deletes even with objects in it.
