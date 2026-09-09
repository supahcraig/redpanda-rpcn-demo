# Redpanda + Redpanda Connect Demo

A self-contained demo showing four real-world streaming patterns on a single
Redpanda cluster: live ingestion, real-time alerting, database-backed
enrichment, and a batch-file-to-lakehouse pipeline — all deployed with
Terraform + Docker Compose to one disposable EC2 host.

## Architecture

- **Terraform** (`terraform/`) provisions an EC2 host in an existing VPC,
  with an IAM instance role scoped to a demo S3 bucket + Glue database, so
  the pipelines authenticate to AWS with no hardcoded credentials.
- **Docker Compose** (`stack/docker-compose.yml`), deployed to that host,
  runs: a 3-broker Redpanda cluster, Redpanda Console, Postgres, an SFTP
  test server, and one Redpanda Connect container per pipeline below.

```
                    ┌─────────────────────────┐
                    │   Redpanda (3 brokers)  │
                    └───────────┬─────────────┘
        sensors.raw             │              customers.enriched
      ┌────────────┐    ┌───────┴────────┐    ┌──────────────┐
      │  MQ         │──▶│  Alerting      │    │  Enrichment  │──▶ Postgres
      │  pipeline   │   │  pipeline      │    │  pipeline    │    lookup
      └────────────┘    └───────┬────────┘    └──────────────┘
                                 ▼
                              alerts

      ┌────────────┐    ┌────────────────┐    ┌──────────────┐
      │  SFTP       │──▶│  FTP→Iceberg   │──▶│  S3 + Glue   │──▶ Athena
      │  server     │   │  pipeline      │    │  (Iceberg)   │    queries
      └────────────┘    └────────────────┘    └──────────────┘
```

## The four pipelines

### 1. MQ pipeline — synthetic sensor feed
**Config:** `stack/pipelines/mq.yaml`

Stands in for a real MQTT/OPC-UA/PLC feed off a manufacturing floor. Uses
Redpanda Connect's `generate` input to fabricate a sensor reading every
500ms — a machine name, temperature, vibration level, and timestamp — and
publishes it straight to Kafka, no external system involved.

**Produces:** topic `sensors.raw`

### 2. Alerting pipeline — real-time thresholding
**Config:** `stack/pipelines/alerting.yaml`

Consumes `sensors.raw` and evaluates each reading against a threshold with
a Bloblang mapping (`temperature_c > X || vibration_mm_s > Y`, tuned so
roughly 5-10% of readings trip it — a realistic anomaly rate). A `switch`
output routes only the flagged readings to a new topic; everything else is
dropped inline, with zero extra infrastructure.

**Consumes:** `sensors.raw` → **Produces:** topic `alerts`

### 3. Enrichment pipeline — real-time database lookup, with an LLM comparison
**Config:** `stack/pipelines/enrichment.yaml`

Generates synthetic customer records (a nickname, last name, address,
favorite band) and uses Redpanda Connect's `branch` + `sql_select`
processors to look up the nickname's formal first name in a Postgres table
(`name_lookup`) — live, per-record, in the stream — then appends
`formal_first_name`, `match_status`, and `match_confidence` onto the
original record. This is the streaming alternative to a nightly batch join:
the lookup happens the instant the record is produced, not hours later.

A second `branch` processor calls an LLM (OpenAI's `gpt-5.6-luna`, via a
plain `http` processor — no dedicated LLM connector needed) to independently
resolve the same nickname, appending `llm_formal_first_name` right next to
the Postgres-derived field. Every record shows both answers side by side —
a live demonstration of the "database lookup vs. LLM call" trade-off the
customer is actually weighing. The API key is never stored in this repo;
it's read from an environment variable at container start.

**Produces:** topic `customers.enriched`

### 4. FTP-to-Iceberg pipeline — batch files into a queryable lakehouse
**Config:** `stack/pipelines/ftp_iceberg.yaml`

Watches a folder over the **real SFTP protocol** (not just a local
filesystem watch) for newly-dropped files. Each file can hold a batch of
records as a JSON array; the pipeline splits it into individual rows, tags
each one with `source_filename` and `processed_at` (so you can `GROUP BY`
per upload later), and writes them straight to a real **Apache Iceberg**
table via AWS Glue's REST catalog + S3 — authenticated through the EC2
instance's IAM role, no access keys anywhere in the config. The result is
queryable immediately from **Amazon Athena**, no Trino or separate catalog
service required.

**Produces:** Iceberg table `sensor_files` in Glue, queryable from Athena

## Try it yourself

```bash
# Generate N synthetic rows and deliver them over real SFTP:
stack/scripts/send-batch.sh 100

# Then query Athena (region us-east-2), e.g.:
#   SELECT source_filename, count(*) FROM <glue_database>.sensor_files
#   GROUP BY source_filename;
```

Redpanda Console (topics, consumer groups, brokers) is reachable at
`http://<instance_public_ip>:8080` — get the IP with
`terraform output -raw instance_public_ip` from `terraform/`.

## Repo layout

- `terraform/` — AWS infrastructure (EC2 host, IAM role, S3 bucket, Glue database)
- `stack/docker-compose.yml` — the full container stack
- `stack/pipelines/` — the four Redpanda Connect pipeline configs above
- `stack/scripts/send-batch.sh` — generates and delivers a batch for the Iceberg pipeline
- `stack/README.md` — operational gotchas found while building this (license setup, SFTP host-key fragility, the Iceberg concurrent-write caveat, etc.)
- `docs/superpowers/plans/` — the original implementation plan this was built from
