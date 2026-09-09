#!/bin/bash
# Generates N synthetic sensor-reading rows as a single JSON array and
# delivers it over real SFTP to the demo's watched folder, so the
# ftp_iceberg pipeline picks it up and writes each row as a separate
# Iceberg row (via the `unarchive: {format: json_array}` step).
set -euo pipefail

ROWS="${1:-100}"
EC2_IP="18.119.235.83"
SSH_KEY="/Users/cnelson/pem/cnelson-prodse-01-15-2025.pem"
SSH_USER="ubuntu"
SFTP_USER="demo"
SFTP_PASS="demopass"

# The atmoz/sftp container publishes no host port (by design — see
# stack/README.md), so we go from the EC2 host itself to the container's
# address on the compose network, not from this laptop directly.
SFTP_CONTAINER_IP=$(ssh -i "$SSH_KEY" "$SSH_USER@$EC2_IP" \
  "docker inspect sftp --format '{{(index .NetworkSettings.Networks \"rp-demo_demo_net\").IPAddress}}'")

if [ -z "$SFTP_CONTAINER_IP" ]; then
  echo "Could not determine the sftp container's network address. Is it running?" >&2
  exit 1
fi

# Self-healing: the watched folder must be writable by the SFTP container's
# "demo" user (UID 1001), not just the host's "ubuntu" (UID 1000) — see
# stack/README.md. Safe to run every time; a no-op once already set.
ssh -i "$SSH_KEY" "$SSH_USER@$EC2_IP" "chmod 777 ~/stack/sftp-data/incoming"

TMPFILE=$(mktemp /tmp/rp-demo-batch-XXXXXX.json)
trap 'rm -f "$TMPFILE"' EXIT

python3 - "$ROWS" "$TMPFILE" <<'PYEOF'
import json, random, sys, time

rows = int(sys.argv[1])
outfile = sys.argv[2]
machines = ["stamping-press", "cnc-mill", "conveyor-belt", "welder-robot", "injection-molder"]
batch_id = int(time.time())

data = [
    {
        "file_id": f"batch-{batch_id}-{i}",
        "machine": random.choice(machines),
        "reading": round(random.uniform(20.0, 100.0), 1),
    }
    for i in range(rows)
]

with open(outfile, "w") as f:
    json.dump(data, f)

print(f"Generated {rows} rows -> {outfile}")
PYEOF

REMOTE_STAGING="/tmp/rp-demo-batch-$(date +%s).json"
REMOTE_NAME="batch-$(date +%s)-$$.json"

echo "Staging file on host..."
scp -q -i "$SSH_KEY" "$TMPFILE" "$SSH_USER@$EC2_IP:$REMOTE_STAGING"

echo "Delivering via SFTP to the watched folder (incoming/$REMOTE_NAME)..."
SFTP_OUTPUT=$(ssh -i "$SSH_KEY" "$SSH_USER@$EC2_IP" \
  "sshpass -p '$SFTP_PASS' sftp -o StrictHostKeyChecking=no -P 22 $SFTP_USER@$SFTP_CONTAINER_IP" <<EOF
put $REMOTE_STAGING incoming/$REMOTE_NAME
bye
EOF
) || { echo "$SFTP_OUTPUT" >&2; echo "SFTP command failed (non-zero exit)." >&2; ssh -i "$SSH_KEY" "$SSH_USER@$EC2_IP" "rm -f $REMOTE_STAGING"; exit 1; }

echo "$SFTP_OUTPUT"
if echo "$SFTP_OUTPUT" | grep -qi "denied\|error\|not found"; then
  echo "SFTP upload reported an error above — see output." >&2
  ssh -i "$SSH_KEY" "$SSH_USER@$EC2_IP" "rm -f $REMOTE_STAGING"
  exit 1
fi

ssh -i "$SSH_KEY" "$SSH_USER@$EC2_IP" "rm -f $REMOTE_STAGING"

echo ""
echo "Sent $ROWS rows as incoming/$REMOTE_NAME"
echo "Give the pipeline a few seconds, then query Athena, e.g.:"
echo "  aws athena start-query-execution --region us-east-2 \\"
echo "    --query-string \"SELECT count(*) FROM rp_demo_db.sensor_files\" \\"
echo "    --result-configuration \"OutputLocation=s3://rp-demo-iceberg-861276079005/athena-results/\""
