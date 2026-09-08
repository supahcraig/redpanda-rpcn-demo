# Stack deployment notes

Operational gotchas for this Docker Compose stack that aren't obvious from
the YAML alone. Read this before redeploying from a clean checkout.

## `connect-ftp-iceberg` requires a Redpanda Connect Enterprise license

The `iceberg` output used by `pipelines/ftp_iceberg.yaml` is an
Enterprise-licensed Redpanda Connect connector. Without a valid license, the
container starts, logs `license_type=open_source`, and then exits with:

```
service closing due to: failed to init output <no label> path root.output:
this feature requires a valid Redpanda Enterprise Edition license that
includes the Connect product.
```

`docker-compose.yml` mounts a license file from the **deploy target host**
(not from this repo) into the container:

```yaml
- /home/ubuntu/secrets/redpanda.license:/etc/redpanda/redpanda.license:ro
```

Before bringing up `connect-ftp-iceberg`, on the deploy host:

1. Get a license — either a real Redpanda Enterprise/Connect license key, or
   a 30-day trial from https://redpanda.com/try-enterprise (self-serve trial
   generation is also possible via `rpk generate license`, but it is rate
   limited to one trial per email/business domain, so it may fail with
   `already_exists` if your organization has already claimed one).
2. Place the license file at `~/secrets/redpanda.license` on the host (a
   sibling of `~/stack`, **not** inside the repo — this is a credential and
   must never be committed).
3. Set its ownership/permissions to match the UID the `connect` container
   process runs as, so it's readable but not world-readable:

   ```bash
   docker exec connect-ftp-iceberg id   # confirms the runtime UID, e.g. 10001(connect)
   sudo chown 10001:10001 ~/secrets/redpanda.license
   sudo chmod 400 ~/secrets/redpanda.license
   ```

   (The bind mount is read-only on the container side regardless, but the
   file should not be left world-readable — `644` — on the host filesystem.)
4. `docker compose up -d connect-ftp-iceberg` (or `--force-recreate` if the
   service already exists in a stopped/errored state).

Confirm success by checking the container logs — you should see
`license_type=enterprise` (not `open_source`) and `Iceberg output ready`,
with no licensing error:

```bash
docker logs connect-ftp-iceberg --tail 20
```

## `sftp` service host-key fragility

`pipelines/ftp_iceberg.yaml`'s `sftp` input hardcodes
`credentials.host_public_key` to the `atmoz/sftp` container's SSH host key:

```yaml
credentials:
  host_public_key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINUcMYOSajHHmsok8eYw1e6hqgjoVg4mjDYN9LciaEdP"
```

This is required because the `redpandadata/connect` image has no
`~/.ssh/known_hosts`, and the `sftp` input has no option to skip host-key
verification — you must supply the server's key explicitly.

The `atmoz/sftp` container generates its host keys on first start and does
**not** persist them across recreation (no volume is mounted for `/etc/ssh`
in the `sftp` service). This means:

- If `sftp` is ever recreated independently of `connect-ftp-iceberg` (e.g.
  `docker compose up -d --force-recreate sftp`, or a full `docker compose
  down` + `up`), it will generate a **new** host key, and the hardcoded
  value above will no longer match — `connect-ftp-iceberg`'s `sftp` input
  will fail to connect.
- To refresh it, get the new key from a container on the same Docker network
  and update `credentials.host_public_key` in `pipelines/ftp_iceberg.yaml`
  accordingly:

  ```bash
  docker run --rm --network rp-demo_demo_net alpine:latest \
    sh -c 'apk add --no-cache openssh-client >/dev/null 2>&1; ssh-keyscan -t ed25519 sftp'
  ```

  Take the third field of the output line (the `ssh-ed25519 AAAA...` key
  itself, without the `sftp` hostname prefix) and paste it into the YAML,
  then redeploy `connect-ftp-iceberg`.
