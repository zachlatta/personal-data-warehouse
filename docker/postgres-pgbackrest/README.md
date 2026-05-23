# Postgres pgBackRest Image

This directory builds the custom Postgres image for the Coolify database
container. It keeps Dagster and the application in their existing image; this
image only runs Postgres plus pgBackRest.

The image currently tracks PostgreSQL `18.4` via the official
`postgres:18.4-bookworm` image. PostgreSQL 18 changed the official Docker
image layout: mount persistent storage at `/var/lib/postgresql`, not directly at
`/var/lib/postgresql/data`. The default `PGDATA` is
`/var/lib/postgresql/18/docker`.

## Build

```bash
docker build -t personal-data-warehouse-postgres-pgbackrest:18.4 docker/postgres-pgbackrest
```

To override the base image:

```bash
docker build \
  --build-arg POSTGRES_IMAGE=postgres:18.4-bookworm \
  -t personal-data-warehouse-postgres-pgbackrest:18.4 \
  docker/postgres-pgbackrest
```

## Published Image

GitHub Actions publishes this image to GitHub Container Registry:

```text
ghcr.io/zachlatta/personal-data-warehouse-postgres-pgbackrest
```

On pushes, the workflow publishes:

- `latest` for the repository default branch
- the full commit SHA, for example `ghcr.io/zachlatta/personal-data-warehouse-postgres-pgbackrest:<commit-sha>`
- a short SHA alias, for example `ghcr.io/zachlatta/personal-data-warehouse-postgres-pgbackrest:sha-<short-sha>`

Pull requests build the image but do not push it.

## Required Runtime Environment

Use Coolify runtime environment variables for these values:

```bash
POSTGRES_USER=pdw
POSTGRES_PASSWORD=change-me
POSTGRES_DB=pdw

PGBACKREST_STANZA=pdw
PGBACKREST_REPO1_S3_BUCKET=...
PGBACKREST_REPO1_S3_ENDPOINT=...
PGBACKREST_REPO1_S3_REGION=auto
PGBACKREST_REPO1_S3_KEY=...
PGBACKREST_REPO1_S3_KEY_SECRET=...
PGBACKREST_REPO1_S3_URI_STYLE=path
PGBACKREST_REPO1_PATH=/personal-data-warehouse
PGBACKREST_REPO1_CIPHER_PASS=...
```

The image defaults to client-side AES-256 encryption:

```bash
PGBACKREST_REPO1_CIPHER_TYPE=aes-256-cbc
```

Keep `PGBACKREST_REPO1_CIPHER_PASS` somewhere outside Coolify as well. Without
that value, encrypted backups cannot be restored.

If a secret contains `$`, enable Coolify's literal environment-variable mode for
that value.

## Coolify Storage

Mount persistent storage at:

```text
/var/lib/postgresql
```

Optional, but useful for retaining local pgBackRest logs/spool across restarts:

```text
/var/log/pgbackrest
/var/spool/pgbackrest
```

The durable backup repository is S3; the local pgBackRest paths are operational
state only.

## Backup Behavior

When `PDW_PGBACKREST_ENABLED=true` or unset, the entrypoint:

- renders `/etc/pgbackrest/pgbackrest.conf` from environment variables
- starts Postgres with WAL archiving enabled
- uses `archive_command='pgbackrest --stanza=<stanza> archive-push %p'`
- starts a background backup loop

The loop waits for Postgres, creates the stanza if needed, runs
`pgbackrest check`, and creates the first full backup if none exists. After that,
it wakes every `PDW_PGBACKREST_BACKUP_INTERVAL_SECONDS` seconds, defaulting to 21600
seconds.

Default backup selection is:

- full: once each Sunday after 08:00 UTC
- differential: once each other day after 08:00 UTC
- incremental: all other loop wakeups

Useful overrides:

```bash
PDW_PGBACKREST_BACKUP_INTERVAL_SECONDS=21600
PDW_PGBACKREST_FULL_BACKUP_WEEKDAY_UTC=7
PDW_PGBACKREST_FULL_BACKUP_HOUR_UTC=08
PDW_PGBACKREST_DIFF_BACKUP_HOUR_UTC=08
PDW_PGBACKREST_BACKUP_FORCE_TYPE=incr
PDW_PGBACKREST_BACKUP_LOOP_ENABLED=true
PDW_PGBACKREST_BACKUP_ON_STARTUP=true
POSTGRES_ARCHIVE_TIMEOUT=300s
```

For a local smoke test without S3 configuration, explicitly disable pgBackRest:

```bash
docker run --rm \
  -e POSTGRES_PASSWORD=postgres \
  -e PDW_PGBACKREST_ENABLED=false \
  personal-data-warehouse-postgres-pgbackrest:18.4
```

## Restore

Restore into a fresh `/var/lib/postgresql` volume with the same S3 and cipher
environment variables:

```bash
docker run --rm \
  -v pdw-postgres-restored:/var/lib/postgresql \
  --env-file .env.restore \
  personal-data-warehouse-postgres-pgbackrest:18.4 \
  pdw-pgbackrest-restore
```

Point-in-time restore example:

```bash
docker run --rm \
  -v pdw-postgres-restored:/var/lib/postgresql \
  --env-file .env.restore \
  personal-data-warehouse-postgres-pgbackrest:18.4 \
  pdw-pgbackrest-restore --type=time --target="2026-05-23 12:00:00+00"
```

After restore, start a normal container with the restored volume and the same
Postgres environment.
