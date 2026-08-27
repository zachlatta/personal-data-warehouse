# Postgres pgBackRest Image

This directory builds the custom Postgres image for the Coolify database
container. It keeps Dagster and the application in their existing image; this
image only runs Postgres plus pgBackRest.

The image currently tracks PostgreSQL `18.4` via the official
`postgres:18.4-bookworm` image. PostgreSQL 18 changed the official Docker
image layout: mount persistent storage at `/var/lib/postgresql`, not directly at
`/var/lib/postgresql/data`. The default `PGDATA` is
`/var/lib/postgresql/18/docker`.

## Bundled Extensions

Beyond the contrib modules that ship with the official image (`pg_trgm`,
`unaccent`, `pg_stat_statements`, ...), the image installs:

- [`pg_textsearch`](https://github.com/timescale/pg_textsearch) (BM25 full-text
  search), pinned by version and SHA-256 in the Dockerfile.

Installing the image only ships the extension files. To enable `pg_textsearch`
in a deployment:

1. Add `pg_textsearch` to `shared_preload_libraries` in the deployment-managed
   config (production passes `-c config_file=/etc/postgresql/postgresql.conf`
   through Coolify; edit that file), for example:

   ```text
   shared_preload_libraries = 'pg_stat_statements,pg_textsearch'
   ```

2. Restart the database container.
3. Run `CREATE EXTENSION pg_textsearch;` in the target database.

`pg_trgm` needs no preload entry; the warehouse application creates the
extension on demand (`CREATE EXTENSION IF NOT EXISTS pg_trgm`) when it ensures
its trigram indexes.

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
PGBACKREST_REPO1_TYPE=s3
PGBACKREST_REPO1_S3_BUCKET=...
PGBACKREST_REPO1_S3_ENDPOINT=...
PGBACKREST_REPO1_S3_REGION=auto
PGBACKREST_REPO1_S3_KEY=...
PGBACKREST_REPO1_S3_KEY_SECRET=...
PGBACKREST_REPO1_S3_URI_STYLE=path
PGBACKREST_REPO1_PATH=/personal-data-warehouse
PGBACKREST_REPO1_CIPHER_PASS=...
PGBACKREST_IO_TIMEOUT=1800
PGBACKREST_ARCHIVE_TIMEOUT=1800
```

## Repository Transport

`PGBACKREST_REPO1_TYPE` selects the transport and defaults to `s3`. Only the
selected transport's credentials are required, so a deployment that uses SFTP
does not need a bucket, endpoint, or access key.

**Do not leave the other transport's variables set.** pgBackRest reads
`PGBACKREST_*` from the environment at a *higher* precedence than any config
file, so a stale `PGBACKREST_REPO1_S3_BUCKET` will both override the rendered
config and fail option validation once the type is `sftp`. Remove them.

For `PGBACKREST_REPO1_TYPE=sftp`:

```bash
PGBACKREST_REPO1_TYPE=sftp
PGBACKREST_REPO1_PATH=/<share>/backups/pgbackrest/pdw
PGBACKREST_REPO1_SFTP_HOST=...
PGBACKREST_REPO1_SFTP_HOST_USER=...
PGBACKREST_REPO1_SFTP_PRIVATE_KEY_FILE=/var/lib/postgresql/.pgbackrest/id_ed25519
PGBACKREST_REPO1_SFTP_PUBLIC_KEY_FILE=/var/lib/postgresql/.pgbackrest/id_ed25519.pub
PGBACKREST_REPO1_SFTP_KNOWN_HOST=/var/lib/postgresql/.pgbackrest/known_hosts
PGBACKREST_REPO1_SFTP_HOST_KEY_HASH_TYPE=sha256
PGBACKREST_REPO1_SFTP_HOST_KEY_CHECK_TYPE=strict
```

Keep the key pair and `known_hosts` under `/var/lib/postgresql`, which is the
persistent volume: anything written elsewhere in the container is lost on the
next recreation, and the repository then becomes unreachable.

Two traps, both measured against Synology DSM on 2026-08-26:

- **The known-hosts file must contain exactly one host key type.** libssh2
  negotiates ECDSA against DSM, and reports
  `LIBSSH2_KNOWNHOST_CHECK_MISMATCH` when the host also has entries of a type
  it did not negotiate. `ssh-keyscan -t ecdsa <host>` is the correct input;
  a plain `ssh-keyscan` returns several types and fails the strict check.
- **DSM presents SFTP paths relative to the share, not the filesystem root.**
  The repository path is `/<share>/backups/...`, never
  `/volume1/<share>/backups/...`. An absolute volume path fails with
  `unable to create path '/volume1': permission denied`, because pgBackRest is
  trying to create a share at the SFTP root.

## Why SFTP Instead of S3 for an HDD Repository

pgBackRest writes a few hundred large sequential files. Garage splits every
object into 1 MiB blocks and indexes each one in LMDB. Measured on
2026-08-26 against the Garage repository on slowking:

| Path | Throughput |
| --- | --- |
| Garage S3, single 512 MB PUT | 0.58 MB/s (886.9 s) |
| SFTP to the same array, same 512 MB | 19.7 MB/s (26 s) |
| Raw TCP over Tailscale | 38.4 MB/s |
| Raw TCP over the LAN | 100.2 MB/s |
| The array's own sequential write | 103 MB/s |

The Garage payload was all zeros, so its own compression meant almost nothing
reached the platters, and it *still* took 886.9 s. The cost is ~1.73 s per
1 MiB block of metadata commit -- an LMDB fsync on btrfs on a RAID5 array whose
queue never drains -- not data bandwidth. Object storage on spinning disks is
the wrong shape for this workload; the network and the disks were never the
limit.

The image defaults to client-side AES-256 encryption:

```bash
PGBACKREST_REPO1_CIPHER_TYPE=aes-256-cbc
```

Keep `PGBACKREST_REPO1_CIPHER_PASS` somewhere outside Coolify as well. Without
that value, encrypted backups cannot be restored.

`archive-timeout` is a **separate** budget and defaults to 60 seconds in pgBackRest.
It governs the wait for the WAL segment that `backup` forces after `pg_backup_start`,
not ordinary I/O, so raising `io-timeout` alone does not cover it. On this HDD-backed
repository WAL pushes measure 5.7-200.3 seconds, and the 60-second default aborted
**every** backup between 2026-08-25 and 2026-08-26 with `ERROR: [082]: WAL segment ...
was not archived before the 60000ms timeout` while `pgbackrest info` reported
`status: error (no valid backups)`. WAL archiving itself kept working throughout, which
is why the outage was invisible. Do not confuse this with `POSTGRES_ARCHIVE_TIMEOUT`,
which is PostgreSQL's own `archive_timeout` (how often an idle server forces a segment
switch).

The generated config uses a 1800-second I/O timeout by default. The Garage S3
repository is backed by an HDD array, where a saturated but healthy read can
exceed pgBackRest's 60-second default. 600 seconds was the first estimate and
proved too low: slowking runs its own offsite `restic` backup against the same
array, and while it does, throughput to Garage collapses to ~13 KB/s. Measured
2026-08-26, that starved a running full backup for 26 minutes, and on 08-25 it
aborted one outright with `ERROR: [042]: timeout after 600000ms waiting for
read`. The two backup jobs contend by schedule, so the budget has to survive
the overlap rather than assume it away. Override `PGBACKREST_IO_TIMEOUT` only
after measuring the repository; the setting applies consistently to WAL
archive commands, checks, backups, and restores.

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
