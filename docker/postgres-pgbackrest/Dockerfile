ARG POSTGRES_IMAGE=postgres:18.4-bookworm
FROM ${POSTGRES_IMAGE}

LABEL org.opencontainers.image.title="personal-data-warehouse-postgres-pgbackrest"
LABEL org.opencontainers.image.description="Postgres with pgBackRest for S3-backed physical backups and WAL archiving"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        pgbackrest \
        tini \
        zstd \
    && rm -rf /var/lib/apt/lists/*

# pg_textsearch: BM25 full-text search (https://github.com/timescale/pg_textsearch).
# The release zip wraps a Debian package targeting the same pgdg paths the official
# postgres image uses. Installing it only ships the extension files; enabling it
# requires shared_preload_libraries to include pg_textsearch in the deployment's
# postgresql.conf, a restart, then CREATE EXTENSION pg_textsearch per database.
# pg_trgm needs no install step here: it ships with the base image's contrib
# modules and the warehouse creates the extension itself.
ARG TARGETARCH
ARG PG_TEXTSEARCH_VERSION=1.3.0
ARG PG_TEXTSEARCH_SHA256_AMD64=4a8291b44c2c700ef71f9230e14fbe434dc0f8b20bca13eb31b049367911de38
ARG PG_TEXTSEARCH_SHA256_ARM64=9590a2c604372d6ff05a8012ffd8f2a0caa74a8a6c81917ea5d12f3d0dad6ec6
RUN set -eu; \
    case "${TARGETARCH}" in \
        amd64) pgts_sha256="${PG_TEXTSEARCH_SHA256_AMD64}" ;; \
        arm64) pgts_sha256="${PG_TEXTSEARCH_SHA256_ARM64}" ;; \
        *) echo "unsupported TARGETARCH for pg_textsearch: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl unzip; \
    curl -fsSL -o /tmp/pg-textsearch.zip \
        "https://github.com/timescale/pg_textsearch/releases/download/v${PG_TEXTSEARCH_VERSION}/pg-textsearch-v${PG_TEXTSEARCH_VERSION}-pg18-${TARGETARCH}.zip"; \
    echo "${pgts_sha256}  /tmp/pg-textsearch.zip" | sha256sum -c -; \
    unzip -o /tmp/pg-textsearch.zip -d /tmp/pg-textsearch; \
    dpkg -i "/tmp/pg-textsearch/pg-textsearch-postgresql-18_${PG_TEXTSEARCH_VERSION}-1_${TARGETARCH}.deb"; \
    test -f "$(pg_config --pkglibdir)/pg_textsearch.so"; \
    test -f "$(pg_config --sharedir)/extension/pg_textsearch.control"; \
    rm -rf /tmp/pg-textsearch /tmp/pg-textsearch.zip; \
    apt-get purge -y curl unzip; \
    apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/pdw-postgres-pgbackrest-entrypoint
COPY backup-loop.sh /usr/local/bin/pdw-pgbackrest-backup-loop
COPY restore.sh /usr/local/bin/pdw-pgbackrest-restore

RUN chmod +x \
        /usr/local/bin/pdw-postgres-pgbackrest-entrypoint \
        /usr/local/bin/pdw-pgbackrest-backup-loop \
        /usr/local/bin/pdw-pgbackrest-restore

ENTRYPOINT ["tini", "-g", "--", "pdw-postgres-pgbackrest-entrypoint"]
CMD ["postgres"]
