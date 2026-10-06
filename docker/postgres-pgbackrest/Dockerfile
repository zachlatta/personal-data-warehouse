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
        # pgvector: ANN vector search for the hybrid (semantic + BM25)
        # timeline retrieval layer. Ships from the same pgdg apt repo the
        # base image installs Postgres from; needs only CREATE EXTENSION
        # vector per database (no shared_preload_libraries entry).
        postgresql-18-pgvector \
    && rm -rf /var/lib/apt/lists/*

# pg_textsearch: BM25 full-text search (https://github.com/timescale/pg_textsearch).
# The release zip wraps a Debian package targeting the same pgdg paths the official
# postgres image uses. Installing it only ships the extension files; enabling it
# requires shared_preload_libraries to include pg_textsearch in the deployment's
# postgresql.conf, a restart, then CREATE EXTENSION pg_textsearch per database.
# pg_trgm needs no install step here: it ships with the base image's contrib
# modules and the warehouse creates the extension itself.
#
# Never below 1.4.0: 1.3.x reused index pages handed out by the non-crash-safe
# FSM, so after crash recovery it overwrote pages live segments still owned
# (upstream #426/#427/#430, fixed by #429). On 2026-10-06 one autovacuum
# segfault on 1.3.0 left every timeline BM25 index unreadable ("Invalid
# logical page", "corrupt segment: term length ...") and all search down.
# After bumping, the warehouse runs ALTER EXTENSION pg_textsearch UPDATE on
# its own; already-damaged indexes still need a REINDEX.
ARG TARGETARCH
ARG PG_TEXTSEARCH_VERSION=1.5.1
ARG PG_TEXTSEARCH_SHA256_AMD64=6adc4c390c61109af337c5d5c15239623d6fc8b89e972625de04f52a5771cebf
ARG PG_TEXTSEARCH_SHA256_ARM64=54843473cbad144812be0437bc708233087c7823018700ffb99a2fb92d1466bb
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
