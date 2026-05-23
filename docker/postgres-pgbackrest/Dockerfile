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

COPY entrypoint.sh /usr/local/bin/pdw-postgres-pgbackrest-entrypoint
COPY backup-loop.sh /usr/local/bin/pdw-pgbackrest-backup-loop
COPY restore.sh /usr/local/bin/pdw-pgbackrest-restore

RUN chmod +x \
        /usr/local/bin/pdw-postgres-pgbackrest-entrypoint \
        /usr/local/bin/pdw-pgbackrest-backup-loop \
        /usr/local/bin/pdw-pgbackrest-restore

ENTRYPOINT ["tini", "-g", "--", "pdw-postgres-pgbackrest-entrypoint"]
CMD ["postgres"]
