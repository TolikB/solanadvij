#!/bin/sh
set -eu

: "${BACKUP_DIR:=/backups}"
: "${BACKUP_RETENTION_DAYS:=14}"

mkdir -p "$BACKUP_DIR"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
target="$BACKUP_DIR/sniper-$stamp.dump"
backup_dsn="$(printf '%s' "$MIGRATION_POSTGRES_DSN" | sed 's#postgresql+asyncpg://#postgresql://#')"
pg_dump --format=custom --no-owner --no-acl "$backup_dsn" --file="$target"
(cd "$BACKUP_DIR" && sha256sum "$(basename "$target")" > "$(basename "$target").sha256")
find "$BACKUP_DIR" -type f -name 'sniper-*.dump' -mtime "+$BACKUP_RETENTION_DAYS" -delete
find "$BACKUP_DIR" -type f -name 'sniper-*.dump.sha256' -mtime "+$BACKUP_RETENTION_DAYS" -delete
