#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: restore.sh BACKUP_FILE TARGET_DSN" >&2
  exit 64
fi

backup_file="$1"
target_dsn="$2"
test -f "$backup_file"
test -f "$backup_file.sha256"
(cd "$(dirname "$backup_file")" && sha256sum -c "$(basename "$backup_file").sha256")
pg_restore --clean --if-exists --no-owner --no-acl --exit-on-error \
  --dbname="$target_dsn" "$backup_file"
