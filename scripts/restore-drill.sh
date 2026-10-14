#!/bin/sh
# Called after integration tests, against their owned disposable fixture only.
# A logical dump is not a production PITR or separately administered backup test.
set -eu
source_database=${1:?pass the completed disposable integration database name}
case "$source_database" in
  facts_engine_test_[0-9]*_[0-9]*) ;;
  *) printf 'Expected a disposable integration database name\n' >&2; exit 1 ;;
esac
: "${TEST_POSTGRES_CONTAINER:?}"
: "${TEST_POSTGRES_PORT:?}"
: "${TEST_CORTEX_CONTAINER:?}"
: "${TEST_CORTEX_PORT:?}"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/facts-restore.XXXXXXXX")
trap 'rm -rf "$work_dir"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
restored_database="facts_restore_$(date +%s)_$$"
index_database="${restored_database}_index"
export TEST_RECOVERY_SOURCE_URL="postgres://postgres:development@127.0.0.1:${TEST_POSTGRES_PORT}/${source_database}?sslmode=disable"
TEST_RECOVERY_PHASE=seed go test -race ./adapters/postgres -run '^TestRecoverySeed$' -count=1 -timeout 2m
# pg_dump/pg_restore come from the running PostgreSQL image, matching its version.
docker exec "$TEST_POSTGRES_CONTAINER" pg_dump -U postgres --format=custom "$source_database" > "$work_dir/ledger.dump"
docker exec "$TEST_POSTGRES_CONTAINER" createdb -U postgres "$restored_database"
docker exec -i "$TEST_POSTGRES_CONTAINER" pg_restore -U postgres --exit-on-error --single-transaction --dbname "$restored_database" < "$work_dir/ledger.dump"
docker exec "$TEST_CORTEX_CONTAINER" createdb -U postgres "$index_database"
export TEST_RECOVERY_RESTORED_URL="postgres://postgres:development@127.0.0.1:${TEST_POSTGRES_PORT}/${restored_database}?sslmode=disable"
export TEST_RECOVERY_INDEX_URL="postgres://postgres:development@127.0.0.1:${TEST_CORTEX_PORT}/${index_database}?sslmode=disable"
TEST_RECOVERY_PHASE=verify go test -race ./adapters/postgres -run '^TestRecoveryRestore$' -count=1 -timeout 2m
printf 'Logical restore, retained content and fresh search rebuild verified\n'
