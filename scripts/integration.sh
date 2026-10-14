#!/bin/sh
# Creates a new disposable database; never resets or drops an existing database.
set -eu
: "${TEST_POSTGRES_CONTAINER:=facts-engine-postgres}"
: "${TEST_POSTGRES_PORT:=55432}"
test_database="facts_engine_test_$(date +%s)_$$"
docker exec "$TEST_POSTGRES_CONTAINER" createdb -U postgres "$test_database"
export TEST_DATABASE_URL="postgres://postgres:development@127.0.0.1:${TEST_POSTGRES_PORT}/${test_database}?sslmode=disable"
export TEST_OPENFGA_URL="${TEST_OPENFGA_URL:-http://127.0.0.1:58080}"
# Cortex tests use fresh dedicated pgvector databases; no existing data is reset.
: "${TEST_CORTEX_CONTAINER:=facts-engine-cortex}"
: "${TEST_CORTEX_PORT:=55433}"
for suffix in a b pipeline rebuild lexical standalone; do
  cortex_database="${test_database}_cortex_${suffix}"
  docker exec "$TEST_CORTEX_CONTAINER" createdb -U postgres "$cortex_database"
  cortex_url="postgres://postgres:development@127.0.0.1:${TEST_CORTEX_PORT}/${cortex_database}?sslmode=disable"
  case "$suffix" in
    a) export TEST_CORTEX_DATABASE_URL="$cortex_url" ;;
    b) export TEST_CORTEX_OTHER_DATABASE_URL="$cortex_url" ;;
    pipeline) export TEST_PIPELINE_DATABASE_URL="$cortex_url" ;;
    lexical) export TEST_LEXICAL_DATABASE_URL="$cortex_url" ;;
    rebuild) export TEST_REBUILD_DATABASE_URL="$cortex_url" ;;
    standalone) export TEST_STANDALONE_DATABASE_URL="$cortex_url" ;;
  esac
done
go test -race ./... -count=1 -timeout 10m
