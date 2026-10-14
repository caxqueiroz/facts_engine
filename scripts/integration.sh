#!/bin/sh
# Creates a new disposable database; never resets or drops an existing database.
set -eu
: "${TEST_POSTGRES_CONTAINER:=facts-engine-postgres}"
: "${TEST_POSTGRES_PORT:=55432}"
test_database="facts_engine_test_$(date +%s)_$$"
docker exec "$TEST_POSTGRES_CONTAINER" createdb -U postgres "$test_database"
export TEST_DATABASE_URL="postgres://postgres:development@127.0.0.1:${TEST_POSTGRES_PORT}/${test_database}?sslmode=disable"
export TEST_OPENFGA_URL="${TEST_OPENFGA_URL:-http://127.0.0.1:58080}"
go test -race ./... -count=1
