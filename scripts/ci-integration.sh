#!/bin/sh
# Own a disposable Compose project; never reset a developer's existing services.
set -eu
ci_project="facts-ci-$(date +%s)-$$"
compose() {
  docker compose -p "$ci_project" -f compose.yaml -f compose.ci.yaml "$@"
}
cleanup() {
  result=$?
  trap - EXIT HUP INT TERM
  if [ "$result" -ne 0 ]; then
    compose ps --all || true
    compose logs --no-color --tail 100 || true
  fi
  if ! compose down --volumes --remove-orphans; then
    printf 'Failed to clean disposable Compose project %s\n' "$ci_project" >&2
    result=1
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
compose up -d --wait --wait-timeout 90
TEST_POSTGRES_CONTAINER=$(compose ps -q postgres)
TEST_CORTEX_CONTAINER=$(compose ps -q cortex)
postgres_address=$(compose port postgres 5432)
cortex_address=$(compose port cortex 5432)
openfga_address=$(compose port openfga 8080)
TEST_POSTGRES_PORT=${postgres_address##*:}
TEST_CORTEX_PORT=${cortex_address##*:}
TEST_OPENFGA_URL="http://$openfga_address"
export TEST_POSTGRES_CONTAINER TEST_CORTEX_CONTAINER TEST_POSTGRES_PORT TEST_CORTEX_PORT TEST_OPENFGA_URL
attempt=0
until curl --fail --silent --max-time 2 --output /dev/null "$TEST_OPENFGA_URL/healthz"; do
  attempt=$((attempt+1))
  if [ "$attempt" -ge 30 ]; then
    printf 'Disposable OpenFGA did not become ready\n' >&2
    exit 1
  fi
  sleep 1
done
sh scripts/integration.sh
