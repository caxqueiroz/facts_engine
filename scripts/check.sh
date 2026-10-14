#!/bin/sh
# Reproduce the CI code checks from the repository root.
set -eu
unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
  printf 'Run gofmt on these files:\n%s\n' "$unformatted" >&2
  exit 1
fi
go mod verify
go vet ./...
golangci-lint run
goose -dir sql/migrations validate
actionlint .github/workflows/ci.yml
buf format --diff --exit-code
buf lint
make generate
generated_changes=$(git status --porcelain --untracked-files=all -- gen/ledgerdb gen/memory authz/model.json)
if [ -n "$generated_changes" ]; then
  printf 'Generated files differ from the committed source:\n%s\n' "$generated_changes" >&2
  exit 1
fi
git diff --check
