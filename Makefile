.PHONY: build test integration ci-integration check vet lint vuln generate migrate tools

build:
	go build -trimpath -o bin/memoryd ./cmd/memoryd
	go build -trimpath -o bin/memoryworker ./cmd/memoryworker

test:
	go test -race ./...

integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Set TEST_DATABASE_URL to a fresh disposable PostgreSQL database'; exit 1)
	go test -race ./adapters/postgres -run TestPostgresLedger -count=1 -v

check:
	sh scripts/check.sh

ci-integration:
	sh scripts/ci-integration.sh

vet:
	go vet ./...

lint:
	golangci-lint run

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

generate:
	sqlc generate
	python3 authz/generate.py
	buf generate

migrate:
	@test -n "$(DATABASE_URL)" || (echo 'Set DATABASE_URL to the migration role connection'; exit 1)
	goose -dir sql/migrations postgres "$(DATABASE_URL)" up

tools:
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
	go install github.com/pressly/goose/v3/cmd/goose@v3.28.0
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
	go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
	go install github.com/bufbuild/buf/cmd/buf@v1.74.0
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install connectrpc.com/connect/v2/cmd/protoc-gen-connect-go@v2.0.1
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
