.PHONY: test integration vet lint vuln generate migrate tools

test:
	go test -race ./...

integration:
	@test -n "$(TEST_DATABASE_URL)" || (echo 'Set TEST_DATABASE_URL to a fresh disposable PostgreSQL database'; exit 1)
	go test -race ./adapters/postgres -run TestPostgresLedger -count=1 -v

vet:
	go vet ./...

lint:
	golangci-lint run

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

generate:
	sqlc generate
	python3 authz/generate.py

migrate:
	@test -n "$(DATABASE_URL)" || (echo 'Set DATABASE_URL to the migration role connection'; exit 1)
	goose -dir sql/migrations postgres "$(DATABASE_URL)" up

tools:
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
	go install github.com/pressly/goose/v3/cmd/goose@v3.28.0
