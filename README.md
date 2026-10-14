# Facts engine

An embeddable Go engine for evidence-backed, policy-governed facts. PostgreSQL is the authoritative ledger; OpenFGA authorizes callers. This repository implements the **initial ledger foundation**, not the complete v0.2 specification or a production banking release.

The public entry point is `factsengine.New(ledger, authorizer, evidenceStore)`. Constructors do not run migrations, start workers/listeners, or change global logging. The host owns the PostgreSQL pools, authentication boundary, encryption keys, OpenFGA provisioning, and shutdown.

## Implemented

- Typed text, boolean, integer, canonical decimal, date, instant, entity-reference, and explicit unknown values.
- Immutable policy and predicate versions; administrative publication activates a predicate version. New proposals and approvals cannot choose an obsolete schema to bypass the active policy. Existing acceptances retain their recorded policy.
- Encrypted development evidence objects with immutable revisions, digest verification, tenant isolation, source-revision integrity conflicts, and exact UTF-8 byte-span validation.
- Evidence-backed proposals, distinct-principal manual approval, and explicitly configured automated acceptance with permitted service principals and version-bound prerequisites.
- Attestations recorded by their authenticated attesting principal; withdrawal is append-only, serializes with acceptance, and makes required-prerequisite facts ineligible on current reads. Delegated client identity binding is deliberately not inferred.
- Current direct fact reads with explicit missing, unknown, resolved, and ineligible outcomes. Unknown interval starts cannot be accepted. Accepted intervals are half-open and non-overlapping.
- Transactional tenant ordering, stream revisions, bounded transaction retries, command receipts, encrypted ledger events, and atomic outbox insertion. Identical authorized retries return the committed result; changed commands conflict.
- Tenant-qualified foreign keys, forced row-level security, transaction-local tenant context, and a runtime role without mutation privileges on immutable tables.
- OpenFGA HTTP adapter with pinned store/model, bounded requests, fresh checks, tenant-qualified identities, and retained authorization receipts.

`model` defines domain contracts, `internal/app/services` implements shared policy logic, and public adapters depend only on domain types. Generated sqlc rows stay inside the PostgreSQL adapter. Encryption uses tenant-bound AES-GCM; a production host must replace the development key management with bank-managed envelope encryption and retention-aware key ownership.

## Local setup

Requires Go 1.25 or newer, Docker, and PostgreSQL client tools. The checked-in development containers bind only to loopback. OpenFGA's development datastore is ephemeral.

```sh
docker compose up -d
make tools
export DATABASE_URL='postgres://postgres:development@127.0.0.1:55432/facts_engine?sslmode=disable'
make migrate
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/runtime-role.sql
```

The migration connection is privileged. **Do not use it as the engine's runtime identity.** Provision a login separately, grant it membership in `facts_runtime`, and connect using that login. The PostgreSQL adapter rejects superusers, `BYPASSRLS` roles, and ownership of the ledger event table. Development credentials in Compose are not deployment secrets.

Publish `authz/model.json` through OpenFGA's authorization-model API and pin the returned model ID. Provision tenant membership and scope assignments as described in [authz/README.md](authz/README.md). Never derive `Caller` from model output or unchecked client headers.

The [embedded example](examples/go/embedded/main.go) composes the adapters and performs an authorized current read. It reads these environment variables:

- `DATABASE_URL`: runtime-role PostgreSQL connection.
- `EVIDENCE_KEY_HEX`: a 32-byte AES key encoded as 64 hex characters; provide securely and retain it between runs.
- `OPENFGA_URL`, `OPENFGA_STORE_ID`, `OPENFGA_MODEL_ID`, optional `OPENFGA_TOKEN`.
- `TENANT_ID`, `PRINCIPAL_ID`, `SCOPE_ID`, `SUBJECT_ID`, `PREDICATE`: trusted test identifiers and selector.

Run `go run ./examples/go/embedded`. The example prints only the resolution state and ledger revision, not fact content. The integration test demonstrates policy/schema publication, evidence registration, proposal, confirmation, acceptance, and prerequisite withdrawal.

## Verification

```sh
make test
make vet
make lint
make generate
```

`make generate` uses sqlc v1.31.1 and regenerates the OpenFGA JSON model from its restricted DSL. Goose v3.28.0 is the migration tool. Lint configuration is for golangci-lint v2 (validated with v2.14.0).

Real PostgreSQL tests require a **fresh disposable database**. They refuse a database containing ledger tables, apply migrations, and exercise the restricted runtime role. They do not drop existing databases.

```sh
docker compose exec postgres createdb -U postgres facts_engine_integration
export TEST_DATABASE_URL='postgres://postgres:development@127.0.0.1:55432/facts_engine_integration?sslmode=disable'
export TEST_OPENFGA_URL='http://127.0.0.1:58080'
go test -race ./... -count=1
```

Create a new disposable database for each run. The OpenFGA contract test creates a separate test store. Without these environment variables, integration tests explicitly skip; ordinary `go test` does not prove database or authorization-engine guarantees.

The suite covers concurrent approval conflicts, committed-response retries, source integrity conflicts, invalid spans, cross-tenant access, pooled-session isolation, immutable-table privileges, manual separation including delegated humans, automated prerequisites, withdrawal, inactive schemas, interval overlap, gapless committed tenant revisions, outbox atomicity, encryption tampering, and live OpenFGA allow/deny/revocation behavior.

## Remaining delivery work

Follow the slices in [spec.md](spec.md). The current API is intentionally incomplete and may change before v1:

1. Finish foundation contracts: scope/subject administration, complete schema constraints and migration rules, confirmation presentation/response artifacts and delegated identity flows, evidence access endpoints, configurable quotas, richer policy controls, structured revision-conflict details, and full sqlc query coverage.
2. Add rejection, supersession, disputes, retractions, bitemporal history, immutable fact snapshots, and the distinct memory lifecycle. Historical queries currently return `ErrInvalid` instead of pretending to provide historical eligibility.
3. Implement leased/fenced workers, outbox delivery/offsets, the pinned CortexDB adapter, per-tenant databases, projection status, rebuilds, and retrieval revalidation. The outbox is persisted but has no consumer yet.
4. Add conversation ingestion, extraction adapters, retained provider artifacts, and bank-approved extraction evaluations.
5. Add context manifests, calculation/decision receipts, audit export/signatures, retention/legal holds, archive coverage, and recovery drills.
6. Add Protobuf/Buf/protovalidate, Connect/gRPC service and worker binaries, gateway identity integration, transport parity tests, injected telemetry, readiness endpoints, CI, and operational packaging.

The first complete end-to-end scenario in the spec (including snapshots, external calculations, supersession, and audit export) is **not yet delivered**. No CortexDB, remote engine API, extraction model, archive, or production deployment has been configured. The engine does not implement wealth allocation, profile completeness, or any business decision algorithm.

Production acceptance also requires the named domain, security, retention, and operational owners described in the spec, plus workload benchmarks and restore drills. Database roles protect normal runtime operations; they do not protect against a malicious database owner. Retained digests are integrity metadata, not anonymization.
