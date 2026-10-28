# Facts engine setup manual

This manual describes the consumer contract for `v0.2.0-rc.5`. It covers an embedded Go integration and the optional Connect/gRPC service. The release is a pre-1.0 release candidate: pin the exact version, and expect the Go and protobuf APIs to change before v1.

## What the consuming application owns

The engine provides the governed ledger, evidence references, fact and memory lifecycles, snapshots, context and decision receipts, audit exports, retrieval adapters, and explicit workers. The consuming application remains responsible for:

- Its domain schemas, predicates, memory types, acceptance policies, source classes, reviewers, validation rules, and client-facing confirmation flow.
- Authentication and trusted tenant/principal mapping. Embedded callers must be created by the host's authenticated boundary. Remote callers must arrive through the configured mTLS or trusted-gateway path.
- PostgreSQL, OpenFGA, evidence and archive storage, keys, worker supervision, telemetry, backups, and shutdown.
- Business completeness and decisions. The engine does not implement a domain catalogue, onboarding workflow, allocation method, or risk decision.

Engine constructors do not migrate databases, start workers/listeners, create authorization tuples, or own injected connections.

## Requirements and version pin

- Go 1.26 or later; the repository has been verified with Go 1.27.2.
- PostgreSQL 17 is used by the checked-in integration environment. Use a currently supported PostgreSQL version and test the migration plan for your deployment.
- OpenFGA with the repository's generated model, a PostgreSQL-backed CortexDB database if vector/lexical retrieval is enabled, and an evidence store appropriate to the data policy.
- The module is public and fetched through the standard Go module proxy and checksum database; no credentials or `GOPRIVATE` setting are needed. Pin an exact version:

```sh
go get github.com/caxqueiroz/facts_engine@v0.2.0-rc.5
```

The package import path is `github.com/caxqueiroz/facts_engine`. Generated Connect and gRPC contracts are under `github.com/caxqueiroz/facts_engine/gen/memory/v1`.

## Choose an integration mode

**Embedded Go library** is appropriate when the application wants in-process calls and already owns service lifecycle and authenticated callers. Compose `factsengine.Engine` with a ledger, authorizer, and evidence store. Start the consumers it needs explicitly.

**Remote service** is appropriate when applications need the Connect API or gRPC over HTTP/2. Build `bin/memoryd`, configure `examples/memoryd.json`, and provide verified client certificates and explicit certificate-to-tenant/principal bindings. Use the generated Go client example in `examples/go/client`. The service does not start extraction or outbox workers; run each required `memoryworker` mode as its own supervised process.

Both modes call the same engine policy. The integration suite exercises all 77 declared RPC methods through Connect and generated gRPC fixtures; it covers representative receipt and error behavior, not every possible input/error combination.

## Local development

The Compose services use development credentials and bind to loopback. Do not reuse these credentials in a deployment.

```sh
docker compose up -d
make tools
export DATABASE_URL='postgres://postgres:development@127.0.0.1:55432/facts_engine?sslmode=disable'
make migrate
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/runtime-role.sql
```

Create a login for the engine and grant it membership in `facts_runtime`; do not connect the engine as PostgreSQL superuser, schema owner, migration login, or `BYPASSRLS` role. Use separate identities for migration, normal runtime, retention, projection, archive, and rebuild functions as described in the role script and operation-specific README sections.

Publish `authz/model.json` to OpenFGA and pin both the store ID and returned model ID. Provision tenant membership and scope relations before making engine calls. The engine does not infer tenant membership or write OpenFGA tuples. See [authz/README.md](authz/README.md) for relation naming and the [embedded example](examples/go/embedded/main.go) for adapter composition.

The embedded example expects `DATABASE_URL`, `EVIDENCE_KEY_HEX`, `OPENFGA_URL`, `OPENFGA_STORE_ID`, `OPENFGA_MODEL_ID`, `TENANT_ID`, `PRINCIPAL_ID`, `SCOPE_ID`, `SUBJECT_ID`, and `PREDICATE`. `EVIDENCE_KEY_HEX` is a 32-byte key encoded as 64 hex characters. Provide it through a secret manager and preserve it for retained ciphertext; the example's AES-GCM adapter is a development/reference key integration.

## First application workflow

1. Authenticate the caller and establish trusted tenant, principal, principal kind, and any delegated initiator at the host boundary.
2. Provision the caller's OpenFGA tenant membership and scope relations. Register the scope and its subjects through the authorized identity API.
3. Register immutable predicate/schema versions, memory types if needed, and acceptance policies. Policy publication is an administrative action; new proposals use the active version.
4. Store evidence bytes before registering their immutable object locator, version, and digest. Do not let the service fetch arbitrary caller URLs. Bind every proposal to verified evidence and exact source spans.
5. Propose facts or memories. Use `ApproveFact`/`ApproveMemory` for manual review, or `AcceptFact`/`AcceptMemory` only for an explicitly configured automated policy and permitted service principal.
6. Read current or revision-bound history as needed. For deterministic application inputs, create a fact snapshot with explicit selectors; for ranked context, persist a context manifest before using the returned bytes.
7. Record calculation and decision receipts from the application that actually performed the work. The engine records host-reported use; it does not execute or independently prove the external algorithm.

Treat `model.ErrDenied`, `model.ErrConflict`, `model.ErrRevision`, `model.ErrPrecondition`, `model.ErrQuota`, and `model.ErrUnavailable` as distinct outcomes. Reuse the same command key and identical request after an ambiguous response; changed content under the same key is a conflict. See the README's lifecycle and API sections for request details and examples.

## Optional workers and retrieval

Workers are not started by `factsengine.New` or `memoryd`. Select only the modes the application needs and run them under a process supervisor with explicit credentials and graceful shutdown. Available standalone modes are `projection`, `audit`, `archive`, `refresh`, `refresh-sweep`, `extraction`, `orphan-cleanup`, and `projection-cleanup`. Use the matching `examples/memoryworker-*.json` configuration as a starting point.

The CortexDB adapter is a rebuildable index, not the source of truth. Recall revalidates candidates against the ledger and reports lag/incompleteness. Provision a separate tenant database as required by the v0.2 architecture. Embeddings are injected by the host; no production model is selected by default.

## Upgrade and compatibility rules

- Treat this RC's Go API, protobuf messages, and database schema as versioned together. Pin the module; do not mix arbitrary service, worker, and migration versions.
- Migrations are forward-only. Back up and rehearse restoration before deployment. Follow each migration's README section; several payload-layout changes require stopping writers and upgrading all readers, writers, projectors, and archive workers together before resuming.
- Run `make check`, `make build`, and `make ci-integration` against disposable infrastructure before adopting the release. The integration script creates and cleans an isolated Compose project and ends with a logical restore plus fresh search rebuild.
- Retain and securely recover encryption, signing, pagination, and provider credentials according to the consuming application's policy. Rotating the pagination signing key invalidates outstanding tokens; changing data encryption keys without a managed re-encryption plan makes retained payloads unreadable.

## Before production

This RC is not a production approval. Before deployment, obtain domain/application, model/provider, identity/security, retention/legal, archive/infrastructure, and operations sign-off. Configure production identity-to-tenant trust, managed keys, immutable archive and versioned evidence storage, retention coverage for exports/indexes/caches/backups, monitoring and alerts, workload capacity targets, and tested restore/RPO/RTO procedures. The complete owner/evidence list is in [spec-acceptance.md](spec-acceptance.md).
