# v0.2.0-rc.5

**Release date:** 2026-10-28
**Status:** Pre-1.0 release candidate for consuming-application integration and acceptance testing.

This release candidate prepares the repository for public release under the MIT license: the module is fetched through the public Go proxy, and the specification, documentation and fixtures are deployment-neutral. The consumer contract of v0.2.0-rc.4 below is otherwise unchanged, except for the rename below.

## Breaking changes

- The conversation source class `bank_record` is renamed `organization_record`, and the OpenFGA relation `can_ingest_bank_record` is renamed `can_ingest_organization_record`. The relation is computed (`administrator and tenant_member`), so no tuples change; publish the regenerated authorization model and pin its new model ID before ingesting `organization_record` messages. Ingestion requests, including retries of earlier commands, must send the new source class; stored conversation records are unchanged. Applications that use only `customer`, `assistant`, `system` and `staff` sources are unaffected.

## Changes

- Repository access: the module is public. No `GOPRIVATE` setting or Git credential is needed; existing private-access settings keep working.
- The evidence disposal worker deletes object versions in request order (root, then derivatives), so retries after a partial failure replay the same sequence.

## Verification

GitHub-hosted CI did not start for this release (account billing). The source passed `make check`, `make vuln` and `make ci-integration` against disposable PostgreSQL, OpenFGA and CortexDB services.

# v0.2.0-rc.4

**Release date:** 2026-10-19
**Status:** Pre-1.0 release candidate for consuming-application integration and acceptance testing.

This release candidate adds a paged listing of a scope's current memories. The consumer contract of v0.2.0-rc.3 below is otherwise unchanged.

## Changes

- `ListMemories` (Go `Engine.ListMemories`, RPC `MemoryService.ListMemories`): the current memories of one scope, optionally of one subtype, at a valid time and knowledge revision, with the same authorization and signed page tokens as `ListFacts` (`memories-page-v1`). A page can hold fewer items than its limit, even none, and still carry a next token; page until the token is empty.
- `model.ReadTx` gains `MemoryRecords`; ledger implementations outside this module must add it.

## Verification

GitHub-hosted CI did not start for this release (account billing). The source passed `make check` and `make ci-integration` against disposable PostgreSQL, OpenFGA and CortexDB services.

# v0.2.0-rc.3

**Release date:** 2026-10-18  
**Status:** Pre-1.0 release candidate for consuming-application integration and acceptance testing.

This release candidate lets a host keep the ledger in a named PostgreSQL schema of its own database and run the migrations from the module. The consumer contract of v0.2.0-rc.2 below is otherwise unchanged.

## Changes

- PostgreSQL ledger in a named schema: `postgres.WithSchema` runs the startup checks against a configured schema (default `public`) and requires the connection `search_path` to resolve to it; `facts.Migrations()` exposes the embedded goose migrations; `scripts/runtime-role.sql` grants usage on the current schema. Behaviour without the option is unchanged.
- The table-owner refusal now holds in a named schema too: with `WithSchema`, a login that owns `ledger_events` in that schema is refused, as it is in `public`.

## Verification

GitHub-hosted CI did not start for this release (account billing). The release source passed `sh scripts/check.sh` with the Makefile's pinned tools, `make vuln`, and `sh scripts/integration.sh` against disposable PostgreSQL 17.6, OpenFGA and CortexDB services, including the logical restore and fresh search rebuild drill.

# v0.2.0-rc.2

**Release date:** 2026-10-15  
**Status:** Pre-1.0 release candidate for consuming-application integration and acceptance testing.

This release candidate updates the consumer setup instructions for the private GitHub repository. It gives consumers a pin-able Go module and protobuf contract for the v0.2 specification. It is not a production approval or a v1 compatibility promise.

## Consumer contract

- Minimum Go version: Go 1.26.0. The repository was verified with Go 1.27.2.
- Go module: `github.com/caxqueiroz/facts_engine`.
- Repository access: private GitHub read access is required. Set `GOPRIVATE=github.com/caxqueiroz/facts_engine` and configure an authenticated Git credential before fetching the module.
- Remote API: generated Connect and gRPC contracts in `github.com/caxqueiroz/facts_engine/gen/memory/v1`.
- Persistence: PostgreSQL is authoritative. The release includes forward-only migrations through `00052_conversation_disposal_storage.sql`; run migrations with a privileged deployment identity and use restricted runtime roles.
- Authorization: OpenFGA model and adapter are included. The host publishes and pins a store/model and provisions tenant/scope tuples.
- Lifecycle: constructors do not migrate, start listeners/workers, provision external systems, or own injected resources. Hosts control setup and shutdown.
- Compatibility: APIs and migrations may change in later 0.2 prereleases. Pin this exact version and upgrade the application, service, workers, and migration set together. A later prerelease may require code changes or a rehearsed database migration.

## Included

- Embedded engine and optional Connect/gRPC service for governed facts and memories, temporal history, snapshots, evidence, attestations, context/decision records, calculations, audit exports, identities, retrieval, extraction scheduling, and retention administration.
- PostgreSQL tenant isolation, OpenFGA authorization, durable outbox workers, CortexDB projections/recall, managed rebuilds, signed event archives, and explicit payload-disposal workflows.
- Eight standalone worker modes: projection, audit, archive, refresh, refresh-sweep, extraction, orphan-cleanup, and projection-cleanup.
- All 77 declared protobuf RPC methods have PostgreSQL integration fixtures that use both Connect and generated gRPC clients. Tested inputs cover representative success, replay, authorization, validation, and lifecycle behavior; this is not an exhaustive input/error cross-product.

## Host responsibilities and known limits

The host supplies trusted identity-to-tenant mapping, OpenFGA tuples, evidence and immutable archive stores, production key management, approved extraction/embedding providers, worker supervision, telemetry collection, and backup/recovery. The host also owns the domain catalogue, business validation, and confirmation presentation. External evidence deletion requires an idempotent versioned-store deleter and a running `evidence-disposal` outbox consumer.

This release ships development/reference key and local storage integrations, not operator-managed envelope encryption or a production immutable archive. Search, cache, export, backup and archive copies follow their own retention controls. Extraction quality, capacity, retrieval quality, RPO/RTO, and compliance require consuming-application evaluation and owner approval. The engine does not implement onboarding, profile completeness, allocation, or business decisions.

See [SETUP.md](SETUP.md) for installation, configuration, the first application workflow, worker setup, upgrades, and the production acceptance gates. See [spec-acceptance.md](spec-acceptance.md) for test evidence and external sign-off owners.

## Verification

The release source passed `make check`, `make build`, and `sh scripts/ci-integration.sh`. The integration suite used disposable PostgreSQL, OpenFGA, and CortexDB services and completed its logical restore and fresh search rebuild drill. These checks establish repository behavior only; they do not replace the production approvals above.
