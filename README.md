# Facts engine

An embeddable Go engine for evidence-backed, policy-governed facts. PostgreSQL is the authoritative ledger; OpenFGA authorizes callers. This repository implements the **ledger foundation, temporal fact lifecycle, immutable fact snapshots, external calculation receipts, signed audit exports, durable outbox workers, CortexDB projection/recall, and signed event archive chains**. The complete v0.2 specification and production banking release remain in progress.

The public entry point is `factsengine.New(ledger, authorizer, evidenceStore)`. Constructors do not run migrations, start workers/listeners, or change global logging. The host owns the PostgreSQL pools, authentication boundary, encryption keys, OpenFGA provisioning, and shutdown.

## Implemented

- Typed text, boolean, integer, canonical decimal, date, instant, entity-reference, and explicit unknown values.
- Immutable policy and predicate versions; administrative publication activates a predicate version. New proposals and approvals cannot choose an obsolete schema to bypass the active policy. Existing acceptances retain their recorded policy.
- Encrypted development evidence objects with immutable revisions, digest verification, tenant isolation, source-revision integrity conflicts, and exact UTF-8 byte-span validation.
- Evidence-backed proposals, distinct-principal manual approval, and explicitly configured automated acceptance with permitted service principals and version-bound prerequisites.
- Attestations recorded by their authenticated attesting principal; withdrawal is append-only, serializes with acceptance, and makes required-prerequisite facts ineligible on current reads. Delegated client identity binding is deliberately not inferred.
- Current and revision-bound historical fact reads with explicit missing, unknown, unresolved, disputed, retracted, resolved, and ineligible outcomes. Unknown interval starts cannot be accepted. Effective boundaries use PostgreSQL microsecond precision; finer boundaries are rejected rather than rounded.
- Partial-interval supersession, future-effective replacements, backdated corrections, rejection, dispute/resolution, and retraction. Complete overlap sets and expected stream revisions are mandatory. Immutable timeline generations preserve every earlier knowledge view.
- Immutable fact snapshots over ordered explicit selectors, with one PostgreSQL selection view, per-item outcomes, exact values and provenance, canonical content digests, separate selection/creation revisions, and current authorization on reads. Retries retain the original selection even after facts change.
- External calculation receipts bind an application/run identity to immutable snapshots, executable/configuration digests, normalization version, and verified input/output evidence. Concurrent retries converge; changed run content conflicts. Reads recheck snapshot and artifact permissions.
- Durable, leased audit exports with fenced completion, revision-bound scope contents, signed entry manifests, explicit coverage gaps, and trusted-key verification.
- Bounded authorized record event history, including actor/check receipts, correction intervals, reason codes, and replaced versions. Histories exceeding 1,000 events fail explicitly pending paginated history support.
- Tenant-bound CortexDB projections of accepted historical versions, injected embeddings, explicit database provisioning, canonical ledger revalidation on recall, and reported indexing lag.
- Signed event archive consumers with immutable encrypted object versions, predecessor links, standalone coverage verification, and crash-safe duplicate delivery. Local storage is a development adapter; production immutable storage remains host-provisioned.
- Explicit outbox workers with lease renewal, fencing, bounded retries, jittered backoff, visible failure state, independent contiguous consumer offsets, and scope-authorized change replay.
- Transactional tenant ordering, stream revisions, bounded transaction retries, command receipts, encrypted ledger events, and atomic outbox insertion. Identical authorized retries return the committed result; changed commands conflict.
- Tenant-qualified foreign keys, forced row-level security, transaction-local tenant context, and a runtime role without mutation privileges on immutable tables.
- OpenFGA HTTP adapter with pinned store/model, bounded requests, fresh checks, tenant-qualified identities, and retained authorization receipts.

`model` defines domain contracts, `internal/app/services` implements shared policy logic, and public adapters depend only on domain types. Generated sqlc rows stay inside the PostgreSQL adapter. Encryption uses tenant-bound AES-GCM; a production host must replace the development key management with bank-managed envelope encryption and retention-aware key ownership.

## Local setup

The library minimum is Go 1.25; use a currently patched, supported toolchain (verified with Go 1.27.2), Docker, and PostgreSQL client tools. The checked-in development containers bind only to loopback. OpenFGA's development datastore is ephemeral.

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

Run `go run ./examples/go/embedded`. The example prints only the resolution state and ledger revision, not fact content. The integration test demonstrates policy/schema publication, evidence registration, proposal, confirmation, acceptance, withdrawal, historical reads, interval corrections, and immutable snapshots.

## Verification

```sh
make test
make vet
make lint
make vuln
make generate
```

`make generate` uses sqlc v1.31.1 and regenerates the OpenFGA JSON model from its restricted DSL. Goose v3.28.0 is the migration tool. Lint configuration is for golangci-lint v2 (validated with v2.14.0). `make vuln` pins govulncheck v1.9.0 and requires Go 1.26 or newer. The pgx driver is pinned to v5.10.0; its dependency graph no longer includes the earlier vulnerable x/crypto version.

Real PostgreSQL tests require **fresh disposable databases**. They refuse a database containing ledger tables, apply migrations, and exercise the restricted runtime role. They do not drop existing databases.

```sh
docker compose exec postgres createdb -U postgres facts_engine_integration
export TEST_DATABASE_URL='postgres://postgres:development@127.0.0.1:55432/facts_engine_integration?sslmode=disable'
export TEST_OPENFGA_URL='http://127.0.0.1:58080'
go test -race ./... -count=1
```

For the complete suite, start the loopback CortexDB/pgvector service as well and run `scripts/integration.sh`. It creates fresh ledger and projection databases without dropping existing data. Cortex contract tests require `TEST_CORTEX_DATABASE_URL` and `TEST_CORTEX_OTHER_DATABASE_URL`; the end-to-end consumer test requires `TEST_PIPELINE_DATABASE_URL`. Each must reference a distinct fresh pgvector database. The script supplies these variables. Without them, the corresponding tests explicitly skip.

Create a new disposable database for each run. The OpenFGA contract test creates a separate test store. Without these environment variables, integration tests explicitly skip; ordinary `go test` does not prove database or authorization-engine guarantees.

The suite covers concurrent approval conflicts, committed-response retries, source integrity conflicts, invalid spans, cross-tenant access, pooled-session isolation, immutable-table privileges, manual separation including delegated humans, automated prerequisites, withdrawal, inactive schemas, interval overlap, gapless committed tenant revisions, outbox atomicity, encryption tampering, and live OpenFGA allow/deny/revocation behavior. Temporal tests cover incomplete overlap rejection, preserved interval segments, historical disputes, retracted-interval replacement, immutable snapshot retries, historical prerequisite eligibility, and a concurrent commit forced between snapshot selector reads.

## Temporal migration and semantics

Apply migration `00004_temporal_snapshots.sql` with the migration role, then reapply the now-idempotent `scripts/runtime-role.sql` grants. The adapter checks for the new schema and does not migrate on startup. The temporal migration backfills immutable timeline generations from earlier acceptance rows. It is forward-only: interval splits cannot be represented by the old schema. Downgrade requires restoring a verified backup.

Old attestation rows do not have recoverable ledger revisions in the initial schema. They remain eligible for current checks; historical reads involving those legacy attestations fail explicitly until a separately reviewed reconciliation supplies their provenance. New attestations and withdrawals are inserted with their exact ledger revision and remain append-only.

`SupersedeFact` uses the proposal's exact interval, an explicit complete list of overlapping prior version IDs, and the same acceptance prerequisites as ordinary approval. `Automated` selects the configured automated path; it cannot bypass the bound policy. `ResolveDispute` restores only disputed intervals; it does not restore retracted content or replace withdrawn prerequisites. A new proposal may fill a retracted interval through ordinary governed acceptance.

Historical eligibility evaluates retained attestations at the recorded time of the requested knowledge revision. Current eligibility uses current time. Evidence availability is checked at disclosure time in both cases; historical reads do not resurrect disposed bytes. `GetFactSnapshot` returns the recorded values and outcomes without re-evaluating current fact eligibility, while rechecking current scope permissions. Snapshot content is encrypted independently of evidence, so the later retention implementation must dispose both according to their policies.

Snapshot canonical version `fact-snapshot-v1` hashes the ordered selection, normalized UTC timestamps, explicit nulls and resolution states, typed values, provenance, caller, purpose, and selection revision. Creation revision and authorization receipts are retained separately from that content digest. Snapshot selection and persistence may be separate transactions; the selected immutable content stays fixed and the later creation event receives its own revision. No model ranking or default substitution participates in selection.

## Calculation receipts and audit exports

Apply migration `00005_calculations_audit.sql` and reapply `scripts/runtime-role.sql` before using these APIs. `RecordCalculation` records externally produced results; it never runs business calculations. Register input/output evidence first and provide their exact digests plus the immutable snapshot ID. The external run identity is unique per tenant and application. `GetCalculationReceipt` preserves the original snapshot binding after corrections.

Compose `factsengine.NewWithAudit(ledger, authorizer, evidenceStore, model.AuditOptions{Signer: signer})`. The signer implements `model.AuditSigner`; `adapters/audit.NewEd25519` supplies a development implementation. The host owns signing keys and key rotation.

Call `StartAuditExport` with a scope, command key, purpose, and `IncludeEvidence`, then explicitly call `RunAuditExport` with the returned ID, scope, purpose, and original authenticated caller. `GetAuditExport` returns job state or the persisted bundle and its content-addressed artifact reference. Constructors start no workers. Attempts have a two-minute deadline and a three-minute lease; failed attempts can be retried after lease expiration. Stale workers cannot commit. A general worker scheduler, failure history, and backoff remain future work.

Exports select one consistent ledger revision. Collection is bounded to 10,000 stored entries and 64 MiB of entry payloads; included evidence also counts against the payload limit. Oversized exports fail explicitly. Metadata exports omit evidence bytes and source quotes. Cross-scope snapshots and their dependent receipts are withheld with explicit gaps. Missing or corrupt evidence also produces a gap. Fact values remain in metadata exports, so these still require fact-read and history permissions.

Verify a retrieved bundle using `adapters/audit.Verify(bundle, trustedKeys)`, where `trustedKeys` is a separately provisioned map of signing key IDs to Ed25519 public keys. Verification checks the signed manifest, ordered entry identities, counts, and payload digests. Preserve the original payload bytes for verification. The encrypted bundle resides in PostgreSQL; its digest does not establish an independently controlled immutable archive or prove coverage beyond the stated scope and revision. Manifests explicitly report unimplemented extraction, context, decision, and retention coverage. Archive-chain receipts are not yet attached to scoped audit exports, and that coverage gap remains explicit.

Integration tests cover external-run races and conflicts, preserved snapshot lineage, current authorization, tenant isolation, cross-scope exclusion, signature/content tampering, metadata redaction, live-lease exclusion, stale completion rollback, and expired-lease recovery.

## Outbox delivery and workers

Apply migration `00006_outbox_consumers.sql` and reapply `scripts/runtime-role.sql`. Provision a separate login with membership in `facts_worker` for delivery; it can read events and maintain consumer progress but cannot append or mutate business events. The pool and cipher are host-owned. The worker port is a trusted administrative interface: tenant/consumer configuration and retry commands must never come directly from an unchecked request. The host must authorize operator retries and retain operator identity in its operational audit log.

Every business event has an outbox row committed in the same transaction. Register a stable `(tenant, consumer)` identity with `ledger.RegisterConsumer(ctx, key, maxAttempts)`. Registration is idempotent with the same retry limit; changed configuration conflicts. New consumers start at revision one, including existing history. Use distinct identities for projection, archive, and application notifications.

Compose `workers.NewOutbox(ledger, key, handler, options)` and explicitly call `Run(ctx)` or `ProcessOne(ctx)`. `handler` implements `Handle(context.Context, model.Delivery) error`. Options require a 3-second to 5-minute lease, a polling interval, and minimum/maximum retry backoff. `Run` renews the lease every third of its duration while the handler runs outside database transactions. Handlers must honor cancellation. Cancel and wait for workers before closing their pools. No constructor starts background work, and no service or worker binary is installed by this slice.

Each tenant/consumer has one event in flight. Acknowledgement advances the contiguous offset by exactly one revision, so later events cannot hide an earlier failure. Parallel workers may process different tenant/consumer pairs; simultaneous claims for the same pair use `SKIP LOCKED`. This deliberately favors straightforward ordering over parallelism within a stream. A missing outbox row below the committed tenant revision is an explicit precondition failure.

Delivery is **at least once**. Consumers must atomically deduplicate external effects using `(tenant, event ID)` or use an idempotent destination operation. A lease only fences queue updates; it cannot undo an external write by a worker that lost its lease. Crashes after an external effect but before acknowledgement cause redelivery of that same immutable event.

Handler errors persist a generic failure code and exponential backoff with jitter; sensitive error text is not stored. Claims, including crash recoveries, count toward the configured attempt limit. Exhausted events enter `failed` and block that consumer. Inspect `ConsumerStatus`, then explicitly call `RetryConsumer(ctx, key, failedFence)` after resolving the cause. The retry resets the attempt budget without advancing the offset, increments the fence, and records an immutable retry receipt. Repeating the same retry after response loss is safe. Failed consumers remain idle until retried; database and lease errors return to the host supervisor. Projection progress and archive progress are separate offsets, not proof that an archive destination meets its retention guarantees.

`Engine.ListChanges` provides current-authorized scope replay with `AfterRevision` and a limit of 1–1,000 events. Continue from `NextRevision`; `ThroughRevision` is the tenant boundary observed for that page. Each page checks `can_read_history` and uses one database view. This feed never changes worker offsets. Its revision cursor carries no authorization grant, and separate pages may observe newer commits. Change events retain version IDs, replaced IDs, affected intervals, reasons, and actor metadata where applicable. No external calculation is automatically rerun.

Recovery tests cover competing claimants, expired leases, duplicates after external effects, stale renewal/acknowledgement/failure, retry exhaustion and recovery, missing-event gaps, independent consumers, tenant isolation, restricted worker privileges, scope replay, heartbeat failure, and cancellation.

## CortexDB projection and recall

The adapter pins CortexDB `v2.129.0`, corresponding to the specification's commit `eaf996b62f75d5e499d1b9ca7fab470a23700385`. It uses the upstream PostgreSQL core directly. This keeps runtime construction free of the facade's automatic initialization and background routines. Projection writes and their immutable receipts share a transaction in the destination database; there is no transaction spanning the ledger and index.

Provision a **fresh, dedicated PostgreSQL database with pgvector for each tenant and generation** using `cortex.Provision(ctx, migrationDB, config)`. This explicit operation initializes the pinned upstream schema and binds tenant, generation, embedding identity/dimensions, and CortexDB version. An existing database is rejected. If provisioning fails partway through, inspect it and provision a new database; do not start runtime against a partial schema. Run [scripts/cortex-runtime-role.sql](scripts/cortex-runtime-role.sql) in that database and provision a separate runtime login. The adapter rejects owner/superuser connections and mismatched bindings. Connection pools must have an explicit maximum of 1–32 connections.

Create `cortex.New(ctx, runtimeDB, config, embedder)` with a bank-approved implementation of `model.Embedder`. Embedding calls have a 20-second deadline and validate dimensions and finite nonzero vectors. No embedding service is selected implicitly. Tests use a constant three-dimensional vector solely to establish database and lifecycle behavior, **not retrieval quality**. This initial implementation uses exact vector search; ANN tuning and relevance evaluations remain future work. Index text contains fact values and must receive appropriate database encryption and retention controls from the host.

Use `workers.NewProjection` with a `facts_projector` ledger login, the tenant-bound index, an authorizer, a trusted worker caller, and generation. Register its outbox identity using `model.ProjectionConsumer(generation)`, then run it through `workers.NewOutbox`. A [compiling composition example](workers/example_test.go) shows the lifecycle. The worker indexes approval/acceptance/supersession events using immutable version IDs and policy-safe metadata. Historical accepted versions remain individually addressable. Each scope requires the worker's current `can_read` permission. Lifecycle events advance coverage without deleting historical versions or changing their content.

Compose `factsengine.NewWithRetrieval` with the same generation, index, and a ledger progress reader. `Recall` accepts explicit scopes, query text, valid time, optional knowledge revision, and a result limit. It authorizes scopes, retrieves bounded opaque candidate IDs, resolves them against one ledger view, checks evidence availability, and returns canonical ledger values. Proposed, rejected, disputed, retracted, or otherwise ineligible versions are excluded. Historical recall additionally requires `can_read_history`. Index text is never the returned authority, and no external reranker receives candidates in this implementation.

Recall reports `Generation`, `IndexedRevision`, `LedgerRevision`, and `Incomplete`. `RequireRevision` fails with unavailable until the consumer has acknowledged that revision; it does not wait implicitly. Candidate over-fetch is bounded, so filtering can return fewer results. Projection failure does not block direct ledger reads or writes. A new generation uses a new database and consumer identity and replays from revision one. Host-controlled binding changes are possible; automated rebuild verification, cutover barriers, and retention/purge fencing are not yet implemented. Do not treat this as a production rebuild controller.

## Signed event archive consumers

Use `workers.NewArchive` with a `facts_archiver` ledger login, a `model.ArchiveStore`, bank-owned signer, independently configured trusted verification keys, an authorizer, and a stable stream ID. Its consumer identity is `model.ArchiveConsumer(stream)`. Archive workers require export, history, fact, and evidence permissions on every referenced scope, including multi-scope snapshots. Signing keys must remain verifiable after rotation.

Each event produces an encrypted payload object and a signed one-event batch manifest. The manifest binds tenant, stream, exact revision/count, payload digest and immutable object version, authorization checks, and the previous manifest's object reference. A retry after an external write but before acknowledgement validates and reuses the existing object. The archive source retains the full event envelope and supported immutable references: fact versions, predicates/policies associated with those versions, evidence metadata, timeline generations, attestations, snapshots, calculation receipts, and completed audit bundles. New predicate-publication events retain their predicate identity for exact reconstruction; legacy events lacking that identity carry an explicit gap. Evidence bytes are not duplicated; exact source locators are retained. These objects are **not a complete database backup**, and payload gaps say so.

`archive.NewFiles` supplies an encrypted local development implementation. Create its directory explicitly, inject an archive-specific cipher/key, and retain that key separately. Atomic create-once publication, fsync, digest checks, and tenant-bound decryption make ordinary retries safe. Filesystem ownership alone does not establish WORM protection: production hosts must use separately administered immutable storage with appropriate retention controls or implement `model.ArchiveStore` for their approved object service. Local tests do not establish protection against a privileged storage administrator.

Retain the archive head object reference and expected revision independently, then call `archive.VerifyChain`. Verification checks signatures, exact object versions, payload hashes, contiguous revision coverage, and predecessor links up to an explicit batch bound. Missing or tampered batches fail. An independently retained head is necessary to detect tail truncation; a mutable database offset alone is insufficient. Archive and projection offsets remain independent. Archive heads are not yet included in scoped `GetAuditExport` bundles, and complete restore/retention drills remain pending.

## Remaining delivery work

Follow the slices in [spec.md](spec.md). The current API is intentionally incomplete and may change before v1:

1. Finish foundation contracts: scope/subject administration, complete schema constraints and migration rules, confirmation presentation/response artifacts and delegated identity flows, evidence access endpoints, configurable quotas, richer policy controls, structured revision-conflict details, and full sqlc query coverage.
2. Extend the temporal slice with the distinct memory lifecycle, derived-memory eligibility, integrity-protected pagination, and known-at timestamp convenience queries. Fact lifecycle, revision-bound reads, and immutable fact snapshots are implemented.
3. Add managed generation rebuilds with count/sample verification and controlled cutover, deadline-based projection waits, workload tuning, and other job types. CortexDB projection, ledger-validated recall, coverage reporting, and signed event archive consumers are implemented.
4. Add conversation ingestion, extraction adapters, retained provider artifacts, and bank-approved extraction evaluations.
5. Add context manifests, decision receipts, retention/legal holds, complete archive/backup coverage, production immutable storage integration, and recovery drills. Calculation receipts, signed PostgreSQL audit bundles, and signed event archive chains are implemented.
6. Add Protobuf/Buf/protovalidate, Connect/gRPC service and worker binaries, gateway identity integration, transport parity tests, injected telemetry, readiness endpoints, CI, and operational packaging.

The integration scenario now preserves an external calculation on snapshot A through later fact corrections and verifies its signed audit export. The full specification remains incomplete. CortexDB and encrypted local archive adapters are exercised in integration tests. No remote engine API, production embedding/extraction model, independently administered immutable archive, or production deployment has been configured. The engine does not implement wealth allocation, profile completeness, or any business decision algorithm.

Production acceptance also requires the named domain, security, retention, and operational owners described in the spec, plus workload benchmarks and restore drills. Database roles protect normal runtime operations; they do not protect against a malicious database owner. Retained digests are integrity metadata, not anonymization.
