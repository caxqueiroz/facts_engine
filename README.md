# Facts engine

An embeddable Go engine for evidence-backed, policy-governed facts. PostgreSQL is the authoritative ledger; OpenFGA authorizes callers. This repository implements the **ledger foundation, temporal fact lifecycle, immutable fact snapshots, external calculation receipts, signed audit exports, durable outbox workers, CortexDB projection/recall, managed index rebuilds, signed event archive chains, immutable conversation ingestion, and durable fact and memory extraction**. The complete v0.2 specification and production banking release remain in progress.

The public entry point is `factsengine.New(ledger, authorizer, evidenceStore)`. Constructors do not run migrations, start workers/listeners, or change global logging. The host owns the PostgreSQL pools, authentication boundary, encryption keys, OpenFGA provisioning, and shutdown.

## Implemented

- Governed scope/subject metadata with immutable revision history, exact qualified subject lookup, administrator authorization and audit/archive coverage. New writes require registered scopes; fact proposals, snapshot selectors and extraction subjects require registered subjects.
- Typed text, boolean, integer, canonical decimal, date, instant, entity-reference, and explicit unknown values.
- Immutable policy and predicate versions; administrative publication activates a predicate version. New proposals and approvals cannot choose an obsolete schema to bypass the active policy. Existing acceptances retain their recorded policy.
- Encrypted development evidence objects with immutable revisions, digest verification, tenant isolation, source-revision integrity conflicts, and exact UTF-8 byte-span validation.
- Evidence-backed proposals, distinct-principal manual approval, and explicitly configured automated acceptance with permitted service principals and version-bound prerequisites.
- Attestations recorded by their authenticated attesting principal; withdrawal is append-only, serializes with acceptance, and makes required-prerequisite facts ineligible on current reads. Delegated client confirmation requires an explicitly configured trusted identity verifier and separately authorized recorder/client.
- Current and revision-bound historical fact reads with explicit missing, unknown, unresolved, disputed, retracted, resolved, and ineligible outcomes. Unknown interval starts cannot be accepted. Effective boundaries use PostgreSQL microsecond precision; finer boundaries are rejected rather than rounded.
- Partial-interval supersession, future-effective replacements, backdated corrections, rejection, dispute/resolution, and retraction. Complete overlap sets and expected stream revisions are mandatory. Immutable timeline generations preserve every earlier knowledge view.
- Immutable fact snapshots over ordered explicit selectors, with one PostgreSQL selection view, per-item outcomes, exact values and provenance, canonical content digests, separate selection/creation revisions, and current authorization on reads. Retries retain the original selection even after facts change.
- Prepared fact and memory contexts retain exact rendered bytes, query and retrieval provenance, authorization receipts and version inventories. Application decision receipts bind declared use to a retained context or snapshot.
- Governed disposal of unreferenced evidence, complete explicitly selected derivative families, orphan uploads, snapshot/context payloads and eligible terminal extraction artifacts, with policy age checks, scope legal holds, atomic tombstones and audit gaps. An opt-in bounded worker schedules orphan cleanup.
- External calculation receipts bind an application/run identity to immutable snapshots, executable/configuration digests, normalization version, and verified input/output evidence. Concurrent retries converge; changed run content conflicts. Reads recheck snapshot and artifact permissions.
- Durable, leased audit exports with fenced completion, revision-bound scope contents, signed entry manifests, explicit coverage gaps, and trusted-key verification.
- Authorized record event history, including actor/check receipts, correction intervals, reason codes, and replaced versions. Revision-pinned pagination supports long histories with explicit gap detection; the legacy single-response API retains its 1,000-event limit.
- Durable generation rebuilds with full canonical-content verification, destination write fencing, atomic active-generation cutover, and restartable cleanup.
- Tenant-bound CortexDB projections of accepted historical versions, injected embeddings, explicit database provisioning, canonical ledger revalidation on recall, and reported indexing lag.
- Signed event archive consumers with immutable encrypted object versions, predecessor links, standalone coverage verification, and crash-safe duplicate delivery. Local storage is a development adapter; production immutable storage remains host-provisioned.
- Explicit outbox workers with lease renewal, fencing, bounded retries, jittered backoff, visible failure state, independent contiguous consumer offsets, and scope-authorized change replay.
- Transactional tenant ordering, stream revisions, bounded transaction retries, command receipts, encrypted ledger events, and atomic outbox insertion. Identical authorized retries return the committed result; changed commands conflict.
- Tenant-qualified foreign keys, forced row-level security, transaction-local tenant context, and a runtime role without mutation privileges on immutable tables.
- OpenFGA HTTP adapter with pinned store/model, bounded requests, fresh checks, tenant-qualified identities, and retained authorization receipts.

`model` defines domain contracts, `internal/app/services` implements shared policy logic, and public adapters depend only on domain types. Generated sqlc rows stay inside the PostgreSQL adapter. Encryption uses tenant-bound AES-GCM; a production host must replace the development key management with bank-managed envelope encryption and retention-aware key ownership.

## Local setup

The module minimum is Go 1.26 (required by the Connect and protovalidate dependencies); use a currently patched, supported toolchain (verified with Go 1.27.2), Docker, and PostgreSQL client tools. The checked-in development containers bind only to loopback. OpenFGA's development datastore is ephemeral.

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

## Remote APIs

Generated Go messages and Connect/gRPC clients are committed under [gen/memory/v1](gen/memory/v1). The following services call the same engine as embedded users:

| Service | Operations |
| --- | --- |
| [FactService](proto/memory/v1/facts.proto) | GetFact, ListFacts, paginated GetFactHistory, ProposeFact, ApproveFact, AcceptFact, RejectFact, RetractFact, DisputeFact, ResolveDispute, SupersedeFact |
| [MemoryService](proto/memory/v1/memories.proto) | ProposeMemory, ApproveMemory, AcceptMemory, RejectMemory, RetractMemory, DisputeMemory, ResolveMemoryDispute, SupersedeMemory, GetMemory, GetMemoryVersion, paginated GetMemoryHistory |
| [OperationsService](proto/memory/v1/operations.proto) | GetProjectionStatus, RebuildIndex |
| [RetentionService](proto/memory/v1/retention.proto) | RegisterRetentionPolicy, PlaceLegalHold, ReleaseLegalHold, DisposeEvidence, DisposeOrphanEvidence, DisposeSnapshot, DisposeContext, GetRetentionStatus |
| [IdentityService](proto/memory/v1/identities.proto) | PutScope, PutSubject, GetScope, GetSubject, LookupSubject |
| [ContextService](proto/memory/v1/contexts.proto) | PrepareContext, GetContextManifest, RecordDecision, GetDecisionReceipt |
| [RetrievalService](proto/memory/v1/retrieval.proto) | Recall, RecallMemories, WaitForProjection |
| [CalculationService](proto/memory/v1/calculations.proto) | RecordCalculation, GetCalculationReceipt |
| [AuditService](proto/memory/v1/audit.proto) | ListChanges, StartAuditExport, GetAuditExport |
| [SnapshotService](proto/memory/v1/snapshots.proto) | CreateFactSnapshot, GetFactSnapshot |
| [EvidenceService](proto/memory/v1/evidence.proto) | RegisterEvidence, RegisterRedactedEvidence, GetEvidenceMetadata, GetEvidenceContent |
| [DefinitionService](proto/memory/v1/definitions.proto) | RegisterAcceptancePolicy, RegisterPredicateVersion, RegisterMemoryType |
| [IngestionService](proto/memory/v1/ingestion.proto) | IngestConversation, GetConversation |
| [ExtractionService](proto/memory/v1/extraction.proto) | StartExtraction, PlanExtractionChunks, GetJob, RetryJob, GetExtractionRun, StartMemoryRefresh, ScanMemoryRefresh, StartMemoryRefreshSweep |
| [AttestationService](proto/memory/v1/attestations.proto) | RecordAttestation, WithdrawAttestation, RecordDelegatedAttestation |

The main specification API groups, including retention administration and refresh scheduling, are available through this handler. The standalone service is implemented; comprehensive transport parity across every operation remains unfinished.

[The PostgreSQL-backed gRPC memory governance test](adapters/postgres/grpc_memory_governance_test.go) exercises generated grpc-go clients over TLS/HTTP2 alongside embedded calls. It compares full command receipts and error codes for review-gated automated acceptance, proposer/reviewer separation, required expiry, expired input and wrong-digest rejection, allowed accepting service identities, attestation withdrawal, historical eligibility, and review binding to a replacement before automated supersession. The fixture injects trusted callers and an allow authorizer to isolate engine policy behavior; identity mapping and OpenFGA isolation have separate tests. This coverage does not establish parity for every API operation. Audit integration tests also compare Connect and generated grpc-go responses for export scheduling replay, exact signed manifest/entry bytes and canonical artifact digests, scope/tenant/evidence-access denials, and revision pagination. Downloaded signatures and content digests are independently verified after gRPC transport.

[Context and decision parity tests](adapters/postgres/remote_context_test.go) also use generated grpc-go clients over TLS/HTTP2 against real PostgreSQL. They compare complete Connect/gRPC manifests and receipts, including rendered bytes and retained authorization metadata, with the embedded results; cover fact-only and mixed-memory contexts, replay with an unavailable search index, decision external-identity retries and conflicts, forged tenant rejection, failed context commits and read-permission denials. These fixtures inject trusted caller identities; production gateway/session authentication remains a separate acceptance requirement.

Calculation tests compare full Connect/gRPC receipts and reads, external-run retries/conflicts and artifact-digest failures.

Retention tests exercise governance and disposal RPCs with real grpc-go clients, compare retained receipts/status with Connect, reject ordinary database roles and forged tenants, enforce legal holds and shared-object blockers, and verify atomic derivative-family disposal replay across gRPC, Connect and embedded calls. Changed family membership under the same command key fails over gRPC as well.

Extraction parity tests use generated gRPC clients for immutable conversation ingestion/readback, chunk planning, fact and memory extraction scheduling, failed-job status, authorized retry/fencing, exact retained model artifacts and memory-refresh replay. Planning and scheduling must not invoke the model provider. Refresh administration tests compare paginated scans and sweep receipts across transports and reject forged triggers/tenants, denied permissions, oversized pages, changed command input, subsecond schedule timestamps and future sweeps. Provider execution remains an explicit worker operation; these synthetic fixtures are not bank-approved model evaluations.

Retrieval tests compare full fact/memory recall responses and projection receipts across Connect and gRPC against the PostgreSQL/CortexDB scenario, including denied reads, unavailable generations, stale candidates and deadline-bounded waits. The rebuild scenario executes start, verify, resume, activate and abandon requests through generated grpc-go clients, compares returned state with PostgreSQL, and compares projection status with Connect. It retains corruption/count checks, stale-fence rejection, cutover handling and operator-permission tests; this verifies the exercised transition scenarios rather than every possible concurrent workload.

Scope/subject tests exercise gRPC writes and immutable historical reads, compare full receipts and definitions with Connect, and enforce expected revisions, unique lookup identifiers, replacement of lookup mappings, immutable subject kinds and administration/read/history permissions. Evidence tests compare original/derivative registration receipts, exact redacted bytes and provenance across transports; metadata-only callers remain unable to read content, and mismatched scopes or caller-supplied derivation metadata are rejected.

Fact tests compare complete proposal receipts, direct reads and structured revision-conflict details over gRPC. Fact-list cursors issued by Connect work over gRPC, and gRPC history cursors work over Connect; continuation responses match exactly, while changed actors or filters are rejected. Snapshots created over gRPC replay through Connect and embedded calls with the same retained content and revisions. Governance scenarios also verify explicit zero/false schema constraints, review-gated automated acceptance, self-review rejection, withdrawal eligibility and delegated confirmation trust failures over gRPC, with full receipt comparisons for successful replay.

Memory lifecycle tests execute manual approval, dispute/resolution, supersession, retraction and rejection through gRPC and compare complete Connect replay receipts. Version provenance and current/historical reads match across transports, including suppressed bodies after disputes, dependency changes and retraction. Memory-history tokens cross transports while retaining actor binding; the fact-history endpoint rejects memory records. The [gRPC fact lifecycle scenario](adapters/postgres/grpc_fact_lifecycle_test.go) exercises manual approval, dispute/resolution, partial-interval supersession, retraction and rejection, comparing full receipts with embedded and Connect retries. It verifies preserved earlier intervals and historical knowledge views after disputes, corrections and retractions. These scenarios establish coverage for the exercised workflows; they do not replace production identity integration or workload acceptance.


[connectapi.NewHandler](adapters/connectapi/server.go) accepts the existing engine, an injected `IdentityVerifier`, and mandatory `Limits` (timeout, concurrent requests, request bytes and response bytes). It returns an `http.Handler` without opening a listener or starting background workers. The host mounts it on its HTTP server, configures TLS and read/write/idle deadlines, and enables HTTP/2 for gRPC. Connect and gRPC-Web use the same handler. A generated Connect client is composed as follows:

```go
transport := connecthttp.NewTransport(httpClient, baseURL)
client := memoryv1connect.NewFactServiceClient(connect.NewClient(transport))
```

The verifier authenticates the peer/session or a signed gateway assertion and returns a trusted `model.Caller`. It must verify any delegated initiator. There is deliberately no default verifier that trusts identity headers. Connect request metadata and HTTP TLS peer information are available through their server-context APIs. The request tenant must match the verified tenant before validation and engine access. Engine authorization and policy checks remain in force on every call, including retries.

Requests pass Protobuf validation and explicit timestamp component checks before conversion. The value oneof preserves false, integer zero, empty text and explicit unknown. Decimal amounts remain canonical strings; intervals retain unknown/open boundaries and reject precision loss. Lifecycle reasons are opaque identifiers. Supersession requests have a transport limit of 1,000 distinct replaced versions. Domain constraints, evidence-span checks and acceptance decisions run in the shared engine.

Listing and history require an engine constructed with `NewWithReads` and a securely provisioned 32-byte pagination key. Page sizes are explicit (1–100); page tokens pass unchanged to the shared engine and bind the authenticated identity, filters, page size and selected revision. Remote `GetFactHistory` uses the paginated engine method, with typed events carrying actor and authorization provenance. It does not invoke the unbounded embedded history method.

Memory proposals use an application-issued scope/ID, registered subtype/version, nonempty body (up to 64 KiB), source spans and up to 64 exact fact dependencies. Dependency labels preserve `assertion`, `uncertain` and `disputed`. Immutable version reads return retained content, proposer, content digest and authorization provenance. Retained content is distinct from a proposal request; original write-command identity is available through event history. Current reads enforce dependency eligibility and withhold stale, disputed or retracted bodies; revision-bound reads retain the historical view. Manual and automated acceptance remain distinct policy-controlled operations. Memory history is paginated with the same identity-bound cursor rules as fact history.

Snapshots accept 1–128 ordered, distinct selectors and return typed values, resolution states, source/evidence/attestation references, retained authorization receipts, the selection and creation revisions, and the engine's versioned content digest. The digest uses the engine's canonical snapshot format, not serialized Protobuf bytes. Remote and embedded retries share the same command identity and return the original persisted snapshot.

A host can require archive progress before delivering contexts with `EngineOptions.ContextArchive = &model.ContextArchiveOptions{Stream: "audit-v1", MaxPendingRevisions: 10, Progress: ledger}`. The progress reader must use the same tenant ledger domain and the actual archive consumer. The optional policy applies to all contexts on that engine; clients cannot select a different stream or threshold. `memoryd` exposes the same policy through `"context_archive": {"stream": "audit-v1", "max_pending_revisions": 10}`. Omit it to preserve ungated context delivery.

When enabled, `PrepareContext` checks before retrieval and persistence, and `GetContextManifest` checks again immediately before returning content, including idempotent preparation retries. Missing progress, unavailable storage, failed consumers, inconsistent offsets, or lag exceeding the inclusive revision threshold fail closed with no rendered bytes. Each progress read has a two-second bound within the caller's deadline. Direct fact reads remain available. The policy measures the tenant ledger head minus the acknowledged archive revision at the check; it is not a promise that future writes will stay within the threshold, a wall-clock lag limit, or independent verification of immutable archive objects.

New manifests retain an optional `Archive` receipt with stream, configured threshold, observed ledger/archive revisions and check time. Like authorization receipts, it is excluded from the canonical context-content digest and included in the retained/exported manifest. It records preparation-time evidence; reads enforce the engine's current policy without rewriting that receipt. A concurrent write or the context's own creation event can exceed the limit after persistence. In that case delivery fails but the manifest remains committed; let the archive worker catch up and retry the same command. In particular, a zero threshold can require that retry. The policy neither starts nor waits for an archive worker, and does not certify production archive integrity or retention.

`OperationsService` is disabled unless `EngineOptions.Operations` supplies a host-configured authorization scope, administrative rebuild store, routed rebuild index and outbox progress port. `can_operate` on that configured scope grants tenant-wide projection control; clients cannot choose an easier authorization scope. Use separately provisioned administrative credentials, and provision destination databases/routes before requesting a rebuild.

`GetProjectionStatus` reports an explicit generation (or the active generation when omitted), active epoch, rebuild state/fence/proof, consumer progress and the observed ledger revision. It marks candidate, lagging, failed or non-active coverage incomplete and rejects active-pointer changes during its read. This is an operational observation, not a snapshot shared with a later search.

`RebuildIndex` accepts `start` (1–100 maximum attempts), `verify`, or fenced `resume`, `activate` and `abandon` actions. Start identity is the generation; changed attempt budgets conflict. Projection workers must separately replay the tenant stream. Verify uses the existing bounded inventory verifier; activation requires the verified fence. The host's configured RPC deadline bounds the action. Transitions retain the full verified caller and authorization receipt in encrypted operational events, and PostgreSQL rejects stale authorization before and after each transition. A timeout or destination-gate failure can leave durable partial progress: inspect status and retry the same action/fence. An unprovisioned destination may remain unavailable even after abandonment was recorded. These APIs do not create databases, launch workers or erase data. `RetryJob` remains in `ExtractionService` with its existing operational permission and fence checks.

`IdentityService` registers governed scopes and subjects before evidence/fact ingestion. Writes require `can_manage_identities`, a command, expected metadata revision and reason. Scope and subject labels are display text, not lookup keys. Subject kind is immutable. Up to 16 namespace-qualified exact external identifiers may be registered; uniqueness is enforced per tenant and scope, and updates replace current lookup bindings while preserving history. `GetScope` and `GetSubject` select current metadata with revision zero or immutable history with a positive revision (requiring history permission). Reads and lookup recheck access before disclosure.

Definition publication uses the same administrative permissions and immutable version activation as embedded calls. Typed constraints preserve presence for explicit false, integer zero and empty optional bounds; cross-field constraints and migration rules are validated by the engine. Acceptance policies retain source classes, permitted accepting services, required attestors, expiry requirements and maximum age.

Direct attestation requests contain no actor or verification receipts: the handler assigns the authenticated caller as attestor. Delegated confirmation is a separate operation carrying an opaque grant ID and an **untrusted claimed client principal**. The engine requires its independently configured `AttestationIdentityVerifier` to bind that claim to the recorder, tenant, command, exact version/digest, confirmation artifacts and times. A transport identity verifier alone does not enable delegated attestations. Retained review/delegation receipts remain server-owned. These operations apply to fact and memory versions using the engine's existing rules; withdrawal immediately changes current eligibility when the attestation is required.

`RetentionService` exposes `RegisterRetentionPolicy`, `PlaceLegalHold`, `ReleaseLegalHold`, `DisposeEvidence`, `DisposeOrphanEvidence`, `DisposeSnapshot`, `DisposeContext`, `DisposeExtraction` and `GetRetentionStatus`. Requests bind to the verified tenant and reuse the embedded engine's authorization, idempotency and database-role checks. Policy registration accepts `evidence_bytes`, `orphan_evidence_bytes`, `snapshot_payload`, `context_payload`, `extraction_job_payload`, `extraction_request_payload` and `extraction_response_payload`, an explicit purpose/jurisdiction and a minimum age from zero to 100 × 366 days. Holds apply to the scope; releasing one requires `can_manage_legal_hold`. Disposal requires `can_dispose`, the matching policy, no active holds and no reference blockers, and returns the durable commit receipt. Status requires `can_manage_retention` and returns registration time, disposal state, active hold IDs and blockers without evidence bytes.

Expose retention mutations only through a host using the separately provisioned `facts_retention` role and matching ledger/evidence retention domain. Registering these RPCs does not elevate the packaged service's database role; an ordinary runtime connection cannot perform new retention mutations. The PostgreSQL disposal adapters cover eligible unreferenced evidence bytes, snapshot/context payloads and terminal extraction artifacts. They do not erase structural history, referenced derivatives, archives or backups, and do not provide coordinated disposal across stores.

Evidence registration takes an immutable object reference already accessible through the host's evidence store; it does not upload bytes or provision storage credentials. The engine verifies the referenced bytes and their digest before registration. Content reads require their own permission, independently of metadata access. Redaction registration requires an approved policy in the engine options, validates at most 256 omission ranges against both objects, and inherits source authority, classification and retention. Ordinary registration rejects supplied derivative provenance. Evidence objects are limited to 16 MiB; set the handler's response-byte limit to accommodate content plus metadata when serving large objects.

Conversation ingestion binds 1–128 immutable UTF-8 message references, source revisions, speaker roles/classes, timezone and source metadata. Each message is limited to 1 MiB and the conversation to 16 MiB. References must already be accessible through the host's evidence store.

Extraction requires approved profiles configured through engine options; execution also requires an injected provider. Clients select a profile ID and may bind its expected digest; prompts, model parameters and provider credentials are host-owned. Fact selectors and memory targets are mutually exclusive. Explicit original-byte ranges and approved redacted evidence IDs pass through the shared validation and provenance checks. Chunk planning is an authorized preview and does not reserve quota.

`ScanMemoryRefresh` exposes the operational dependency scan with a retained event ID/revision, target scope, explicit limit (1–100) and optional `after_version_id` cursor. Each page rechecks source/target permissions and reads the event-time inventory; it returns version references, not memory bodies. Pass the last returned version ID as the next cursor and stop at an empty page. The scan does not create jobs or establish current refresh eligibility.

`StartMemoryRefreshSweep` commits an idempotent sweep event for an approved memory-refresh profile. Supply a stream, UTC bucket timestamp and integer interval of 1–1440 minutes. The engine checks exact interval alignment, rejects future buckets and binds the profile digest into replay identity. It requires read/history/extraction permissions. This call neither starts a periodic timer nor invokes a model; an explicit refresh worker must consume the matching stream. `StartMemoryRefresh` remains the per-version scheduling operation. The standalone `refresh-sweep` mode owns periodic execution when configured.

`StartExtraction` and `StartMemoryRefresh` commit durable jobs and outbox events. A separately started worker invokes the provider, retains exact request/response artifacts, applies budgets and records proposals for review. Scheduling does not call the provider or accept candidates. `GetJob` reports status; `RetryJob` requires operational authorization and the expected fence. `GetExtractionRun` takes an attempt ID and returns authorized retained artifacts, digests, usage/reservations, validation issues, relations and proposal IDs. Worker ownership nonces stay internal. Configure response-byte limits for the approved artifact sizes. Production providers, evaluations and process supervision remain host responsibilities.

`Recall` and `RecallMemories` accept 1–8 distinct scopes, a query of at most 4096 bytes, a valid-time boundary and a result limit of 1–20. The engine revalidates candidates against the authoritative ledger and permissions before returning content. Scores express relevance. Results include the selected generation, ledger/indexed revisions and an explicit `incomplete` flag. Historical reads require history permission; generation or minimum-indexed-revision requirements fail with `unavailable` when unmet. `WaitForProjection` binds a receipt to a generation and revision, rechecks permissions and respects the RPC deadline (also capped at two minutes by the engine). It occupies an admitted request slot until completion; hosts should set timeout and concurrency limits accordingly.

`PrepareContext` persists the exact rendered bytes before returning them. Its command purpose must match the query purpose, and its scope must be among the query scopes. Optional mixed fact/memory contexts retain explicit item order and dependency-bound provenance. Responses include the original request, retrieval identity, authorization receipts, canonical/redaction versions, digests and selection/index/creation revisions. `GetContextManifest` rechecks retained-content permissions; command retries preserve the original bytes even after lifecycle changes or index outages. The rendered-byte limit is 8 MiB; configure transport response limits for those bytes plus metadata and encoding overhead.

`RecordDecision` records an application's actual use of a context or snapshot; it does not execute the decision. Exactly one source is required, used items must be selected, and selected versions must belong to the retained source. Model methods require exact model request/response artifact references; rule methods omit them. Optional calculation and action-receipt references, input/output digests, method versions and decision time are retained. External decision identity is idempotent per tenant/application, and changed content conflicts. `GetDecisionReceipt` rechecks source and evidence permissions. Context and decision digests use the engine's canonical representation, not Protobuf encoding.

Calculation recording binds externally produced input/output artifacts to an immutable snapshot; it never executes the calculation. The external run identity remains idempotent per tenant and application across command retries. Reads recheck snapshot and evidence access.

`StartAuditExport` commits a durable job; an explicitly started embedded audit worker builds and signs the bundle. Pending results omit the artifact and bundle. `GetAuditExport` rechecks scope and evidence permissions. Its completed bundle includes typed metadata, exact signed `manifest_json` bytes and exact `payload_json` entry bytes. Verify the signature using independently trusted keys, then verify entry identities, ordering, counts and digests against the signed manifest. Go clients can decode `manifest_json` into `model.AuditManifest`, preserve each raw entry payload and signature in a `model.AuditBundle`, and call `adapters/audit.Verify`. Protobuf serialization is not the signed encoding. Configure server and client response limits for bundle sizes and encoding overhead; audit payloads may exceed the 16 MiB evidence-object limit.

`ListChanges` returns authorized scope events with a revision cursor and a limit of 1–1000. Resume using `next_revision`; `through_revision` records the observed ledger boundary. Future cursors are rejected.

Errors use RPC status codes, including `invalid_argument`, `permission_denied`, `failed_precondition`, `already_exists` for changed command identities, and `aborted` with a typed `RevisionConflict` refresh hint. Unexpected dependency error text is not exposed. Admission fails immediately when capacity is full; the request context carries the configured deadline through identity verification and engine work. The host and injected dependencies must honor cancellation.

Tests cover value presence and validation boundaries, tenant binding, authentication failure, deadlines, a generated grpc-go client over HTTP/2, and PostgreSQL-backed embedded/remote command replay, revision conflicts, signed fact/history pagination, snapshot creation/read/retry parity, evidence and redaction registration/retry parity, independent evidence-content permissions, explicit constraint presence, review-gated acceptance/withdrawal, delegated-grant binding, the remote memory lifecycle with dependency invalidation and historical reads, and conversation/extraction scheduling, retry fencing and retained-artifact parity, calculation replay and artifact binding, and audit scheduling/download, signature and artifact digest verification, tamper rejection and revision pagination, and remote fact/memory recall with stale-hit suppression, historical reads, coverage parity and bounded projection waits, plus persisted context/decision replay, exact mixed-context bytes and order, index-outage retries, rollback without context delivery and revoked-access checks, plus remote scope/subject creation, revision conflicts, immutable history, qualified lookup replacement and administrative denial, and remote rebuild corruption checks, fencing, cutover recovery and retained operator authorization. Production gateway/Kratos session validation, deployment-specific acceptance gates and remaining transport parity coverage still require work.

## Standalone workload service

`make build` builds `bin/memoryd`, `bin/memoryworker`, the offline scorer `bin/memoryeval` and the archive verifier `bin/memoryverify`. Configure the service using [examples/memoryd.json](examples/memoryd.json), then run `bin/memoryd -config /path/to/config.json`. Configuration parsing rejects unknown fields, trailing JSON and files larger than 1 MiB. Paths resolve from the process working directory. Provision migrations, restricted runtime credentials, the pinned OpenFGA model/tuples, and certificates before startup; the service does not provision infrastructure or start workers.

Supply secrets through the deployment's secret manager as `MEMORY_DATABASE_URL`, `MEMORY_ENCRYPTION_KEY_HEX` (32-byte AES key encoded as hex), `MEMORY_PAGINATION_KEY_HEX` (a separate 32-byte hex key), and optional `MEMORY_OPENFGA_TOKEN`. If `audit_key_id` is configured, supply `MEMORY_AUDIT_PRIVATE_KEY_HEX` as a 64-byte Ed25519 private key encoded as hex. Keep keys stable across restarts for retained data and pagination. The supplied AES-GCM and Ed25519 implementations remain development/reference key integrations; production envelope encryption and managed signing require approved adapters and key-recovery procedures.

The service accepts TLS 1.3 client certificates chaining to the configured client CA. Each leaf must contain exactly one URI SAN matching an explicit tenant/principal binding. Workload bindings in `identities` produce a service caller with no inferred human initiator and ignore identity headers. Optional gateway bindings are described below. The [mTLS adapter](adapters/mtlsidentity/identity.go) rechecks certificate-chain validity on every request, including reused connections. The host owns CA issuance, revocation policy, certificate rotation and binding rollout; configuration reload and CRL/OCSP integration are not implemented. Restart to load changed bindings, roots or server keys. This workload path does not implement gateway/Kratos user-session or delegated-client verification.

`GET /livez` and `GET /readyz` use the same mTLS boundary. Liveness reports the HTTP process. Readiness checks PostgreSQL and a fresh check against the configured OpenFGA model; a valid authorization denial still establishes dependency health and grants no access. `GET /componentsz` separately reports optional retrieval connectivity under the same mTLS boundary, using only the fixed `retrieval` boolean. It checks every configured index pool and returns 503 on failure or a two-second deadline; an unconfigured retrieval component is omitted. With no optional components configured, the response is healthy with an empty component map. Index failure does not change `/livez` or core `/readyz`, preserving direct ledger operations. This is a connectivity check, not a claim about active generation, projection lag, search quality, or index integrity; use progress gauges and ledger-revalidated recall for those checks. Optional probes have separate single-request admission, so a slow index probe does not occupy readiness admission. Health responses are not cached. Component results contain booleans only. Concurrent readiness probes are bounded; a busy or draining process reports 503. Request concurrency, byte limits, deadlines, pool size, header limits and shutdown grace are bounded. SIGINT/SIGTERM stops admission and drains active HTTP requests before closing dependencies; grace expiry forces connections closed. Startup and shutdown logs omit credentials, request content and raw dependency errors.

The service composes the ledger/evidence APIs, signed pagination, optional audit signing, and optional lexical retrieval/rebuild routing. Approved extraction profiles and redaction-policy version identifiers can be configured for scheduling only. Provider execution belongs to a separate worker; delegated-attestation verification can use an explicitly configured HTTPS identity authority; APIs fail closed when required capabilities are absent. Additional worker modes, optional runtime configuration, production operational packaging remain delivery work. Evidence objects must already be registered in the configured store through an authorized host ingestion path; this binary adds no raw-byte upload endpoint.

[examples/go/client](examples/go/client/main.go) shows a generated Connect client with verified server TLS and a client certificate. Set `MEMORY_SERVICE_URL`, `MEMORY_TLS_CERT`, `MEMORY_TLS_KEY`, `MEMORY_SERVER_CA`, `TENANT_ID` and `SCOPE_ID`, then run `go run ./examples/go/client`. The configured workload needs the corresponding OpenFGA membership and read permission.

To enable remote extraction scheduling, start from [examples/memoryd-extraction.json](examples/memoryd-extraction.json). The optional `extraction` section contains approved profiles, `max_concurrent_per_tenant`, `daily_token_budget`, and optional cost-unit/budget settings. It always enables `ScheduleOnly`; it accepts no provider implementation or credentials. Full profile validation runs before the service starts accepting requests. Omit the section to keep scheduling unavailable. The development example enables the refresh parser; approve and configure profiles with `facts-extraction-json-v1` or `memory-extraction-json-v1` to schedule those job types. Profiles use the same native JSON format described for refresh workers below.

For profiles requiring approved redaction, configure `extraction.redaction_policy_versions` with the exact approved version identifiers. This allows the engine to validate previously registered derivatives and their provenance; it does not create redacted evidence, establish a redaction service, or waive evidence permissions. Refresh profiles continue to reject redaction settings that their parser does not support. Use the same pinned profiles and budget configuration on the separate executor. Clients can schedule jobs, inspect status and retained artifacts, and retry jobs with operational permission; they cannot cause this API process to invoke a model. The mTLS integration test verifies a configured profile creates a pending refresh job, an unapproved profile is rejected, and retries preserve job identity and the authenticated caller.

## Trusted gateway user identity

`memoryd` optionally accepts a `gateways` array in addition to its workload `identities`; see [examples/memoryd-gateway.json](examples/memoryd-gateway.json). Each entry binds one verified client-certificate URI SAN to exactly one tenant:

```json
"gateways": [
  {"URI": "spiffe://example.test/facts-gateway-bank1", "Tenant": "bank1"}
]
```

Only a certificate explicitly listed in `gateways` can assert user identity. On that mutually authenticated hop, the gateway must send exactly one `X-Facts-Verified-Tenant` header matching its configured tenant and one `X-Facts-Verified-Principal` containing the authenticated user's opaque engine principal ID. Missing, duplicate, combined, invalid or cross-tenant assertions fail authentication. A URI cannot be both a gateway and a workload binding, or bind multiple tenants. Configuration is copied at construction and requires restart to change.

The resulting caller is a direct user (`Service=false`, empty `Initiator`). Headers cannot turn it into a service or a delegated actor. The engine applies its ordinary tenant checks and OpenFGA `user` permissions; gateway trust grants no scope permissions. Recorded commands retain the asserted user as their actor. Workload certificates continue to use their fixed configured service identity even when these headers are present. At least one workload binding remains required for the existing service/readiness configuration; use a workload certificate for health probes that have no user assertion.

This is a trust contract for the **internal gateway-to-service hop**, not a public-header authentication mechanism. A configured gateway certificate has authority to assert any user ID within its fixed tenant. The gateway must validate the upstream session through the deployment's approved identity integration, derive tenant/principal from that verified identity, strip all client-supplied identity headers, and inject its own values. It must never copy an unverified public header into this contract. Restrict service reachability and protect the gateway's private key; tenant selection for multi-tenant gateways must choose the appropriate configured certificate based on verified identity.

Embedded HTTP hosts can use `mtlsidentity.NewWithGateways(workloadBindings, gatewayBindings)`, its TLS-terminating `Wrap` middleware and `Verify` callback. `mtlsidentity.New` continues to accept workloads only. TLS/HTTP2 tests cover header ambiguity, tenant binding, unmapped certificates and workload spoofing. The standalone integration verifies user authorization, retained user identity, request-tenant mismatch and access revocation against real PostgreSQL/OpenFGA.

No KrakenD/Ory Kratos session validator or production gateway configuration is shipped by this contract. The identity/security owner must configure and approve session validation, stable user/tenant mapping, header stripping/injection, network isolation, certificate rotation/revocation and the gateway-to-service trust boundary. Delegated client-confirmation grants still require the separate `AttestationIdentityVerifier` contract; gateway user identity does not bypass it.

## Delegated-attestation identity authority

`memoryd` can configure a trusted HTTPS authority for `RecordDelegatedAttestation`; see [examples/memoryd-attestation.json](examples/memoryd-attestation.json):

```json
"attestation_identity": {
  "url": "https://identity.example.test/facts/verify-attestation",
  "authority": "bank-identity-v1",
  "token_env": "MEMORY_IDENTITY_AUTHORITY_TOKEN",
  "timeout_seconds": 5,
  "max_authentication_age_seconds": 900
}
```

The named environment variable must contain the authority's bearer credential. Optional `root_ca_file` supplies a private PEM CA bundle; otherwise system trust applies. HTTPS requires TLS 1.3. The URL is the complete verification endpoint; credentials, queries and fragments in URLs are rejected. Timeouts must be 1–30 seconds and maximum authentication age 60–86400 seconds. Invalid configuration, missing credentials or unreadable CA files fail startup. Omitting this block leaves delegated verification disabled. Authority outages fail delegated operations closed without changing core service readiness.

Embedded hosts can construct `attestationhttp.New(Config)`, inject it through `model.AttestationIdentityOptions`, and call `Close` during shutdown. The runtime owns and closes its configured adapter. No constructor connects to the authority or installs global providers.

The **facts attestation identity v1** protocol is a POST with `Content-Type: application/json`, `X-Facts-Attestation-Identity-Protocol: v1`, `Cache-Control: no-store` and bearer authentication. The body is `attestationhttp.Request`: `request_digest` is `model.AttestationIdentityDigest(request)`, and `request` is the complete `model.AttestationIdentityRequest` serialized using Go's model JSON field names. It includes the trusted recorder, opaque grant reference, command, claimed attestor, exact target version/digest, confirmation artifacts and timestamps. The request is capped at 1 MiB.

The authority must independently authenticate the grant, resolve its verified attestor, and check the complete requested action against that grant. Echoing the request's identity or digest is not verification. On approval, return HTTP 200 with one JSON `model.AttestationIdentityReceipt` containing `Authority`, `BindingID`, `RequestDigest`, `Attestor`, `AuthenticatedAt`, `VerifiedAt` and `ExpiresAt`. Timestamps use the model's RFC3339 JSON representation. Return HTTP 403 for a denied grant. Other statuses, including invalid service credentials, are dependency failures. Identical authorized retries must produce the same binding ID and request digest with fresh verification metadata; verification alone must not make an uncommitted request impossible to retry.

Responses are capped at 64 KiB and reject unknown fields, duplicate fields (including case aliases), excessive nesting, trailing JSON, compression, wrong media types and mismatched authority/attestor/request bindings. Redirects, proxy discovery and automatic application retries are disabled. A maximum of four authority connections shares the configured timeout. Returned errors exclude credentials, URLs, request fields and authority error bodies. The engine independently enforces authentication age, confirmation chronology, receipt expiry, both actors' permissions and single-use binding constraints; it rechecks freshness after ledger lock waits before commit. This adapter does not relax those rules or turn a gateway assertion into a client-confirmation grant.

TLS adapter tests cover binding, malformed responses, denial, cancellation, redirects and trust failures. PostgreSQL-backed fact/memory delegated-attestation tests exercise the HTTPS authority adapter through embedded and remote operations, including exact retries and changed-request rejection. Production acceptance still requires an independently implemented and approved authority, issuer/session trust, grant lifecycle/revocation, managed credentials, clock synchronization and bank-approved presentation/response capture. No production issuer, grant store or Ory Kratos deployment is supplied.

## Ledger contention benchmark

`make ci-benchmark` creates an owned disposable Compose stack, runs integration and restore checks, then runs `BenchmarkLedgerWorkload` without the race detector for timing. The stack is removed on exit. Normal `make ci-integration` does not run performance measurements.

```sh
BENCH_TENANTS=1 BENCH_CONCURRENCY=8 BENCH_CONNECTIONS=8 \
  BENCH_READ_PERCENT=0 BENCH_SCOPES_PER_TENANT=1 \
  BENCH_TIME=3s BENCH_COUNT=3 make ci-benchmark
```

| Setting | Default | Supported range |
| --- | --- | --- |
| `BENCH_TENANTS` | 1 | 1–1024 |
| `BENCH_CONCURRENCY` | 8 | 1–256 concurrent callers |
| `BENCH_CONNECTIONS` | 8 | 1–64 runtime database connections |
| `BENCH_READ_PERCENT` | 0 | 0–100 percent read selection |
| `BENCH_SCOPES_PER_TENANT` | 1 | 1–10000 pre-seeded scopes per tenant |
| `BENCH_TIME` | `3s` | Go benchmark duration or iteration count, such as `100x` |
| `BENCH_COUNT` | 3 | Go benchmark repetition count |

Each run uses new tenant IDs and encrypted test payloads. Writes create independent scopes through `Engine.PutScope`, contending on the real per-tenant ledger append path; reads use `Engine.GetScope` against the seeded corpus. The deterministic operation selection distributes tenants independently of the requested read percentage. The workload includes the reference in-process authorizer, PostgreSQL transactions and bounded internal retries, encryption, command receipts, and atomic outbox writes. It excludes external authorization latency, fact validation/acceptance, retrieval, model execution and production network topology. It is an append-contention diagnostic, not a complete bank workload.

The PostgreSQL adapter makes at most three attempts per transaction. Serialization failures and deadlocks use bounded randomized backoff after rollback releases the connection: nominally 1–5 ms before the second attempt and 2–10 ms before the third. The caller's context can interrupt either wait. Unique-key races retain their existing immediate retry behavior so a committed command receipt can be reread. Other errors are not retried. Exhausted serialization/deadlock retries return the existing revision-conflict error. This reduces synchronized retry pressure without weakening `RepeatableRead` isolation, increasing the attempt limit, or turning a failed command into a reported success. The nominal additional delay is at most 15 ms, excluding scheduling delays and transaction time; contention can still exhaust retries, so callers must retain stable command IDs when explicitly retrying commands.

Ledger transactions classify pgx connection-establishment failures, socket operation errors, closed driver connections, PostgreSQL connection failures, shutdown/recovery refusal and connection exhaustion as `ErrUnavailable`; remote handlers return sanitized `Unavailable` responses. Explicit cancellation and deadlines retain their own outcomes. SQL syntax/protocol defects and unrelated application errors remain unexpected failures. Classification uses typed driver errors and [PostgreSQL SQLSTATE codes](https://www.postgresql.org/docs/current/errcodes-appendix.html), not message matching. Connection outages and unknown statement/transaction completion are not automatically retried: a failed response does not prove rollback, so callers must retry with the original command identity. Tests terminate a dedicated PostgreSQL test connection and inject subsequent dial refusal, verify one callback attempt and empty/sanitized embedded/gRPC results, then verify connection recovery. The PostgreSQL evidence adapter shares this outage classification for its transactions; absent or corrupt evidence remains a distinct integrity failure.

Output reports successful `commits/s` and `reads/s`, `conflicts/op`, unexpected `failures/op`, and `p50-upper-us`, `p95-upper-us`, `p99-upper-us`. Percentiles are upper bounds from fixed power-of-two nanosecond buckets, not exact sample percentiles. Latency includes pool waiting and internal retries; all outcomes, including failed attempts, contribute to latency and Go's `ns/op`. Conflict rates must therefore be reviewed alongside throughput. Unexpected failures invalidate the run. Requests have a 30-second deadline; the runner caps the benchmark process at 30 minutes. Setup and final invariant checks are excluded from timing. After each sample, every tenant's committed write count must match its contiguous ledger revisions and outbox row count.

For an already initialized **disposable** integration database, set `TEST_BENCH_DATABASE_URL` and invoke `go test ./adapters/postgres -run '^$' -bench '^BenchmarkLedgerWorkload$' -benchtime 3s -count 3`. This requires the fixture schema/roles and a connection authorized for its runtime role and final inventory checks. It writes new benchmark tenants and does not delete them; never point it at a production database.

Compare a single hot tenant with multiple tenants, vary connection/concurrency limits and read mix, and retain the exact command, Go/PostgreSQL versions, CPU/memory/storage/container settings, dataset size and raw results. The operations owner must supply representative production workloads and approved latency/capacity targets. No development benchmark result establishes an approved service limit or substitutes for fact/search/model workload evaluations.

## Disposable restore drill

`make ci-integration` now finishes with [scripts/restore-drill.sh](scripts/restore-drill.sh). After the ordinary integration fixture stops writing, it adds a dedicated accepted fact and encrypted evidence, takes a PostgreSQL custom-format logical dump, and restores it into a newly created database. It uses `pg_dump` and `pg_restore` from the same PostgreSQL container, fails on restore errors, and never uses `--clean` or resets an existing database. A separate fresh Cortex database receives the rebuilt search state. The surrounding CI runner owns and removes the entire disposable Compose project.

The verification compares the inventory and deterministic row digests of every public table before changing the restored database. This covers retained ciphertext, immutable records, receipts and outbox offsets from the integration fixture. It then reads the dedicated accepted fact and exact evidence bytes through the restored runtime adapter with the original test key, verifies that a wrong key fails, replays the restored tenant's outbox into the fresh lexical index, checks full revision coverage, and recalls the original immutable fact version/value. Runtime and projector database roles remain separate; the privileged connection is used for backup inventory and provisioning.

This is a logical-backup regression drill using test data and the fixture's known reference encryption key. It does not establish physical-backup/PITR recovery, independent key custody, production evidence/archive-store recovery, production identity restoration, or approved RPO/RTO. The temporary dump is permission-restricted and deleted by the script; it is not an encrypted production backup artifact. When running `scripts/integration.sh` against manually managed development containers, the newly created test/restore databases remain there for inspection, just like its other disposable databases.

Production recovery must be exercised by the operations, security and archive owners against their approved backup systems: record the backup identity and target recovery point; restore ledger/evidence metadata and exact object versions with their encryption keys; reconcile legal holds, disposal state and outbox offsets; rebuild and verify derived search generations; test authorization, retained context/snapshot reads and archive coverage; and record observed data loss and recovery time against approved RPO/RTO. Restoring the database alone does not restore OpenFGA policy state, certificates, signing/encryption key custody or an independent archive head. Do not reopen writes or publish a rebuilt generation until the deployment's integrity and authorization checks pass.

## Runtime container images

`make containers container-smoke` builds and checks separate `facts-engine/memoryd:local` and `facts-engine/memoryworker:local` images. Docker and the repository's Go toolchain are required. The build cross-compiles static Linux binaries with CGO disabled; `CONTAINER_ARCH=amd64` or `arm64` selects the architecture (the host Go architecture is the default). Smoke tests must run on a matching Docker host or one with the relevant emulation installed. Set `IMAGE_PREFIX` and `IMAGE_TAG` to choose image names; these targets do not push images or deploy services. CI builds and exercises both entrypoints.

The [Dockerfile](deploy/container/Dockerfile) uses `scratch`, numeric user/group `65532:65532`, direct binary entrypoints and SIGTERM shutdown. The build script sends Docker only a temporary directory containing the two binaries and Dockerfile, never the repository, local environment files or secrets. Images contain no shell, package manager, migrations, configuration, credentials or CA bundle.

Provision PostgreSQL schemas/roles, OpenFGA models, certificates, encryption/signing keys and any retrieval destinations separately before starting a process. Use the existing JSON examples as templates. For a service reachable through a container port, set `listen` to `0.0.0.0:8443` inside the container; the host publish address controls external exposure. All dependency URLs must be reachable from the container: `127.0.0.1` refers to the container itself. Use container paths for certificates and other files, such as `/run/facts/server.pem`.

For example, after provisioning a Docker network named `facts-runtime` and placing configuration and credentials outside the repository:

```sh
docker run --name memoryd --network facts-runtime \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --stop-timeout 60 --publish 127.0.0.1:8443:8443 \
  --env-file /etc/facts-engine/memoryd.env \
  --env SSL_CERT_FILE=/run/facts/ca-bundle.pem \
  --mount type=bind,src=/etc/facts-engine/memoryd,dst=/run/facts,readonly \
  facts-engine/memoryd:local -config /run/facts/config.json
```

The host directory must be traversable and its required files readable by UID/GID 65532, without making secrets broadly readable. Keep the environment file restricted to the host operator; it supplies the documented `MEMORY_*` variables. Mount a reviewed PEM trust bundle at `SSL_CERT_FILE` for outbound TLS using system trust. Explicit adapter CA files and PostgreSQL `sslrootcert` paths must also exist in the container. The image cannot validate public roots without an operator-supplied trust source.

Run `memoryworker` with the same restrictions, a separate per-mode configuration/environment and no published port. Projection, audit, refresh, refresh-sweep, extraction and orphan-cleanup workers need no writable container filesystem. The local reference archive mode additionally requires a durable writable mount at its configured absolute `archive.directory`, owned by the runtime identity; never store archive objects in the ephemeral container layer. That writable local store is not production WORM storage. Separate service/worker credentials retain the existing least-privilege database-role boundaries.

SIGTERM uses the binaries' existing cancellation and graceful-shutdown paths. Set the supervisor's stop grace above `shutdown_seconds` plus the five-second telemetry flush for `memoryd`; allow worker cancellation and telemetry flush before forced termination. Restart policy, resource limits and network access rules belong to the deployment configuration. An exhausted outbox consumer still needs explicit operator repair/retry; restarting its container does not reset attempts.

Probe `/livez`, `/readyz` and `/componentsz` from an authorized mTLS client outside the image. No unauthenticated probe port or baked-in probe credential is added. The smoke target verifies both binaries start as non-root on a read-only filesystem, report CLI usage and reject invalid configuration; the existing integration suite separately exercises service and worker behavior. These checks do not establish a production deployment, approved capacity, backup recovery, certificate rotation or restore-drill acceptance.

## Standalone telemetry export

Both `memoryd` and `memoryworker` accept this optional top-level configuration:

```json
"telemetry": {
  "endpoint": "https://collector.example.com",
  "token_env": "MEMORY_OTLP_TOKEN",
  "export_timeout_seconds": 5,
  "metric_interval_seconds": 30,
  "trace_sample_ratio": 0.1
}
```

The runtime appends `/v1/traces` and `/v1/metrics` to the endpoint. Set optional `root_ca_file` for a private CA; otherwise system trust applies. HTTPS uses TLS 1.3. Local development can explicitly enable `allow_plaintext_loopback` for an HTTP endpoint using a literal loopback IP. Credentials are read from the named environment variable. Redirects, automatic exporter retries, proxy discovery and compression are disabled.

Omitting `telemetry` disables runtime instrumentation and exporters. When enabled, each process owns its SDK providers, bounded trace queue, periodic metric reader and transport, and attempts a five-second flush on shutdown. Collector outages do not affect core readiness or business authorization. Export errors are sanitized; delivery is best effort, so these measurements are not an audit ledger. Sampling accepts 0–1, export timeout 1–30 seconds, and metric interval 1–300 seconds. Exported resource attributes are restricted to service name and a random process instance ID, including when ambient OpenTelemetry resource variables are present. No global providers are installed.

The service injects the same providers into its engine and RPC handler. Outbox workers also register lag gauges for their configured consumer; refresh-sweep and orphan-cleanup have no outbox consumer. Remote trace context still requires the separate explicit propagation settings described below. Production collector provisioning, access control, storage retention, alert thresholds and dashboards remain operator responsibilities.

## Injected engine telemetry

`NewWithOptions` accepts host-owned OpenTelemetry providers and an optional structured logger:

```go
engine, err := facts.NewWithOptions(ledger, authorizer, evidence, model.EngineOptions{
    Telemetry: model.TelemetryOptions{
        TracerProvider: tracerProvider,
        MeterProvider:  meterProvider,
        Logger:         logger,
    },
})
```

The host configures SDK exporters, sampling, resource attributes, flushing and shutdown, following the [OpenTelemetry Go instrumentation guidance](https://opentelemetry.io/docs/languages/go/instrumentation/). Constructors neither install nor read global providers, open exporter connections, nor start background workers. Omitted providers use local no-ops; omitting all telemetry fields leaves instrumentation disabled. Existing context parentage is preserved through engine authorization, evidence, transaction callbacks and provider calls.

Current stages cover authorization checks, evidence loading/content reads, conversation ingestion, fact/memory proposals and approvals/acceptance, extraction execution, fact/memory recall, context preparation, audit export and common mutation transactions. Spans use fixed `verb.noun` names. `facts.operations` counts completed stage invocations, and `facts.operation.duration` records seconds, with only `facts.stage` and classified `facts.outcome` labels. Nested stages count separately; these are not RPC request counts or model-job outcome metrics. Return errors map to bounded outcomes such as `denied`, `unavailable`, `conflict` or `internal`; panic observations are recorded and rethrown.

`facts.extraction.results` counts successfully persisted extraction attempt results, labeled only by bounded `extraction.kind` and `extraction.outcome`. Outcomes distinguish completed parsing, provider failures, response limits, invalid metadata/responses, changed authorization and exhausted leases. `facts.extraction.proposals` and `facts.extraction.issues` count proposals and validation issues retained with those results; `completed` can still include rejected candidates. `facts.extraction.rejected_jobs` separately counts durable input-limit rejections before a provider attempt exists. Measurements happen after successful persistence, outside transaction retry callbacks, and terminal job replays do not increment them. These are operational counters, not an exactly-once audit ledger: process termination or an ambiguous commit can leave a durable result unobserved. A persisted failed attempt can leave its job pending for retry.

Logs include stage, outcome, duration and valid trace/span IDs. They omit caller/tenant/record IDs, purposes, correlation IDs, evidence, values, prompts, responses, credentials, and error/panic text. Successful operations log at debug level; failures at warning level. Tests use injected in-memory SDK providers to verify parentage, metric labels, transaction context propagation and sensitive-text exclusion. Hosts must separately review exporter resources, custom logger attributes and upstream adapter logs.

For RPC observability, use `connectapi.NewHandlerWithOptions(engine, verify, limits, connectapi.HandlerOptions{Telemetry: telemetryOptions, AcceptTraceContext: true})`, passing the same host-owned providers used by the engine. The original `NewHandler` keeps observability and remote context extraction disabled. Registered RPC invocations produce a `serve.rpc` server span, `facts.rpc.requests` counter and `facts.rpc.duration` histogram in seconds. Labels contain only the registered method and bounded RPC outcome. Measurements begin before capacity admission and identity verification, so those rejections are included; unknown HTTP paths and protocol failures before RPC dispatch are not counted. Logs contain method, outcome, timing and trace/span IDs, without request/response contents or error text.

`AcceptTraceContext` is optional and independent of authentication. It accepts only one bounded W3C `traceparent` header; malformed or duplicate values are ignored. Request `tracestate` and `baggage` are not extracted. The host controls sampling and whether to accept remote parenting. Trace headers cannot supply a caller, change the verified tenant, or grant a permission. Tests cover remote parenting, default isolation, authentication failures, request counts, bounded labels and private-content exclusion.

Outbox hosts can pass the same provider/logger options through `workers.Options.Telemetry`. Each `ProcessOne` creates a `process.outbox` consumer span and records `facts.outbox.polls` plus `facts.outbox.duration` in seconds. Outcomes distinguish idle polls, claim errors, acknowledgement success/failure, durable failure-recording success/failure, parent cancellation, lease errors and panics. A handler error whose failure record commits is `failure_recorded`, not acknowledgement; that record may exhaust attempts rather than schedule another delivery. Worker kind is selected from the known handler type or `custom`, never from a tenant/consumer ID or event string.

`facts.outbox.retries` counts claimed deliveries with attempt number greater than one. `facts.outbox.event_age` records seconds since the claimed event's retained timestamp, skipping absent timestamps and clamping future timestamps to zero. It measures age of claimed work, not queue depth or the age of an unclaimed queue head. These instruments label only bounded worker kind (and poll outcome where applicable). Spans/logs omit tenant, consumer, event, scope and command identifiers and handler/queue error text. Parent context reaches claim, handler, renewal and acknowledgement operations; no trace context is inferred from event payloads. SDK tests exercise acknowledgement, retries, idle polling, queue failures, cancellation and lease loss with privacy assertions.

Outbound OpenFGA and extraction-gateway adapters support `PropagateTraceContext: true`. They copy only a valid active W3C `traceparent` into their newly constructed requests, without reading global propagators or forwarding baggage/tracestate. Bearer credentials and retained request/response bytes are unchanged. Propagation is disabled by default and adds no header when the context has no valid span.

For packaged runtimes, `memoryd` accepts `accept_trace_context: true` for incoming RPC trace parenting. Both `memoryd` and `memoryworker` accept `openfga_trace_context: true`; extraction gateway bindings accept `propagate_trace_context: true`. These are separate, default-off decisions for each boundary. They propagate an existing context and do not create SDK providers or configure exporters. Hosts remain responsible for trusted endpoints and sampling; trace metadata never establishes a caller identity.

`facts.recall.filtered` counts candidates excluded by successful ledger revalidation in fact and memory recall. Its only labels are bounded `recall.kind` (`fact`, `memory`, or `other`) and `recall.reason` (`missing_version` or `ineligible_version`). Ineligible includes superseded/retracted versions and unresolved lifecycle or dependency state at the requested knowledge revision. Malformed hits, duplicates, other record kinds, out-of-scope candidates and later disclosure failures are not counted as stale versions. Counts reset on transaction retries and are emitted after the read transaction succeeds, even if a later disclosure or generation check fails. They measure revalidation work, not unique records or successful responses.

`facts.export.integrity_failures` counts failed checks with only the bounded `export.check` label. During export preparation, `evidence_content` records confirmed size/digest mismatches; unavailable reads retain the existing coverage gap without being counted as confirmed corruption. For downloaded bundle verification, construct `audit.NewVerifier(trustedKeys, telemetryOptions)` and call `verifier.Verify(ctx, bundle)`. It copies the independently supplied trust map and uses the same signature, ordering, count and entry-digest checks as `audit.Verify`, adding a `verify.audit` stage and `audit_bundle` failure counts. Unknown signing keys also fail verification; a failure is not proof of malicious modification. Canceled calls before verification do not increment integrity failures. Repeated checks count separately. The original `audit.Verify` remains available without instrumentation. No artifact, key, tenant, scope or record identifiers become labels.

Hosts can explicitly register `workers.NewProgressMonitor(progressReader, targets, meterProvider, timeout)` with trusted `ProgressTarget{Key: consumerKey, Kind: "projection"}` or `Kind: "archive"` (also `audit`, `extraction`, `refresh`). PostgreSQL implements `model.OutboxProgressReader` using a consistent snapshot of the tenant ledger head, consumer offset and next unacknowledged event. The monitor registers an SDK collection callback without starting goroutines; call `Close` before shutting down the host-owned provider. It supports up to 256 unique consumers and a collection timeout from one millisecond to 30 seconds. Readers must honor context cancellation.

The callback exports gauges labeled only by bounded `worker.kind`: `facts.outbox.pending_revisions` sums ledger-minus-acknowledged revisions, `facts.outbox.oldest_pending_age` reports the maximum next-event age in seconds, `facts.outbox.failed_consumers` counts failed consumers, and `facts.outbox.monitored_consumers` reports coverage. Pending revisions include running, delayed and failed work. For projection/archive consumers this is acknowledgement lag, not proof of destination integrity or a count of unique events across consumers. A caught-up consumer reports zero lag/age. Future event timestamps clamp age to zero. Multiple streams of the same kind are aggregated; use host-owned resource boundaries when operational isolation is required. Use one monitor per provider containing all consumers to aggregate; separate monitors with the same kind and provider would share a gauge series.

All target reads must succeed before any samples are emitted. Read failures, inconsistent snapshots, overflow or deadlines fail collection without partial or cached healthy values; storage error text is sanitized. Configure exporter/collector alerts for missing samples and collection errors as well as lag thresholds. The monitor does not poll independently. Packaged outbox workers register it when their optional telemetry configuration is enabled.

For archive-chain monitoring, `archive.NewChainVerifier(trustedKeys, telemetryOptions)` copies the independently configured trust map. Its `Verify` method accepts the store, tenant, stream, independently retained head, expected revision and maximum batch count, recording a `verify.archive` stage and `facts.export.integrity_failures{export.check="archive_chain"}` for confirmed failed checks or missing referenced objects. Both this method and the original `archive.VerifyChain` independently compare manifest/payload sizes and digests with their references before parsing, in addition to checking signatures and revision continuity. Custom storage adapters cannot bypass those byte checks by returning unchecked content. Read outages, invalid request bounds and cancellation are not counted as confirmed integrity failures; opaque storage errors remain operation failures, including errors from adapter-internal decryption checks. Repeated verification failures count separately. No tenant, stream, object locator or signing-key ID is emitted in telemetry.

Projection and archive handlers accept host-owned providers through `workers.NewProjectionWithTelemetry` and `workers.NewArchiveWithTelemetry`. They emit `project.document` and `archive.event` spans plus the shared operation count/duration metrics, preserving the parent context through authorization and storage calls. Outcomes include denied access, dependency failure, cancellation and panic without logging raw errors, event contents or identifiers. An ignored projection event still completes a handler operation; these counts do not measure index writes. The original constructors keep instrumentation disabled. Packaged workers inject their configured providers into both handlers.

Extraction emits `call.extraction_provider` for the deadline-bound provider invocation and `validate.fact_proposals`, `validate.memory_proposals`, or `validate.memory_refresh` for the transactional candidate validation and staging pass. Provider spans inherit the run context; errors, cancellation and panics use bounded outcomes without recording request, response or error text. Validation passes that return review issues successfully have operation outcome `ok`; use `facts.extraction.issues` for committed issue counts. Transaction retries can repeat validation spans, so these stage counts are execution attempts, not committed proposals. Full internal-stage coverage remains unfinished. Both packaged binaries support the optional OTLP exporter configuration above and retain their sanitized lifecycle logs and health endpoints.

## Standalone workers and lexical retrieval

`bin/memoryworker -config /path/to/worker.json` runs one configured tenant/generation using [examples/memoryworker.json](examples/memoryworker.json). Supported modes are `projection`, `audit`, `archive`, `refresh`, `refresh-sweep`, `extraction` and `orphan-cleanup`; use a separate configured process for each. Consumer modes use the existing outbox claim, heartbeat, fencing, retry/backoff and acknowledgement code; `refresh-sweep` emits events without claiming deliveries, and `orphan-cleanup` scans pending uploads without an outbox consumer. A delivery deadline bounds processing. SIGINT/SIGTERM cancels in-flight work; an unacknowledged lease remains retryable after expiry. Queue errors, delivery timeouts and exhausted consumers exit nonzero for the host's process supervisor. Restarting does not reset attempts or acknowledge incomplete work. Operator retry and destination repair remain explicit actions. The worker logs mode and outcome without event content, identifiers or dependency error text.

Supply `MEMORY_WORKER_DATABASE_URL` using a separately provisioned `facts_projector` login, the same `MEMORY_ENCRYPTION_KEY_HEX` used by the ledger, optional `MEMORY_OPENFGA_TOKEN`, and the index connection URL in the environment variable named by `projection.database_env`. Grant the configured service principal tenant membership and the projection read permissions for every relevant scope; memory projections also require history/evidence access across dependency scopes. The configured maximum attempts must match an existing consumer. Projection mode does not migrate, provision an index, select an active generation or retry an exhausted consumer. Orphan retention cleanup runs only in a separately configured `orphan-cleanup` process using the retention credentials described below.

Projection, audit and archive workers reject denied, stale, future-dated or incomplete authorization decisions. Allowed decisions must include a model ID and `HIGHER_CONSISTENCY`, with a check age of at most 30 seconds (at most one second of future clock skew). Projection and archive publication recheck the decision age after dependency work; hosts must keep worker and authorization clocks synchronized.

For audit exports, use [examples/memoryworker-audit.json](examples/memoryworker-audit.json). Set `MEMORY_WORKER_DATABASE_URL` to the queue-only `facts_worker` login and `MEMORY_DATABASE_URL` to a separate `facts_runtime` login for export execution, along with the shared encryption key, `MEMORY_AUDIT_PRIVATE_KEY_HEX` and optional OpenFGA token. `audit.key_id` identifies the signing key; configure the corresponding audit capability in `memoryd`. Each pool is bounded by `max_connections`. The audit worker's service principal needs tenant membership, `can_operate` and `can_export_audit` in each export scope.

Audit delivery consumes only trusted `start_audit_export` ledger events. It first checks the configured worker's permissions, then runs the engine with the event's original requester and purpose. The engine matches that requester to the retained job and rechecks current export/history/read/evidence permissions. A changed or revoked requester cannot gain access through the worker. Completion is committed before the event is acknowledged; replay returns the same signed export. Events unrelated to starting an export are acknowledged without initiating another export.

Audit attempts have a two-minute execution deadline and a separate three-minute job lease. After an execution error, outbox retry scheduling respects that lease as a minimum delay, even if the ordinary backoff cap is shorter. This prevents rapid retries from exhausting delivery attempts while a job is still held. Process cancellation leaves the outbox unacknowledged; operator retry remains necessary if the configured attempt budget is exhausted. Audit exports remain PostgreSQL bundles with explicit coverage gaps, not independently controlled immutable archives.

For event-driven memory refresh, use [examples/memoryworker-refresh.json](examples/memoryworker-refresh.json). Set `MEMORY_WORKER_DATABASE_URL` to the queue-only `facts_worker` login, `MEMORY_DATABASE_URL` to a separate `facts_runtime` login, the shared ledger encryption key, and optional OpenFGA token. Each pool is bounded by `max_connections`. Grant the service principal tenant membership plus extraction/proposal, read, history and evidence permissions across configured memory scopes and their dependency scopes.

Configure one tenant/stream, a purpose, and up to 64 unique scope-to-profile bindings. Profiles use the native `model.ExtractionProfile` JSON representation: capitalized field names, base64 prompt bytes, duration integers in nanoseconds, and the SHA-256 digest of the decoded prompt. The example is a development template, not an approved production prompt or provider. Replace provider/model/build identities and obtain profile approval before deployment. Startup validates full profiles and budgets before registering the consumer. This mode always constructs a scheduling-only engine with no provider credentials or implementation; a separate execution host must use the same profile digest.

The refresh consumer scans dependencies in bounded pages after relevant fact lifecycle events and consumes matching durable refresh sweep events. It creates idempotent pending jobs only for affected accepted memories that need refresh. It never calls a model, accepts a replacement, or resets attempts. A delivery deadline bounds each scan; incomplete processing remains retryable with stable job command keys. Run the separate `refresh-sweep` mode for time-based expiry. Run the separate `extraction` mode to execute those jobs. Changing the stream creates a distinct consumer; preserve stream, purpose and profile bindings for replay, and coordinate profile changes with pending jobs and sweep events.

For periodic refresh sweeps, use [examples/memoryworker-refresh-sweep.json](examples/memoryworker-refresh-sweep.json) with `MEMORY_DATABASE_URL` (`facts_runtime`), the ledger encryption key and optional OpenFGA token. This mode needs no `MEMORY_WORKER_DATABASE_URL`, registers no queue consumer, and opens one pool bounded by `max_connections`. Omit the queue attempt, lease, polling and backoff fields; nonzero values are rejected. Configure `refresh.sweep_interval_minutes` from 1 to 1440 and keep tenant, principal, stream, purpose, profiles and bindings aligned with the refresh consumer. The engine authorizes each sweep for extraction, read and history access.

The ticker emits the current UTC bucket immediately at startup, then runs at interval boundaries. `delivery_timeout_seconds` bounds an emission across all bindings; failures exit nonzero for the supervisor. Shutdown cancels emission and waiting. Stable per-stream/scope/bucket command keys make identical restarts reuse committed sweep events, including after partial success across scopes. Configuration changes within an already committed bucket can conflict; coordinate changes at bucket boundaries or use a deliberately new stream. Missed historical buckets are not replayed after downtime: the current sweep scans retained accepted memories and checks their current eligibility. The separate refresh consumer creates pending jobs; neither process calls a model or accepts a replacement. The integration test runs both processes with real PostgreSQL/OpenFGA and verifies a persisted sweep produces a pending refresh job.

For extraction execution, use [examples/memoryworker-extraction.json](examples/memoryworker-extraction.json), a queue-only `facts_worker` connection in `MEMORY_WORKER_DATABASE_URL`, a separate `facts_runtime` connection in `MEMORY_DATABASE_URL`, and the shared ledger encryption key. Supply the exact profiles and budget configuration used to schedule jobs under `extraction.scheduling`, with one `extraction.gateways` binding per profile. Each binding contains the profile ID, gateway URL, optional `token_env` and optional `root_ca_file`. A configured token variable must be populated. Profile or gateway validation failure stops startup before consumer registration. The worker's principal and each recorded requester need the engine's extraction/proposal, evidence and dependency permissions. Each database pool is bounded by `max_connections`; each profile's gateway transport permits one concurrent connection. Use a new profile ID for profile revisions and retain older profiles/gateway bindings while their jobs remain queued; a digest mismatch stops execution rather than substituting a new prompt or model.

The gateway adapter implements this repository's **facts extraction gateway v1 contract**, not a vendor API. It sends a POST with the exact retained engine request bytes, `Content-Type: application/json`, `X-Facts-Extraction-Protocol: v1`, and an optional bearer token. The gateway must respond with HTTP 200, uncompressed `application/json` candidate bytes, and exactly one of each header: `X-Facts-Model-Identity`, `X-Facts-Input-Tokens`, and `X-Facts-Output-Tokens`. Usage values are nonnegative decimal integers; engine profile limits are checked separately. The returned model identity is retained as reported by the gateway. The gateway owner must enforce the requested model/profile and preserve any vendor-bound request transformations for wire-level audit.

HTTPS with certificate verification and TLS 1.3 is required; system roots are used unless an explicit CA file is provided. Plain HTTP is available only with `allow_plaintext_loopback: true` and a literal loopback IP for development. Redirects, proxy environment routing, compressed responses and malformed metadata are rejected. Profile deadlines bound requests; headers are capped at 16 KiB. Body reads stop at the profile output limit plus one sentinel byte so the engine can record an oversized response and a bounded prefix. Non-200 or malformed transport responses become sanitized provider failures; their error bodies are not retained. The adapter performs no application-level retries. The engine and outbox own durable attempts, quotas, leases and backoff, and provider connections close when the process stops.

The TLS fixture integration executes a scheduled refresh job, checks exact gateway request/response bytes and usage in retained artifacts, and verifies that the resulting proposal does not replace accepted memory without review. No production gateway, model, credentials, bank-domain evaluation or data-processing approval is included. Those remain deployment acceptance requirements owned by the bank and gateway/model operators.

For the encrypted local archive reference mode, use [examples/memoryworker-archive.json](examples/memoryworker-archive.json). Set `MEMORY_WORKER_DATABASE_URL` to a separately provisioned `facts_archiver` login and supply the ledger encryption key, a separate `MEMORY_ARCHIVE_ENCRYPTION_KEY_HEX` (32-byte hex key), `MEMORY_ARCHIVE_PRIVATE_KEY_HEX` (64-byte Ed25519 private key encoded as hex), and optional OpenFGA token. The configured worker principal needs tenant membership and export/history/read/evidence permissions across every scope referenced by the tenant stream.

Pre-create the absolute `archive.directory`; startup never creates it. Replace the example's zero public-key placeholder with the independently provisioned public key matching `archive.key_id`. The explicit `trusted_keys` map supports up to 32 signing key IDs and is required for existing-chain verification; the worker does not infer trust from a key embedded in an archive or silently derive the trust set from its private key. Retain older trusted public keys when rotating the active signer. The integration test restarts with a new signing key, verifies the chain across both keys and confirms that old encrypted object versions remain unchanged.

Archive publication precedes queue acknowledgement. The existing create-once store and signed previous-manifest links make delivery retries reuse matching artifacts. Missing, mismatched or unverifiable previous manifests stop delivery. `archive.VerifyChain` remains the independent verification API. Preserve the archive encryption key and directory across restarts; changing either does not start a valid continuation of the same stream. The binary does not delete archive objects or reset consumer history.

This mode uses ordinary encrypted files and is a reference deployment, **not production WORM storage or a full backup**. A privileged storage operator can delete files or roll back the directory. Production acceptance still requires a separately administered immutable-storage adapter/mount, approved retention and legal-hold enforcement, protected signing/encryption keys, archive-head monitoring, and restore/recovery drills. Scoped audit exports still report that independent archive-chain coverage is not attached.

For `memoryd`, add a `retrieval` array with entries such as:

```json
{"tenant":"bank1","generation":"lexical-v1","database_env":"MEMORY_INDEX_DATABASE_URL","max_connections":4}
```

Each URL must identify a separately provisioned CortexDB database with `EmbeddingID: "none"` and `Dimensions: 1`, using the existing `cortex.Provision` deployment step and restricted runtime grants. The single physical dimension is a storage placeholder; these routes perform lexical search and retain a lexical retrieval identity. Both binaries validate the stored tenant, generation, provider identity, CortexDB version and runtime database role at startup. They never silently reinterpret a semantic index as lexical. Routes are immutable until restart, limited to 32 entries, and bounded to 256 aggregate index connections. Semantic embedding providers remain an external integration requirement.

To enable remote rebuild administration, set `operations_scope` to a host-controlled scope and supply `MEMORY_OPERATIONS_DATABASE_URL` with the separate `facts_rebuild` credentials. The service uses the same routed index and verifies `can_operate` on that configured scope. Provision the route, start the candidate with `RebuildIndex(start)`, run the projection worker, inspect `GetProjectionStatus`, verify the complete inventory, then activate its verified fence. Recall uses the active pointer from the ledger registry. Adding a route alone never activates it. Core readiness continues to describe ledger/authorization availability; projection generation and lag are reported through the operational API.

Integration tests run the packaged service and worker with real PostgreSQL, CortexDB and OpenFGA: an approved fact is projected, the candidate is remotely verified/activated, and mTLS remote recall returns the authoritative version with complete coverage. A separate audit worker completes a queued export, whose downloaded signature and original requester authorization are verified.

## Verification

```sh
make test
make vet
make lint
make vuln
make generate
```

`make generate` uses sqlc v1.31.1, regenerates the OpenFGA JSON model from its restricted DSL, and generates the typed remote API with Buf v1.74.0. The pinned Protobuf generators are protoc-gen-go v1.36.12, protoc-gen-connect-go v2.0.1 and protoc-gen-go-grpc v1.6.2. `buf.lock` pins the protovalidate schema dependency. `make check` checks Protobuf formatting, schema/rule lint and generated-code drift. Run `make tools` and put its installation directory (`$(go env GOPATH)/bin` by default) first on PATH. Goose v3.28.0 is the migration tool. Lint configuration is for golangci-lint v2 (validated with v2.14.0). `make vuln` pins govulncheck v1.9.0 and requires Go 1.26 or newer. The pgx driver is pinned to v5.10.0; its dependency graph no longer includes the earlier vulnerable x/crypto version.

The [CI workflow](.github/workflows/ci.yml) runs on pushes to `master`, pull requests and manual dispatch. It uses Go 1.27.2 from [.go-version](.go-version), Ubuntu 24.04, commit-pinned GitHub Actions, read-only repository permissions and no deployment secrets. Three independent checks run: source/migration/generated-file checks, the full race-enabled integration suite, and reachable Go vulnerability scanning. Duplicate runs for the same branch or pull request are cancelled. Each job uses a distinct cache key; changes to pinned build tools also invalidate the code-check cache.

Reproduce the checks locally from the repository root:

```sh
make tools
export PATH="$(go env GOPATH)/bin:$PATH"
make check
make ci-integration
make vuln
```

`make tools` installs pinned sqlc, goose, golangci-lint and actionlint v1.7.12. `make check` checks Go formatting, module integrity, vet, lint, migration syntax and workflow syntax, then regenerates sqlc/OpenFGA outputs and rejects changed or untracked generated files. Changes to generated outputs must be reviewed and committed with their source changes. Keep the Go/tool versions and digest-pinned service images updated through reviewed changes.

`make check` also requires Python 3. `make ci-integration` requires Docker, curl, and Compose 2.24.4 or newer. It creates its own uniquely named Compose project with ephemeral loopback ports using [compose.ci.yaml](compose.ci.yaml), waits for PostgreSQL/pgvector and OpenFGA readiness, creates fresh databases, and runs every package with the race detector and a ten-minute test timeout. It removes only its own containers, network and anonymous volumes on completion or failure. This works alongside the regular development services. Service logs are printed on failure; the runner contains synthetic test fixtures only. Compose's [`!override` merge behavior](https://docs.docker.com/reference/compose-file/merge/#replace-value) replaces the development port bindings.

The workflow reports checks; it does not configure branch protection, deploy services or publish packages. A repository owner must decide which checks are required for merges and keep runner/action policies enabled. Passing CI does not establish production model quality, restore readiness, workload capacity, external identity integration or regulatory acceptance; those still require the named owners and production evidence described below.

Real PostgreSQL tests require **fresh disposable databases**. They refuse a database containing ledger tables, apply migrations, and exercise the restricted runtime role. They do not drop existing databases.

```sh
docker compose exec postgres createdb -U postgres facts_engine_integration
export TEST_DATABASE_URL='postgres://postgres:development@127.0.0.1:55432/facts_engine_integration?sslmode=disable'
export TEST_OPENFGA_URL='http://127.0.0.1:58080'
go test -race ./... -count=1
```

For the complete suite, start the loopback CortexDB/pgvector service as well and run `scripts/integration.sh`. It creates fresh ledger and projection databases without dropping existing data. Cortex contract tests require `TEST_CORTEX_DATABASE_URL` and `TEST_CORTEX_OTHER_DATABASE_URL`; the end-to-end consumer test requires `TEST_PIPELINE_DATABASE_URL`, and its managed rebuild test requires `TEST_REBUILD_DATABASE_URL`. Each must reference a distinct fresh pgvector database. The script supplies these variables. Without them, the corresponding tests explicitly skip.

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

Verify a retrieved bundle using `adapters/audit.Verify(bundle, trustedKeys)`, where `trustedKeys` is a separately provisioned map of signing key IDs to Ed25519 public keys. Verification checks the signed manifest, ordered entry identities, counts, and payload digests. Preserve the original payload bytes for verification. The encrypted bundle resides in PostgreSQL; its digest does not establish an independently controlled immutable archive or prove coverage beyond the stated scope and revision. Manifests explicitly report missing coordinated retention coverage. Model requests and responses require evidence access and are withheld from metadata-only exports. Archive-chain receipts are not yet attached to scoped audit exports, and that coverage gap remains explicit.

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

Create `cortex.New(ctx, runtimeDB, config, embedder)` with a bank-approved implementation of `model.Embedder`. Embedding calls have a 20-second deadline and validate dimensions and finite nonzero vectors. No embedding service is selected implicitly. Tests use a constant three-dimensional vector solely to establish database and lifecycle behavior, **not retrieval quality**. This initial implementation uses exact vector search; ANN tuning and relevance evaluations remain future work. Index text contains fact values and accepted memory bodies and must receive appropriate database encryption and retention controls from the host.

Use `workers.NewProjection` with a `facts_projector` ledger login, the tenant-bound index, an authorizer, a trusted worker caller, and generation. Register its outbox identity using `model.ProjectionConsumer(generation)`, then run it through `workers.NewOutbox`. A [compiling composition example](workers/example_test.go) shows the lifecycle. The worker indexes approval/acceptance/supersession events using immutable version IDs and policy-safe metadata. Historical accepted versions remain individually addressable. Each scope requires the worker's current `can_read` permission. Lifecycle events advance coverage without deleting historical versions or changing their content.

Compose `factsengine.NewWithRetrieval` with the same generation, index, and a ledger progress reader. `Recall` accepts explicit scopes, query text, valid time, optional knowledge revision, and a result limit. It authorizes scopes, retrieves bounded opaque candidate IDs, resolves them against one ledger view, checks evidence availability, and returns canonical ledger values. Proposed, rejected, disputed, retracted, or otherwise ineligible versions are excluded. Historical recall additionally requires `can_read_history`. Index text is never the returned authority, and no external reranker receives candidates in this implementation.

Recall reports `Generation`, `IndexedRevision`, `LedgerRevision`, and `Incomplete`. `RequireRevision` fails with unavailable until the consumer has acknowledged that revision; it does not wait implicitly. Candidate over-fetch is bounded, so filtering can return fewer results. Projection failure does not block direct ledger reads or writes. A new generation uses a new database and consumer identity and replays from revision one. Use the managed rebuild controller below for verified cutover. Retention/purge orchestration remains separate and is not yet implemented.

Cortex destinations expose the optional `model.ProjectionDisposer` contract through `Index`, `LexicalIndex`, `Router` and `Generations`. `DisposeProjection` targets one explicit tenant, generation, kind (`fact` or `memory`) and version ID. It atomically deletes the embedding row (content, vector and metadata) and write receipt, retaining only an opaque projection-ID tombstone. An absent document can be fenced before a delayed writer reaches it. Identical retries succeed, and a deleted fact does not delete a memory with the same version ID.

Fresh `cortex.Provision` calls install this schema. For existing dedicated index databases, stop writers and call `cortex.ProvisionRetention` once with the migration-owner connection, then refresh [Cortex role grants](scripts/cortex-runtime-role.sql) before restarting. Constructors do not migrate. Projection writers need read access to `facts_projection_tombstones`; use a separately provisioned login belonging to `facts_cortex_retention` for disposal. Ordinary projection credentials cannot delete rows or insert tombstones. A database trigger rejects re-insertion or update of a disposed projection, including attempts from older adapters.

Disposal and projection writes share the destination's per-document lock and rebuild gate. New disposal is rejected while the generation is sealed; an exact retry of an already committed disposal is read-only and can succeed. Delayed embedding work rechecks the tombstone before publishing. **This adapter primitive does not authorize disposal or enforce ledger retention policies/legal holds.** The host must coordinate approved ledger disposal and cleanup across every active, inactive and rebuilding generation. That durable coordination remains unfinished, and no public fact/memory disposal command or cleanup worker is enabled by this change.

Projection reads recognize durable fact/memory disposal tombstones and return no document for those acceptance events. The tombstone must match the event's tenant, scope, record and version and contain only the defined metadata shape; a marker without a disposal row, missing content or malformed disposal metadata remains an integrity failure. This prevents new source reads from rebuilding removed content. It does **not** remove an already indexed copy: durable cleanup across all generations and fences against already loaded writes still belong to the unfinished disposal coordinator.

Rebuild inventories omit validated disposed versions. Paged inventory cursors identify the last scanned acceptance, so a page can contain zero documents and still advance across disposed content. Both scan work and payload bytes remain bounded. The Cortex verifier checks cursor progress and document ordering, including empty pages, and still requires its complete receipt/embedding set to match the retained-document inventory. Residual index copies fail verification. Acceptance revision/fence checks remain in force; the eventual coordinator must also serialize retention changes with rebuild verification and cutover.

Apply migration `00042_projection_destinations.sql` and refresh runtime role grants to retain the destination inventory needed by coordinated disposal. `projection_destinations` binds each tenant/generation to its exact `model.ProjectionConsumer` ID. Migrations recover all existing managed generations, including retired and abandoned ones; hashes of legacy static consumers cannot be reversed and remain explicit inventory gaps. Destination mappings are append-only under worker/rebuild credentials, and ordinary runtime credentials cannot register them.

The standalone projection worker registers its configured destination after registering its consumer and before processing deliveries. Managed rebuild creation registers its destination atomically, and abandonment/retirement preserves that mapping. Embedded hosts using static destinations must call `ledger.RegisterProjectionDestination(ctx, model.GenerationKey{Tenant: tenant, Generation: generation})` after `RegisterConsumer` and before index writes. This host maintenance method has no end-user RPC, creates no physical index, and never resets consumer progress. Repeating a registration is safe.

`ledger.ProjectionDestinationInventory(ctx, tenant)` returns all known destination keys, unmapped projection consumers and the observed epoch in one ledger snapshot, bounded to 10,000 destinations and 10,000 gaps. A nonempty `MissingConsumers` list must block any claim of complete cleanup; reconcile it from trusted deployment configuration. An empty list covers ledger-known consumers only, not unregistered external indexes or caches. The host remains responsible for registering every destination before use. This inventory is neither disposal authorization nor a cleanup-completion receipt.

New projection consumers, destination registrations and managed generation changes advance a shared tenant retention epoch. Rebuild operations acquire that guard before reading generation state. A stale repeatable-read transaction cannot acquire the guard after a concurrent registration/change; the future disposal coordinator must use the same guard when selecting targets and coordinating cutover. This adds the inventory and serialization boundary; durable cleanup plans, policy-authorized fact/memory removal and cross-store completion tracking remain unfinished.

Migration `00043_projection_cleanup_obligations.sql` creates an immutable cleanup job and its complete registered-generation target set whenever a fact/memory tombstone is inserted. They commit or roll back with the tombstone. Capture uses the shared retention epoch and rejects unmapped projection consumers, so a stale disposal transaction cannot miss a concurrently registered destination. Retired and abandoned generations are included. Jobs bind to their fact or memory tombstone; targets bind to registered destinations, and acknowledgements bind to individual targets. No cleanup is acknowledged merely by removing ledger content.

The migration also records current known destinations for pre-existing tombstones. If those tenants have unmapped static consumers, migration fails: first reconcile generation names from trusted deployment configuration and register the exact bindings using the migration-owner connection (or the migration-42 maintenance tooling), then retry. Do not infer names from consumer hashes. The upgraded application requires migration 43 before starting. The migration does not validate historical external-index coverage or erase any index bytes.

`ledger.ProjectionCleanupStatus(ctx, model.ProjectionCleanupKey{Tenant: tenant, Kind: "fact", VersionID: version})` is a trusted maintenance reader. It returns the captured inventory epoch, creation time and each destination's acknowledgement/time. `Complete` means all captured destinations have acknowledgements; an empty target set covers no ledger-known index copies. It is not an independently verified index-state proof, a policy authorization or an overall erasure claim. Missing jobs fail explicitly, and a target-count mismatch is an integrity failure. New destinations registered after disposal do not rewrite that historical target set; hosts must register destinations before writing, and new projections already skip validated disposed content.

Ordinary runtime, worker and retention roles can read cleanup metadata but cannot insert, alter or delete jobs, targets or acknowledgements. No acknowledgement-writing worker or public fact/memory disposal command is enabled yet. Tests use owner-only storage transitions to exercise migration/backfill, rejection of incomplete inventories, rollback, concurrent registration, per-kind isolation and partial/completed acknowledgement coverage. Execution, fresh policy/hold checks, handling sealed generations and auditable cross-store completion remain coordinator work.

## Managed index rebuilds and cutover

Apply ledger migration `00007_projection_rebuilds.sql` and reapply `scripts/runtime-role.sql`. Rebuild control uses the separate `facts_rebuild` administrative login; ordinary engine runtime credentials can read the active pointer but cannot change rebuild state. Supply a trusted `model.RebuildActor` principal and purpose. The host authorizes this operator before invoking the administrative port. Each state transition retains an encrypted append-only operational receipt. These receipts are separate from business ledger revisions and are not yet included in scoped audit exports.

For existing projection databases, explicitly apply [scripts/cortex-rebuild-schema.sql](scripts/cortex-rebuild-schema.sql) with the migration owner, then reapply the projection runtime grants. Fresh `cortex.Provision` creates the gate automatically. The gate serializes against destination writes; its monotonic fence prevents a delayed controller from reopening a newer sealed state. Projection runtime credentials remain trusted backend credentials, not end-user access.

Provision the next tenant/generation database, create its `cortex.Index`, and build a fixed `cortex.NewGenerations(oldIndex, newIndex, ...)` routing table. Keep the old generation reachable until retirement cleanup completes. Compose `workers.NewRebuild(rebuildLedger, routes, actor)` and use this lifecycle:

1. `Start(ctx, key, maxAttempts)` durably registers the candidate and its consumer. It records the active epoch from which this candidate may cut over. Run the normal projection worker for the new generation to replay from revision one; the existing active generation keeps serving.
2. `Verify(ctx, key)` requires the candidate's acknowledged offset to equal the current ledger revision and no delivery to be running. It pauses/fences the consumer, seals destination writes, inventories historical accepted versions through that revision, and checks every expected receipt and physical index row. Counts, canonical content, metadata, and vector shape must match. Missing or extra rows fail. The verified digest and count are retained. The PostgreSQL/Cortex path verifies bounded pages and retains a versioned incremental inventory digest, so it does not load the entire inventory or impose the former 10,000-document/64 MiB total limit. It does not establish embedding relevance or reproduce model outputs.
3. `Activate(ctx, key, verified.Fence)` checks the captured active epoch and verified source revision under a short PostgreSQL barrier. If another business write committed since verification, activation fails without switching readers. Call `Resume(ctx, key, verified.Fence)`, catch up, verify again, and retry. This design needs a quiet interval long enough for verification; it never holds a ledger write lock across external work.
4. Successful activation atomically moves the active pointer, retires/fences the old consumer, and retains an activation receipt. The controller then seals the old destination, opens the new destination, and resumes its consumer. If this cleanup fails after the switch, state remains `activating`; repeat `Activate` with the original verified fence to finish. The already-verified new index can serve reads while cleanup is pending, and subsequent writes are reported as indexing lag.

Apply migration 00028 before using paged verification. The PostgreSQL source implements `model.PagedRebuildStore`: each page is bound to the frozen tenant/generation, fence and `ThroughRevision`, ordered by record kind and version ID using bytewise collation. Cursor queries have supporting indexes. Pages contain at most 256 documents and 8 MiB of canonical document payloads; the controller requests 100 documents at a time. No page silently drops an over-limit document. Concurrent ledger writes do not change earlier immutable acceptance content, and activation still rejects a source revision that has advanced.

Cortex and its generation router implement `model.PagedRebuildIndex`. Verification holds the sealed destination gate and a repeatable-read snapshot across all pages. It checks exact receipts, canonical bodies, metadata and vector shape, rejects repeated/out-of-order pages, and compares the final unique document count with both physical embeddings and receipts. Missing rows, extra rows, corrupt late pages, loader errors and cancelled scans fail verification. The `projection-inventory-v2` digest hashes a domain-separated, length-delimited sequence of canonical JSON documents in kind/version order; `RebuildStatus.VerificationVersion` identifies that contract. Memory use is bounded by the page, though verification still examines every document and retains a database snapshot until it finishes. Hosts must set an appropriate context deadline and benchmark the source queries, connection use and required quiet interval for their workload.

Custom adapters can implement the optional paged ports. If either side lacks them, the controller retains the legacy path limited to 10,000 documents and 64 MiB; it cannot downgrade a v2 proof to the legacy algorithm. An already-verified legacy generation keeps its original digest contract when verification is retried. Resume and freeze it again to create a v2 proof. Upgrade all rebuild controllers before producing v2 receipts; older controllers do not preserve the algorithm-version field. Verification failures do not reopen the destination. Retries scan from the beginning; durable mid-scan checkpoints are not implemented. Cutover fencing and reader revalidation are unchanged.

Tests cover a destination with more than 10,000 documents and 64 MiB of canonical payloads, multi-page fact/memory source coverage, stable proof retries, legacy-proof upgrade, late-page corruption, duplicate pages, extra rows and gate locking. These are correctness tests, not a production capacity benchmark.

`Resume` uses a durable `resuming` state so failures between opening the destination and resuming delivery can be retried. `Abandon(ctx, key, currentFence)` fences an unfinished candidate and seals its destination without deleting data. Active or retired generations cannot be abandoned or reopened. A candidate whose captured active epoch has lost to another cutover must be abandoned and rebuilt under a new generation; stale verification never overwrites a newer active pointer. Retired databases are retained for host-governed cleanup, not automatically dropped or used for rollback.

For managed reads, set `RetrievalOptions.Registry` to the ledger, `Index` to the generation routing table, and leave `Generation` empty. Each recall pins one active generation/epoch and rechecks it before returning; a request crossing a switch fails unavailable and can be retried. No registered active generation also fails unavailable. Static `Generation` configuration remains available for explicitly pinned integrations; it does not participate in managed retirement, so all serving hosts must use registry-based routing to receive the cutover guarantee. As with any in-flight response, a switch after the final check cannot revoke bytes already being returned.

Real PostgreSQL/pgvector tests cover corrupted canonical content, extra rows, lagging consumers, a ledger write after verification, stale verification/gate tokens, lost activation responses, an in-flight old-generation worker, cutover during recall, failure after pointer switch, runtime-role isolation, tenant isolation, and abandoning an unfinished candidate.

## Signed event archive consumers

Use `workers.NewArchive` with a `facts_archiver` ledger login, a `model.ArchiveStore`, bank-owned signer, independently configured trusted verification keys, an authorizer, and a stable stream ID. Its consumer identity is `model.ArchiveConsumer(stream)`. Archive workers require export, history, fact, and evidence permissions on every referenced scope, including multi-scope snapshots. Signing keys must remain verifiable after rotation.

Each event produces an encrypted payload object and a signed one-event batch manifest. The manifest binds tenant, stream, exact revision/count, payload digest and immutable object version, authorization checks, and the previous manifest's object reference. A retry after an external write but before acknowledgement validates and reuses the existing object. The archive source retains the full event envelope and supported immutable references: fact versions, predicates/policies associated with those versions, evidence metadata, timeline generations, attestations, snapshots, calculation receipts, and completed audit bundles. New predicate-publication events retain their predicate identity for exact reconstruction; legacy events lacking that identity carry an explicit gap. Evidence bytes are not duplicated; exact source locators are retained. These objects are **not a complete database backup**, and payload gaps say so.

`archive.NewFiles` supplies an encrypted local development implementation. Create its directory explicitly, inject an archive-specific cipher/key, and retain that key separately. Atomic create-once publication, fsync, digest checks, and tenant-bound decryption make ordinary retries safe. Filesystem ownership alone does not establish WORM protection: production hosts must use separately administered immutable storage with appropriate retention controls or implement `model.ArchiveStore` for their approved object service. Local tests do not establish protection against a privileged storage administrator.

Retain the archive head object reference and expected revision independently, then call `archive.VerifyChain`. Verification checks signatures, exact object versions, payload hashes, contiguous revision coverage, and predecessor links up to an explicit batch bound. Missing or tampered batches fail. An independently retained head is necessary to detect tail truncation; a mutable database offset alone is insufficient. Archive and projection offsets remain independent. Archive heads are not yet included in scoped `GetAuditExport` bundles. The disposable logical restore and fresh index rebuild drill is implemented; production recovery and coordinated retention drills remain pending.

For the encrypted local archive adapter, `make build` also produces `bin/memoryverify`. Run `bin/memoryverify -config /absolute/path/verification.json` with `MEMORY_ARCHIVE_ENCRYPTION_KEY_HEX` supplied through the environment. The command only reads archive objects; it needs no database credentials, signing private key or running worker. Its configuration is bounded to 1 MiB and rejects unknown fields and trailing JSON:

```json
{
  "directory": "/srv/facts/archive",
  "tenant": "bank1",
  "stream": "ledger-v1",
  "head": {
    "Locator": "<64 lowercase hex characters>",
    "Version": "<64 lowercase hex characters>",
    "Digest": "<64 lowercase hex characters>",
    "Size": 1234
  },
  "expected_revision": 100,
  "max_batches": 100,
  "timeout_seconds": 60,
  "trusted_keys": {"archive-key-v1": "<32-byte Ed25519 public key in hex>"}
}
```

Replace every placeholder and the illustrative size/revision with the independently retained head receipt and approved trust configuration. Keep every trusted signing key needed across rotation. Do not derive the expected head from the directory being checked: an attacker could remove its tail. The expected revision must be positive and no greater than `max_batches` (1–1,000,000); timeout is explicit (1–3,600 seconds). Cancellation and deadlines are checked between archive operations; synchronous local filesystem reads must return before cancellation can be observed.

Exit 0 means the entire chain through the supplied head verified; exit 1 means configuration, key, storage, verification, cancellation or report-output failure; exit 2 means invalid command usage. Failures emit a generic diagnostic to stderr. Verification failures emit no success report; an output-write failure may leave a partial report, so always check the exit status. Success emits one JSON report containing the tenant, stream, exact head, verified revision, and a digest of the parsed configuration (including public trust keys but no encryption secret). The report is unsigned operational evidence: retain it with the configuration, verifier build/commit and independently held anchor. It does not prove immutable storage, freshness beyond the supplied anchor, bank approval or scoped export coverage. Tests exercise actual encrypted files and signed two-key chains, including missing predecessors, tail truncation, incorrect trust/identity, wrong encryption keys and cancellation.


## Registered redacted evidence

Apply migrations 00026–00027 and refresh `scripts/runtime-role.sql` before upgrading the PostgreSQL adapter. `RegisterRedactedEvidence` registers an immutable omission-only derivative of existing UTF-8 `text/plain` or `text/markdown` evidence. Configure the deployment-approved version IDs in `EngineOptions.RedactionPolicyVersions` (at most 64); redacted registration is disabled by default. The caller needs proposal and original evidence-read permissions. Approval of a version ID does not detect sensitive data or verify the host's choice of removed ranges; a named security/domain owner must approve the external redaction rules and evaluations.

The host first finalizes the derivative object in its evidence store, then supplies `model.RedactedEvidenceRequest` with `ID`, `Scope`, `ParentEvidenceID`, `PolicyVersion`, the immutable `Object` reference, and up to 256 `EvidenceCopyRange` entries. Each range specifies `OriginalStart`, `OriginalEnd` and `DerivedStart`, using half-open UTF-8 byte offsets. Original ranges must be ordered and nonoverlapping; derivative offsets must cover the whole derivative contiguously. The engine verifies both objects, UTF-8 boundaries and exact equality of every copied segment. Empty ranges are allowed only for an empty derivative. No replacement, normalization, OCR or nested derivation is supported by `utf8-omit-v1`.

The derivative retains its parent ID, transformation/policy versions and offset map in `Evidence.Derivation`. It inherits the parent's source system, speaker, source authority, source time, media type, classification and retention policy. This API cannot downgrade classification. Direct `RegisterEvidence` requests cannot set derivation metadata. A derivative remains in the same tenant and scope as its original; its creation, command receipt and outbox event commit atomically. Registration is idempotent under the usual command contract, and changed mappings require a new derivative identity.

Fact and memory proposals may cite a derivative only within one copied segment. A quote spanning a removed section is rejected even when its bytes match the concatenated derivative. `EvidenceDerivation.OriginalSpan` maps a valid segment citation back to its parent evidence and offsets. Both original and derivative metadata are included in event archives. A retained derivative blocks single-object original-byte disposal through the `evidence_derivatives` retention reference. Atomic disposal of an original with its complete, explicitly selected set of derivatives is available as described below. Custom ledger adapters must preserve this parent reference and serialize registration against parent disposal.

For atomic PostgreSQL lineage disposal, set `EvidenceDisposalRequest.DerivativeEvidenceIDs` (RPC `derivative_evidence_ids`) to the complete list of direct derivative IDs, up to 256. The original and every selected derivative must be in the requested scope and satisfy the same retention policy, purpose, jurisdiction and minimum age. The service rejects duplicate IDs, the original in its own derivative list, unrelated evidence, incomplete selection and unsupported adapters. Derivative chains are not supported by registration. An empty list preserves the existing single-object behavior. Upgrade handlers and adapters before using the new field.

The transaction checks every member before deleting any bytes, retains all structural metadata and tombstones, and appends one retention event containing the exact selected IDs. Event archives include the original and derivative metadata with their exact object locators and digests. A failed ledger append rolls back every deletion. Scope locks and repeatable-read retries serialize concurrent legal holds, references and derivative registration; a newly published derivative makes an incomplete selection fail. The same command can be retried through embedded or RPC access; changed membership under the same command key conflicts. Custom transaction adapters must explicitly implement `model.EvidenceLineageDisposalTx`; an older adapter cannot silently dispose only the original.

This operation bypasses the original's derivative-reference blocker only after validating complete membership. It also permits a stored object shared exclusively by the selected original and derivatives: every alias must be selected, every selected identity must pass the same policy/hold/reference checks, and each distinct locator/version is deleted once. An unselected alias, including one in another scope, blocks the entire operation. Object epoch locks prevent a concurrent alias registration from escaping those checks. Unrelated evidence cannot be added to the derivative list to bypass this boundary. Fact/memory links, conversations, queued extraction, attestations, calculation/decision artifacts, export copies and legal holds remain blockers on every member. The original must still have live bytes. Already disposed derivatives must still be explicitly selected and pass policy/hold checks; their tombstones are preserved without a second deletion. They still block ordinary single-object parent disposal. Exports report disposed payloads as unavailable, while archives/backups expire under their separate policies. Coordinated disposal of referenced payloads, aliases outside one derivative family, index/cache copies and external stores remains unfinished.


Upgrade all writers before enabling redacted registration: older proposal writers do not enforce gap-boundary checks. Conversation extraction can require these derivatives using the profile setting described below. No production redaction policy or detector is configured by this repository.

Tests cover exact copied bytes, malformed mappings, UTF-8 boundaries, policy/permission checks, retries, inherited classification/authority, parent retention protection, archive ancestry, and fact/memory rejection of citations across removed text.

## PostgreSQL query generation

`make generate` compiles the query sources against the migration schema with sqlc. Generated code lives in `gen/ledgerdb`; CI checks that regeneration produces no differences.

| Query source | Converted operations |
| --- | --- |
| `sql/queries/ledger.sql` | Tenant binding and revision allocation, record locks/revisions, event append, current attestations, active predicates and outbox insertion |
| `sql/queries/identities.sql` | Scope and subject history, revision locks, exact lookups, registration guards |
| `sql/queries/commands.sql` | Command receipts, policy/predicate publication, evidence and fact-version registration, conversations, confirmation artifacts |
| `sql/queries/memories.sql` | Memory definitions, proposals, evidence/dependency links, attestations |
| `sql/queries/temporal.sql` | Fact and memory timelines, rejection checks, historical attestations, revision reads, snapshots |
| `sql/queries/context_audit.sql` | Context manifests, decision/calculation receipts, audit-export requests, claims and fenced completion |
| `sql/queries/extraction.sql` | Extraction jobs, attempts/results, proposals, redaction/dependency bindings, retry/lease fencing and tenant token/cost reservations |
| `sql/queries/reads.sql` | Known-at revision lookup, fact/history pagination, immutable event lookup and memory-refresh dependent discovery |
| `sql/queries/outbox.sql` | Consumer registration, delivery claims/renewals/acknowledgements/failures, operator retries and scoped change feeds |
| `sql/queries/rebuild.sql` | Generation state, management locks, consumer pause/resume, active-generation cutover and legacy/paged verification inventories |
| `sql/queries/retention.sql` | Retention locks/policies, legal holds, reference blockers, age checks and atomic unreferenced-evidence disposal |
| `sql/queries/archive.sql` | Revision-bound event archives and scope-qualified audit enumeration, including retained artifact links |
| `sql/queries/evidence.sql` | Encrypted development evidence-object storage and reads |
| `sql/queries/runtime.sql` | Runtime role safety and migration readiness checks against explicitly qualified PostgreSQL catalogs |

Streaming readers use exported sqlc query constants to preserve incremental decoding and existing count/byte limits. The shared outbox update path uses generated constants for complete fencing statements. Typed writes retain encrypted payloads, explicit tenant predicates and transaction-owned locks. Ledger and PostgreSQL evidence-store SQL is now centralized in these sources. Adapter orchestration still owns encryption, streaming limits, transaction ordering and authorization-related checks. CortexDB index SQL uses its separate upstream-managed schema and is outside this ledger query set.

## Remaining delivery work

Follow [spec.md](spec.md). The implementation and production acceptance are still incomplete; the API may change before v1. The remaining work is:

1. **Coordinated payload disposal.** Cover referenced fact/memory content, conversations, model/calculation/decision artifacts and their index/cache copies. Evidence, explicitly selected derivative families, orphan uploads, eligible snapshot/context payloads and terminal extraction artifacts already have governed disposal; they do not establish complete erasure.
2. **Archive and recovery coverage.** Attach appropriate independent archive-chain evidence to scoped audit exports without disclosing other scopes. Integrate independently administered immutable storage and production object-version retention. Signed event archives, the standalone archive verifier and a disposable logical restore with retained-row comparison, decrypted evidence checks and a fresh search rebuild are implemented. Production key recovery, backup expiry, PITR and approved RPO/RTO drills remain deployment work.
3. **Extraction and redaction.** Complete range-based redaction within dependency payloads and refresh fields. Approved whole-field omission, conversation derivative substitution, deterministic chunk planning, durable fact/memory extraction and refresh scheduling/sweeps are implemented. Integrate an approved provider and run evaluations against bank-approved ground truth; the offline scorer and synthetic fixtures do not establish model acceptance.
4. **Deployment identity and operations.** Provision production gateway/session validation, delegated identity authority, certificates, authorization policy and managed encryption/signing keys. The mTLS service, trusted gateway assertion path, HTTPS authority adapter, runtime containers and standalone worker modes are implemented. Deployment configuration, supervision, telemetry collection/alerts and owner sign-off remain external requirements.
5. **Acceptance coverage and workload tuning.** Reconcile every specification acceptance criterion with tests and deployment evidence; the existing embedded/Connect/gRPC suites cover many operations but do not prove every operation/error combination. Run approved capacity, retrieval-quality and contention benchmarks, tune indexing for the actual workload, and close remaining telemetry gaps.

All seven standalone worker modes are available: `projection`, `audit`, `archive`, `refresh`, `refresh-sweep`, `extraction` and `orphan-cleanup`. Each runs as a separately configured process; the engine constructor and service do not start them automatically. The [worker instructions](#standalone-workers-and-lexical-retrieval) and [retention instructions](#retention-governance-and-evidence-disposal) describe their credentials and prerequisites.

The integration scenario now preserves an external calculation on snapshot A through later fact corrections and verifies its signed audit export. The full specification remains incomplete. CortexDB and encrypted local archive adapters are exercised in integration tests. Remote fact, memory, snapshot, evidence, definition, attestation, ingestion, extraction, calculation, audit, retrieval, context/decision and scope/subject administration and projection operations handlers are available for host integration; a local standalone workload service is available, but no remote service deployment, production gateway identity verifier, production embedding/extraction model, independently administered immutable archive, or production deployment has been configured. The engine does not implement wealth allocation, profile completeness, or any business decision algorithm.

Production acceptance also requires the named domain, security, retention, and operational owners described in the spec, plus workload benchmarks and restore drills. Database roles protect normal runtime operations; they do not protect against a malicious database owner. Retained digests are integrity metadata, not anonymization.

The following deployment acceptance gates require external infrastructure or a named accountable owner. Repository tests do not establish these approvals; no owner assignment or production sign-off is recorded here.

| Required owner | Acceptance evidence required before production |
| --- | --- |
| Domain/application owner | Approved versioned schemas, acceptance/review policies, allowed principals and source classes, and domain validation/confirmation rules tested with the consuming application. |
| Model/provider owner | Approved gateway/provider endpoints and model identities, data-processing approval, extraction evaluation results on bank-approved datasets, and approved resource/cost budgets. |
| Identity/security owner | Production identity-to-tenant mapping, service and delegated-user trust configuration, authorization model rollout, certificate issuance/rotation/revocation procedures, and managed signing/encryption key custody and recovery. |
| Retention/legal owner | Approved purpose/retention schedules, legal-hold handling, and evidence that disposal covers derivative payloads, indexes, caches, object versions, archives and backups as applicable. Coordinated disposal remains implementation work. |
| Archive/infrastructure owner | Separately administered immutable storage and signing keys, verified chain/head monitoring and missing-batch detection. The local reference archive does not establish production immutability. |
| Operations owner | Provisioned runtime services, telemetry collection/alerts, approved capacity/latency/RPO/RTO targets, workload benchmarks including append-lock contention, and recorded ledger/evidence/key restore plus search-rebuild drills. |



### Context manifests and decision receipts

Apply migration 00008, refresh runtime/archive grants with `scripts/runtime-role.sql`, and republish the OpenFGA model (including `can_prepare_context`). `PrepareContext` uses the configured retrieval adapter and persists the ordered canonical facts, exact JSON bytes and digest, query, selection/index revisions, retrieval identity, redaction version, caller and authorization checks before returning any context. The default format is `facts-context-json-v1`, with an 8 MiB rendered limit; it supplies facts without evidence quotes. Set `IncludeMemories: true` for the mixed format described below. Custom retrieval adapters must implement `model.RetrievalDescriber`. Repeating a committed command returns its original manifest even if retrieval is unavailable. `GetContextManifest` rechecks current scope permissions without rerunning fact eligibility.

`RecordDecision` accepts exactly one context or snapshot ID, application/decision identity, ordered selected and used version references, method identity, transformation version, final input/output evidence references, outcome and decision time. Used items must be selected, and selected items must occur in the retained source. Model methods also require registered model request/response artifacts; rule methods retain a rule ID/version. An external action artifact is optional. A calculation link requires the same snapshot source. Authenticated callers supply executor/initiator provenance. The engine verifies registered artifact digests and bytes at recording time; later reads authorize retained references without requiring those bytes to remain available.

A tenant/application/decision identity is immutable: identical requests from the same caller converge across command keys, while changed payloads conflict. Decision receipts describe the host's declared use, not independent proof of external execution. Contexts and decisions participate in ledger/outbox ordering, encrypted storage, scoped signed audit exports and signed event archives. Exports withhold records that span other scopes. Coordinated disposal of referenced payloads and production recovery drills remain outstanding; unreferenced evidence lineage disposal is supported as described below.


Prepared-context payloads use a separate `context_payloads` table from migration `00033_context_payloads.sql`. Stop application writers, apply the migration, refresh `scripts/runtime-role.sql`, and upgrade readers and workers together. Existing ciphertext moves without decryption or re-encoding. Context bytes, structural manifests, scope and fact/memory version inventories, and the ledger event still commit atomically. Runtime credentials can insert/read content but cannot update, delete or truncate it; tenant RLS covers the new table. Reads, decisions, audits and archives resolve the retained payload, and missing bytes fail closed without silently removing the manifest from audit/archive scans. Governed context disposal is available with migration `00034_context_disposal.sql`, described below; migration 33 alone does not grant deletion rights.

Snapshot content is stored separately from the immutable identity, digest, ordering and source inventory. Apply `00031_snapshot_payloads.sql` with application writers stopped, then refresh `scripts/runtime-role.sql` and upgrade readers/workers together. The forward-only migration moves existing encrypted snapshot bytes without decrypting or re-encoding them. New snapshot content, structural rows, source items and the ledger event commit in one transaction. Runtime credentials may insert/read content but cannot update, delete or truncate it; tenant RLS also covers the content table. Snapshot reads, calculations, decisions, audit exports and archives resolve the same retained bytes. Missing content fails closed, and archive/audit scans retain the structural row rather than silently skipping it. Apply migration `00032_snapshot_disposal.sql` and refresh grants for governed snapshot disposal. Upgrade all calculation/decision writers and archive workers before enabling it: older adapters do not take the snapshot guards.


Fact and memory version content is separated from structural version rows by forward-only migration `00035_version_payloads.sql`. Stop writers, apply the migration, refresh `scripts/runtime-role.sql`, and upgrade **all readers, projection/rebuild workers and archive workers** together before resuming. The migration moves existing ciphertext intact into `fact_version_payloads` and `memory_version_payloads`; it does not decrypt or re-encode values, memory bodies or copied source quotes.

New version headers, encrypted payloads, source/dependency inventories and ledger events still commit atomically. Runtime credentials can insert/read payloads but cannot update, delete or truncate them. Projectors and archivers receive read access, and tenant RLS covers both tables. Single-version reads and revision-bound history fail closed when a structural version has missing content; audit/archive queries preserve that row so missing bytes cannot silently become complete coverage. This storage migration does not authorize fact or memory disposal. Coordinated removal of their dependent copies, projections, model artifacts and retained references remains implementation work.

Fact and memory versions now have an optional `PayloadDisposed` marker and deterministic `model.VersionTombstone` / `model.MemoryVersionTombstone` helpers. These remove typed values, memory bodies and source quotes while retaining original digests, source locators, dependency assertion/uncertainty labels and governance provenance. Unmarked legacy JSON and fact content digests retain their existing encoding. PostgreSQL single-version and historical readers reject marked envelopes with a precondition error, even if content remains in the envelope.

Archive retries can recognize only the exact full-version-to-tombstone transition, with a matching `version_content` or `memory_version_content` disposal gap. They validate the original content digest and event binding, preserve every other field and gap, and verify the previously stored signature and bytes before retaining the original immutable archive. This is preparatory support: no fact/memory disposal command or coordinated cleanup workflow is enabled yet. Archives remain subject to their own retention policy.

Apply migration `00036_version_disposal_storage.sql` and refresh runtime roles for durable fact/memory tombstone storage. Each kind has a tenant-isolated encrypted disposal table and a per-version guard. Payload insertion/update and tombstone insertion share that guard; committed tombstones reject payload resurrection. Runtime, projector and archive identities can read disposal metadata but cannot insert, alter or delete it. This migration deliberately grants no version-disposal writes to the retention role until the coordinated policy command is implemented.

Version queries prefer tombstones over live payloads, so an interrupted or invalid state retaining both cannot disclose content through normal reads. Archive readers lock the version guard and retain metadata with explicit disposal gaps; scoped exports include same-scope metadata without source content and continue withholding memory metadata with dependencies outside the export scope. Marked envelopes containing values, bodies or quotes are rejected. Signed archive retries preserve the original immutable bytes. Projection replay and rebuild verification still fail closed on disposed versions; treating disposal as completed requires the pending coordinator to account for every destination, retained reference and policy/hold decision. Upgrade all writers and archive workers before enabling any future disposal workflow.

Migration `00037_version_reference_guards.sql` fences new payload-bearing references against version disposal. Database triggers cover fact snapshot items, fact/memory context and decision items, fact/memory attestations, both sides of memory dependencies, extraction fact dependencies, and refresh-job source memories. Inserts and changes to their tenant/version keys require retained source content and no disposal tombstone. Nullable snapshot resolutions and non-refresh extraction jobs remain valid. Unrelated job status updates and structural timelines/events are not treated as new payload references.

These checks share the per-version epoch lock with tombstone insertion. A writer using an older repeatable-read snapshot is aborted if disposal has committed, and a fresh writer is rejected. The eventual disposal coordinator must acquire the same guard and inspect retained references before removal; the triggers do not authorize disposal or remove existing derivatives. Integration tests exercise all guarded insert paths, stale-snapshot rejection, and the existing live workflows. Apply this migration to every writer's ledger before enabling future coordinated disposal.

Migration `00038_extraction_input_inventory.sql` adds append-only, tenant-isolated inventories for retained extraction attempt inputs. The PostgreSQL adapter derives the fact and memory version references from the exact request bytes, verifies their request digest and tenant/job/attempt bindings, and records the inventory in the same transaction as the attempt, admission reservation and event. An inventory receipt is stored even when a request contains no version references. Repeated version references are deduplicated; a copied payload takes precedence over an omitted occurrence.

Inventories distinguish retained proposal payloads from version metadata left by approved whole-field omission. Copied inputs acquire the same database guard used by version disposal before the attempt can commit and reach the provider. Redacted metadata retains version identity without being counted as a copied body. Refresh inventories include the original memory and the current fact versions actually supplied, which can differ from the memory's original dependencies. Result artifacts remain linked to their input attempt.

Existing encrypted attempts are not silently backfilled by this migration: a missing `extraction_attempt_inventories` row is an explicit inventory gap, detectable by the generated `HasUninventoriedExtractionAttempts` query. The pending disposal coordinator must fail closed on those gaps until the maintenance backfill below has completed and its results are verified. The new inventory supplies dependency evidence; it does not authorize deletion, remove model inputs/results or establish complete derivative disposal. Upgrade every extraction writer and refresh runtime grants before relying on these inventories.

For existing installations, the PostgreSQL adapter exposes `ledger.BackfillExtractionInputInventories(ctx, tenant, limit)` as a host-authorized maintenance operation. It is not a public RPC or an engine disposal command. Upgrade and stop legacy extraction writers first, supply the correct tenant decryption keys and inventory write credentials, and invoke it separately for each authorized tenant. Each call reads at most 1–100 missing attempts, bounded by 64 MiB of ciphertext; use a smaller page if that byte limit is exceeded.

```go
for {
    page, err := ledger.BackfillExtractionInputInventories(ctx, tenant, 25)
    if err != nil {
        return err
    }
    if page.Complete {
        break
    }
}
```

A page validates the retained envelope against its database identity, verifies the request digest and input bindings, and derives references using the same rules as new attempts. It commits all inventories in the page together, returns a committed `Inventoried` count, and never rewrites retained request bytes or completed inventories. Missing source content, undecryptable/malformed input, and partial inventory rows fail the whole page. Partial rows require explicit operator investigation; the backfill does not overwrite them or infer completeness. Per-tenant maintenance locks serialize simultaneous callers. Repeat calls resume from remaining missing receipts, so no external cursor is needed. `Complete` means no missing inventory receipts were observed at the final query; it does not certify policy eligibility, every retained derivative or production erasure.

Migration `00039_extraction_inventory_seals.sql` closes each extraction inventory when its receipt is inserted. Fact and memory reference inserts and receipt finalization share a tenant/attempt epoch lock. Finalization checks both stored counts against the actual reference rows; subsequent inserts are rejected, including `ON CONFLICT DO NOTHING` retries and stale repeatable-read writers. Normal command retries reuse the already committed attempt instead of appending to its inventory. Runtime credentials cannot update/delete reference rows or receipts, or delete guard rows.

The migration validates all existing receipt counts as the migration owner and aborts if they disagree with retained references. Investigate inconsistent inventories against the retained encrypted requests before upgrading; do not guess counts or claim complete coverage. Missing legacy receipts remain permitted and can use the bounded backfill. Refresh runtime role grants for the new guard table. These constraints protect inventory membership and counts under normal runtime privileges; matching them does not independently prove the request digest or authorize disposal.

Migration `00040_extraction_payloads.sql` separates encrypted extraction job, attempt and result content from immutable headers into `extraction_job_payloads`, `extraction_attempt_payloads` and `extraction_result_payloads`. Existing ciphertext moves unchanged, and new headers, payloads, admission state, input inventories and ledger events still commit atomically. Runtime roles can insert/read these payloads but cannot update, delete or truncate them; archives receive read access and tenant RLS applies to every new table.

Stop writers and upgrade service instances, extraction workers, audit/archive workers and maintenance tools together, then refresh runtime grants. Old queries that read `extraction_jobs.payload`, `extraction_attempts.payload` or `extraction_results.payload` no longer work. Typed reads and inventory backfill fail explicitly when a retained header has missing bytes. Audit/archive queries use left joins so a missing artifact cannot silently disappear from coverage. This migration preserves retained prompts, responses and job dependency copies; it does not grant deletion rights or enable model-artifact disposal. Use the governed terminal-job disposal command below after applying migration 41 and refreshing retention-role grants.

Extraction artifacts have optional `PayloadDisposed` markers and deterministic `model.ExtractionJobTombstone`, `model.ExtractionAttemptTombstone` and `model.ExtractionResultTombstone` helpers. Job metadata removes profile prompt/parameter bytes, copied schema constraints and dependency fact values/source quotes. Attempt/result metadata removes request/response bytes, including retained response prefixes. IDs, scope inventories, original digests, profile/schema bindings, admission reservations, proposal references and issue codes remain. Helpers do not mutate their inputs or authorize disposal; unmarked legacy encodings are unchanged.

PostgreSQL artifact readers reject marked envelopes even if content remains, inventory backfill refuses marked requests, and service execution/disclosure paths reject disposed jobs or attempts before provider use or return. Archive retries accept only the exact metadata transition with the corresponding `extraction_job_content`, `extraction_attempt_content` or `extraction_result_content` gap, retaining the original signed archive bytes. Complete response digests are checked; a truncated response retains its original full-response digest and incomplete-capture flag without pretending the prefix verifies that digest. `DisposeExtraction` uses the durable storage and policy checks described below.

Migration `00041_extraction_disposal_storage.sql` adds encrypted tombstone tables for extraction jobs, attempts and results, protected by tenant RLS and a shared per-job content guard. Reads prefer disposal metadata over live bytes, and normal typed APIs continue to reject disposed artifacts. Payload inserts/updates, new attempts/results and job transitions back to pending/running are fenced against disposed source content. Archive readers acquire the same epoch guard so stale transactions cannot publish an inconsistent content view.

Archive/export paths validate that marked metadata contains no retained model content, preserve scope filtering, and emit explicit artifact-content disposal gaps. Signed archive retries retain the existing immutable bytes. Runtime and archive roles can read tombstones but cannot insert or modify them. Refresh `scripts/runtime-role.sql` to grant the separate retention role tombstone inserts and payload deletes. Storage tests exercise rollback, stale writers, resurrection rejection, archive replay and signed exports. Upgrade service and worker readers/writers together before enabling disposal.

`Engine.DisposeExtraction` and the matching Connect/gRPC operation dispose one terminal (`completed` or `failed`) job together with **all** retained attempts and results, including earlier manual retries. Supply a reason and one `ExtractionRetentionBinding` per content scope, selecting `JobPolicyID`, `RequestPolicyID`, `ResponsePolicyID` and jurisdiction. Each scope needs independently registered `extraction_job_payload`, `extraction_request_payload` and `extraction_response_payload` policies matching the command purpose. Minimum ages use the database creation event times of each class, including the youngest retained attempt/response, rather than provider timestamps. Policies are required for all three classes even when a job has no retained response.

The command checks fresh `can_dispose` authorization in every scope, active legal holds, complete scope and input inventories, original digest/identity bindings, live fact/memory proposals, retained scope exports and configured archive-consumer progress. Missing legacy input inventories require backfill before disposal. A shared job guard serializes deletion with execution, retries and archive reads. Tombstones, removal, the retention event, outbox and command receipt commit atomically. Exact replay rechecks authorization and returns the original receipt; changing policy selections under the same command key conflicts. The implementation bounds a transaction to 1,000 attempts and 128 MiB of encrypted artifact payloads; larger histories fail the precondition without partial deletion.

Disposal preserves source facts, memories, conversation/evidence objects and structural provenance. Live derived proposals block this operation until coordinated derivative disposal is available. No extraction artifact is projected into the search index by this command; fact/memory index cleanup remains part of the unfinished coordinator. Existing immutable archives retain their signed bytes and expire under separate policies, as do backups. Tests cover independent class ages, holds, archive/export blockers, incomplete inventory, rollback after partial deletes, all-attempt removal after manual retry, cross-scope dependency authorization/holds and source preservation, embedded/Connect/gRPC replay and signed disposal gaps.

## Retention governance and evidence disposal

Apply migration `00009_retention.sql`, refresh runtime grants, and republish the OpenFGA model. Retention mutation uses a separate `facts_retention` database login and explicit OpenFGA roles: `retention_administrator`, `legal_custodian`, and `disposal_operator`. Ordinary runtime credentials can inspect status when authorized but cannot publish retention policies, place/release holds, change disposal tombstones, or delete evidence objects.

`RegisterRetentionPolicy` publishes an immutable policy ID for a scope, record class, processing purpose, jurisdiction, and minimum age. The first supported disposal class is `evidence_bytes`. Age starts at database registration time, not a backdated source timestamp. A new policy never silently replaces the policy ID recorded on existing evidence. `PlaceLegalHold` applies a scope-wide hold. `ReleaseLegalHold` appends an authorized release with a reason; it does not delete the original hold. Hold IDs cannot be reused.

Apply migration `00029_pending_evidence_uploads.sql` and refresh `scripts/runtime-role.sql` before using pending uploads. The development PostgreSQL evidence adapter exposes `PutPending(ctx, tenant, model.PendingEvidenceUpload{ID: ..., Scope: ..., PolicyID: ...}, payload)` for new objects awaiting ledger registration. Its immutable policy must use `orphan_evidence_bytes`.

Upload metadata and encrypted bytes commit together; the database records upload time, and runtime credentials cannot backdate, update or delete that metadata. Identical retries preserve the original timestamp; changed scope or policy bindings conflict. Existing untracked objects cannot be adopted retrospectively.

The host must authorize uploads; this adapter method is not a public upload RPC or permission grant. Later evidence registration still requires its own `evidence_bytes` policy and preserves the original upload metadata.

`Engine.DisposeOrphanEvidence` removes an explicitly selected unregistered upload through the retention role and `can_dispose` permission. Supply its exact object reference, scope, orphan policy, jurisdiction, processing purpose and reason. The command requires mature database-recorded upload age, no active scope holds, no registered aliases in any scope and a complete legacy evidence inventory. Scope/object locks serialize cleanup with registration and holds. Deletion, the non-resurrectable tombstone, retention event, receipt and outbox event commit atomically; identical command retries return the original receipt. Upload provenance remains retained. The same command is available through Connect/gRPC `RetentionService.DisposeOrphanEvidence`. Raw `Put` remains available but does not record pending-upload provenance.

`DisposeSnapshot` is available through the embedded engine and Connect/gRPC. Supply a snapshot ID, reason and one `SnapshotRetentionBinding` for every scope represented by the snapshot, with an immutable `snapshot_payload` policy ID and jurisdiction. Bindings are sorted before command hashing; every scope requires `can_dispose`, a matching policy purpose/jurisdiction, mature database creation age and no active hold. The operation uses the separate `facts_retention` role. It does not apply one scope's policy to another scope or infer policies from the source facts.

Snapshot disposal blocks retained calculation or decision references, existing scoped export requests/copies, and archive consumers that have not acknowledged the snapshot's creation revision. Snapshot guards serialize new references and archive reads with disposal; a stale transaction retries. The ciphertext deletion, metadata-only tombstone, retention event, command receipt and outbox event commit atomically. A database insert trigger rejects resurrection. Original fact values, source evidence, snapshot identity/digest/order/source inventory and authorization metadata remain retained; this operation removes the snapshot's copied typed values, not its source facts or separately governed backups/archives.

`GetFactSnapshot` fails its precondition after disposal, so calculations and decisions cannot use removed content. Signed exports and archive payloads retain a snapshot manifest marked `PayloadDisposed: true`, with typed values omitted and a `snapshot_content` gap explaining `disposed_under_retention_policy`. The retained digest identifies the original payload; the metadata-only manifest is not a reconstruction of those bytes. Scoped exports still withhold multi-scope snapshot manifests and retention details that would disclose another scope. Archive retries preserve an already signed batch when the current source differs only by that exact snapshot tombstone and disposal gap. The worker verifies the retained payload bytes, original snapshot digest and unchanged event/scopes/other entries; unrelated changes still conflict, and current authorization is required. Newly archived disposed snapshots carry the explicit gap. Snapshot disposal is explicit; the orphan cleanup worker does not schedule it. Broader coordinated disposal of referenced snapshots, fact/memory payloads, model artifacts and index/cache copies remains unfinished.

`DisposeContext` is available through the embedded engine and Connect/gRPC after applying forward-only migration `00034_context_disposal.sql` and refreshing `scripts/runtime-role.sql`. Upgrade **all decision writers and archive workers** before enabling disposal: the updated adapters serialize references and archive reads using context guards. Ordinary runtime credentials cannot delete context bytes or write disposal tombstones.

Supply the context ID, reason and one `ContextRetentionBinding` for every retained scope, including memory dependency scopes outside the original query. Each binding requires `can_dispose`, an immutable `context_payload` policy with matching purpose/jurisdiction, mature database-recorded creation age and no active legal hold. Missing or incomplete scope bindings fail closed. Retained decision references, existing export requests in any context scope and archive consumers behind the creation revision block disposal. Concurrent decision recording and disposal cannot both succeed.

Context ciphertext deletion, the encrypted tombstone, retention event, receipt and outbox event commit atomically. A database trigger prevents re-insertion after disposal. The tombstone removes rendered bytes, query text, copied fact values, memory bodies and dependency labels; it retains the original digests, version inventories, scope boundaries and authorization metadata. Source facts, memories and evidence remain retained. This command does not remove separately governed archive or backup copies or content already delivered to an application.

Reads and preparation retries fail with `failed_precondition` after disposal, and new decisions cannot use the removed context. Authorized audit exports and archive payloads carry `PayloadDisposed: true` and a `context_content` / `disposed_under_retention_policy` gap. Single-scope exports withhold contexts and disposal details that span additional scopes. Archive retries verify the exact tombstone transition and preserve the already signed batch; the metadata is not a reconstruction of the original content. Context disposal is explicit and is not scheduled by the orphan cleanup worker.

`GetRetentionStatus` reports active holds and dependency blockers. `DisposeEvidence` supports unreferenced payloads and explicitly selected derivative families in the development PostgreSQL evidence store, bound to the same database identity as the ledger. References outside the selected family remain blockers. Deletion, a non-resurrectable object tombstone, the retention event, command receipt and outbox event commit atomically. Scope/object guards serialize disposal with new references, registrations, exports and holds; a stale transaction retries against current state. A hold committed before disposal blocks it. A later hold cannot restore already disposed bytes.

Disposal fails closed for referenced fact evidence, attestations, calculation/decision artifacts, pending or retained scope exports, unselected shared-object aliases, or incomplete legacy object inventory. Single-object disposal always rejects shared objects; an explicit complete lineage selection can dispose an object shared only within that family. Existing installations with evidence predating this migration require an explicit inventory migration before disposal; the engine does not guess which encrypted legacy locators alias an object. These blockers preserve copies outside the explicitly authorized lineage selection until broader coordinated disposal is implemented. Re-uploading the same disposed object/version is rejected. This API does not delete source metadata, encrypted event history, production object-store versions, archives or backups. It is not an erasure/anonymization claim; backups and immutable archives expire under separately approved policies.

Signed export manifests retain the `coordinated_derivative_disposal` gap kind with reason `referenced_payload_disposal_not_implemented`, identifying the remaining referenced-copy limitation without claiming that supported unreferenced lineage disposal is absent. The separate `independent_archive_chain` / `archive_coverage_not_attached` gap remains: an export signature does not establish independent archive-chain coverage.

Retention events appear in signed scoped exports and event archives. Exports explicitly mark disposed evidence content as unavailable. Integration tests verify legal holds, stale-snapshot races, database role separation, dependency/shared-object blockers, registration-based age, rollback, retry, tenant isolation, resurrection prevention and signed export gaps. Registered evidence disposal remains explicit; orphan cleanup can be scheduled through the opt-in worker below.


Apply migration `00030_orphan_cleanup_scan.sql` for the scope/policy cursor index. For scheduled orphan cleanup, use [examples/memoryworker-orphan-cleanup.json](examples/memoryworker-orphan-cleanup.json):

```sh
export MEMORY_WORKER_DATABASE_URL='postgres://retention-login@localhost/facts'
export MEMORY_ENCRYPTION_KEY_HEX='<ledger encryption key>'
bin/memoryworker -config examples/memoryworker-orphan-cleanup.json
```

Provision that login with `facts_retention`, and grant the configured service principal tenant membership plus `disposal_operator` on each configured scope. Publish each immutable `orphan_evidence_bytes` policy before enabling the worker. The policy purpose and jurisdiction must match the configuration. Raw uploads without `PutPending` provenance remain ineligible. This mode requires no search-index service, model provider, writable filesystem or outbox-consumer credentials.

`orphan-cleanup` runs an immediate pass, then waits `interval_minutes` (1–1440) after each completed pass. Each pass scans at most `max_pages` (1–100) pages of `page_size` (1–100) objects for each of at most 64 explicit scope/policy bindings, within `delivery_timeout_seconds`. Scans exclude registered, disposed, held and immature objects and fail closed for incomplete legacy inventories. `Engine.ScanOrphanEvidence` exposes the same authorized, bounded live scan to embedded hosts; its locator/version cursor is not a snapshot or deletion permission. The worker rechecks every deletion, skips objects that become ineligible, and exits on authorization, database or deadline failure for the process supervisor to handle. Counts are logged without object IDs or dependency error text.

Omit queue lease, attempt, polling and backoff fields for this mode. Stable object-specific command keys make retries safe; restart scans again, and deleted objects no longer appear. Keep stream, principal, purpose, reason and bindings stable during recovery. The worker does not dispose registered evidence or modify holds, policies, retained upload provenance, archives or backups. Integration tests exercise real OpenFGA permissions, runtime-role denial, graceful daemon shutdown, page bounds and Connect/gRPC receipt parity.


## Evidence access and lexical retrieval

`GetEvidenceMetadata` requires the distinct `can_read_evidence_metadata` permission (readers in the supplied OpenFGA model). `GetEvidenceContent` requires `can_read_evidence` (auditors), verifies retained bytes/digest/size and rechecks permission before disclosure. Both require the claimed scope, authorize before lookup, and mask absent or cross-scope/tenant IDs as denied. Metadata remains inspectable after approved byte disposal; content retrieval then fails its integrity/availability precondition.

For a generation without an embedding provider, provision `cortex.Config{Tenant: tenant, Generation: generation, EmbeddingID: "none", Dimensions: 1}` and call `cortex.New` with a nil embedder. The physical one-dimensional vector is a Cortex storage placeholder and is never used for similarity. Search uses PostgreSQL full-text matching with the explicit `simple` dictionary, plain query parsing, cover-density ranking and deterministic version-ID ties. See the [PostgreSQL text search contract](https://www.postgresql.org/docs/17/textsearch-controls.html). A lexical context manifest reports `EmbeddingID: "none"`, zero embedding dimensions, and its lexical search version.

`index.Lexical()` provides an explicitly selected lexical read view over an existing vector generation without calling the embedding provider for queries. Its writes retain the original generation's embedding requirements. There is no silent per-request switch between search methods. Apply `scripts/cortex-lexical-schema.sql` to existing databases for the GIN expression index; new provisioning includes it. Scope/tenant filtering, ledger revalidation, projection coverage and canonical-content guarantees apply to both search methods. These tests establish behavior and isolation, not bank-domain relevance quality.


## Revision-pinned listing and timestamp convenience reads

Compose optional capabilities together with `NewWithOptions` and `model.EngineOptions{Audit: ..., Retrieval: ..., Reads: ...}`. Existing specialized constructors remain available. `ReadOptions` requires a host-owned 32-byte pagination signing key. Tokens expire after 15 minutes by default (configurable from one second to one hour). Use one securely distributed key across serving replicas; rotating it invalidates prior tokens. Constructors copy the key and never install global configuration.

`ListFacts` accepts one scope, optional exact subject/predicate filters, valid time, purpose, and a page limit of 1–100. Every page is authorized before lookup and again before disclosure. The first page pins the tenant revision and eligibility evaluation time. Later pages exclude records created after that revision and retain the same historical selection; current permissions and evidence availability still apply. Results include resolution states rather than substituting values for missing, disputed or ineligible facts. These short-lived cursors do not create an immutable snapshot receipt.

`NextPageToken` is HMAC-authenticated and bound to the caller (including tenant and initiator), complete query, purpose, page size, revision, evaluation time, expiry and record cursor. Altered, expired, cross-caller or changed-query tokens fail. Send the original query unchanged with the next token. The token is opaque application state, not an authorization grant; every page rechecks permissions. Tokens are authenticated rather than encrypted and contain only hashes, opaque record IDs and revision/time metadata.

`GetFact` and `ListFacts` also accept `KnownAt`, mutually exclusive with an explicit knowledge revision. This convenience filter requires history permission and resolves the latest recorded tenant revision at or before the timestamp. A boundary before the first event produces a missing fact/empty page, never an accidental latest-state read. `recorded_at` is a database recording timestamp, not proof of the instant another client observed commit. Store explicit revision tokens or immutable snapshots for reproducible decisions. `GetFact.Purpose` is optional for compatibility and defaults to `fact_read`; new callers should supply their processing purpose.


## Immutable conversation ingestion

Apply migration `00010_conversations.sql`, refresh grants, and republish OpenFGA. `IngestConversation` takes a source-system/conversation/revision identity, scope, explicit timezone, and an ordered list of immutable message revisions. Messages reference already finalized objects; this operation never fetches caller-provided URLs. It verifies UTF-8 bytes, digests and sizes before opening the append transaction, then commits all evidence links, the encrypted conversation record, event, outbox and command receipt together. Limits are 128 messages, 1 MiB per message and 16 MiB total; text/plain and text/markdown are supported.

Speaker identity, role, source authority, source time, classification and retention policy remain explicit. Customer, assistant, staff and system turns are distinct; an assistant turn cannot claim customer authority. Bank-record ingestion requires the separate `can_ingest_bank_record` permission. Ingestors also need `can_ingest` and metadata read permission to receive the retained record. These are declarations from an authorized source integration, not proof that its statements are true. Prompt-like text remains inert evidence; ingestion neither runs extraction nor accepts facts.

Repeating a conversation source revision with identical content returns the original capture. Changed ordered content or metadata under that identity conflicts. A new conversation revision can reuse unchanged message revisions; corrected messages require new message revisions. Evidence registration now rejects changed source metadata as well as changed bytes under the same source revision, preventing speaker/authority changes disguised as retries. The digest excludes command metadata; the first captured actor and purpose remain retained.

`GetConversation` requires current evidence-metadata permission and returns source/object metadata and ordered evidence IDs. Exact message bytes still require `GetEvidenceContent`. Conversation metadata participates in signed audit exports and event archives, and its evidence links block uncoordinated disposal. Integration tests cover rollback without partial message registration, concurrent source deduplication, corrected-message lineage, invalid authority/timezone, tenant isolation and the archive pipeline.


Evidence verification preserves `context.Canceled` and `context.DeadlineExceeded`, including wrapped adapter errors, without exposing storage error text. Interrupted fact reads, lists, snapshots, recall and context preparation return errors rather than converting the interruption into an ineligible item or a partial successful result. Audit preparation aborts rather than recording cancellation as an evidence coverage gap. Temporary evidence-store outages that wrap `model.ErrUnavailable` are also preserved as sanitized errors: fact/memory reads, lists, snapshots, recall and context preparation cannot turn an outage into a partial successful result, and audit preparation cannot turn it into a retained coverage gap. Custom evidence adapters must distinguish temporary dependency outages from missing/corrupt content using that sentinel. Actual missing/corrupt evidence retains the existing integrity-failure behavior. The PostgreSQL evidence adapter checks cancellation before accessing its pool and after storage work; custom adapters must still honor context deadlines during their own I/O. Both PostgreSQL adapters give deferred transaction rollback a fresh five-second cleanup deadline, independent of request cancellation. Cleanup errors do not replace the original operation result; pgx discards a connection if rollback fails. This bounds rollback I/O, not the duration of arbitrary custom adapter code.

Retrieval also preserves cancellation/deadline errors from generation registries, projection progress and candidate search, including wrapped adapter errors while the parent context is active. Final generation checks, projection waits and context archive-lag checks follow the same rule. Other errors at these dependency boundaries remain sanitized `Unavailable` results. A failure after an earlier scope returned candidates discards that selection; fact/memory recall and context preparation cannot disclose it as a successful result. Unit and PostgreSQL/gRPC tests cover these failure paths.

## Offline extraction evaluation

`memoryeval` scores owner-labelled extraction observations without calling a model or modifying the ledger. It measures extraction errors separately from retrieval relevance. Run the intentionally imperfect synthetic fixture with `make eval-example`, or score your own files:

```sh
make build
bin/memoryeval -dataset approved-dataset.json -run labelled-run.json > report.json
```

The [`evaluation`](evaluation/score.go) package also exposes `Evaluate(datasetJSON, runJSON)`. Both files use schema `facts-extraction-evaluation-v1`; see [dataset.json](examples/evaluation/dataset.json) and [run.json](examples/evaluation/run.json). Each file is limited to 16 MiB, the dataset to 10,000 cases, and each assertion/conflict list to 256 distinct entries. Unknown fields, duplicate or mixed-case JSON keys, excessive nesting, trailing JSON, duplicate case IDs and incomplete case coverage are rejected.

Dataset cases contain a category, an explicit list of supported assertion digests, a subset of required assertion digests, and expected conflict IDs. Empty lists must be explicit. Categories cover prompt injection, ambiguous subjects, assistant claims, negation, historical dates, truncation, contradiction and controls. The domain owner must define a stable assertion-normalization/digest convention and conflict identity scheme, label the source-backed ground truth, and independently map observed output to those identities. Exact identifier matching is used; the scorer does not infer semantic equivalence, truth, source support or contradictions.

A run binds the **exact dataset file bytes** with SHA-256 in `dataset_digest` and records the extraction profile digest and provider-reported model identity. Every case needs an outcome and request digest; completed cases also require a response digest and explicit observed assertion/conflict lists. Failed cases have no scored output. Hosts should obtain request/response digests from authorized retained extraction artifacts and retain the labelled observations alongside those artifacts. The scorer validates digest syntax and the dataset binding, but does not independently fetch artifacts or attest that the labels, model identity or supplied artifact digests are truthful. Reformatting the dataset changes its digest and requires an explicit new run binding.

Reports retain input-file digests and model/profile metadata, with overall counts and a fixed category breakdown. Rates are pooled counts, not averages of case-level percentages:

| Metric | Numerator / denominator |
| - | - |
| Unsupported assertion rate | Proposed assertion IDs outside the supported set / all proposed assertions |
| Missed conflict rate | Expected conflict IDs absent from detected conflicts / all expected conflicts |
| Missed required assertion rate | Required assertion IDs absent from proposals / all required assertions |
| False conflict rate | Detected conflict IDs outside the expected set / all detected conflicts |

A zero denominator produces `null`, never a claim of zero error. Failed cases remain in case counts and still contribute their missing required assertions/conflicts. The report includes `failed_cases`; a deployment gate must consider failures and missing category/denominator coverage as well as the rates. Declining every case cannot silently become a perfect score: unsupported-assertion rate is then undefined while required-assertion/conflict misses remain visible. Reports contain aggregate counts rather than case IDs or assertion content, and are deterministic for identical file bytes. CLI exit zero means scoring succeeded, **not** that a model passed an approval gate; invalid inputs/read/write failures exit 1 and invalid command usage exits 2.

The included dataset and observations are synthetic scorer fixtures, not a provider run or a bank-approved extraction evaluation. The bank's model/domain owners must supply and approve representative datasets, normalization and labelling rules, actual retained provider runs, evaluation review, per-category coverage requirements and release thresholds. Retain the scorer commit/build and report with that approval evidence. Retrieval relevance and production workload evaluation remain separate activities.

## Durable fact extraction

Apply migrations `00011_extraction_jobs.sql` and `00012_extraction_runs.sql`, refresh `scripts/runtime-role.sql` grants, and republish the OpenFGA model. Configure `model.EngineOptions.Extraction` through `NewWithOptions`. Each execution host’s `ApprovedExtractor` supplies a host-owned `ExtractionProvider` and a pinned profile: provider/model identity, prompt bytes and digest, prompt/parser/build versions, JSON parameters, timeout, byte/candidate/attempt limits, token reservation and retry delay. Fact jobs use the parser `facts-extraction-json-v1`; memory extraction and refresh use separate approved parsers described below. Constructors never start workers or contact a provider.

A scheduling host can set `ExtractionOptions.ScheduleOnly: true`, supply the same approved profiles and budget configuration, and leave every `ApprovedExtractor.Provider` nil. Profile, prompt-digest, parser, redaction and budget validation remain mandatory. This host can plan chunks, create extraction/refresh jobs and emit refresh sweeps without provider credentials; `RunExtraction` returns `ErrPrecondition` before reading or mutating a job. Supplying a provider in this mode is a configuration error. A separate execution host must configure the retained profile exactly and supply its provider; the profile digest is unchanged by scheduling-only mode. The integration test schedules an event-triggered refresh through a provider-free engine, proves rejected execution leaves the job unchanged, and completes it through a separate execution engine. The standalone `refresh` worker supports these profiles for event-driven scheduling; standalone `extraction` mode can execute these profiles through an approved gateway.

The embedded workflow is:

1. Call `IngestConversation` to retain ordered source revisions and evidence links.
2. Call `StartExtraction` with the conversation ID, approved profile ID, explicitly bound subjects and predicate/schema versions. This commits a durable job without calling the provider.
3. Have a trusted service worker call `RunExtraction` with the job ID, scope, original purpose and a command key. Use the same key for delivery retries of one invocation; a later attempt needs a new key. Use `workers.NewExtraction` with `workers.NewOutbox` for durable delivery, as shown below. For standalone execution, run `memoryworker` in `extraction` mode using [examples/memoryworker-extraction.json](examples/memoryworker-extraction.json) and the gateway configuration described above. The host owns process supervision and shutdown.
4. Inspect `GetJob` for `pending`, `running`, `completed` or `failed`, attempt count, fence, next available time and generic failure code. Fetch retained attempt input and results through `GetExtractionRun` using the attempt ID; this requires current extraction and evidence permissions.
5. Review resulting proposals through the ordinary fact lifecycle. Extraction never accepts a fact. `RetryJob` requires operator permission and the expected fence to reset an exhausted job's attempt budget.

Extraction delivery respects the job's persisted `AvailableAt` and active `LeaseUntil`. The consumer defers provider execution while either relevant deadline is in the future, and outbox failure scheduling uses that time as a minimum delay even when its normal backoff cap is shorter. After an execution error it re-reads authorized job status to account for a claim or commit whose response was lost. If that status read fails, ordinary outbox backoff applies; it never assumes completion. Returned job IDs/scopes are checked before acknowledgement, and terminal job replays do not invoke the executor. Exhausted delivery attempts still require operator action.

Before invoking the provider, the engine commits the exact JSON request bytes, digest, pinned profile, source identities, timezone, original message contents and authorization receipts. The provider receives a copy of those bytes. This records the engine/provider boundary; a vendor adapter must separately preserve any transformed HTTP request if wire-level replay is required. Provider implementations must honor cancellation and enforce transport response limits. The interface supplies no ledger mutation or tool capability.

Input defaults to the entire conversation. Profiles can approve selected original byte ranges, deterministic chunk planning, and required registered redacted derivatives as described below. Configured input limits include the serialized envelope; an oversized envelope fails the job explicitly. Maximum configured input is 16 MiB, response size is 2 MiB, candidates per response are 64, and attempts per retry cycle are 10. Provider timeouts range from one second to two minutes. An oversized response retains a bounded prefix and the full received body digest, marked incomplete; the digest cannot reconstruct omitted bytes. A crash or lost authorization can leave an attempt without a retained response. Full audit manifests explicitly report absent or incomplete responses at the export revision.

To approve explicit input selection, set `ExtractionProfile.InputSelectionVersion` to `"original-ranges-v1"`. This setting is included in the pinned profile digest. A caller with the usual extraction and evidence permissions can then set `ExtractionRequest.InputRanges`:

```go
InputRanges: []model.ExtractionInputRange{
    {EvidenceID: conversation.EvidenceIDs[0], Start: 9, End: 12},
},
```

Ranges use half-open UTF-8 byte offsets in the original evidence, must follow conversation order, and cannot overlap. At most 256 ranges are allowed; an empty list preserves whole-conversation behavior. Unknown evidence, invalid bounds, split UTF-8 characters and selection under an unapproved profile are rejected. Input admission counts selected source bytes; execution also checks the complete serialized envelope against the profile limit.

Each selected range becomes a separate input message with `SourceStart` and unchanged source metadata. `ExcludedRanges` records every omitted original range, including whole omitted messages. The immutable job retains the requested selection, and each attempt retains the exact provider input before dispatch. Provider citations must use **original evidence offsets** and fit wholly inside one supplied range; relative offsets and citations crossing gaps are rejected. Fact and memory proposals retain those original offsets for ordinary evidence validation. For original-byte selection without a redaction policy, no bytes are normalized or replaced and `RedactionVersion` remains `original-utf8-v1`.

Explicit selection defines the input to a single job. Use the planner below to divide larger inputs into multiple job requests. Hosts must approve prompts/providers that understand `SourceStart` and original-offset citations. Memory refresh jobs do not support selection. Upgrade all extraction workers before publishing selection-enabled profiles; older workers do not enforce these new fields. No database migration is required.

`PlanExtractionChunks` is an authorized, read-only planning operation. It requires current extraction, evidence and evidence-metadata permissions, verifies immutable source content, and requires a selection-enabled profile. Pass `Scope`, `ConversationID`, `ProfileID`, `Purpose`, optional `InputRanges`, and `MaxSourceBytes` in `model.ExtractionChunkQuery`. The returned `ExtractionChunkPlan` identifies the tenant, scope, conversation digest and approved profile digest, with ordered chunks and explicitly excluded ranges.

The `original-chunks-v1` planner packs selected ranges in source order, splitting only at UTF-8 boundaries. Every selected byte appears exactly once across the plan; gaps remain separate ranges, and empty messages contribute no bytes. Planning is deterministic and capped at 256 chunks with at most 256 ranges per chunk. Exceeding the cap fails the whole operation; no partial plan is returned. `MaxSourceBytes` must be at least four and no greater than the profile input limit. This bounds **raw source bytes**, not JSON escaping, prompts, metadata or bound dependencies. Leave envelope headroom; each job still rejects an oversized serialized input explicitly. Byte-based boundaries do not promise sentence, assertion or grapheme boundaries; production extraction evaluations must account for lost context at chunk edges.

For each returned chunk, submit an ordinary `StartExtraction` request with its `InputRanges`, the plan's `ProfileID`, and `ExpectedProfileDigest: plan.ProfileDigest`. Include the usual declared subjects/predicates or memory targets. The optional expected digest prevents admission under a changed profile between planning and job creation. Use stable, distinct command keys derived from the application's operation ID, plan digest (`model.Digest(plan)`) and chunk index. Jobs and their retained attempts preserve the actual ranges supplied; independent chunk jobs use the normal durable queue, retries, quotas and proposal validation.

Planning creates no jobs, calls no provider, reserves no quota and supplies no authorization for later execution. Submission of several chunk jobs is not atomic; a host should resume partially submitted plans using the same command keys. Persist the plan externally if batch-level coverage or orchestration is needed. A conversation containing only empty messages produces an empty chunk list. Cross-chunk context overlap, semantic chunking and automatic batch supervision are not provided.

To require registered redaction for conversation extraction, apply migration 00027, refresh database grants, and configure both:

```go
// EngineOptions:
RedactionPolicyVersions: []string{"approved-v1"},
// ExtractionProfile:
InputSelectionVersion: "original-ranges-v1",
RedactionPolicyVersion: "approved-v1",
```

Set `ExtractionRequest.RedactedEvidenceIDs` to registered derivatives of the conversation evidence. Each selected message must have one derivative under the exact approved policy version, including a fully redacted derivative for a selected message that should supply no text. At most 128 unique parent bindings are allowed. Missing derivatives, duplicate parents, unrelated originals, policy mismatches, unverified bytes or references under a profile without redaction approval fail before provider invocation. The required policy is included in the pinned profile digest; there is no fallback to original text. The engine verifies original and derivative bytes again at execution, so requester and worker still need original evidence access.

Input selection intersects the requested original ranges with the derivative's copied segments. Each intersection becomes a separate message retaining the original `EvidenceID` and `SourceStart`, plus `DerivativeEvidenceID`, `DerivativeDigest` and `DerivativeStart`. `RedactionVersion` becomes `utf8-omit-v1`; `ExcludedRanges` covers both selection omissions and redacted regions. This mode allows at most 256 supplied segments per job and rejects a job with no surviving source bytes. The request is retained before dispatch. Providers must cite original evidence offsets; concealed bytes, derivative IDs and citations across removed gaps are rejected. Proposals retain original evidence citations rather than changing source authority to the redaction process.

Pass the same derivative IDs to `PlanExtractionChunks`; it plans only surviving bytes and returns `RedactedEvidenceIDs` to copy into each chunk's job request. A fully concealed plan has no chunks. Queued jobs retain tenant/scope-qualified derivative references that block disposal, and job archives include derivative and original evidence metadata. Custom ledger adapters must enforce these retention references when staging jobs. Upgrade all request handlers and workers before enabling required-redaction profiles.

This mode supports fact extraction and conversation memory extraction. A memory job with bound dependencies additionally requires approved dependency-payload omission as described below; otherwise it is rejected before provider invocation. Memory refresh profiles cannot enable conversation-range redaction, but can use the separate whole-field omission policy. The policy covers conversation text; hosts must separately govern prompts, schemas, identifiers and other envelope metadata, and supply approved sensitive-data detection rules and evaluation evidence.

Memory profiles can additionally specify `ExtractionProfile.PayloadRedaction`:

```go
PayloadRedaction: &model.ExtractionPayloadRedaction{
    PolicyVersion:         "approved-memory-fields-v1",
    DependencyPayloads:    true,
    OriginalMemoryPayload: true, // refresh profiles only
    RefreshEvidence:       true, // refresh profiles only
},
```

Register that exact policy version in `EngineOptions.RedactionPolicyVersions`. At least one omission must be selected, and this option is limited to memory extraction/refresh parsers. Conversation memory extraction can select `DependencyPayloads` only. The entire policy, including each flag, is included in the profile digest and copied at construction; scheduling and execution hosts must use the same approved profile. No policy means the existing wire encoding is preserved.

`field-omit-v1` removes complete JSON fields from a separate provider-input view. `DependencyPayloads` removes each bound fact version's `Proposal` and `Proposer`, including its value, source quotes and command metadata. It applies to both conversation memory dependencies and refresh dependency versions. `OriginalMemoryPayload` removes the original memory's `Proposal`, `Proposer` and `Authorization`. `RefreshEvidence` removes the refresh `Evidence` array. Version/record IDs, digests, policy IDs, dependency timelines, schema metadata and other unselected envelope fields remain visible. This is selective input omission, not anonymization or sensitive-data detection.

The retained request contains `PayloadRedactionVersion: "field-omit-v1"` and ordered `PayloadOmittedPaths` identifying the actual removed fields with JSON Pointer paths. The provider receives those exact retained bytes. Ledger-side proposal construction, source preservation and dependency eligibility still use the original immutable versions, so omitted fields cannot erase validation requirements or rewrite source provenance. Conversation citation validation continues to enforce the approved evidence-derivative mappings. Existing source-construction limits still apply, and the final serialized provider request must fit the profile's input limit.

For packaged scheduling, configure `extraction.redaction_policy_versions` in `memoryd`, or `refresh.redaction_policy_versions` in the refresh/sweep worker. The extraction executor uses its existing scheduling configuration's `redaction_policy_versions` list. [examples/memoryworker-refresh-redacted.json](examples/memoryworker-refresh-redacted.json) omits the original memory payload and source evidence while leaving current dependency facts visible; apply its matching profile and policy approval to the executor. All three flags can be enabled when an approved policy requires it, but a provider may then lack enough information and should decline. These settings do not waive original evidence access or automatically accept generated memories.

Tests verify field absence, immutable canonical input, profile approval/copying/digest binding, exact retained provider bytes, dependency-bound proposals, redacted refresh with original sources preserved, and composition with registered conversation derivatives. Partial field ranges, replacement text and automated classification of dependency/refresh content are not implemented by this transform.

Claims use database leases and fences. Completion requires the current, unexpired claim, preventing a stale worker from committing proposals. Tenant concurrency and UTC-day token reservations are enforced in PostgreSQL. Each admitted attempt reserves its maximum token allowance; reservations are not refunded, including failed or interrupted calls. Provider-reported token usage is retained but is not an independently verified cost record. Pending provider failures use bounded exponential backoff with jitter; exhausted jobs remain visibly failed until an authorized retry.

The original requester and executing worker need current extraction, proposal and evidence-read permissions. These are checked again at completion. The worker's delegated human identity comes from the authenticated requester, preserving manual separation of duties. Revocation of the requester prevents proposal creation; the response can be retained with an authorization-change outcome only while the worker still has the required access.

The parser rejects unknown fields, trailing JSON, missing candidate arrays and excessive candidates. Candidates must use bound subjects and predicate versions, valid typed values and intervals, exact UTF-8 source spans, permitted source classes, and an affirmative assertion marker. Assistant/system-only sources and uncertain or nonaffirmative markers cannot produce proposals. Confidence is diagnostic only. These structural checks do not establish semantic entailment or reliably detect a model that incorrectly labels negation as an assertion; production evaluation and governed acceptance remain necessary.

Duplicate keys within one output are flagged for review. Exact existing value/interval/schema/source matches produce duplicate relations; differing values overlapping accepted or disputed intervals produce conflict relations. New proposals, result artifacts and job completion commit atomically. Versions retain their extraction attempt ID, and reported model identity is preserved separately from the requested model. A provider alias is not proof of an immutable model revision.

Encrypted jobs, attempts and results participate in event archives and full signed audit exports. Metadata-only exports omit model payloads because prompts, requests and responses may contain source content. Evidence disposal remains blocked by conversation references until coordinated retention is implemented.

Integration tests verify deterministic chunk coverage, planning authorization, stale-profile rejection, selected-range retention, original-offset citation checks, input retention before provider invocation, candidate filtering, proposals remaining unaccepted, manual separation of duties, exact duplicate detection, concurrent execution fencing, profile pinning, token-budget rejection, provider failure/retry recovery, tenant isolation and signed exports without metadata-only payload disclosure. No production model or bank-approved extraction evaluation has been configured.


### Running extraction from the outbox

Create one extraction consumer per tenant and logical worker stream. The queue connection uses `facts_worker`; the engine connection uses `facts_runtime`. The configured service identity must have the scope permissions needed by extraction. Keep the consumer stream name stable across restarts so its durable offset is reused. Registering a new stream replays the tenant ledger from the beginning; completed and failed jobs are acknowledged without invoking the provider again.

```go
key := model.ConsumerKey{Tenant: caller.Tenant, Consumer: model.ExtractionConsumer("primary")}
if err := queue.RegisterConsumer(ctx, key, 20); err != nil {
    return err
}
handler, err := workers.NewExtraction(engine, caller, "primary") // caller.Service must be true
if err != nil {
    return err
}
worker, err := workers.NewOutbox(queue, key, handler, workers.Options{
    Lease: time.Minute, PollInterval: time.Second,
    MinBackoff: 5 * time.Second, MaxBackoff: 5 * time.Minute,
})
if err != nil {
    return err
}
return worker.Run(ctx) // host cancels ctx and closes its resources on shutdown
```

The handler processes `start_extraction` and `retry_extraction` events and skips other events. Each delivery fence supplies a distinct invocation key; repeated handling of that same delivery uses the same key. An operator's retry purpose is retained on the retry event, while provider execution uses the job's original purpose, exposed by authorized `GetJob` reads. The engine still rechecks both requester and worker authorization.

Pending or running jobs keep their notification unacknowledged. Outbox backoff and job backoff are independent, so choose a delivery attempt budget that covers provider retries and lease recovery. A terminal failed job is acknowledged and remains visible in `GetJob`; `RetryJob` emits a new notification. A failed consumer instead requires `RetryConsumer` with its current fence. This ordered consumer can delay later jobs while one job is waiting; separate consumer streams replay the same jobs and do not partition work. Database job claims still prevent concurrent provider invocation while a lease is live.

Integration tests replay real outbox events through this handler and verify that tenant concurrency limits hold across different jobs. They also expire a live job lease, complete a replacement attempt, and verify that the old provider response cannot commit after its fence has been replaced. Missing responses remain explicit gaps in full audit exports.


### Waiting for searchable revisions

`WaitForProjection` waits for a tenant's projection consumer to acknowledge a specific ledger revision in a specific generation. Pass the committed command's `LedgerRevision`, the intended generation, authorized scopes and purpose. The context must have a deadline; the engine additionally caps each call at two minutes. It polls without holding a ledger transaction and never runs projection work itself.

```go
waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
defer cancel()
receipt, err := engine.WaitForProjection(waitCtx, caller, model.ProjectionWaitRequest{
    Scopes: []string{scope}, Generation: "g1",
    Revision: committed.LedgerRevision, Purpose: "review",
})
if err != nil {
    return err
}
result, err := engine.Recall(ctx, caller, model.RecallRequest{
    Scopes: []string{scope}, Query: "client preference", Purpose: "review",
    ValidAt: time.Now().UTC(), Limit: 10,
    RequireGeneration: receipt.Generation, RequireRevision: committed.LedgerRevision,
})
```

Authorization is checked before waiting and again before returning a receipt. A managed generation or epoch change fails the wait; callers must explicitly choose whether to retry against the new generation. Missing progress, an unavailable adapter, a wrong generation, or a failed consumer below the requested revision returns `ErrUnavailable`. Deadline expiry and cancellation return context errors without a partial receipt. Already acknowledged revisions remain covered even if a consumer subsequently fails on later work.

A receipt establishes that the configured consumer processed the requested revision. It does not promise a search hit, remove search limits, or include later commits. Recall still revalidates candidates against the ledger and reports incomplete coverage when newer revisions are pending. `RequireGeneration` prevents a later search from silently using another generation, and managed recall also checks for cutover during the search. Direct fact reads remain immediately consistent and require no projection wait.

Tests cover deadline expiry, progress arriving during a wait, failed consumers, authorization revocation, epoch changes, and wrong-generation search rejection. Real PostgreSQL/Cortex integration verifies a wait before and after outbox delivery.


### Handling concurrent fact changes

Fact proposal, approval/acceptance, supersession, rejection, dispute, resolution and retraction commands compare `ExpectedRevision` against the record stream under its database lock. A mismatch returns `*model.RevisionConflict` (also exported as `factsengine.RevisionConflict`) with `RecordID`, `ExpectedRevision` and `CurrentRevision`. Existing `errors.Is(err, model.ErrRevision)` handling remains valid; use `errors.As` to inspect details:

```go
var conflict *model.RevisionConflict
if errors.As(err, &conflict) {
    // Refresh the authorized fact/history and review the intervening changes.
    // Build a new command only after deciding whether the intended change still applies.
    currentRevision := conflict.CurrentRevision
    _ = currentRevision
}
```

The reported revision is the state observed while the record was locked, not a reservation: it can change again before a retry. The engine never automatically overwrites intervening work. Reusing a committed command with identical input still returns its original receipt; a revised intent needs a new command key. Conflict details contain no fact values, evidence text or other callers' commands, and the error's text stays generic. Mutation authorization is rechecked before returning the details; denial or an authorization outage withholds them. Operational lease/fence conflicts continue to use the generic `ErrRevision` contract and should be handled through the corresponding job/consumer status API.

The real PostgreSQL concurrent-approval test verifies that the losing command receives the winning stream revision. Disclosure tests verify that authorization revocation suppresses typed conflict details.


### Registered predicate constraints

`PredicateVersion` supports optional immutable `Constraints`, `Cardinality` and `TemporalPrecision` fields. These are retained with the encrypted definition and included in extraction inputs and audit/archive payloads. No new database migration is needed: the fields extend the existing versioned payload. Omitted fields preserve earlier behavior and serialization; explicit `Cardinality` currently accepts only `single`, matching the engine's one-value-per-fact-key contract. Separate memberships still require explicit discriminators.

`model.ValueConstraints` supports:

- `AllowUnknown`: optional boolean; omission permits explicit unknown values, while false rejects them. Unknown remains distinct from false, zero and absence.
- `AllowedValues`: up to 64 exact typed values. Unknown is controlled separately. Enumerations also obey the other declared constraints.
- `MinTextRunes` / `MaxTextRunes`: inclusive Unicode code-point counts, independent of UTF-8 byte length; the existing 64 KiB text byte limit still applies.
- `MinInteger` / `MaxInteger`: inclusive signed integer bounds.
- `MinDecimal` / `MaxDecimal`: inclusive canonical decimal bounds, compared exactly without binary floating point.
- `DecimalPlaces`: maximum fractional digits in the canonical decimal representation. Trailing-zero strings remain invalid under the existing canonical-value contract.
- `Units`: an allowlist for decimal units/currencies, including an explicit empty string if unitless values are permitted. Numeric bounds apply in each allowed unit; the engine does not convert currencies or units.

Constraint definitions are limited to 64 KiB. Wrong-type constraints, inverted bounds, duplicate enumeration values/units, invalid canonical decimals and unsupported cardinalities are rejected at publication. Manual proposals and extracted candidates share `ValidateFact`; invalid extracted values become review issues. Acceptance rechecks the bound immutable definition. Register a new schema version for changes; existing accepted versions retain their original schema and history. Replacement publication requires the explicit migration rule described below; the host supplies any transformed values through new governed proposals.

`TemporalPrecision` accepts `microsecond`, `second` or `day`. It applies to effective interval boundaries and instant values; `day` requires UTC midnight. It never rounds timestamps or infers a local timezone. Omission retains the existing interval microsecond contract without adding a restriction to instant values. Lifecycle changes also validate their affected intervals against the version's declared precision.

Tests cover exact decimal comparisons beyond floating-point precision, Unicode lengths, ranges, units, enumerations, unknown values, time boundaries, legacy serialization, persisted definitions and extraction filtering. A PostgreSQL scenario rejects an invalid proposal without consuming its command key, then accepts a valid proposal through the ordinary review policy.


### Publishing replacement schemas

Apply `00013_predicate_migrations.sql` and refresh runtime grants before upgrading the adapter. A first predicate version has no `Migration`. Every replacement must carry `PredicateMigration` with the currently active `FromVersion`, `Mode: "preserve_history_repropose"`, and an opaque reason code. The command still requires `can_manage_definitions`, an idempotency key and purpose.

```go
next := model.PredicateVersion{
    Scope: scope, Predicate: "preference", Version: "v2", PolicyID: "review-v2",
    Kind: model.Text, Classification: "confidential",
    Migration: &model.PredicateMigration{
        FromVersion: "v1", Mode: "preserve_history_repropose", Reason: "schema_review",
    },
}
_, err := engine.RegisterPredicateVersion(ctx, caller, model.PredicateRequest{
    Command: model.Command{Key: "publish-preference-v2", Purpose: "schema_administration"},
    Definition: next,
})
```

Publication checks the active predecessor under a database lock, then atomically stores the definition, immutable migration link, active pointer, event and command receipt. Concurrent replacements of one predecessor cannot both activate: the stale publisher receives `ErrPrecondition` and must review the new active state. An identical retry of a committed publication returns its original receipt even after another version becomes active. Database foreign keys bind both sides of the migration to the same tenant, scope and predicate.

The supported rule leaves accepted facts, historical resolutions, evidence, snapshots and calculation receipts unchanged. Pending proposals under an inactive schema cannot be accepted. New values must be proposed against the active schema and pass its policy; replacing an overlapping accepted value still requires ordinary supersession with evidence and review. The engine does not reinterpret old payloads, convert units, or carry attestations onto a new version. Schema rollback likewise means publishing a new version derived from the current active version; existing immutable version IDs cannot be reactivated by republishing them.

The migration reason and rule are retained inside the encrypted predicate definition and therefore appear in authorized audit exports and event archives. Legacy definitions remain readable; the deployment migration does not invent predecessor links or reasons for publications made before this contract. Tests cover missing rules, concurrent replacement conflicts, committed retries and preserved accepted history.


### Paginated fact history

Use `ListFactHistory` for histories that may exceed one response. Configure `EngineOptions.Reads` with a stable 32-byte pagination signing key (the same configuration used by `ListFacts`), then pass `HistoryPageQuery` with scope, record ID, purpose and a limit from 1 to 100. The first page selects the current committed tenant revision and record stream revision. Every continuation page uses that fixed boundary, even if more events commit between calls.

```go
query := model.HistoryPageQuery{
    HistoryQuery: model.HistoryQuery{Scope: scope, RecordID: recordID, Purpose: "audit"},
    Limit: 100,
}
for {
    page, err := engine.ListFactHistory(ctx, caller, query)
    if err != nil {
        return err
    }
    // Consume page.Events in ascending stream-revision order.
    if page.NextPageToken == "" {
        break
    }
    query.PageToken = page.NextPageToken
}
```

Tokens are integrity-protected and bound to tenant, authenticated caller, scope, record, purpose, page size and the pinned revision. Keep query fields unchanged while following a token. Key rotation or expiry requires restarting the traversal. Each page rechecks `can_read_history` before reading and before returning content. The response reports both `LedgerRevision` and `StreamRevision`; starting a fresh traversal includes later commits.

Missing, reordered or inconsistent stream events fail with `ErrPrecondition` rather than producing a shortened history. Pages contain event metadata and authorization receipts, not source bytes or a recomputed current value. `GetFactHistory` remains available for existing callers and keeps its explicit 1,000-event bound; use `ListFactHistory` for larger histories. No database migration is required for pagination.

Tests traverse a 1,002-event history and reject an injected stream gap. Real PostgreSQL integration verifies that an intervening proposal does not enter an existing page sequence, that a fresh traversal includes it, and that changing the purpose invalidates the continuation token.


## Immutable memory proposals

Apply forward-only migration `00014_memory_proposals.sql` and refresh runtime/archive grants. Records now carry a database-enforced `fact` or `memory` kind. Memory payloads and event links have separate tables and foreign keys; they cannot be inserted or read as typed fact versions. Existing fact identities and payloads remain unchanged.

`RegisterMemoryType` publishes an application-owned subtype/version, acceptance-policy ID and classification under `can_manage_definitions`. A replacement requires the same explicit predecessor/migration contract as predicate publication. The engine does not choose a policy from model confidence or from the memory's body.

`ProposeMemory` takes an explicit `MemoryKey{Scope, ID}`, subtype/schema version, UTF-8 body, effective interval, exact evidence spans, optional fact-version dependencies, expected stream revision and command metadata. Bodies are limited to 64 KiB, sources to 64 and dependencies to 64. A dependency names its scope, record/version IDs, exact content digest and an `assertion`, `uncertain` or `disputed` label. These labels preserve the author's declared context; they do not certify the referenced fact's current eligibility or the summary's truth.

The engine verifies source bytes/spans, dependency ownership/digests, active subtype and revision before atomically appending the encrypted proposal, source/dependency links, stream event, tenant revision, outbox entry and command receipt. Cross-scope dependencies require current read and history permission in each referenced scope; those authorization receipts are retained. Repeated identical commands converge, changed commands conflict, and corrections create new immutable versions under the same explicit memory identity.

`GetMemoryVersion` returns an exact retained version after current read/history/evidence permissions in the memory scope and dependency scopes, source-byte checks and digest verification. It is a historical proposal read, not a current accepted-memory resolution. A missing or cross-tenant version is withheld under the disclosure policy. Memory versions never appear as typed fact values or through fact history APIs.

Memory proposals and subtype definitions participate in scoped signed audits and event archives. Metadata-only audits withhold memory bodies because they may quote evidence. A scoped audit also withholds a memory version with dependencies outside that scope; archive workers authorize every dependency scope. Memory evidence links block uncoordinated byte disposal.

Memory approval, configured automated acceptance, temporal lifecycle and direct current/historical resolution are now implemented as described below. Memory recall is now implemented as described below. Explicit memory refresh jobs and prepared memory contexts are implemented below; automatic scheduling from fact-change events is implemented below. Tests cover immutable corrections, retries/conflicts, dependency digest rejection, tenant/permission boundaries, fact/memory separation, retention blockers and signed audit disclosure.


### Memory acceptance, lifecycle and dependency eligibility

Apply forward-only migration `00015_memory_lifecycle.sql` and refresh runtime/archive grants. `ApproveMemory` enforces manual policy and distinct proposing/approving principals, including delegated human identity. `AcceptMemory` requires an explicitly configured automated policy and accepting service principal. Both check active subtype schema, exact stream revision, known interval start, permitted source classes and dependency eligibility. Assistant/system evidence cannot independently establish an accepted memory. Required attestations must be present, unwithdrawn, unexpired and bound to the exact memory version and content digest. Missing prerequisites fail with `ErrPrecondition`.

`SupersedeMemory` takes the complete set of overlapping version IDs and the candidate's exact interval. Ordinary acceptance cannot overwrite an overlapping accepted/disputed version. Supersession preserves unaffected pieces and commits one new immutable timeline generation. `RejectMemory` only rejects a never-accepted proposal. `DisputeMemory`, `ResolveMemoryDispute` and `RetractMemory` operate on fully covered intervals of the named version, retain reason codes and preserve earlier timelines. Retraction permits a separately reviewed replacement; rejection does not. These commands use the existing `Approval`, `Supersession` and `LifecycleChange` request structures.

`GetMemory` takes a `MemoryQuery` with explicit key, valid-time instant and purpose, plus optional `KnowledgeRevision` or `KnownAt`. It reports `missing`, `unresolved`, `resolved`, `disputed`, `retracted`, `stale` or `ineligible`. Body content is returned only for `resolved`; `GetMemoryVersion` remains the explicit historical-content API. Current access and evidence availability are checked even for historical queries.

Dependency eligibility covers the entire interval described by the memory, not just the query instant. Each referenced fact version must still cover that interval with the declared label: `assertion` requires accepted coverage, `disputed` requires disputed coverage, and `uncertain` requires accepted explicit-unknown coverage. Required fact attestations are evaluated under their recorded acceptance policy and bound attestation IDs. Corrections, gaps, disputes, retractions or expired/withdrawn prerequisites can therefore make a memory stale. A current read never replaces its dependency with a newer fact version or chooses a winner by similarity. Restoring the exact dependency's eligible coverage can restore resolution without rewriting the memory.

Acceptance locks the memory and dependency records in deterministic order before appending, so a concurrent fact change cannot slip between dependency validation and commit. Reads evaluate timelines and dependency eligibility in one ledger view and report that revision. Earlier knowledge revisions preserve earlier resolutions. Evidence-byte verification and fresh scope checks occur before disclosure; unavailable evidence withholds the body.

Signed audit exports and event archives retain memory timeline generations and lifecycle events. Memory context integration is implemented below. Integration tests cover self-approval denial, automatic/manual policy separation, required-prerequisite rejection, overlap prevention, supersession, dispute/resolution, retraction, rejection, whole-interval dependency staleness and preserved historical reads.


### Memory attestations and withdrawal

Apply forward-only migration `00016_memory_attestations.sql` and refresh runtime/archive grants. The migration backfills a shared attestation identity registry for existing fact attestations. Fact and memory attestations share a tenant-wide ID namespace, so `WithdrawAttestation` cannot ambiguously target two record kinds. Tenant-qualified foreign keys bind each identity, memory version, scope and evidence object; normal runtime roles cannot update or delete these immutable rows.

Use the existing `RecordAttestation` API for both facts and memories. The engine determines the record kind from its retained identity. A memory attestation supplies the exact `RecordID`, `VersionID`, `ContentDigest`, permitted attestation kind, evidence ID, authenticated attestor and optional expiry. For direct recording, the attestor must match the trusted caller and be allowed for that kind by the memory's bound policy. Delegated client recording uses the separate verified flow below. Evidence is verified before recording. A caller-supplied person name or a confirmation attached to an earlier version cannot satisfy these checks.

`ApproveMemory`, `AcceptMemory` and `SupersedeMemory` retain the prerequisite attestation IDs used at acceptance. `GetMemory` evaluates only those bound IDs under the recorded policy. Expiry or withdrawal makes the accepted memory `ineligible` and withholds its body. Adding another attestation does not silently replace a withdrawn prerequisite; a corrected memory version needs its own attestations and governed acceptance/supersession.

Only the recorded attestor can withdraw its attestation through an authorized, idempotent `WithdrawAttestation` command. Withdrawal takes the memory record lock, advances its stream and tenant revisions, and appends an immutable event. Concurrent acceptance therefore cannot commit against a withdrawal that won the record lock first. Historical reads use the requested ledger revision and its recorded time, preserving eligibility before a later withdrawal or expiry.

Memory attestations and withdrawals participate in signed audit/export and event archive coverage. Their evidence references block disposal, and resolved memory reads verify bound attestation evidence before disclosure. Client confirmations additionally require the presentation/response artifacts described below. Direct recording authenticates the attestor through the host's trusted caller boundary; delegated client recording requires the verifier flow below.

Integration tests cover forged attestors, wrong/obsolete digests, expired submissions, cross-kind ID collisions, idempotent recording/withdrawal, unauthorized withdrawal, acceptance with valid prerequisites, current ineligibility after withdrawal, preserved historical eligibility and a separately validated replacement.


### Paginated memory history

`GetMemoryHistory` uses `HistoryPageQuery` and `HistoryPage`, with the same `EngineOptions.Reads` signing key and 1–100 page-size bounds as fact history. Pass the memory record ID returned by `ProposeMemory`, its scope, a purpose, and the prior page's `NextPageToken` when continuing. The first page pins both ledger and stream revisions; new proposals or lifecycle changes appear only in a fresh traversal.

```go
page, err := engine.GetMemoryHistory(ctx, caller, model.HistoryPageQuery{
    HistoryQuery: model.HistoryQuery{
        Scope: scope, RecordID: memoryRecordID, Purpose: "review_history",
    },
    Limit: 50,
})
```

Pages contain proposal, acceptance, supersession, rejection, dispute/resolution, retraction and attestation-withdrawal events that advance the memory record stream. Exact bodies remain available through authorized `GetMemoryVersion` reads. Standalone attestation recording and subtype publication do not advance a record stream; those events remain in the tenant change feed and signed audit exports.

Continuation tokens are bound to the authenticated caller, tenant, scope, record, purpose, page size, pinned revisions and record kind. Memory tokens cannot be used with `ListFactHistory`, or transferred to another principal. Current history permission is checked before reading and before disclosure. Missing or inconsistent stream events fail explicitly. No additional database migration is required.

Real PostgreSQL tests verify that a concurrent proposal stays outside an existing page sequence, a fresh traversal includes it, withdrawals remain visible, and tokens cannot cross caller or fact/memory boundaries.


### Memory projection and recall

`RecallMemories` accepts the same `model.RecallRequest` as `Recall`, returning typed `MemoryRecallResult` items. `Recall` continues to return facts only. Both vector and lexical Cortex adapters distinguish memory candidates, and managed rebuild verification includes accepted memory versions.

```go
memories, err := engine.RecallMemories(ctx, caller, model.RecallRequest{
    Scopes: []string{scope}, Query: "customer preferences", Purpose: "case_review",
    ValidAt: validAt, Limit: 10,
})
```

Memory recall resolves candidates in one ledger snapshot, then checks current access and evidence before disclosure. Only the eligible version at the requested valid time and knowledge revision can be returned. Disputed, retracted, stale-dependency and attestation-ineligible versions are excluded even when their old index rows remain. Returned bodies and dependency references come from the ledger, never index text. Historical recall additionally requires history access; dependency scopes require read/history access, and source evidence requires evidence access.

Refresh [runtime grants](scripts/runtime-role.sql) before running the updated projector. Indexing a memory requires the worker's current `can_read`, `can_read_history` and `can_read_evidence` permissions in the memory scope and every dependency scope. **Rebuild into a new generation for existing deployments:** older consumers may already have acknowledged memory events without indexing them. Restarting an existing consumer does not replay those events. Use the managed rebuild and verified cutover procedure above.

Coverage and generation requirements match fact recall. Candidate over-fetch is bounded and shared across fact and memory candidates, so filtering can return fewer results than requested; `Incomplete` describes ledger/index coverage, not exhaustive search. `PrepareContext` can opt into memory items as described below. Explicit refresh jobs are implemented below; automatic scheduling from dependency-change events is implemented below.

Integration tests exercise memory projection through the outbox, vector and lexical retrieval, fact/memory separation, tampered index text, evidence-permission denial, stale-index retraction filtering, historical recall, and rebuild verification.


### Prepared memory contexts

Apply forward-only migration `00017_memory_context.sql` and refresh [runtime/archive grants](scripts/runtime-role.sql). `ContextRequest.IncludeMemories` enables mixed retrieval:

```go
manifest, err := engine.PrepareContext(ctx, caller, model.ContextRequest{
    Command: model.Command{Key: commandKey, Purpose: "case_review"},
    Scope: scope, IncludeMemories: true,
    Query: model.RecallRequest{
        Scopes: []string{scope}, Query: "customer preferences", Purpose: "case_review",
        ValidAt: validAt, Limit: 10,
    },
})
```

Mixed contexts resolve fact and memory candidates in one ledger snapshot. `Limit` bounds their combined count. The v2 manifest retains facts in `Items`, memories in `Memories`, and the combined retrieval order in `Order`. Rendered bytes use `facts-memories-context-json-v1`, with `Facts`, `Memories` and `Order` fields. Both kinds carry the same selection revision. Retrieval identity, coverage, authorization receipts, exact bytes and digests are persisted before delivery. Fact-only requests and existing v1 manifests preserve their original format and digests.

Memory entries require current read, history, evidence and context-preparation permissions in their own scope and all dependency scopes. Those scopes participate in persisted context and decision access boundaries, scoped audit filtering and archive authorization. Retrieving a retained manifest rechecks read/history/evidence permissions; it preserves the originally supplied bytes without re-evaluating later eligibility. A retry returns the same committed manifest even after a dispute or index outage. A fresh context excludes newly disputed or otherwise ineligible memories.

`RecordDecision` accepts memory version references from the retained manifest in `SelectedItems` and `UsedItems`. Used items must be selected, and every selection must have been supplied. Separate tenant-qualified foreign keys bind fact and memory items to their respective immutable versions. Memory bodies may quote evidence, so metadata-only audit exports withhold memory-bearing contexts with an explicit gap; authorized full exports retain the exact bytes. Cross-scope contexts remain withheld from a single-scope export.

Tests cover mixed ordering and revision consistency, memory decision links, unauthorized and unsupplied references, preserved manifests after disputes, exclusion from fresh contexts, current evidence/history permissions, dependency-scope authorization, metadata redaction and signed full exports.


### Durable memory refresh proposals

Apply forward-only migration `00018_memory_refresh.sql` and refresh runtime/archive grants. Configure an approved extraction profile with `ParserVersion: "memory-refresh-json-v1"`. It uses the existing `ExtractionProvider` contract, concurrency/token budgets, bounded response handling, expiring leases, fenced completion, retries and retained model artifacts. Fact-extraction profiles remain separate.

`StartMemoryRefresh` schedules work for an exact accepted memory version whose dependency coverage is currently stale:

```go
job, err := engine.StartMemoryRefresh(ctx, caller, model.MemoryRefreshRequest{
    Command: model.Command{Key: commandKey, Purpose: "dependency_review"},
    Scope: scope, RecordID: memoryRecordID, VersionID: memoryVersionID,
    ProfileID: approvedRefreshProfile,
})
```

Use `workers.NewExtraction` through the outbox to handle `start_memory_refresh` notifications, or call `RunExtraction` explicitly with a trusted service caller. `GetJob`, `GetExtractionRun` and `RetryJob` support refresh jobs; their kind is `memory_refresh`. Replaying a completed delivery does not call the provider again.

The retained request includes the original immutable memory, bound subtype, source evidence, and current ledger timelines and versions for its original dependency records. The provider returns `MemoryRefreshOutput`: a new `Body` and exact `Dependencies`, or a `DeclineReason`. It cannot change the memory identity, subtype, effective interval or original source spans, remove dependency records, or introduce unsupplied versions. Eligibility is rechecked before proposing. A changed memory stream, retired original, restored dependencies, changed schema, ineligible replacement dependencies or provider decline produces a retained issue instead of a proposal. Fragmented or conflicting dependency coverage is not resolved by similarity or model confidence.

Successful work atomically appends a new **unaccepted** memory proposal and links it to the retained extraction result. It never approves or supersedes the original. The original remains stale until an authorized acceptance/supersession satisfies the normal policy, source and attestation requirements. Both versions remain available through authorized history reads.

The requester and worker need extraction/proposal permissions in the memory scope and read/history/evidence permissions across its dependency scopes. Retained refresh artifacts recheck those scope permissions before disclosure; full single-scope exports withhold cross-scope refresh payloads, and archive workers authorize all involved scopes. Request/response byte limits and profile identity remain pinned to each durable job. No production refresh model has been selected or evaluated.

Tests cover durable scheduling, request retention before provider invocation, corrected dependency provenance, strict output validation, worker routing, replay without duplicate provider calls, permission denial, proposals remaining unaccepted, governed supersession and preservation of the original body. Automatic scheduling from dependency-change events is available as described below. Hosts may also call `StartMemoryRefresh` explicitly.


### Automatic refresh scheduling

Apply forward-only migration `00019_refresh_triggers.sql` and refresh runtime/archive grants. Run a separate scheduling consumer with an immutable stream identity and explicit target-scope/profile bindings:

```go
scheduler, err := workers.NewMemoryRefresh(engine, trustedServiceCaller,
    "dependency-refresh-v1", "dependency_review",
    []model.MemoryRefreshBinding{{Scope: scope, ProfileID: approvedRefreshProfile}},
)
consumer := model.ConsumerKey{
    Tenant: tenant, Consumer: model.MemoryRefreshConsumer("dependency-refresh-v1"),
}
// Register consumer, compose workers.NewOutbox(queue, consumer, scheduler, options),
// then run it under the host's cancellation and shutdown lifecycle.
```

The scheduler handles fact acceptance, supersession, dispute/resolution, retraction and fact-attestation withdrawal events. It scans the configured scopes' dependency inventory at the canonical triggering revision, in pages of 100. `ScanMemoryRefresh` is an operational read API with an explicit event ID/revision and version-ID cursor; each page independently authorizes the target scope. A matching dependency additionally requires read/history access to the source scope. Unrelated tenant events do not require content access to their source scopes. Starting a job still checks every dependency scope and all normal refresh permissions.

Jobs use stable command keys derived from the consumer, event, target scope and memory version. Lost acknowledgements and new delivery fences therefore reuse committed jobs. Each job retains its trigger ID/revision, and a tenant-qualified foreign key links it to the ledger event. The scheduler skips only `ErrRefreshNotNeeded` (a retired or restored target); configuration, evidence, authorization and database failures remain visible to outbox retry/failure handling. Changing bindings or purpose requires a new stream identity, which replays from revision one.

Scheduling never invokes a model. A separately configured extraction consumer executes the resulting refresh jobs; its profiles must include the referenced approved refresh profiles. Scheduling coverage and extraction completion are separate statuses. Constructors start no goroutines and configure no production provider automatically. Event-driven scheduling does not detect the passage of time alone; use the periodic sweep worker below for expiry-driven staleness.

Tests cover real outbox scheduling, trigger provenance, replay without duplicate jobs, discovery pagination, forged revisions, tenant/permission boundaries, unrelated scope isolation, partial failure across more than 100 candidates, stable keys across delivery fences and explicit handling of obsolete targets versus operational failures.


### Periodic refresh sweeps

`workers.NewMemoryRefreshTicker` complements the event-driven scheduler by creating durable `sweep_memory_refresh` events. Use the same caller, stream, purpose and scope/profile bindings as the scheduling consumer:

```go
ticker, err := workers.NewMemoryRefreshTicker(engine, trustedServiceCaller,
    "dependency-refresh-v1", "dependency_review", bindings, time.Hour,
)
// Run under host supervision with a cancellable context:
err = ticker.Run(ctx)
```

Intervals are whole minutes from one minute through 24 hours. `Run` emits a tick immediately, then at UTC interval boundaries; it returns on cancellation or an error so the host can supervise recovery. `Tick(ctx, at)` supports explicit host scheduling. Stable command keys make retries within the same UTC bucket idempotent under the same trusted caller identity, including partial failures across scopes. A restart sweeps the current inventory; it does not replay every missed clock interval. Constructors start no goroutines. Change the stream identity when changing the cadence, bindings, purpose or approved profile configuration.

Each sweep retains its scope, stream, scheduled bucket, interval and approved profile digest in the immutable ledger event. The existing scheduling consumer scans memory dependency inventory at that event's revision, then checks current eligibility before creating jobs. Expired fact attestations can therefore make a memory stale and schedule refresh even when no new fact event was appended. `StartMemoryRefreshSweep` exposes the same durable event operation for an authorized host. It rejects future or unaligned buckets, unapproved profiles and invalid intervals. No new migration beyond `00019` is required.

The scheduled bucket identifies a run; eligibility uses current time when processed. Restored or retired targets are skipped. Expiry does not authorize fabricating replacement evidence or accepting a new memory: the provider may decline, and any proposed dependency must still pass eligibility checks. Later sweep buckets can schedule additional jobs while a memory remains stale, so choose the cadence with the configured model/token budgets and review workload in mind. Job retries and consumer retries remain explicit and separately visible.

Integration tests expire a bound fact attestation without advancing the ledger, verify that the memory becomes stale, then verify that a subsequent durable sweep schedules exactly one job and that replaying the tick creates no duplicate sweep. A provider decline is retained without creating a replacement. Worker tests cover partial tick failures, stable bucket keys, stream/profile routing, cancellation and interval bounds.


### Memory extraction from conversations

Apply forward-only migration `00020_memory_extraction.sql` and refresh runtime/archive grants. Configure an approved profile with `ParserVersion: "memory-extraction-json-v1"`. `StartExtraction` accepts memory-only requests with explicit application-owned target IDs and registered subtype/schema bindings:

```go
job, err := engine.StartExtraction(ctx, caller, model.ExtractionRequest{
    Command: model.Command{Key: commandKey, Purpose: "conversation_review"},
    Scope: scope, ConversationID: conversationID, ProfileID: memoryProfile,
    MemoryTargets: []model.MemoryExtractionTarget{{
        ID: "conversation-summary", Subtype: "episode", SchemaVersion: "v1",
        Dependencies: []model.MemoryDependency{dependency},
    }},
})
```

A request contains either fact subjects/predicates or memory targets. Targets are limited to 64, with at most 64 distinct dependency versions across the job. Dependencies are optional for memories grounded directly in conversation evidence. When supplied, the host binds exact fact record/version IDs, scope, content digest and dependency label; the provider cannot remove, replace or invent them. Tenant-qualified foreign keys preserve subtype and dependency bindings. The retained model input includes the pinned definitions, dependency versions and exact authorized conversation bytes.

The provider returns `MemoryExtractionOutput` with `Candidates`. Each candidate contains the bound `ID`, `Subtype`, `SchemaVersion`, UTF-8 `Body`, effective `Valid` interval, exact `Sources`, `Uncertainty`, short `Rationale` and diagnostic `Confidence`. Unknown identities or schemas, unsupported uncertainty, invalid spans and assistant/system-only support become review issues. Source classes must satisfy the bound subtype's acceptance policy. Confidence never grants acceptance authority. Bodies are capped at 64 KiB and require a known interval start.

Valid candidates atomically create immutable **unaccepted** memory proposals linked to their extraction attempt. All bound dependencies are attached to each target's proposal. Duplicate candidate IDs are rejected for review; exact matches to retained versions produce duplicate links. Different content overlapping accepted or disputed memory intervals produces `overlapping_different_content` links, without declaring a factual contradiction or selecting a winner. Current subtype changes after enqueueing produce `schema_changed` issues. Approval, attestations, dependency eligibility and supersession remain separate governed operations.

The requester and worker need the usual extraction/proposal/evidence permissions, plus current read/history/evidence access across the memory and dependency scopes. Refresh requests include each source evidence ID once, in first-citation order, while retaining every original citation span. Distinct evidence IDs remain separate even when they refer to the same object. Input-size accounting counts each included evidence body once and still checks the full serialized envelope. Retained artifacts preserve those content scopes for later reads, scoped audit filtering and archive authorization. Fact and memory jobs share the existing durable queue, profile pinning, byte/candidate/token budgets, retries, leases and fencing. Use the same extraction worker and inspection APIs; the job kind is `memory_extraction`. Explicit original-byte input ranges are supported for these jobs too. Deterministic chunk planning is also available. Required conversation redactions can be combined with approved whole dependency-payload omission. Memory refresh supports separate whole-field omission of dependency payloads, the original memory payload and evidence bodies. Applying conversation omission ranges alone cannot redact those fields, so refresh profiles still reject that conversation-specific setting. Range-based redaction within dependency/refresh fields and approved production model evaluations remain unfinished.

Tests cover source and identity rejection, immutable dependency binding, two proposals committed in one run, duplicate and overlap links, proposals remaining unaccepted, separation of proposer/approver, schema changes, retained-input verification, dependency permissions and cross-scope artifact redaction.


## Scope and subject administration

Apply forward-only migration `00021_identities.sql`, refresh [runtime/archive grants](scripts/runtime-role.sql), and publish/pin the updated OpenFGA model. `PutScope` and `PutSubject` require `can_manage_identities` (scope administrator plus parent-tenant membership). Provision those OpenFGA relations before registering the scope; the engine does not create authorization tuples.

```go
scopeResult, err := engine.PutScope(ctx, administrator, model.ScopeRequest{
    Command: model.Command{Key: "register-case", Purpose: "case_setup"},
    Definition: model.ScopeDefinition{ID: scope, Label: "Customer case"},
    Reason: "Open case",
})
if err != nil { return err }
_ = scopeResult // StreamRevision is the metadata revision, initially 1.

subjectResult, err := engine.PutSubject(ctx, administrator, model.SubjectRequest{
    Command: model.Command{Key: "register-subject", Purpose: "case_setup"},
    Definition: model.SubjectDefinition{
        Scope: scope, ID: subjectID, Kind: "customer", Label: "Case customer",
        Lookup: map[string]string{"crm": externalOpaqueID},
    },
    Reason: "Bind verified CRM identity",
})
if err != nil { return err }
_ = subjectResult
```

IDs are issued by the application. Labels are display metadata and never resolve identities. Subject kind is immutable, and a subject requires a registered parent scope. Each subject can hold up to 16 qualified lookup identifiers. Namespaces and values use the engine's opaque-ID format (1–128 ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit). Labels are nonempty UTF-8 up to 256 bytes; an administrative reason is required and bounded to 2,048 bytes.

Use `ExpectedRevision: 0` to create metadata and the returned `StreamRevision` for updates, with a new command key. Retrying an identical command returns its receipt; changing it conflicts. Each update replaces the complete metadata, including the lookup map. Removed lookup identifiers cease resolving; historical versions retain their original values. A qualified lookup value belongs to at most one subject per tenant and scope, including under concurrent claims. A collision returns `ErrConflict` without disclosing the other identity. No automatic identity merge or reassignment is performed.

`GetScope` and `GetSubject` take `model.IdentityQuery{Scope: scope, ID: subjectID, Purpose: "case_review"}`; omit `ID` for `GetScope`. Revision zero returns current metadata; a positive `Revision` selects an exact immutable metadata revision, not a tenant ledger revision. Current reads and `LookupSubject` require `can_read`; historical reads additionally require `can_read_history`. Lookup takes `model.SubjectLookupQuery{Scope: scope, Namespace: "crm", Value: externalOpaqueID, Purpose: "case_review"}` and uses exact matching. Missing identities return `ErrNotFound` after authorization.

Metadata history is encrypted and linked to its governed ledger event, authorization receipt and atomic outbox notification. Subject lookup uses tenant/scope-qualified digests in its searchable projection; those digests are not anonymization. Normal runtime roles can advance current revisions and lookup projections but cannot modify immutable history. Scoped audit exports and event archives include the retained metadata revisions.

**Migration boundary:** migration 21 introduces metadata administration; migration 22 enforces registration on new writes and provides an adoption path for pre-registry records, described below. Administration does not establish verified real-world ownership of an identity.

Integration coverage includes authorized creation/update, exact retry, changed-command and stale-revision conflicts, immutable kind, lookup replacement, concurrent duplicate claims, historical reads, tenant isolation, permission denial, rollback of rejected registrations, and audit/archive retention.


## Mandatory identity registration and legacy adoption

Apply `00022_identity_enforcement.sql` and refresh [runtime/archive grants](scripts/runtime-role.sql). The updated PostgreSQL adapter refuses startup without this migration. New deployments must register scopes and subjects through the administration API before publishing policies, ingesting conversations/evidence, or proposing facts. Scope and subject registration use separate administrative permission; proposer access cannot create identities implicitly.

Every new engine mutation except scope registration requires an existing scope. Fact record creation/lifecycle locks, fact attestations, snapshot selectors and fact-extraction subject bindings require an existing subject in that same tenant and scope. Missing registration returns `ErrPrecondition` after authorization. Snapshot selectors may still return a missing fact for a registered subject. Historical read APIs continue to expose authorized pre-registry history without rewriting it. Identical committed-command retries retain the existing authorization and receipt semantics.

PostgreSQL foreign keys independently enforce scope registration on ledger events, records, policies, evidence, conversations, extraction jobs, retention scope guards, snapshot items, and context/decision scope inventories. Fact records have a generated subject reference; memory records have no fact subject. Newly scheduled fact-extraction jobs persist tenant/scope-qualified subject bindings with foreign keys. Before provider execution, the engine rechecks every declared subject, including those in pre-registry queued jobs. An unregistered legacy job remains blocked until its identities are adopted; registering an identity does not automatically approve any extracted proposal.

For an existing deployment, the migration installs the foreign keys as `NOT VALID`: PostgreSQL enforces new references immediately while allowing old rows to remain intact. No label, subject kind, verified ownership, historical registration event, or external lookup binding is fabricated. Use this rollout procedure:

1. Schedule the migration with the database owner and pause writers/workers during the upgrade. Adding the stored generated column can rewrite the records table; assess its size and maintenance window first. Take and verify a backup under the deployment's recovery procedure.
2. Apply the migration, refresh grants, and start the updated engine with the published/pinned OpenFGA model. Reads remain available; writes against unregistered identities fail closed.
3. Run `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/identity-inventory.sql` with the migration-owner connection. It reports missing scopes, missing fact subjects, and pre-registry pending/running fact jobs that need review. The inventory is read-only and includes opaque IDs, not personal metadata.
4. Have the application/domain owner supply approved labels, subject kinds and any verified lookup mappings. A scope administrator calls `PutScope` followed by `PutSubject`, preserving the original tenant/scope/subject IDs, with a documented adoption reason and stable command keys. These are new, governed registration events; existing fact IDs, ciphertext and historical revisions remain unchanged.
5. Review the declared subjects in flagged legacy jobs using an authorized host migration process with the ledger's `ExtractionTx.ExtractionJob` reader and retained encryption keys. Adopt those IDs before resuming the jobs. The SQL inventory cannot decrypt job payloads. Old missing-fact snapshot selectors may also contain IDs that never became records; their immutable history stays intact, and reuse in a new snapshot requires registration.
6. Run `psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/validate-identities.sql`. It validates every new foreign key in one transaction and fails if legacy references remain unregistered. Re-run the inventory and resolve failures before treating adoption as complete, then resume workers and normal writes.

Custom ledger adapters must implement the new `Tx.RequireScope` and `Tx.RequireSubject` contracts; `LockRecord` must check subject registration before creating or locking a record. The PostgreSQL adapter enforces these checks and the database constraints. Registry identities cannot be deleted through the runtime API, so a successful check cannot race a supported identity-removal operation.

Integration tests cover preservation and governed adoption of pre-registry records, refusal to validate unadopted history, successful constraint validation after adoption, direct-SQL foreign-key enforcement, tenant-qualified subjects, rejection of unknown proposal/extraction subjects, and all existing ingestion, lifecycle, snapshot, retention, outbox and retrieval scenarios. Production identity mapping and migration sign-off still require the named application/domain, security and database owners; fixture mappings are not a production identity source.


## Client confirmation artifacts

Apply forward-only migration `00023_confirmation_artifacts.sql` and refresh runtime/archive grants. `RecordAttestation` now requires `Confirmation` for `Kind: "client_confirmation"`, for both facts and memories:

```go
_, err := engine.RecordAttestation(ctx, authenticatedClient, model.AttestationRequest{
    Command: model.Command{Key: confirmationCommandID, Purpose: "client_confirmation"},
    Attestation: model.Attestation{
        ID: attestationID, Scope: scope, RecordID: proposed.RecordID,
        VersionID: proposed.VersionID, ContentDigest: proposed.ContentDigest,
        Kind: "client_confirmation", EvidenceID: responseEvidenceID,
        Attestor: authenticatedClient,
        Confirmation: &model.ClientConfirmation{
            Presentation: model.ArtifactReference{EvidenceID: presentationEvidenceID, Digest: presentationDigest},
            Response: model.ArtifactReference{EvidenceID: responseEvidenceID, Digest: responseDigest},
            PresentationVersion: "client-ui-v1",
            PresentedAt: presentedAt, RespondedAt: respondedAt,
        },
    },
})
```

Register the exact presentation and affirmative response as distinct immutable evidence objects in the record's scope before recording the attestation. The presentation should preserve what the client actually received, including the exact version being confirmed, wording, units and relevant context. The application owns its rendering and response capture; the engine verifies references, bytes and digests, but does not interpret an image, PDF, natural-language response or model output as consent. `PresentationVersion` identifies the host's rendering/template contract. Evidence locators and versions remain available through normal evidence and audit APIs.

The trusted caller must be the authenticated human attestor, permitted for `client_confirmation` by the version's bound acceptance policy. Recording requires `can_attest` and `can_read_evidence`; the evidence authorization decision is retained in the encrypted attestation as server-owned `EvidenceAuthorization` (leave it unset in requests). Caller-supplied names, service principals and service-initiated identities do not establish client consent. A service must use the separate verified delegated-recording API below.

`EvidenceID` must equal the response evidence ID. Presentation and response evidence IDs must differ. Timestamps must be nonzero, at microsecond precision, and ordered; future response times beyond one second are rejected. Expiry, when provided, must follow the response and still be in the future at recording. A response does not satisfy a prerequisite before its recorded response time. Other attestation kinds must omit `Confirmation`.

A response object can bind only one attestation in a tenant, including across fact and memory versions. Reusing it with a new command/attestation, or registering the same object locator/version under a different evidence ID, conflicts. Identical authorized command retries return the existing receipt. A replacement version or a renewed confirmation needs a fresh response; this API does not provide a multi-version batch-consent contract.

Acceptance and supersession verify the selected prerequisite evidence before acquiring record/tenant locks, then require the transaction to use only the verified attestation IDs. A concurrently added prerequisite can require a retry. Resolved fact and memory reads verify their bound attestation evidence; missing or corrupted confirmation bytes make the result ineligible. Presentation and response references have tenant/scope-qualified foreign keys, block evidence disposal, and are included in scoped audit exports and event archives. Withdrawal preserves the original artifacts and adds its usual immutable event.

Legacy client confirmations without these artifacts remain readable in retained history but no longer satisfy acceptance prerequisites, including when reconstructing eligibility. Their previously accepted versions can therefore become ineligible. Obtain a new valid confirmation and perform the governed lifecycle action required to replace the old acceptance; adding an unrelated attestation never silently repairs its bound prerequisites. No migration fabricates a presentation or client response.

Tests exercise fact and memory confirmation, missing/mismatched artifacts, authorization denial, timestamp validation, legacy prerequisite rejection, response replay through evidence aliases, exact command retry, acceptance/read integrity failures, disposal blockers and archive inclusion. Production acceptance requires the application owner to approve the presentation/capture contract and security owner to verify the authenticated client boundary; no external client-identity deployment is configured by this change.


## Trusted delegated client recording

Apply `00024_attestation_delegation.sql`, refresh runtime/archive grants, and publish/pin the updated OpenFGA model. Configure `EngineOptions.AttestationIdentity` with a host-owned `AttestationIdentityVerifier` and a pinned authority ID:

```go
engine, err := factsengine.NewWithOptions(ledger, authorizer, evidenceStore, model.EngineOptions{
    AttestationIdentity: model.AttestationIdentityOptions{
        Verifier: approvedIdentityVerifier,
        Authority: "approved-client-idp",
        Timeout: 5 * time.Second,
        MaxAuthenticationAge: 15 * time.Minute,
    },
})
// The request contains the same exact confirmation/version/artifact fields
// as direct RecordAttestation, with Attestor set to the verified human client.
result, err := engine.RecordDelegatedAttestation(ctx, trustedServiceCaller,
    model.DelegatedAttestationRequest{
        AttestationRequest: confirmationRequest,
        GrantID: opaqueConfirmationGrantID,
    })
```

`GrantID` is a bounded opaque reference, never a bearer token, raw JWT, cookie or identity assertion. The verifier must independently validate that grant using an approved authentication/session or signed-grant system, and bind the entire `AttestationIdentityRequest`: recording service and tenant, human client, scope, command/purpose, attestation ID, exact record/version/content digest, presentation/response references and digests, timestamps and expiry. Use `model.AttestationIdentityDigest(request)` only after validating those bindings. Echoing the requested client or computing this digest without authenticating the grant is not a verifier implementation.

The verifier returns `AttestationIdentityReceipt` with its pinned `Authority`, stable single-use `BindingID`, exact `RequestDigest`, authenticated human `Attestor`, and authentication/verification/expiry times. The engine rejects identity, authority or digest mismatches; stale/future verification; expired or excessively long receipts; and responses outside the authenticated interval. Authentication must precede the response, which must precede verification. Verification must be within 30 seconds (at most one second of future clock skew), and receipt expiry must follow verification and be no later than five minutes after it. Authentication age defaults to 15 minutes and is configurable from one minute through 24 hours. Verifier timeout defaults to five seconds and is bounded to 1–30 seconds. Implementations must honor context cancellation. `ErrDenied` reports an invalid grant; other verifier errors/timeouts fail closed as `ErrUnavailable` without exposing provider details.

The recording caller must be a service. It needs `can_record_delegated_attestation`, provided by the service-only `confirmation_recorder` relation, plus `can_attest` and `can_read_evidence`. The verified client independently needs `can_attest` and `can_read_evidence`. Every permission requires tenant membership. If the service caller carries an initiating principal, it must match this client; an initiator string alone never enables delegation. The version's acceptance policy checks the human attestor, not the recording service.

The engine retains the recorder, human attestor, verification receipt and all five authorization decisions in the encrypted attestation. `Recorder`, `Delegation`, `EvidenceAuthorization`, `ReviewAuthorization` and `RecordedAt` are server-owned fields and must be omitted from requests. A tenant-qualified unique binding registry prevents reusing an authority/binding ID for another attestation, even if a verifier approves changed input. Recording, binding consumption, immutable artifacts, ledger event, receipt and outbox insertion commit atomically. Identity expiry and authorization freshness are checked again after append-lock waits, before commit, and before returning an existing receipt. An identical command retry must pass fresh authorization and verification and returns the original committed result.

The direct `RecordAttestation` API still requires the caller to be the attestor. `WithdrawAttestation` is performed by the authenticated client who owns a delegated confirmation; the recording service receives no withdrawal right. Actor comparison includes tenant, principal kind and initiator, so a same-named service cannot impersonate a human. Delegated withdrawal is not exposed by this API. Verification receipts are retained with scoped audit and archive payloads; they prove the checks observed at recording time, not perpetual authorization or automatic revocation when an external session ends.

Integration tests cover facts and memories, explicit verifier configuration, client/recorder denial before provider calls, grant-subject mismatch, exact retries, durable binding replay rejection with fresh response artifacts, rollback when a grant expires during an append-lock wait, policy binding to the client, archive identity lineage and direct client withdrawal. Unit tests cover receipt authority, tenant, principal kind, command/version binding, time limits and configuration bounds; live OpenFGA tests exercise the new service-only permission and membership requirement.

**Production acceptance:** the bank's security/identity owner must implement and approve the verifier, issuer trust and key/session handling, revocation policy, authentication-age limit and clock synchronization. The application owner must bind the actual presentation/response capture to that grant. The engine supplies and tests the verifier contract, an HTTPS authority adapter and optional service configuration; it does not deploy Ory Kratos, a gateway, an issuer or a production identity authority. Production approval of those external integrations remains a release requirement. PostgreSQL-backed tests cover delegated-attestation behavior through embedded and remote operations; those fixtures do not validate a production issuer or session service.


## Policy-controlled review and attestation lifetime

Acceptance policies can require a distinct human review alongside automated acceptance. Add `human_review` to the immutable policy's `Required` list; the accepting service cannot replace it with its own approval or validation claim:

```go
policy := model.AcceptancePolicy{
    Scope: scope, ID: "reviewed-v1", Mode: "automated",
    AcceptingServices: []string{"approved-acceptor"},
    SourceClasses: []string{"customer", "bank_record"},
    Required: []model.AttestationRequirement{
        {Kind: "human_review", Principals: []string{"approved-reviewer"},
            RequireExpiry: true, MaxAgeSeconds: 86400},
        {Kind: "application_validation", Principals: []string{"validator"}},
    },
}
```

The reviewer calls `RecordAttestation` with `Kind: "human_review"`, the exact record/version/content digest, a registered review-report `EvidenceID`, their authenticated human `Attestor`, and any required `ExpiresAt`. Both `can_attest` and `can_approve` are required. The reviewer must differ from the proposer and from the initiating human behind a proposing service. A service account cannot supply a human-review attestation, and delegated client-confirmation grants do not authorize review. The engine retains the reviewer authorization receipt and report reference; the application owns the review report's domain-specific conclusions.

`RequireExpiry` rejects an attestation without an explicit expiry, both at recording and prerequisite evaluation. `MaxAgeSeconds` imposes an additional lifetime measured from the engine's immutable ledger recording timestamp, independently of the caller's chosen expiry. Zero leaves this age bound unset; positive values range from one second through 365 days. The effective eligibility end is the earlier of explicit expiry and the maximum-age boundary. The boundary is exclusive: an attestation at exactly its maximum age is ineligible. Acceptance rechecks its bound prerequisites after append-lock waits, inside the transaction; expiry during contention rolls the command back.

New fact and memory attestations retain server-owned `RecordedAt`; callers must omit it and `ReviewAuthorization` from requests. A legacy attestation without a trusted recording timestamp cannot satisfy a positive maximum-age requirement. No timestamp is fabricated from source time. Policy age controls apply to all attestation kinds, including client confirmation, source verification, application validation and human review.

Acceptance, supersession, current reads, historical reads, snapshots and memory dependency evaluation use the same prerequisite rules. Historical eligibility uses the selected ledger evaluation time. Once an accepted version's bound review expires, it becomes ineligible on current reads; a new unbound review does not silently alter the original acceptance. Replacement versions require their own review/validation artifacts, even when their typed content matches the old version.

An omitted policy `Mode` now canonicalizes to `manual`; missing policy definitions still fail closed. Policies remain immutable and schema-bound. Registration bounds source classes and accepting-service lists to 64 entries, each requirement to 64 distinct principals, and requirements to the four supported distinct kinds. Identifiers must use the opaque-ID format. Assistant/system source classes cannot be configured as factual authority; fact acceptance now explicitly excludes both, matching memory acceptance.

No SQL migration is needed for these additive encrypted payload fields. **Upgrade every engine reader and writer before publishing policies with the new controls.** Older binaries cannot enforce fields or attestation kinds they do not understand. Existing policies without the new fields retain their original lifetime rules. Publish a new policy ID and use the existing governed predicate/memory-type migration mechanism to bind new proposals to it; callers cannot choose a weaker policy at acceptance.

Tests cover manual defaults and canonical retries, required review before automated fact/memory acceptance, proposer separation, review permission denial, mandatory expiry, maximum-age expiration despite a later explicit expiry, historical eligibility, exact expiry boundaries, legacy timestamps and replacement-version isolation. The domain/risk owner must approve permitted reviewers and lifetime values; operational clock synchronization remains necessary for time-based controls.


## Extraction cost reservations

Apply forward-only migration `00025_extraction_cost.sql` before starting the updated PostgreSQL adapter. Token reservations and concurrent-lease limits now report `ErrQuota` when admission would exceed a configured limit; provider/dependency failures remain `ErrUnavailable`. Cost budgeting adds a separate integer reservation to the same atomic tenant admission transaction:

```go
profile.CostUnit = "USD-microunits" // Host-defined unit; no built-in pricing or FX.
profile.MaxCostUnitsPerAttempt = approvedWorstCaseCost
options.Extraction = model.ExtractionOptions{
    Profiles: []model.ApprovedExtractor{{Profile: profile, Provider: provider}},
    MaxConcurrentPerTenant: 4,
    DailyTokenBudget: approvedDailyTokenLimit,
    CostUnit: profile.CostUnit,
    DailyCostBudgetUnits: approvedDailyCostLimit,
}
```

The cost unit and positive per-attempt cap are pinned into the approved profile digest and durable job. When cost budgeting is enabled, every configured profile must use that unit and have a cap no greater than the daily cost limit. Empty unit and zero cost fields retain token/concurrency-only operation for tenants that have never enabled a cost meter. A profile price/cap change changes its digest: queued jobs cannot silently adopt new pricing. Use a new approved profile and new jobs when the old bound is no longer valid.

Each provider attempt reserves its maximum token allowance and cost cap **before** the external call. Tokens, cost, active-lease admission, attempt identity, ledger event and command receipt commit together. Concurrent workers share the tenant row lock and cannot admit spending beyond the configured bound. A rejected admission rolls back without consuming a job attempt or calling the provider. Failed calls, malformed responses, timeouts and abandoned leases retain their reservations because they may already have incurred a charge. A new attempt reserves again; a completed-job or identical committed-command retry does not.

Budgets are independent per tenant and aggregate all extraction scopes/profiles, including fact extraction, memory extraction and memory refresh. Counters reset on the first admitted transaction of a new UTC day. The budget belongs to the attempt's admission day; provider billing dates can differ. `GetExtractionRun` exposes the immutable attempt's `Reservation`, including the canonical UTC day, token/cost amounts, accounting unit and applied limits. These fields are retained in the ordinary encrypted attempt, audit and archive records. They are reserved capacity, not reported invoice amounts.

An enabled tenant meter rejects attempts from an unpriced worker or a different cost unit, even after daily rollover. Enabling a cost meter after unpriced attempts in the same day is rejected; begin on a fresh UTC day so unmeasured earlier calls are not represented as cost-controlled spending. The accounting unit cannot be changed through ordinary extraction calls. Changing units requires an explicitly planned administrative migration and reconciliation.

Daily limits and concurrency limits remain trusted host configuration, applied at admission rather than chosen by callers. Keep those values consistent across replicas. A reviewed configuration increase can admit previously quota-blocked work. Budget rejection leaves the extraction job pending; the outbox consumer still follows its existing backoff/attempt-limit policy and may need an operator retry after capacity becomes available. This change does not introduce automatic daily consumer resets or refunds.

Upgrade every extraction worker before enabling cost accounting; older workers do not implement these checks. Existing runtime table grants cover the added quota columns. Custom `ExtractionRunTx` adapters must implement the updated `BeginExtraction` contract with `ExtractionCostBudget` and return `ExtractionAdmission` with its exact reservation metadata.

Integration tests cover failure reservations, exact completed retries, quota rejection without attempt consumption, budget increases, tenant isolation, concurrent admission, UTC rollover, activation after an unpriced day, incompatible/disabled meters and integer-overflow boundaries. Configuration tests reject missing, mismatched, negative and oversized caps and verify profile-digest pinning.

The model/provider and cost-policy owners must approve the accounting unit, per-attempt upper bounds, daily limits and provider-side enforcement. Include input/output pricing, fixed fees and retry behavior in a conservative bound, and reconcile actual billing externally. The engine cannot guarantee an invoice ceiling if a provider exceeds its approved bound or continues charging after cancellation. No production prices, provider adapter or billing reconciliation service are configured here.
