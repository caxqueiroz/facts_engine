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
| [IdentityService](proto/memory/v1/identities.proto) | PutScope, PutSubject, GetScope, GetSubject, LookupSubject |
| [ContextService](proto/memory/v1/contexts.proto) | PrepareContext, GetContextManifest, RecordDecision, GetDecisionReceipt |
| [RetrievalService](proto/memory/v1/retrieval.proto) | Recall, RecallMemories, WaitForProjection |
| [CalculationService](proto/memory/v1/calculations.proto) | RecordCalculation, GetCalculationReceipt |
| [AuditService](proto/memory/v1/audit.proto) | ListChanges, StartAuditExport, GetAuditExport |
| [SnapshotService](proto/memory/v1/snapshots.proto) | CreateFactSnapshot, GetFactSnapshot |
| [EvidenceService](proto/memory/v1/evidence.proto) | RegisterEvidence, RegisterRedactedEvidence, GetEvidenceMetadata, GetEvidenceContent |
| [DefinitionService](proto/memory/v1/definitions.proto) | RegisterAcceptancePolicy, RegisterPredicateVersion, RegisterMemoryType |
| [IngestionService](proto/memory/v1/ingestion.proto) | IngestConversation, GetConversation |
| [ExtractionService](proto/memory/v1/extraction.proto) | StartExtraction, PlanExtractionChunks, GetJob, RetryJob, GetExtractionRun, StartMemoryRefresh |
| [AttestationService](proto/memory/v1/attestations.proto) | RecordAttestation, WithdrawAttestation, RecordDelegatedAttestation |

The main specification API groups are available through this handler. Retention administration and refresh scheduling still use embedded/worker interfaces; standalone service deployment and full transport parity remain unfinished.

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

`OperationsService` is disabled unless `EngineOptions.Operations` supplies a host-configured authorization scope, administrative rebuild store, routed rebuild index and outbox progress port. `can_operate` on that configured scope grants tenant-wide projection control; clients cannot choose an easier authorization scope. Use separately provisioned administrative credentials, and provision destination databases/routes before requesting a rebuild.

`GetProjectionStatus` reports an explicit generation (or the active generation when omitted), active epoch, rebuild state/fence/proof, consumer progress and the observed ledger revision. It marks candidate, lagging, failed or non-active coverage incomplete and rejects active-pointer changes during its read. This is an operational observation, not a snapshot shared with a later search.

`RebuildIndex` accepts `start` (1–100 maximum attempts), `verify`, or fenced `resume`, `activate` and `abandon` actions. Start identity is the generation; changed attempt budgets conflict. Projection workers must separately replay the tenant stream. Verify uses the existing bounded inventory verifier; activation requires the verified fence. The host's configured RPC deadline bounds the action. Transitions retain the full verified caller and authorization receipt in encrypted operational events, and PostgreSQL rejects stale authorization before and after each transition. A timeout or destination-gate failure can leave durable partial progress: inspect status and retry the same action/fence. An unprovisioned destination may remain unavailable even after abandonment was recorded. These APIs do not create databases, launch workers or erase data. `RetryJob` remains in `ExtractionService` with its existing operational permission and fence checks.

`IdentityService` registers governed scopes and subjects before evidence/fact ingestion. Writes require `can_manage_identities`, a command, expected metadata revision and reason. Scope and subject labels are display text, not lookup keys. Subject kind is immutable. Up to 16 namespace-qualified exact external identifiers may be registered; uniqueness is enforced per tenant and scope, and updates replace current lookup bindings while preserving history. `GetScope` and `GetSubject` select current metadata with revision zero or immutable history with a positive revision (requiring history permission). Reads and lookup recheck access before disclosure.

Definition publication uses the same administrative permissions and immutable version activation as embedded calls. Typed constraints preserve presence for explicit false, integer zero and empty optional bounds; cross-field constraints and migration rules are validated by the engine. Acceptance policies retain source classes, permitted accepting services, required attestors, expiry requirements and maximum age.

Direct attestation requests contain no actor or verification receipts: the handler assigns the authenticated caller as attestor. Delegated confirmation is a separate operation carrying an opaque grant ID and an **untrusted claimed client principal**. The engine requires its independently configured `AttestationIdentityVerifier` to bind that claim to the recorder, tenant, command, exact version/digest, confirmation artifacts and times. A transport identity verifier alone does not enable delegated attestations. Retained review/delegation receipts remain server-owned. These operations apply to fact and memory versions using the engine's existing rules; withdrawal immediately changes current eligibility when the attestation is required.

Evidence registration takes an immutable object reference already accessible through the host's evidence store; it does not upload bytes or provision storage credentials. The engine verifies the referenced bytes and their digest before registration. Content reads require their own permission, independently of metadata access. Redaction registration requires an approved policy in the engine options, validates at most 256 omission ranges against both objects, and inherits source authority, classification and retention. Ordinary registration rejects supplied derivative provenance. Evidence objects are limited to 16 MiB; set the handler's response-byte limit to accommodate content plus metadata when serving large objects.

Conversation ingestion binds 1–128 immutable UTF-8 message references, source revisions, speaker roles/classes, timezone and source metadata. Each message is limited to 1 MiB and the conversation to 16 MiB. References must already be accessible through the host's evidence store.

Extraction requires approved profiles configured through engine options; execution also requires an injected provider. Clients select a profile ID and may bind its expected digest; prompts, model parameters and provider credentials are host-owned. Fact selectors and memory targets are mutually exclusive. Explicit original-byte ranges and approved redacted evidence IDs pass through the shared validation and provenance checks. Chunk planning is an authorized preview and does not reserve quota.

`StartExtraction` and `StartMemoryRefresh` commit durable jobs and outbox events. A separately started worker invokes the provider, retains exact request/response artifacts, applies budgets and records proposals for review. Scheduling does not call the provider or accept candidates. `GetJob` reports status; `RetryJob` requires operational authorization and the expected fence. `GetExtractionRun` takes an attempt ID and returns authorized retained artifacts, digests, usage/reservations, validation issues, relations and proposal IDs. Worker ownership nonces stay internal. Configure response-byte limits for the approved artifact sizes. Production providers, evaluations and process supervision remain host responsibilities.

`Recall` and `RecallMemories` accept 1–8 distinct scopes, a query of at most 4096 bytes, a valid-time boundary and a result limit of 1–20. The engine revalidates candidates against the authoritative ledger and permissions before returning content. Scores express relevance. Results include the selected generation, ledger/indexed revisions and an explicit `incomplete` flag. Historical reads require history permission; generation or minimum-indexed-revision requirements fail with `unavailable` when unmet. `WaitForProjection` binds a receipt to a generation and revision, rechecks permissions and respects the RPC deadline (also capped at two minutes by the engine). It occupies an admitted request slot until completion; hosts should set timeout and concurrency limits accordingly.

`PrepareContext` persists the exact rendered bytes before returning them. Its command purpose must match the query purpose, and its scope must be among the query scopes. Optional mixed fact/memory contexts retain explicit item order and dependency-bound provenance. Responses include the original request, retrieval identity, authorization receipts, canonical/redaction versions, digests and selection/index/creation revisions. `GetContextManifest` rechecks retained-content permissions; command retries preserve the original bytes even after lifecycle changes or index outages. The rendered-byte limit is 8 MiB; configure transport response limits for those bytes plus metadata and encoding overhead.

`RecordDecision` records an application's actual use of a context or snapshot; it does not execute the decision. Exactly one source is required, used items must be selected, and selected versions must belong to the retained source. Model methods require exact model request/response artifact references; rule methods omit them. Optional calculation and action-receipt references, input/output digests, method versions and decision time are retained. External decision identity is idempotent per tenant/application, and changed content conflicts. `GetDecisionReceipt` rechecks source and evidence permissions. Context and decision digests use the engine's canonical representation, not Protobuf encoding.

Calculation recording binds externally produced input/output artifacts to an immutable snapshot; it never executes the calculation. The external run identity remains idempotent per tenant and application across command retries. Reads recheck snapshot and evidence access.

`StartAuditExport` commits a durable job; an explicitly started embedded audit worker builds and signs the bundle. Pending results omit the artifact and bundle. `GetAuditExport` rechecks scope and evidence permissions. Its completed bundle includes typed metadata, exact signed `manifest_json` bytes and exact `payload_json` entry bytes. Verify the signature using independently trusted keys, then verify entry identities, ordering, counts and digests against the signed manifest. Go clients can decode `manifest_json` into `model.AuditManifest`, preserve each raw entry payload and signature in a `model.AuditBundle`, and call `adapters/audit.Verify`. Protobuf serialization is not the signed encoding. Configure server and client response limits for bundle sizes and encoding overhead; audit payloads may exceed the 16 MiB evidence-object limit.

`ListChanges` returns authorized scope events with a revision cursor and a limit of 1–1000. Resume using `next_revision`; `through_revision` records the observed ledger boundary. Future cursors are rejected.

Errors use RPC status codes, including `invalid_argument`, `permission_denied`, `failed_precondition`, `already_exists` for changed command identities, and `aborted` with a typed `RevisionConflict` refresh hint. Unexpected dependency error text is not exposed. Admission fails immediately when capacity is full; the request context carries the configured deadline through identity verification and engine work. The host and injected dependencies must honor cancellation.

Tests cover value presence and validation boundaries, tenant binding, authentication failure, deadlines, a generated grpc-go client over HTTP/2, and PostgreSQL-backed embedded/remote command replay, revision conflicts, signed fact/history pagination, snapshot creation/read/retry parity, evidence and redaction registration/retry parity, independent evidence-content permissions, explicit constraint presence, review-gated acceptance/withdrawal, delegated-grant binding, the remote memory lifecycle with dependency invalidation and historical reads, and conversation/extraction scheduling, retry fencing and retained-artifact parity, calculation replay and artifact binding, and audit scheduling/download, signature and artifact digest verification, tamper rejection and revision pagination, and remote fact/memory recall with stale-hit suppression, historical reads, coverage parity and bounded projection waits, plus persisted context/decision replay, exact mixed-context bytes and order, index-outage retries, rollback without context delivery and revoked-access checks, plus remote scope/subject creation, revision conflicts, immutable history, qualified lookup replacement and administrative denial, and remote rebuild corruption checks, fencing, cutover recovery and retained operator authorization. Production gateway/Kratos verification, optional standalone capabilities, deployment, telemetry and parity coverage for the remaining services are still delivery work.

## Standalone workload service

`make build` builds `bin/memoryd` and `bin/memoryworker`. Configure it using [examples/memoryd.json](examples/memoryd.json), then run `bin/memoryd -config /path/to/config.json`. Configuration parsing rejects unknown fields, trailing JSON and files larger than 1 MiB. Paths resolve from the process working directory. Provision migrations, restricted runtime credentials, the pinned OpenFGA model/tuples, and certificates before startup; the service does not provision infrastructure or start workers.

Supply secrets through the deployment's secret manager as `MEMORY_DATABASE_URL`, `MEMORY_ENCRYPTION_KEY_HEX` (32-byte AES key encoded as hex), `MEMORY_PAGINATION_KEY_HEX` (a separate 32-byte hex key), and optional `MEMORY_OPENFGA_TOKEN`. If `audit_key_id` is configured, supply `MEMORY_AUDIT_PRIVATE_KEY_HEX` as a 64-byte Ed25519 private key encoded as hex. Keep keys stable across restarts for retained data and pagination. The supplied AES-GCM and Ed25519 implementations remain development/reference key integrations; production envelope encryption and managed signing require approved adapters and key-recovery procedures.

The service accepts TLS 1.3 client certificates chaining to the configured client CA. Each leaf must contain exactly one URI SAN matching an explicit tenant/principal binding. The authenticated caller is always a service, with no inferred human initiator. Identity headers are ignored. The [mTLS adapter](adapters/mtlsidentity/identity.go) rechecks certificate-chain validity on every request, including reused connections. The host owns CA issuance, revocation policy, certificate rotation and binding rollout; configuration reload and CRL/OCSP integration are not implemented. Restart to load changed bindings, roots or server keys. This workload path does not implement gateway/Kratos user-session or delegated-client verification.

`GET /livez` and `GET /readyz` use the same mTLS boundary. Liveness reports the HTTP process. Readiness checks PostgreSQL and a fresh check against the configured OpenFGA model; a valid authorization denial still establishes dependency health and grants no access. Component results contain booleans only. Concurrent readiness probes are bounded; a busy or draining process reports 503. Request concurrency, byte limits, deadlines, pool size, header limits and shutdown grace are bounded. SIGINT/SIGTERM stops admission and drains active HTTP requests before closing dependencies; grace expiry forces connections closed. Startup and shutdown logs omit credentials, request content and raw dependency errors.

The service composes the ledger/evidence APIs, signed pagination, optional audit signing, and optional lexical retrieval/rebuild routing. Approved extraction profiles and redaction-policy version identifiers can be configured for scheduling only. Provider execution belongs to a separate worker; delegated-attestation verification is not yet configurable in `memoryd`; APIs fail closed when required capabilities are absent. Additional worker modes, optional runtime configuration, telemetry and production operational packaging remain delivery work. Evidence objects must already be registered in the configured store through an authorized host ingestion path; this binary adds no raw-byte upload endpoint.

[examples/go/client](examples/go/client/main.go) shows a generated Connect client with verified server TLS and a client certificate. Set `MEMORY_SERVICE_URL`, `MEMORY_TLS_CERT`, `MEMORY_TLS_KEY`, `MEMORY_SERVER_CA`, `TENANT_ID` and `SCOPE_ID`, then run `go run ./examples/go/client`. The configured workload needs the corresponding OpenFGA membership and read permission.

To enable remote extraction scheduling, start from [examples/memoryd-extraction.json](examples/memoryd-extraction.json). The optional `extraction` section contains approved profiles, `max_concurrent_per_tenant`, `daily_token_budget`, and optional cost-unit/budget settings. It always enables `ScheduleOnly`; it accepts no provider implementation or credentials. Full profile validation runs before the service starts accepting requests. Omit the section to keep scheduling unavailable. The development example enables the refresh parser; approve and configure profiles with `facts-extraction-json-v1` or `memory-extraction-json-v1` to schedule those job types. Profiles use the same native JSON format described for refresh workers below.

For profiles requiring approved redaction, configure `extraction.redaction_policy_versions` with the exact approved version identifiers. This allows the engine to validate previously registered derivatives and their provenance; it does not create redacted evidence, establish a redaction service, or waive evidence permissions. Refresh profiles continue to reject redaction settings that their parser does not support. Use the same pinned profiles and budget configuration on the separate executor. Clients can schedule jobs, inspect status and retained artifacts, and retry jobs with operational permission; they cannot cause this API process to invoke a model. The mTLS integration test verifies a configured profile creates a pending refresh job, an unapproved profile is rejected, and retries preserve job identity and the authenticated caller.

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

Logs include stage, outcome, duration and valid trace/span IDs. They omit caller/tenant/record IDs, purposes, correlation IDs, evidence, values, prompts, responses, credentials, and error/panic text. Successful operations log at debug level; failures at warning level. Tests use injected in-memory SDK providers to verify parentage, metric labels, transaction context propagation and sensitive-text exclusion. Hosts must separately review exporter resources, custom logger attributes and upstream adapter logs.

For RPC observability, use `connectapi.NewHandlerWithOptions(engine, verify, limits, connectapi.HandlerOptions{Telemetry: telemetryOptions, AcceptTraceContext: true})`, passing the same host-owned providers used by the engine. The original `NewHandler` keeps observability and remote context extraction disabled. Registered RPC invocations produce a `serve.rpc` server span, `facts.rpc.requests` counter and `facts.rpc.duration` histogram in seconds. Labels contain only the registered method and bounded RPC outcome. Measurements begin before capacity admission and identity verification, so those rejections are included; unknown HTTP paths and protocol failures before RPC dispatch are not counted. Logs contain method, outcome, timing and trace/span IDs, without request/response contents or error text.

`AcceptTraceContext` is optional and independent of authentication. It accepts only one bounded W3C `traceparent` header; malformed or duplicate values are ignored. Request `tracestate` and `baggage` are not extracted. The host controls sampling and whether to accept remote parenting. Trace headers cannot supply a caller, change the verified tenant, or grant a permission. Tests cover remote parenting, default isolation, authentication failures, request counts, bounded labels and private-content exclusion.

Worker queue/retry/lag metrics, business outcome counters, full internal-stage coverage, outbound trace-context propagation and standalone exporter configuration remain unfinished. The packaged binaries currently retain their existing sanitized lifecycle logs and health endpoints; they do not configure an OpenTelemetry backend.

## Standalone workers and lexical retrieval

`bin/memoryworker -config /path/to/worker.json` runs one configured tenant/generation using [examples/memoryworker.json](examples/memoryworker.json). Supported modes are `projection`, `audit`, `archive`, `refresh`, `refresh-sweep` and `extraction`; use a separate configured process for each. Consumer modes use the existing outbox claim, heartbeat, fencing, retry/backoff and acknowledgement code; `refresh-sweep` emits events without claiming deliveries. A delivery deadline bounds processing. SIGINT/SIGTERM cancels in-flight work; an unacknowledged lease remains retryable after expiry. Queue errors, delivery timeouts and exhausted consumers exit nonzero for the host's process supervisor. Restarting does not reset attempts or acknowledge incomplete work. Operator retry and destination repair remain explicit actions. The worker logs mode and outcome without event content, identifiers or dependency error text.

Supply `MEMORY_WORKER_DATABASE_URL` using a separately provisioned `facts_projector` login, the same `MEMORY_ENCRYPTION_KEY_HEX` used by the ledger, optional `MEMORY_OPENFGA_TOKEN`, and the index connection URL in the environment variable named by `projection.database_env`. Grant the configured service principal tenant membership and the projection read permissions for every relevant scope; memory projections also require history/evidence access across dependency scopes. The configured maximum attempts must match an existing consumer. The binary does not migrate, provision an index, select an active generation, retry an exhausted consumer, or start retention workers.

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

Retain the archive head object reference and expected revision independently, then call `archive.VerifyChain`. Verification checks signatures, exact object versions, payload hashes, contiguous revision coverage, and predecessor links up to an explicit batch bound. Missing or tampered batches fail. An independently retained head is necessary to detect tail truncation; a mutable database offset alone is insufficient. Archive and projection offsets remain independent. Archive heads are not yet included in scoped `GetAuditExport` bundles, and complete restore/retention drills remain pending.

## Registered redacted evidence

Apply migrations 00026–00027 and refresh `scripts/runtime-role.sql` before upgrading the PostgreSQL adapter. `RegisterRedactedEvidence` registers an immutable omission-only derivative of existing UTF-8 `text/plain` or `text/markdown` evidence. Configure the deployment-approved version IDs in `EngineOptions.RedactionPolicyVersions` (at most 64); redacted registration is disabled by default. The caller needs proposal and original evidence-read permissions. Approval of a version ID does not detect sensitive data or verify the host's choice of removed ranges; a named security/domain owner must approve the external redaction rules and evaluations.

The host first finalizes the derivative object in its evidence store, then supplies `model.RedactedEvidenceRequest` with `ID`, `Scope`, `ParentEvidenceID`, `PolicyVersion`, the immutable `Object` reference, and up to 256 `EvidenceCopyRange` entries. Each range specifies `OriginalStart`, `OriginalEnd` and `DerivedStart`, using half-open UTF-8 byte offsets. Original ranges must be ordered and nonoverlapping; derivative offsets must cover the whole derivative contiguously. The engine verifies both objects, UTF-8 boundaries and exact equality of every copied segment. Empty ranges are allowed only for an empty derivative. No replacement, normalization, OCR or nested derivation is supported by `utf8-omit-v1`.

The derivative retains its parent ID, transformation/policy versions and offset map in `Evidence.Derivation`. It inherits the parent's source system, speaker, source authority, source time, media type, classification and retention policy. This API cannot downgrade classification. Direct `RegisterEvidence` requests cannot set derivation metadata. A derivative remains in the same tenant and scope as its original; its creation, command receipt and outbox event commit atomically. Registration is idempotent under the usual command contract, and changed mappings require a new derivative identity.

Fact and memory proposals may cite a derivative only within one copied segment. A quote spanning a removed section is rejected even when its bytes match the concatenated derivative. `EvidenceDerivation.OriginalSpan` maps a valid segment citation back to its parent evidence and offsets. Both original and derivative metadata are included in event archives. A retained derivative blocks original-byte disposal through the `evidence_derivatives` retention reference, including when the derivative's own bytes have been disposed; coordinated lineage disposal remains unfinished. Custom ledger adapters must preserve this parent reference and serialize registration against parent disposal.

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

Follow the slices in [spec.md](spec.md). The current API is intentionally incomplete and may change before v1:

1. Ledger and PostgreSQL evidence-store sqlc query coverage is implemented, including runtime catalog checks and archive enumeration. Extraction concurrency, token and optional cost-reservation budgets are implemented. Human-review prerequisites and policy-controlled attestation expiry/maximum age are implemented. Fact mutations now return structured revision-conflict details, and registered predicates enforce typed value constraints, temporal precision and explicit migration rules.
2. Complete redaction support for memory dependency payloads and memory refresh jobs. Conversation extraction supports required, policy-bound derivative substitution. Versioned omission-only evidence derivatives with parent mappings and retention guards are implemented. Explicit input ranges and authorized deterministic chunk planning are implemented. Conversation memory candidate extraction is implemented. Durable refresh jobs, automatic scheduling from fact-change events and periodic expiry sweeps are implemented. Ledger-revalidated memory recall, mixed contexts and memory decision references are implemented. Paginated memory lifecycle history is implemented. Version-bound memory attestations and withdrawal are implemented. Memory acceptance, temporal lifecycle and synchronous dependency eligibility are implemented. Distinct memory identities, subtype definitions, immutable proposals, dependency provenance and authorized version reads are implemented. Fact listing now has integrity-protected pagination and known-at timestamp convenience reads. Fact lifecycle, revision-bound reads, and immutable fact snapshots are implemented.
3. Complete workload tuning and other job types. Paginated rebuild verification with bounded memory is implemented. Deadline-bounded projection waits and generation-bound recall are implemented. Managed rebuilds with exhaustive canonical-content verification and controlled cutover, CortexDB projection, ledger-validated recall, coverage reporting, and signed event archive consumers are implemented.
4. Integrate an approved production extraction provider and bank-approved evaluations. Durable fact and memory extraction jobs, retained model artifacts and immutable conversation ingestion are implemented. Extraction delivery uses an explicit outbox handler; process supervision remains host-owned.
5. Complete coordinated disposal of referenced payloads and derivatives, archive/backup coverage, production immutable storage integration, and recovery drills. Scope legal holds, versioned evidence-byte retention policies, and atomic unreferenced PostgreSQL evidence disposal are implemented. Calculation receipts, signed PostgreSQL audit bundles, and signed event archive chains are implemented.
6. Complete the remaining retention/refresh administrative surfaces, optional service configuration and remaining worker modes, gateway identity integration, full transport parity tests, injected telemetry, readiness endpoints, and operational packaging. Typed fact, memory, snapshot, evidence, definition, attestation, ingestion, extraction, calculation, audit, retrieval, context/decision and scope/subject administration and projection operations Protobuf schemas, pinned Buf generation, protovalidate checks and an injectable Connect/gRPC handler are implemented. A bounded `memoryd` workload service with mutual TLS, health checks, lexical routing and graceful shutdown, plus standalone projection, audit-export, refresh scheduling/sweeps, gateway extraction and local-reference archive workers, is implemented. GitHub Actions CI with reproducible code, integration and vulnerability checks is implemented.

The integration scenario now preserves an external calculation on snapshot A through later fact corrections and verifies its signed audit export. The full specification remains incomplete. CortexDB and encrypted local archive adapters are exercised in integration tests. Remote fact, memory, snapshot, evidence, definition, attestation, ingestion, extraction, calculation, audit, retrieval, context/decision and scope/subject administration and projection operations handlers are available for host integration; a local standalone workload service is available, but no remote service deployment, production gateway identity verifier, production embedding/extraction model, independently administered immutable archive, or production deployment has been configured. The engine does not implement wealth allocation, profile completeness, or any business decision algorithm.

Production acceptance also requires the named domain, security, retention, and operational owners described in the spec, plus workload benchmarks and restore drills. Database roles protect normal runtime operations; they do not protect against a malicious database owner. Retained digests are integrity metadata, not anonymization.


### Context manifests and decision receipts

Apply migration 00008, refresh runtime/archive grants with `scripts/runtime-role.sql`, and republish the OpenFGA model (including `can_prepare_context`). `PrepareContext` uses the configured retrieval adapter and persists the ordered canonical facts, exact JSON bytes and digest, query, selection/index revisions, retrieval identity, redaction version, caller and authorization checks before returning any context. The default format is `facts-context-json-v1`, with an 8 MiB rendered limit; it supplies facts without evidence quotes. Set `IncludeMemories: true` for the mixed format described below. Custom retrieval adapters must implement `model.RetrievalDescriber`. Repeating a committed command returns its original manifest even if retrieval is unavailable. `GetContextManifest` rechecks current scope permissions without rerunning fact eligibility.

`RecordDecision` accepts exactly one context or snapshot ID, application/decision identity, ordered selected and used version references, method identity, transformation version, final input/output evidence references, outcome and decision time. Used items must be selected, and selected items must occur in the retained source. Model methods also require registered model request/response artifacts; rule methods retain a rule ID/version. An external action artifact is optional. A calculation link requires the same snapshot source. Authenticated callers supply executor/initiator provenance. The engine verifies registered artifact digests and bytes at recording time; later reads authorize retained references without requiring those bytes to remain available.

A tenant/application/decision identity is immutable: identical requests from the same caller converge across command keys, while changed payloads conflict. Decision receipts describe the host's declared use, not independent proof of external execution. Contexts and decisions participate in ledger/outbox ordering, encrypted storage, scoped signed audit exports and signed event archives. Exports withhold records that span other scopes. Coordinated derivative disposal and production recovery drills remain outstanding.


## Retention governance and evidence disposal

Apply migration `00009_retention.sql`, refresh runtime grants, and republish the OpenFGA model. Retention mutation uses a separate `facts_retention` database login and explicit OpenFGA roles: `retention_administrator`, `legal_custodian`, and `disposal_operator`. Ordinary runtime credentials can inspect status when authorized but cannot publish retention policies, place/release holds, change disposal tombstones, or delete evidence objects.

`RegisterRetentionPolicy` publishes an immutable policy ID for a scope, record class, processing purpose, jurisdiction, and minimum age. The first supported disposal class is `evidence_bytes`. Age starts at database registration time, not a backdated source timestamp. A new policy never silently replaces the policy ID recorded on existing evidence. `PlaceLegalHold` applies a scope-wide hold. `ReleaseLegalHold` appends an authorized release with a reason; it does not delete the original hold. Hold IDs cannot be reused.

`GetRetentionStatus` reports active holds and dependency blockers. `DisposeEvidence` currently supports only unreferenced payloads in the development PostgreSQL evidence store, bound to the same database identity as the ledger. Deletion, a non-resurrectable object tombstone, the retention event, command receipt and outbox event commit atomically. Scope/object guards serialize disposal with new references, registrations, exports and holds; a stale transaction retries against current state. A hold committed before disposal blocks it. A later hold cannot restore already disposed bytes.

Disposal fails closed for referenced fact evidence, attestations, calculation/decision artifacts, pending or retained scope exports, shared objects, or incomplete legacy object inventory. Existing installations with evidence predating this migration require an explicit inventory migration before disposal; the engine does not guess which encrypted legacy locators alias an object. These blockers deliberately preserve dependent copies until coordinated derivative disposal is implemented. Re-uploading the same disposed object/version is rejected. This API does not delete source metadata, encrypted event history, production object-store versions, archives or backups. It is not an erasure/anonymization claim; backups and immutable archives expire under separately approved policies.

Retention events appear in signed scoped exports and event archives. Exports explicitly mark disposed evidence content as unavailable. Integration tests verify legal holds, stale-snapshot races, database role separation, dependency/shared-object blockers, registration-based age, rollback, retry, tenant isolation, resurrection prevention and signed export gaps. No disposal is scheduled automatically.


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


## Durable fact extraction

Apply migrations `00011_extraction_jobs.sql` and `00012_extraction_runs.sql`, refresh `scripts/runtime-role.sql` grants, and republish the OpenFGA model. Configure `model.EngineOptions.Extraction` through `NewWithOptions`. Each execution host’s `ApprovedExtractor` supplies a host-owned `ExtractionProvider` and a pinned profile: provider/model identity, prompt bytes and digest, prompt/parser/build versions, JSON parameters, timeout, byte/candidate/attempt limits, token reservation and retry delay. Fact jobs use the parser `facts-extraction-json-v1`; memory extraction and refresh use separate approved parsers described below. Constructors never start workers or contact a provider.

A scheduling host can set `ExtractionOptions.ScheduleOnly: true`, supply the same approved profiles and budget configuration, and leave every `ApprovedExtractor.Provider` nil. Profile, prompt-digest, parser, redaction and budget validation remain mandatory. This host can plan chunks, create extraction/refresh jobs and emit refresh sweeps without provider credentials; `RunExtraction` returns `ErrPrecondition` before reading or mutating a job. Supplying a provider in this mode is a configuration error. A separate execution host must configure the retained profile exactly and supply its provider; the profile digest is unchanged by scheduling-only mode. The integration test schedules an event-triggered refresh through a provider-free engine, proves rejected execution leaves the job unchanged, and completes it through a separate execution engine. The standalone `refresh` worker supports these profiles for event-driven scheduling; standalone `extraction` mode can execute these profiles through an approved gateway.

The embedded workflow is:

1. Call `IngestConversation` to retain ordered source revisions and evidence links.
2. Call `StartExtraction` with the conversation ID, approved profile ID, explicitly bound subjects and predicate/schema versions. This commits a durable job without calling the provider.
3. Have a trusted service worker call `RunExtraction` with the job ID, scope, original purpose and a command key. Use the same key for delivery retries of one invocation; a later attempt needs a new key. Use `workers.NewExtraction` with `workers.NewOutbox` for durable delivery, as shown below. The host owns startup and shutdown; a standalone worker binary is not yet supplied.
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

This mode supports fact extraction and conversation memory extraction **without bound fact dependencies**. It rejects memory jobs containing dependency payloads, since those payloads include values and quotes beyond the redacted conversation. Memory refresh profiles cannot enable it. Those inputs need further redaction support. The policy covers conversation text; hosts must separately govern prompts, schemas, identifiers and other envelope metadata, and supply approved sensitive-data detection rules and evaluation evidence.

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

The requester and worker need the usual extraction/proposal/evidence permissions, plus current read/history/evidence access across the memory and dependency scopes. Retained artifacts preserve those content scopes for later reads, scoped audit filtering and archive authorization. Fact and memory jobs share the existing durable queue, profile pinning, byte/candidate/token budgets, retries, leases and fencing. Use the same extraction worker and inspection APIs; the job kind is `memory_extraction`. Explicit original-byte input ranges are supported for these jobs too. Deterministic chunk planning is also available. Required registered redactions are supported when no bound dependency payloads are supplied. Redaction of dependency payloads and approved production model evaluations remain unfinished.

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

**Production acceptance:** the bank's security/identity owner must implement and approve the verifier, issuer trust and key/session handling, revocation policy, authentication-age limit and clock synchronization. The application owner must bind the actual presentation/response capture to that grant. The engine supplies and tests the verifier contract; it does not deploy Ory Kratos, a gateway, an issuer or a production grant-verification adapter. Those external integrations and remote transport parity remain release requirements.


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
