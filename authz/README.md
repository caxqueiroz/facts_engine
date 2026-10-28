# Authorization model

`model.fga` is the source; `python3 authz/generate.py` emits `model.json`. The converter intentionally accepts only the DSL constructs used here and fails on unsupported syntax. The integration test deploys the JSON to a real OpenFGA server and exercises allow, deny, and tuple revocation.

The host provisions one tenant parent per scope. Every permission intersects the assigned relation with membership in that parent tenant. User and service identities are distinct. Operator access does not grant fact or evidence access. OpenFGA membership is necessary but does not bypass the engine's tenant, source, policy, or separation-of-duties checks.

For user/service/scope object suffixes, use `openfga.ObjectID(tenantID, opaqueID)`: unpadded base64url tenant ID, a literal dot, and unpadded base64url opaque ID. Example object types are `user:...`, `service:...`, and `scope:...`. Do not put personal identifiers in relation tuples. Tenant parent IDs must likewise be opaque and provisioned consistently by the host.

A reader requires both a `reader` relation on the scope and a `member` relation on the scope's parent tenant. Approvers require `approver`; accepting services require `acceptor`. Attestors additionally need to be explicitly permitted for the attestation kind in the immutable acceptance policy. Scope/subject administration requires `can_manage_identities`, which combines `administrator` with parent-tenant membership. Provision the scope relations before calling `PutScope`. The engine does not write OpenFGA tuples.

Pin the store and authorization model IDs returned by provisioning. Roll out model changes explicitly, run the contract tests, migrate tuples under operator ownership, and then change the injected adapter configuration. The adapter always requests `HIGHER_CONSISTENCY` and fails closed on transport or malformed-response errors. Successful mutation events retain the check request, actor, model ID, consistency mode, purpose, correlation ID, digest, and timestamp.

Receipts establish the authorization check observed by the engine. They do not freeze changing tuples, reconstruct historical authorization, or make PostgreSQL and OpenFGA one atomic transaction. Permission removal is enforced by fresh checks, with a bounded 30-second check window for append transactions. A deployment requiring coordinated instantaneous revocation needs a separate authorization-revision protocol.

Delegated client-confirmation recording additionally requires the service-only `confirmation_recorder` relation (`can_record_delegated_attestation`). The recorder and verified client each need `can_attest` and `can_read_evidence`, with tenant membership. An approved host identity verifier is mandatory; OpenFGA permission or `Caller.Initiator` alone does not prove that a client confirmed the specific version and artifacts.
