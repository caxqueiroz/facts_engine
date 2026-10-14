// Package postgres implements the authoritative ledger. The caller owns the pool.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Ledger struct {
	pool   *pgxpool.Pool
	cipher Cipher
}

func New(ctx context.Context, pool *pgxpool.Pool, cipher Cipher) (*Ledger, error) {
	if pool == nil || cipher == nil {
		return nil, model.ErrInvalid
	}
	var safe bool
	if err := pool.QueryRow(ctx, `SELECT NOT rolsuper AND NOT rolbypassrls AND NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='ledger_events' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user`).Scan(&safe); err != nil {
		return nil, fmt.Errorf("check runtime role: %w", err)
	}
	if !safe {
		return nil, fmt.Errorf("runtime role must not own ledger tables or bypass RLS: %w", model.ErrPrecondition)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.command_receipts') IS NOT NULL AND to_regclass('public.audit_exports') IS NOT NULL AND to_regclass('public.consumer_offsets') IS NOT NULL AND to_regclass('public.active_projections') IS NOT NULL AND to_regclass('public.decision_receipts') IS NOT NULL AND to_regclass('public.retention_events') IS NOT NULL AND to_regclass('public.conversations') IS NOT NULL AND to_regclass('public.extraction_jobs') IS NOT NULL AND to_regclass('public.extraction_attempts') IS NOT NULL AND to_regclass('public.predicate_migrations') IS NOT NULL AND to_regclass('public.memory_versions') IS NOT NULL AND to_regclass('public.memory_timeline_history') IS NOT NULL AND to_regclass('public.memory_attestations') IS NOT NULL AND to_regclass('public.context_memory_items') IS NOT NULL AND to_regclass('public.decision_memory_items') IS NOT NULL AND to_regclass('public.extraction_memory_proposals') IS NOT NULL AND to_regclass('public.extraction_refresh_triggers') IS NOT NULL AND to_regclass('public.extraction_memory_targets') IS NOT NULL AND to_regclass('public.extraction_memory_dependencies') IS NOT NULL AND to_regclass('public.subject_lookup') IS NOT NULL AND to_regclass('public.scope_versions') IS NOT NULL AND to_regclass('public.subject_versions') IS NOT NULL AND to_regclass('public.extraction_subjects') IS NOT NULL AND to_regclass('public.confirmation_artifacts') IS NOT NULL`).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("apply migrations explicitly: %w", model.ErrPrecondition)
	}
	return &Ledger{pool, cipher}, nil
}

type transaction struct {
	pendingScope             *model.ScopeVersion
	pendingSubject           *model.SubjectVersion
	pendingMemoryAttestation *model.Attestation
	pendingMemoryWithdrawal  string
	pendingMemoryRecord      string
	pendingMemoryTimeline    []model.Acceptance
	pendingMemoryRejection   string
	pendingMemoryType        *model.MemoryType
	pendingMemoryVersion     *model.MemoryVersion
	pendingExtractionAttempt *model.ExtractionAttempt
	pendingExtractionResult  *model.ExtractionResult
	pendingExtraction        *model.ExtractionJob
	pendingExtractionRetry   string
	pendingConversation      *model.ConversationRecord
	pendingRetention         *model.RetentionEvent
	pendingContext           *model.ContextManifest
	pendingDecision          *model.DecisionRecord
	decisionItemScopes       map[model.VersionReference]string
	pendingCalculation       *model.CalculationRecord
	calculationSnapshots     []model.FactSnapshot
	pendingAuditJob          *model.AuditExportJob
	pendingAuditBundle       *model.AuditBundle
	auditCompletionJob       *model.AuditExportJob
	pendingAttestation       *model.Attestation
	pendingWithdrawal        string
	pendingRecord            string
	pendingTimeline          []model.Acceptance
	pendingRejection         string
	pendingSnapshot          *model.FactSnapshot
	tx                       pgx.Tx
	tenant                   string
	cipher                   Cipher
}

func (l *Ledger) Transact(ctx context.Context, tenant string, fn func(model.Tx) error) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = l.transact(ctx, tenant, fn)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || (pe.Code != "40001" && pe.Code != "40P01" && pe.Code != "23505") {
			return translate(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return translate(err)
}
func (l *Ledger) transact(ctx context.Context, tenant string, fn func(model.Tx) error) error {
	if !model.ValidID(tenant) {
		return model.ErrInvalid
	}
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return translate(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err = ledgerdb.New(tx).BindTenant(ctx, tenant); err != nil {
		return translate(err)
	}
	if err = fn(&transaction{tx: tx, tenant: tenant, cipher: l.cipher}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ErrNotFound
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "40001", "40P01":
			return model.ErrRevision
		case "23505":
			return model.ErrConflict
		case "23503", "23P01":
			return model.ErrPrecondition
		case "42501":
			return model.ErrDenied
		}
	}
	return err
}
func (t *transaction) encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return t.cipher.Seal(t.tenant, b)
}
func (t *transaction) decode(b []byte, v any) error {
	p, err := t.cipher.Open(t.tenant, b)
	if err != nil {
		return err
	}
	return json.Unmarshal(p, v)
}
func (t *transaction) read(ctx context.Context, v any, query string, args ...any) error {
	var b []byte
	if err := t.tx.QueryRow(ctx, query, args...).Scan(&b); err != nil {
		return translate(err)
	}
	return t.decode(b, v)
}
func (t *transaction) Receipt(ctx context.Context, c model.Caller, op, key, digest string) (*model.CommitResult, error) {
	// Serialize retries before looking up a receipt. Repeatable-read conflicts are retried by the caller.
	lock := model.Digest([]string{t.tenant, c.Principal, op, key})
	if _, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lock); err != nil {
		return nil, err
	}
	var stored string
	var b []byte
	err := t.tx.QueryRow(ctx, `SELECT digest,payload FROM command_receipts WHERE tenant_id=$1 AND principal=$2 AND operation=$3 AND command_key=$4`, t.tenant, c.Principal, op, key).Scan(&stored, &b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if stored != digest {
		return nil, model.ErrConflict
	}
	var r model.CommitResult
	err = t.decode(b, &r)
	return &r, err
}
func (t *transaction) SaveReceipt(ctx context.Context, c model.Caller, op, key, digest string, r model.CommitResult) error {
	b, err := t.encode(r)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO command_receipts VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, c.Principal, op, key, digest, b)
	return err
}
func (t *transaction) Policy(ctx context.Context, scope, id string) (model.AcceptancePolicy, error) {
	var p model.AcceptancePolicy
	err := t.read(ctx, &p, `SELECT payload FROM policy_versions WHERE tenant_id=$1 AND scope_id=$2 AND id=$3`, t.tenant, scope, id)
	return p, err
}
func (t *transaction) InsertPolicy(ctx context.Context, p model.AcceptancePolicy) error {
	b, err := t.encode(p)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO policy_versions VALUES($1,$2,$3,$4)`, t.tenant, p.Scope, p.ID, b)
	return err
}
func (t *transaction) Predicate(ctx context.Context, scope, predicate, version string) (model.PredicateVersion, error) {
	var d model.PredicateVersion
	err := t.read(ctx, &d, `SELECT payload FROM predicate_versions WHERE tenant_id=$1 AND scope_id=$2 AND predicate=$3 AND version=$4`, t.tenant, scope, predicate, version)
	return d, err
}
func (t *transaction) InsertPredicate(ctx context.Context, d model.PredicateVersion) error {
	b, err := t.encode(d)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO predicate_versions VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, d.Scope, d.Predicate, d.Version, d.PolicyID, b)
	if err != nil {
		return err
	}
	if d.Migration != nil {
		if _, err = t.tx.Exec(ctx, `INSERT INTO predicate_migrations VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, d.Scope, d.Predicate, d.Version, d.Migration.FromVersion, d.Migration.Mode); err != nil {
			return err
		}
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO active_predicates VALUES($1,$2,$3,$4) ON CONFLICT(tenant_id,scope_id,predicate) DO UPDATE SET version=EXCLUDED.version`, t.tenant, d.Scope, d.Predicate, d.Version)
	return err
}
func (t *transaction) Evidence(ctx context.Context, id string) (model.Evidence, error) {
	var e model.Evidence
	err := t.read(ctx, &e, `SELECT payload FROM evidence WHERE tenant_id=$1 AND id=$2`, t.tenant, id)
	return e, err
}
func (t *transaction) InsertEvidence(ctx context.Context, e model.Evidence) (string, error) {
	if err := t.lockRetentionScope(ctx, e.Scope); err != nil {
		return "", err
	}
	if err := t.lockEvidenceObject(ctx, e.Object); err != nil {
		return "", err
	}
	source := model.Digest([]string{e.SourceSystem, e.SourceObject, e.SourceRevision})
	var existing, digest string
	err := t.tx.QueryRow(ctx, `SELECT id,digest FROM evidence WHERE tenant_id=$1 AND scope_id=$2 AND source_identity=$3`, t.tenant, e.Scope, source).Scan(&existing, &digest)
	if err == nil {
		if digest != e.Object.Digest {
			return "", model.ErrConflict
		}
		stored, err := t.Evidence(ctx, existing)
		if err != nil {
			return "", err
		}
		candidate := e
		candidate.ID = stored.ID
		candidate.SourceTime = candidate.SourceTime.UTC()
		stored.SourceTime = stored.SourceTime.UTC()
		if model.Digest(stored) != model.Digest(candidate) {
			return "", model.ErrConflict
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	b, err := t.encode(e)
	if err != nil {
		return "", err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO evidence VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, e.ID, e.Scope, source, e.Object.Digest, b)
	if err != nil {
		return "", err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO evidence_retention(tenant_id,evidence_id,scope_id,locator,object_version) VALUES($1,$2,$3,$4,$5)`, t.tenant, e.ID, e.Scope, e.Object.Locator, e.Object.Version)
	return e.ID, err
}
func (t *transaction) Record(ctx context.Context, id string) (model.Record, error) {
	var r model.Record
	err := t.tx.QueryRow(ctx, `SELECT id,scope_id,subject_id,predicate,discriminator,stream_revision,kind FROM records WHERE tenant_id=$1 AND id=$2`, t.tenant, id).Scan(&r.ID, &r.Key.Scope, &r.Key.Subject, &r.Key.Predicate, &r.Key.Discriminator, &r.Revision, &r.Kind)
	return r, translate(err)
}
func (t *transaction) LockRecord(ctx context.Context, k model.FactKey) (model.Record, error) {
	if err := t.RequireSubject(ctx, k.Scope, k.Subject); err != nil {
		return model.Record{}, err
	}
	id := model.Digest(k)
	_, err := t.tx.Exec(ctx, `INSERT INTO records(tenant_id,id,scope_id,subject_id,predicate,discriminator,stream_revision) VALUES($1,$2,$3,$4,$5,$6,0) ON CONFLICT DO NOTHING`, t.tenant, id, k.Scope, k.Subject, k.Predicate, k.Discriminator)
	if err != nil {
		return model.Record{}, err
	}
	rev, err := ledgerdb.New(t.tx).LockRecord(ctx, ledgerdb.LockRecordParams{TenantID: t.tenant, ID: id})
	return model.Record{ID: id, Key: k, Revision: rev}, err
}
func (t *transaction) SetRevision(ctx context.Context, id string, rev int64) error {
	err := ledgerdb.New(t.tx).UpdateStreamRevision(ctx, ledgerdb.UpdateStreamRevisionParams{TenantID: t.tenant, ID: id, StreamRevision: rev})
	return err
}
func (t *transaction) Version(ctx context.Context, record, id string) (model.Version, error) {
	var v model.Version
	err := t.read(ctx, &v, `SELECT payload FROM record_versions WHERE tenant_id=$1 AND record_id=$2 AND id=$3`, t.tenant, record, id)
	return v, err
}
func (t *transaction) InsertVersion(ctx context.Context, v model.Version) error {
	for _, source := range v.Proposal.Sources {
		if err := t.guardEvidenceReference(ctx, source.EvidenceID); err != nil {
			return err
		}
	}
	b, err := t.encode(v)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO record_versions VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, t.tenant, v.RecordID, v.ID, v.Proposal.Key.Scope, v.Proposal.Key.Predicate, v.Proposal.SchemaVersion, v.Digest, b)
	if err != nil {
		return err
	}
	for _, s := range v.Proposal.Sources {
		_, err = t.tx.Exec(ctx, `INSERT INTO version_evidence VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, t.tenant, v.ID, s.EvidenceID, v.Proposal.Key.Scope)
		if err != nil {
			return err
		}
	}
	return nil
}
func (t *transaction) Attestations(ctx context.Context, version string) ([]model.Attestation, error) {
	rows, err := t.tx.Query(ctx, `SELECT a.payload FROM attestations a WHERE a.tenant_id=$1 AND a.version_id=$2 AND NOT EXISTS(SELECT 1 FROM attestation_events e WHERE e.tenant_id=a.tenant_id AND e.attestation_id=a.id) ORDER BY a.id`, t.tenant, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Attestation{}
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		var a model.Attestation
		if err := t.decode(b, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (t *transaction) InsertAttestation(ctx context.Context, a model.Attestation) error {
	if err := t.guardEvidenceReference(ctx, a.EvidenceID); err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, `INSERT INTO attestation_identities VALUES($1,$2,$3,'fact')`, t.tenant, a.ID, a.Scope); err != nil {
		return err
	}
	if err := t.stageConfirmation(ctx, a, "fact"); err != nil {
		return err
	}
	t.pendingAttestation = &a
	return nil
}
func (t *transaction) WithdrawAttestation(ctx context.Context, id, scope string, c model.Caller) (model.CommitResult, error) {
	var kind string
	if err := t.tx.QueryRow(ctx, `SELECT kind FROM attestation_identities WHERE tenant_id=$1 AND id=$2`, t.tenant, id).Scan(&kind); err != nil {
		return model.CommitResult{}, translate(err)
	}
	if kind == "memory" {
		return t.withdrawMemoryAttestation(ctx, id, scope, c)
	}
	out := model.CommitResult{}
	var a model.Attestation
	if err := t.read(ctx, &a, `SELECT payload FROM attestations WHERE tenant_id=$1 AND id=$2`, t.tenant, id); err != nil {
		return out, err
	}
	if a.Scope != scope || a.Attestor.Principal != c.Principal {
		return out, model.ErrDenied
	}
	rec, err := t.Record(ctx, a.RecordID)
	if err != nil {
		return out, err
	}
	rec, err = t.LockRecord(ctx, rec.Key)
	if err != nil {
		return out, err
	}
	t.pendingWithdrawal = id
	out = model.CommitResult{ID: id, RecordID: a.RecordID, VersionID: a.VersionID, StreamRevision: rec.Revision + 1}
	return out, t.SetRevision(ctx, rec.ID, out.StreamRevision)
}
func (t *transaction) InsertAcceptance(ctx context.Context, record string, a model.Acceptance) error {
	all, err := t.Acceptances(ctx, record, 0)
	if err != nil {
		return err
	}
	next := []model.Acceptance{}
	for _, existing := range all {
		if existing.State != "retracted" {
			next = append(next, existing)
			continue
		}
		for _, part := range model.Subtract(existing.Valid, a.Valid) {
			piece := existing
			piece.Valid = part
			next = append(next, piece)
		}
	}
	a.State = "accepted"
	next = append(next, a)
	model.SortTimeline(next)
	return t.StageTimeline(ctx, record, next)
}
func (t *transaction) Append(ctx context.Context, e model.Event) (model.Event, error) {
	err := ledgerdb.New(t.tx).EnsureTenant(ctx, t.tenant)
	if err != nil {
		return e, err
	}
	e.LedgerRevision, err = ledgerdb.New(t.tx).AllocateRevision(ctx, t.tenant)
	if err != nil {
		return e, err
	}
	b, err := t.encode(e)
	if err != nil {
		return e, err
	}
	err = t.tx.QueryRow(ctx, `INSERT INTO ledger_events(tenant_id,id,ledger_revision,event_type,scope_id,payload) VALUES($1,$2,$3,$4,$5,$6) RETURNING recorded_at`, t.tenant, e.ID, e.LedgerRevision, e.Type, e.Scope, b).Scan(&e.RecordedAt)
	if err != nil {
		return e, err
	}
	if err = t.flushIdentities(ctx, e); err != nil {
		return e, err
	}
	if err = t.flushMemoryLifecycle(ctx, e); err != nil {
		return e, err
	}
	if err = t.flushMemory(ctx, e); err != nil {
		return e, err
	}
	if e.RecordKind == "memory" {
		_, err = t.tx.Exec(ctx, `INSERT INTO memory_record_events VALUES($1,$2,$3,$4,$5)`, t.tenant, e.ID, e.RecordID, e.VersionID, e.StreamRevision)
		if err != nil {
			return e, err
		}
	} else if e.RecordID != "" {
		_, err = t.tx.Exec(ctx, `INSERT INTO record_events VALUES($1,$2,$3,$4,$5)`, t.tenant, e.ID, e.RecordID, e.VersionID, e.StreamRevision)
		if err != nil {
			return e, err
		}
	}
	err = t.flushTemporal(ctx, e)
	if err != nil {
		return e, err
	}
	if err = t.flushExtractionRun(ctx, e); err != nil {
		return model.Event{}, err
	}
	if err = t.flushExtraction(ctx, e); err != nil {
		return model.Event{}, err
	}
	if err = t.flushConversation(ctx, e); err != nil {
		return model.Event{}, err
	}
	if err = t.flushRetention(ctx, e); err != nil {
		return model.Event{}, err
	}
	if err = t.flushContext(ctx, e); err != nil {
		return model.Event{}, err
	}
	if err = t.flushAudit(ctx, e); err != nil {
		return e, err
	}
	err = ledgerdb.New(t.tx).InsertOutbox(ctx, ledgerdb.InsertOutboxParams{TenantID: t.tenant, EventID: e.ID, LedgerRevision: e.LedgerRevision})
	return e, err
}
func (t *transaction) Revision(ctx context.Context) (int64, error) {
	rev, err := ledgerdb.New(t.tx).GetTenantRevision(ctx, t.tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return rev, err
}

func (t *transaction) ActivePredicate(ctx context.Context, scope, predicate string) (string, error) {
	var version string
	err := t.tx.QueryRow(ctx, `SELECT version FROM active_predicates WHERE tenant_id=$1 AND scope_id=$2 AND predicate=$3 FOR SHARE`, t.tenant, scope, predicate).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return version, model.ErrNotFound
	}
	// Preserve serialization/deadlock errors for the transaction retry loop.
	return version, err
}
