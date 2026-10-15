// Package postgres implements the authoritative ledger. The caller owns the pool.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/internal/pgcleanup"
	"github.com/caxqueiroz/facts_engine/internal/pgfailure"
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
	if err := pool.QueryRow(ctx, ledgerdb.IsSafeLedgerRole).Scan(&safe); err != nil {
		return nil, fmt.Errorf("check runtime role: %w", err)
	}
	if !safe {
		return nil, fmt.Errorf("runtime role must not own ledger tables or bypass RLS: %w", model.ErrPrecondition)
	}
	var exists bool
	if err := pool.QueryRow(ctx, ledgerdb.IsLedgerSchemaReady).Scan(&exists); err != nil {
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
	return retryTransaction(ctx, func() error { return l.transact(ctx, tenant, fn) })
}

func (l *Ledger) transact(ctx context.Context, tenant string, fn func(model.Tx) error) error {
	if !model.ValidID(tenant) {
		return model.ErrInvalid
	}
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return translate(err)
	}
	defer pgcleanup.Rollback(ctx, tx.Rollback)
	if err = ledgerdb.New(tx).BindTenant(ctx, tenant); err != nil {
		return translate(err)
	}
	if err = fn(&transaction{tx: tx, tenant: tenant, cipher: l.cipher}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func translate(err error) error {
	err = pgfailure.Normalize(err)
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
	if err := ledgerdb.New(t.tx).LockCommandReceipt(ctx, lock); err != nil {
		return nil, err
	}
	receipt, err := ledgerdb.New(t.tx).GetCommandReceipt(ctx, ledgerdb.GetCommandReceiptParams{TenantID: t.tenant, Principal: c.Principal, Operation: op, CommandKey: key})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if receipt.Digest != digest {
		return nil, model.ErrConflict
	}
	var r model.CommitResult
	err = t.decode(receipt.Payload, &r)
	return &r, err
}
func (t *transaction) SaveReceipt(ctx context.Context, c model.Caller, op, key, digest string, r model.CommitResult) error {
	b, err := t.encode(r)
	if err != nil {
		return err
	}
	err = ledgerdb.New(t.tx).InsertCommandReceipt(ctx, ledgerdb.InsertCommandReceiptParams{TenantID: t.tenant, Principal: c.Principal, Operation: op, CommandKey: key, Digest: digest, Payload: b})
	return err
}
func (t *transaction) Policy(ctx context.Context, scope, id string) (model.AcceptancePolicy, error) {
	var p model.AcceptancePolicy
	b, err := ledgerdb.New(t.tx).GetPolicyPayload(ctx, ledgerdb.GetPolicyPayloadParams{TenantID: t.tenant, ScopeID: scope, ID: id})
	if err != nil {
		return p, translate(err)
	}
	err = t.decode(b, &p)
	return p, err
}
func (t *transaction) InsertPolicy(ctx context.Context, p model.AcceptancePolicy) error {
	b, err := t.encode(p)
	if err != nil {
		return err
	}
	err = ledgerdb.New(t.tx).InsertPolicyPayload(ctx, ledgerdb.InsertPolicyPayloadParams{TenantID: t.tenant, ScopeID: p.Scope, ID: p.ID, Payload: b})
	return err
}
func (t *transaction) Predicate(ctx context.Context, scope, predicate, version string) (model.PredicateVersion, error) {
	var d model.PredicateVersion
	b, err := ledgerdb.New(t.tx).GetPredicatePayload(ctx, ledgerdb.GetPredicatePayloadParams{TenantID: t.tenant, ScopeID: scope, Predicate: predicate, Version: version})
	if err != nil {
		return d, translate(err)
	}
	err = t.decode(b, &d)
	return d, err
}
func (t *transaction) InsertPredicate(ctx context.Context, d model.PredicateVersion) error {
	b, err := t.encode(d)
	if err != nil {
		return err
	}
	err = ledgerdb.New(t.tx).InsertPredicatePayload(ctx, ledgerdb.InsertPredicatePayloadParams{TenantID: t.tenant, ScopeID: d.Scope, Predicate: d.Predicate, Version: d.Version, PolicyID: d.PolicyID, Payload: b})
	if err != nil {
		return err
	}
	if d.Migration != nil {
		if err = ledgerdb.New(t.tx).InsertPredicateMigration(ctx, ledgerdb.InsertPredicateMigrationParams{TenantID: t.tenant, ScopeID: d.Scope, Predicate: d.Predicate, Version: d.Version, FromVersion: d.Migration.FromVersion, Mode: d.Migration.Mode}); err != nil {
			return err
		}
	}
	err = ledgerdb.New(t.tx).SetActivePredicate(ctx, ledgerdb.SetActivePredicateParams{TenantID: t.tenant, ScopeID: d.Scope, Predicate: d.Predicate, Version: d.Version})
	return err
}
func (t *transaction) Evidence(ctx context.Context, id string) (model.Evidence, error) {
	var e model.Evidence
	b, err := ledgerdb.New(t.tx).GetEvidencePayload(ctx, ledgerdb.GetEvidencePayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return e, translate(err)
	}
	err = t.decode(b, &e)
	return e, err
}
func (t *transaction) InsertEvidence(ctx context.Context, e model.Evidence) (string, error) {
	if e.Derivation != nil {
		parent, err := t.Evidence(ctx, e.Derivation.ParentEvidenceID)
		if err != nil {
			return "", err
		}
		if parent.Scope != e.Scope || parent.Derivation != nil {
			return "", model.ErrPrecondition
		}
		if err := t.guardEvidenceReference(ctx, parent.ID); err != nil {
			return "", err
		}
	}
	if err := t.lockRetentionScope(ctx, e.Scope); err != nil {
		return "", err
	}
	if err := t.lockEvidenceObject(ctx, e.Object); err != nil {
		return "", err
	}
	source := model.Digest([]string{e.SourceSystem, e.SourceObject, e.SourceRevision})
	sourceRow, err := ledgerdb.New(t.tx).FindEvidenceSource(ctx, ledgerdb.FindEvidenceSourceParams{TenantID: t.tenant, ScopeID: e.Scope, SourceIdentity: source})
	if err == nil {
		if sourceRow.Digest != e.Object.Digest {
			return "", model.ErrConflict
		}
		stored, err := t.Evidence(ctx, sourceRow.ID)
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
		return sourceRow.ID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	b, err := t.encode(e)
	if err != nil {
		return "", err
	}
	err = ledgerdb.New(t.tx).InsertEvidencePayload(ctx, ledgerdb.InsertEvidencePayloadParams{TenantID: t.tenant, ID: e.ID, ScopeID: e.Scope, SourceIdentity: source, Digest: e.Object.Digest, Payload: b})
	if err != nil {
		return "", err
	}
	err = ledgerdb.New(t.tx).InsertEvidenceRetention(ctx, ledgerdb.InsertEvidenceRetentionParams{TenantID: t.tenant, EvidenceID: e.ID, ScopeID: e.Scope, Locator: e.Object.Locator, ObjectVersion: e.Object.Version})
	if err != nil {
		return "", err
	}
	if e.Derivation != nil {
		err = ledgerdb.New(t.tx).InsertEvidenceDerivation(ctx, ledgerdb.InsertEvidenceDerivationParams{TenantID: t.tenant, ScopeID: e.Scope, EvidenceID: e.ID, ParentEvidenceID: e.Derivation.ParentEvidenceID})
	}
	return e.ID, err
}
func (t *transaction) Record(ctx context.Context, id string) (model.Record, error) {
	row, err := ledgerdb.New(t.tx).GetRecord(ctx, ledgerdb.GetRecordParams{TenantID: t.tenant, ID: id})
	return model.Record{ID: row.ID, Key: model.FactKey{Scope: row.ScopeID, Subject: row.SubjectID, Predicate: row.Predicate, Discriminator: row.Discriminator}, Revision: row.StreamRevision, Kind: row.Kind}, translate(err)
}
func (t *transaction) LockRecord(ctx context.Context, k model.FactKey) (model.Record, error) {
	if err := t.RequireSubject(ctx, k.Scope, k.Subject); err != nil {
		return model.Record{}, err
	}
	id := model.Digest(k)
	err := ledgerdb.New(t.tx).EnsureFactRecord(ctx, ledgerdb.EnsureFactRecordParams{TenantID: t.tenant, ID: id, ScopeID: k.Scope, SubjectID: k.Subject, Predicate: k.Predicate, Discriminator: k.Discriminator})
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
	v, err := t.retainedVersion(ctx, record, id)
	if err == nil && v.PayloadDisposed {
		return model.Version{}, model.ErrPrecondition
	}
	return v, err
}
func (t *transaction) retainedVersion(ctx context.Context, record, id string) (model.Version, error) {
	var v model.Version
	b, err := ledgerdb.New(t.tx).GetVersionPayload(ctx, ledgerdb.GetVersionPayloadParams{TenantID: t.tenant, RecordID: record, ID: id})
	if err != nil {
		return v, translate(err)
	}
	if len(b) == 0 {
		return v, model.ErrPrecondition
	}
	err = t.decode(b, &v)
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
	err = ledgerdb.New(t.tx).InsertVersionHeader(ctx, ledgerdb.InsertVersionHeaderParams{TenantID: t.tenant, RecordID: v.RecordID, ID: v.ID, ScopeID: v.Proposal.Key.Scope, Predicate: v.Proposal.Key.Predicate, SchemaVersion: v.Proposal.SchemaVersion, Digest: v.Digest})
	if err != nil {
		return err
	}
	err = ledgerdb.New(t.tx).InsertVersionPayload(ctx, ledgerdb.InsertVersionPayloadParams{TenantID: t.tenant, VersionID: v.ID, Payload: b})
	if err != nil {
		return err
	}
	for _, s := range v.Proposal.Sources {
		err = ledgerdb.New(t.tx).InsertVersionEvidence(ctx, ledgerdb.InsertVersionEvidenceParams{TenantID: t.tenant, VersionID: v.ID, EvidenceID: s.EvidenceID, ScopeID: v.Proposal.Key.Scope})
		if err != nil {
			return err
		}
	}
	return nil
}
func (t *transaction) Attestations(ctx context.Context, version string) ([]model.Attestation, error) {
	rows, err := t.tx.Query(ctx, ledgerdb.ListCurrentFactAttestations, t.tenant, version)
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
		if len(b) == 0 {
			return nil, model.ErrPrecondition
		}
		var a model.Attestation
		if err := t.decode(b, &a); err != nil {
			return nil, err
		}
		if a.PayloadDisposed {
			if !model.ValidAttestationTombstone(a, t.tenant, a.ID) {
				return nil, model.ErrPrecondition
			}
		} else if a.DisposedPayloadDigest != "" {
			return nil, model.ErrPrecondition
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (t *transaction) InsertAttestation(ctx context.Context, a model.Attestation) error {
	if err := t.guardEvidenceReference(ctx, a.EvidenceID); err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, ledgerdb.InsertFactAttestationIdentity, t.tenant, a.ID, a.Scope); err != nil {
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
	if err := t.tx.QueryRow(ctx, ledgerdb.GetAttestationKind, t.tenant, id).Scan(&kind); err != nil {
		return model.CommitResult{}, translate(err)
	}
	if kind == "memory" {
		return t.withdrawMemoryAttestation(ctx, id, scope, c)
	}
	out := model.CommitResult{}
	var a model.Attestation
	payload, err := ledgerdb.New(t.tx).GetFactAttestationPayload(ctx, ledgerdb.GetFactAttestationPayloadParams{TenantID: t.tenant, ID: id})
	if err != nil {
		return out, translate(err)
	}
	if len(payload) == 0 {
		return out, model.ErrPrecondition
	}
	if err = t.decode(payload, &a); err != nil {
		return out, err
	}
	if a.PayloadDisposed {
		return out, model.ErrPrecondition
	}
	if a.Scope != scope || a.Attestor != c {
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
	err = t.tx.QueryRow(ctx, ledgerdb.InsertLedgerEvent, t.tenant, e.ID, e.LedgerRevision, e.Type, e.Scope, b).Scan(&e.RecordedAt)
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
		_, err = t.tx.Exec(ctx, ledgerdb.InsertMemoryRecordEvent, t.tenant, e.ID, e.RecordID, e.VersionID, e.StreamRevision)
		if err != nil {
			return e, err
		}
	} else if e.RecordID != "" {
		_, err = t.tx.Exec(ctx, ledgerdb.InsertFactRecordEvent, t.tenant, e.ID, e.RecordID, e.VersionID, e.StreamRevision)
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
	err := t.tx.QueryRow(ctx, ledgerdb.GetActivePredicate, t.tenant, scope, predicate).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return version, model.ErrNotFound
	}
	// Preserve serialization/deadlock errors for the transaction retry loop.
	return version, err
}
