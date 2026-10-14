package workers

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/adapters/archive"
	internaltelemetry "github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
)

type Archive struct {
	observer *internaltelemetry.Observer
	source   model.ArchiveSource
	store    model.ArchiveStore
	signer   model.AuditSigner
	keys     map[string]ed25519.PublicKey
	auth     model.Authorizer
	caller   model.Caller
	stream   string
}

func NewArchive(source model.ArchiveSource, store model.ArchiveStore, signer model.AuditSigner, keys map[string]ed25519.PublicKey, auth model.Authorizer, c model.Caller, stream string) (*Archive, error) {
	return NewArchiveWithTelemetry(source, store, signer, keys, auth, c, stream, model.TelemetryOptions{})
}

// NewArchiveWithTelemetry uses host-owned providers without installing globals.
func NewArchiveWithTelemetry(source model.ArchiveSource, store model.ArchiveStore, signer model.AuditSigner, keys map[string]ed25519.PublicKey, auth model.Authorizer, c model.Caller, stream string, telemetry model.TelemetryOptions) (*Archive, error) {
	if source == nil || store == nil || signer == nil || auth == nil || len(keys) == 0 || !model.ValidID(c.Tenant) || !model.ValidID(c.Principal) || !model.ValidID(stream) {
		return nil, model.ErrInvalid
	}
	copied := map[string]ed25519.PublicKey{}
	for id, k := range keys {
		if len(k) != ed25519.PublicKeySize {
			return nil, model.ErrInvalid
		}
		copied[id] = append(ed25519.PublicKey(nil), k...)
	}
	observer, err := internaltelemetry.New(telemetry)
	if err != nil {
		return nil, err
	}
	return &Archive{observer, source, store, signer, copied, auth, c, stream}, nil
}
func (a *Archive) Handle(ctx context.Context, d model.Delivery) (err error) {
	ctx, operation := a.observer.Start(ctx, "archive.event")
	defer operation.End(&err)
	if d.Key.Tenant != a.caller.Tenant || d.Key.Consumer != model.ArchiveConsumer(a.stream) || d.Event.LedgerRevision < 1 {
		return model.ErrDenied
	}
	payload, err := a.source.ArchivePayload(ctx, d)
	if err != nil {
		return err
	}
	if payload.Event.ID != d.Event.ID || payload.Event.LedgerRevision != d.Event.LedgerRevision || payload.Event.Caller.Tenant != a.caller.Tenant || len(payload.Scopes) == 0 || len(payload.Scopes) > 128 {
		return model.ErrPrecondition
	}
	checks := []model.AuthorizationDecision{}
	for _, scope := range payload.Scopes {
		for _, permission := range []string{"can_export_audit", "can_read_history", "can_read", "can_read_evidence"} {
			decision, err := a.auth.Check(ctx, model.AuthorizationRequest{Caller: a.caller, Scope: scope, Permission: permission, Purpose: "audit_archive"})
			if err != nil {
				return model.ErrUnavailable
			}
			if err := validateWorkerAuthorization(decision); err != nil {
				return err
			}
			checks = append(checks, decision)
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(raw) > 64<<20 {
		return model.ErrPrecondition
	}
	revision := d.Event.LedgerRevision
	_, existing, err := a.store.Load(ctx, a.caller.Tenant, archive.ManifestKey(a.stream, revision))
	if err == nil {
		var batch model.ArchiveBatch
		if err = json.Unmarshal(existing, &batch); err != nil {
			return err
		}
		if err = archive.VerifyBatch(batch, a.keys); err != nil {
			return err
		}
		m := batch.Manifest
		if m.Tenant != a.caller.Tenant || m.Stream != a.stream || m.ThroughRevision != revision || m.Payload.Digest != model.BytesDigest(raw) {
			return model.ErrConflict
		}
		_, err = a.store.Read(ctx, a.caller.Tenant, m.Payload)
		return err
	}
	if !errors.Is(err, model.ErrNotFound) {
		return err
	}
	var previous *model.ObjectRef
	if revision > 1 {
		ref, old, err := a.store.Load(ctx, a.caller.Tenant, archive.ManifestKey(a.stream, revision-1))
		if err != nil {
			return err
		}
		var prior model.ArchiveBatch
		if err = json.Unmarshal(old, &prior); err != nil {
			return err
		}
		if err = archive.VerifyBatch(prior, a.keys); err != nil {
			return err
		}
		if prior.Manifest.Tenant != a.caller.Tenant || prior.Manifest.Stream != a.stream || prior.Manifest.ThroughRevision != revision-1 {
			return model.ErrPrecondition
		}
		previous = &ref
	}
	for _, check := range checks {
		if err := validateWorkerAuthorization(check); err != nil {
			return err
		}
	}
	ref, err := a.store.Put(ctx, a.caller.Tenant, archive.PayloadKey(a.stream, revision), raw)
	if err != nil {
		return err
	}
	manifest := model.ArchiveManifest{Authorization: checks, Schema: "facts-event-archive-v1", Encoding: "json-utf8-v1", Tenant: a.caller.Tenant, Stream: a.stream, FromRevision: revision, ThroughRevision: revision, EventCount: 1, Payload: ref, Previous: previous}
	signBytes, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	signature, err := a.signer.Sign(ctx, signBytes)
	if err != nil {
		return err
	}
	batch := model.ArchiveBatch{Manifest: manifest, Signature: signature}
	if err = archive.VerifyBatch(batch, a.keys); err != nil {
		return err
	}
	b, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	for _, check := range checks {
		if err := validateWorkerAuthorization(check); err != nil {
			return err
		}
	}
	_, err = a.store.Put(ctx, a.caller.Tenant, archive.ManifestKey(a.stream, revision), b)
	return err
}
