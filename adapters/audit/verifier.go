package audit

import (
	"context"
	"crypto/ed25519"
	"github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
)

// Verifier owns a copy of an independently supplied trust map. Telemetry
// providers remain host-owned; construction does not start exporters.
type Verifier struct {
	keys     map[string]ed25519.PublicKey
	observer *telemetry.Observer
}

func NewVerifier(keys map[string]ed25519.PublicKey, options model.TelemetryOptions) (*Verifier, error) {
	if len(keys) == 0 {
		return nil, model.ErrInvalid
	}
	copied := make(map[string]ed25519.PublicKey, len(keys))
	for id, key := range keys {
		if !model.ValidID(id) || len(key) != ed25519.PublicKeySize {
			return nil, model.ErrInvalid
		}
		copied[id] = append(ed25519.PublicKey(nil), key...)
	}
	observer, err := telemetry.New(options)
	if err != nil {
		return nil, err
	}
	return &Verifier{keys: copied, observer: observer}, nil
}

// Verify checks the same signature, ordering, counts and digests as Verify.
// Repeated checks are separate observations, not unique corrupted artifacts.
func (v *Verifier) Verify(ctx context.Context, bundle model.AuditBundle) (err error) {
	ctx, operation := v.observer.Start(ctx, "verify.audit")
	defer operation.End(&err)
	if err = ctx.Err(); err != nil {
		return err
	}
	err = Verify(bundle, v.keys)
	if err != nil {
		v.observer.ExportIntegrityFailure(ctx, "audit_bundle")
	}
	return err
}
