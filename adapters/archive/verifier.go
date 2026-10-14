package archive

import (
	"context"
	"crypto/ed25519"
	"github.com/caxqueiroz/facts_engine/internal/telemetry"
	"github.com/caxqueiroz/facts_engine/model"
)

// ChainVerifier owns a copy of an independently supplied trust map. Telemetry
// providers remain host-owned; construction does not start exporters.
type ChainVerifier struct {
	keys     map[string]ed25519.PublicKey
	observer *telemetry.Observer
}

func NewChainVerifier(keys map[string]ed25519.PublicKey, options model.TelemetryOptions) (*ChainVerifier, error) {
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
	return &ChainVerifier{keys: copied, observer: observer}, nil
}

// Verify checks an independently retained head and expected revision. Storage
// outages and caller cancellation are not classified as confirmed corruption.
func (v *ChainVerifier) Verify(ctx context.Context, store model.ArchiveStore, tenant, stream string, head model.ObjectRef, expected int64, maxBatches int) (err error) {
	ctx, operation := v.observer.Start(ctx, "verify.archive")
	defer operation.End(&err)
	var failedIntegrity bool
	failedIntegrity, err = verifyChain(ctx, store, tenant, stream, head, expected, maxBatches, v.keys)
	if failedIntegrity {
		v.observer.ExportIntegrityFailure(ctx, "archive_chain")
	}
	return err
}
