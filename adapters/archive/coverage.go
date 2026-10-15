package archive

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

// CoverageVerifier binds the configured archive stream to the ledger's
// acknowledged offset and verifies its signed immutable chain through an
// export's exact tenant revision.
type CoverageVerifier struct {
	store    model.ArchiveStore
	progress model.OutboxProgressReader
	stream   string
	max      int
	keys     map[string]ed25519.PublicKey
}

func NewCoverageVerifier(store model.ArchiveStore, progress model.OutboxProgressReader, stream string, maxBatches int, keys map[string]ed25519.PublicKey) (*CoverageVerifier, error) {
	if store == nil || progress == nil || !model.ValidID(stream) || maxBatches < 1 || maxBatches > 1_000_000 || len(keys) == 0 || len(keys) > 32 {
		return nil, model.ErrInvalid
	}
	trusted := make(map[string]ed25519.PublicKey, len(keys))
	for id, key := range keys {
		if !model.ValidID(id) || len(key) != ed25519.PublicKeySize {
			return nil, model.ErrInvalid
		}
		trusted[id] = append(ed25519.PublicKey(nil), key...)
	}
	return &CoverageVerifier{store: store, progress: progress, stream: stream, max: maxBatches, keys: trusted}, nil
}

func (v *CoverageVerifier) VerifyAuditCoverage(ctx context.Context, tenant, scope string, through int64) (model.ArchiveCoverageReceipt, error) {
	if v == nil || !model.ValidID(tenant) || !model.ValidID(scope) || through < 1 || through > int64(v.max) {
		return model.ArchiveCoverageReceipt{}, model.ErrInvalid
	}
	progress, err := v.progress.ReadOutboxProgress(ctx, model.ConsumerKey{Tenant: tenant, Consumer: model.ArchiveConsumer(v.stream)})
	if err != nil {
		if ctx.Err() != nil {
			return model.ArchiveCoverageReceipt{}, ctx.Err()
		}
		if errors.Is(err, model.ErrInvalid) || errors.Is(err, model.ErrPrecondition) {
			return model.ArchiveCoverageReceipt{}, model.ErrPrecondition
		}
		return model.ArchiveCoverageReceipt{}, model.ErrUnavailable
	}
	if progress.LedgerRevision < through || progress.AcknowledgedRevision < through {
		return model.ArchiveCoverageReceipt{}, model.ErrUnavailable
	}
	head, raw, err := v.store.Load(ctx, tenant, ManifestKey(v.stream, through))
	if err != nil {
		if ctx.Err() != nil {
			return model.ArchiveCoverageReceipt{}, ctx.Err()
		}
		if errors.Is(err, model.ErrNotFound) {
			return model.ArchiveCoverageReceipt{}, model.ErrPrecondition
		}
		if errors.Is(err, model.ErrPrecondition) || errors.Is(err, model.ErrInvalid) {
			return model.ArchiveCoverageReceipt{}, model.ErrPrecondition
		}
		return model.ArchiveCoverageReceipt{}, model.ErrUnavailable
	}
	if int64(len(raw)) != head.Size || model.BytesDigest(raw) != head.Digest {
		return model.ArchiveCoverageReceipt{}, model.ErrPrecondition
	}
	var batch model.ArchiveBatch
	if err := json.Unmarshal(raw, &batch); err != nil {
		return model.ArchiveCoverageReceipt{}, model.ErrPrecondition
	}
	if batch.Manifest.Tenant != tenant || batch.Manifest.Stream != v.stream || batch.Manifest.ThroughRevision != through || VerifyBatch(batch, v.keys) != nil {
		return model.ArchiveCoverageReceipt{}, model.ErrPrecondition
	}
	missing, err := verifyChain(ctx, v.store, tenant, v.stream, head, through, v.max, v.keys)
	if err != nil || missing {
		if ctx.Err() != nil {
			return model.ArchiveCoverageReceipt{}, ctx.Err()
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return model.ArchiveCoverageReceipt{}, err
		}
		if errors.Is(err, model.ErrUnavailable) {
			return model.ArchiveCoverageReceipt{}, model.ErrUnavailable
		}
		if missing || errors.Is(err, model.ErrNotFound) || errors.Is(err, model.ErrPrecondition) || errors.Is(err, model.ErrInvalid) {
			return model.ArchiveCoverageReceipt{}, model.ErrPrecondition
		}
		return model.ArchiveCoverageReceipt{}, model.ErrUnavailable
	}
	return model.ArchiveCoverageReceipt{Tenant: tenant, Scope: scope, Stream: v.stream, ThroughRevision: through, HeadDigest: head.Digest, KeyID: batch.Signature.KeyID, VerifiedAt: time.Now().UTC()}, nil
}

var _ model.ArchiveCoverageVerifier = (*CoverageVerifier)(nil)
