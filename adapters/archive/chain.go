package archive

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/caxqueiroz/facts_engine/model"
)

func ManifestKey(stream string, revision int64) string {
	return fmt.Sprintf("%s/manifest/%020d", stream, revision)
}
func PayloadKey(stream string, revision int64) string {
	return fmt.Sprintf("%s/payload/%020d", stream, revision)
}
func VerifyBatch(b model.ArchiveBatch, keys map[string]ed25519.PublicKey) error {
	m := b.Manifest
	if m.Schema != "facts-event-archive-v1" || m.Encoding != "json-utf8-v1" || !model.ValidID(m.Tenant) || !model.ValidID(m.Stream) || m.FromRevision < 1 || m.FromRevision != m.ThroughRevision || m.EventCount != 1 || b.Signature.Algorithm != "Ed25519" || ((m.FromRevision == 1) != (m.Previous == nil)) {
		return model.ErrPrecondition
	}
	key := keys[b.Signature.KeyID]
	if len(key) != ed25519.PublicKeySize {
		return model.ErrPrecondition
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, payload, b.Signature.Bytes) {
		return model.ErrPrecondition
	}
	return nil
}

// VerifyChain requires an independently retained head and expected revision.
// Without that anchor, deleting the tail could masquerade as a shorter archive.
// The explicit maximum bounds work; exceeding it fails instead of truncating.
func VerifyChain(ctx context.Context, store model.ArchiveStore, tenant, stream string, head model.ObjectRef, expected int64, maxBatches int, keys map[string]ed25519.PublicKey) error {
	_, err := verifyChain(ctx, store, tenant, stream, head, expected, maxBatches, keys)
	return err
}

func verifyChain(ctx context.Context, store model.ArchiveStore, tenant, stream string, head model.ObjectRef, expected int64, maxBatches int, keys map[string]ed25519.PublicKey) (bool, error) {
	if store == nil || !model.ValidID(tenant) || !model.ValidID(stream) || expected < 1 || maxBatches < 1 || expected > int64(maxBatches) {
		return false, model.ErrInvalid
	}
	ref := head
	for revision := expected; revision >= 1; revision-- {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		raw, err := store.Read(ctx, tenant, ref)
		if canceled := ctx.Err(); canceled != nil {
			return false, canceled
		}
		if err != nil {
			return errors.Is(err, model.ErrNotFound), err
		}
		if int64(len(raw)) != ref.Size || model.BytesDigest(raw) != ref.Digest {
			return true, model.ErrPrecondition
		}
		var b model.ArchiveBatch
		if err = json.Unmarshal(raw, &b); err != nil {
			return true, model.ErrPrecondition
		}
		if err = VerifyBatch(b, keys); err != nil {
			return true, err
		}
		m := b.Manifest
		if m.Tenant != tenant || m.Stream != stream || m.ThroughRevision != revision {
			return true, model.ErrPrecondition
		}
		payload, err := store.Read(ctx, tenant, m.Payload)
		if canceled := ctx.Err(); canceled != nil {
			return false, canceled
		}
		if err != nil {
			return errors.Is(err, model.ErrNotFound), err
		}
		if int64(len(payload)) != m.Payload.Size || model.BytesDigest(payload) != m.Payload.Digest {
			return true, model.ErrPrecondition
		}
		var p model.ArchivePayload
		if err = json.Unmarshal(payload, &p); err != nil {
			return true, model.ErrPrecondition
		}
		if p.Event.Caller.Tenant != tenant || p.Event.LedgerRevision != revision || !model.ValidID(p.Event.ID) {
			return true, model.ErrPrecondition
		}
		if m.Previous != nil {
			ref = *m.Previous
		}
	}
	return false, nil
}
