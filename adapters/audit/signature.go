// Package audit supplies a development Ed25519 signer and standalone verification.
// Production deployments inject a signer backed by a separately managed bank key.
package audit

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"github.com/caxqueiroz/facts_engine/model"
)

type Ed25519 struct {
	keyID string
	key   ed25519.PrivateKey
}

func NewEd25519(keyID string, key ed25519.PrivateKey) (*Ed25519, error) {
	if !model.ValidID(keyID) || len(key) != ed25519.PrivateKeySize {
		return nil, model.ErrInvalid
	}
	return &Ed25519{keyID, append(ed25519.PrivateKey(nil), key...)}, nil
}
func (s *Ed25519) Sign(ctx context.Context, payload []byte) (model.AuditSignature, error) {
	if err := ctx.Err(); err != nil {
		return model.AuditSignature{}, err
	}
	return model.AuditSignature{Algorithm: "Ed25519", KeyID: s.keyID, Bytes: ed25519.Sign(s.key, payload)}, nil
}

// Verify requires a trusted key mapping from outside the untrusted bundle.
// It checks ordering, counts, every entry digest, and the signed coverage claims.
func Verify(bundle model.AuditBundle, trustedKeys map[string]ed25519.PublicKey) error {
	m := bundle.Manifest
	if m.SchemaVersion != "facts-audit-v1" || m.Encoding != "json-utf8-v1" || m.EntryCount != len(bundle.Entries) || len(m.Entries) != len(bundle.Entries) || bundle.Signature.Algorithm != "Ed25519" {
		return model.ErrPrecondition
	}
	key := trustedKeys[bundle.Signature.KeyID]
	if len(key) != ed25519.PublicKeySize {
		return model.ErrPrecondition
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if !ed25519.Verify(key, payload, bundle.Signature.Bytes) {
		return fmt.Errorf("audit signature: %w", model.ErrPrecondition)
	}
	previous := ""
	for i, entry := range bundle.Entries {
		identity := entry.Kind + "\x00" + entry.ID
		if identity <= previous || !json.Valid(entry.Payload) {
			return model.ErrPrecondition
		}
		previous = identity
		declared := m.Entries[i]
		if declared.Kind != entry.Kind || declared.ID != entry.ID || declared.Digest != model.BytesDigest(entry.Payload) {
			return fmt.Errorf("audit entry integrity: %w", model.ErrPrecondition)
		}
	}
	return nil
}
