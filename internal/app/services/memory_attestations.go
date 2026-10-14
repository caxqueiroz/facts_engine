package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

// RecordAttestation validates the trusted actor and evidence before dispatching
// here. A global attestation identity prevents ambiguous withdrawal routing.
func (s *Service) recordMemoryAttestation(ctx context.Context, tx model.Tx, c model.Caller, r model.AttestationRequest, out *model.CommitResult) error {
	t, err := memoryLifecycleTx(tx)
	if err != nil {
		return err
	}
	a := r.Attestation
	v, err := t.MemoryVersion(ctx, a.RecordID, a.VersionID)
	if err != nil {
		return err
	}
	if v.Proposal.Key.Scope != a.Scope || v.Digest != a.ContentDigest {
		return model.ErrPrecondition
	}
	policy, err := tx.Policy(ctx, a.Scope, v.PolicyID)
	if err != nil {
		return err
	}
	if err := attestationPolicyChecks(policy, a, v.Proposer); err != nil {
		return err
	}
	out.ID = a.ID
	return t.StageMemoryAttestation(ctx, a)
}
