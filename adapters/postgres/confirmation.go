package postgres

import (
	"context"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) stageConfirmation(ctx context.Context, a model.Attestation, kind string) error {
	if a.Delegation != nil {
		r := a.Delegation.Receipt
		if err := ledgerdb.New(t.tx).InsertAttestationDelegation(ctx, ledgerdb.InsertAttestationDelegationParams{TenantID: t.tenant, Authority: r.Authority, BindingID: r.BindingID, RequestDigest: r.RequestDigest, AttestationID: a.ID, ScopeID: a.Scope, Kind: kind}); err != nil {
			return err
		}
	}
	if a.Confirmation == nil {
		return nil
	}
	for _, binding := range []struct {
		role string
		ref  model.ArtifactReference
	}{{"presentation", a.Confirmation.Presentation}, {"response", a.Confirmation.Response}} {
		if err := t.guardEvidenceReference(ctx, binding.ref.EvidenceID); err != nil {
			return err
		}
		evidence, err := t.Evidence(ctx, binding.ref.EvidenceID)
		if err != nil {
			return err
		}
		if evidence.Scope != a.Scope || evidence.Object.Digest != binding.ref.Digest {
			return model.ErrPrecondition
		}
		if err = ledgerdb.New(t.tx).InsertConfirmationArtifact(ctx, ledgerdb.InsertConfirmationArtifactParams{TenantID: t.tenant, AttestationID: a.ID, ScopeID: a.Scope, Kind: kind, Role: binding.role, EvidenceID: binding.ref.EvidenceID, Digest: binding.ref.Digest, ObjectLocator: evidence.Object.Locator, ObjectVersion: evidence.Object.Version}); err != nil {
			return err
		}
	}
	return nil
}
