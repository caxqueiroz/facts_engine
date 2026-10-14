package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) stageConfirmation(ctx context.Context, a model.Attestation, kind string) error {
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
		if _, err = t.tx.Exec(ctx, `INSERT INTO confirmation_artifacts VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, t.tenant, a.ID, a.Scope, kind, binding.role, binding.ref.EvidenceID, binding.ref.Digest, evidence.Object.Locator, evidence.Object.Version); err != nil {
			return err
		}
	}
	return nil
}
