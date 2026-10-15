package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

type extractionInputReference struct {
	RecordID, VersionID string
	PayloadRetained     bool
}
type extractionInputVersion struct {
	ID, RecordID    string
	PayloadDisposed bool
	Proposal        json.RawMessage
}

// Derive references from the exact retained wire request, after approved field
// omission. No caller-supplied inventory can conceal a copied version body.
func extractionInputReferences(a model.ExtractionAttempt) ([]extractionInputReference, []extractionInputReference, error) {
	if len(a.RequestBytes) == 0 || len(a.RequestBytes) > 64<<20 || model.BytesDigest(a.RequestBytes) != a.RequestDigest {
		return nil, nil, model.ErrInvalid
	}
	var input struct {
		Schema, Tenant, JobID, AttemptID, PayloadRedactionVersion string
		Profile                                                   model.ExtractionProfile
		DependencyVersions                                        []extractionInputVersion
		Refresh                                                   *struct {
			Original     extractionInputVersion
			Dependencies []struct {
				RecordID string
				Versions []extractionInputVersion
			}
		}
	}
	if err := json.Unmarshal(a.RequestBytes, &input); err != nil {
		return nil, nil, model.ErrInvalid
	}
	if input.Tenant != a.Tenant || input.JobID != a.JobID || input.AttemptID != a.ID {
		return nil, nil, model.ErrInvalid
	}
	switch input.Schema {
	case "facts-extraction-input-v1":
		if input.Refresh != nil || len(input.DependencyVersions) > 0 {
			return nil, nil, model.ErrInvalid
		}
	case "memory-extraction-input-v1":
		if input.Refresh != nil {
			return nil, nil, model.ErrInvalid
		}
	case "memory-refresh-request-v1":
		if input.Refresh == nil || len(input.DependencyVersions) > 0 {
			return nil, nil, model.ErrInvalid
		}
	default:
		return nil, nil, model.ErrInvalid
	}
	facts, memories := map[string]extractionInputReference{}, map[string]extractionInputReference{}
	omittedDependency, omittedOriginal := false, false
	if p := input.Profile.PayloadRedaction; p != nil && input.PayloadRedactionVersion == "field-omit-v1" {
		omittedDependency, omittedOriginal = p.DependencyPayloads, p.OriginalMemoryPayload
	}
	add := func(refs map[string]extractionInputReference, v extractionInputVersion, omissionAllowed bool) error {
		if !model.ValidID(v.ID) || !model.ValidID(v.RecordID) || v.PayloadDisposed {
			return model.ErrInvalid
		}
		retained := len(v.Proposal) > 0
		if (!retained && !omissionAllowed) || (retained && bytes.Equal(bytes.TrimSpace(v.Proposal), []byte("null"))) {
			return model.ErrInvalid
		}
		old, exists := refs[v.ID]
		if exists && old.RecordID != v.RecordID {
			return model.ErrInvalid
		}
		refs[v.ID] = extractionInputReference{RecordID: v.RecordID, VersionID: v.ID, PayloadRetained: retained || old.PayloadRetained}
		if len(refs) > 10000 {
			return model.ErrInvalid
		}
		return nil
	}
	for _, v := range input.DependencyVersions {
		if err := add(facts, v, omittedDependency); err != nil {
			return nil, nil, err
		}
	}
	if input.Refresh != nil {
		if err := add(memories, input.Refresh.Original, omittedOriginal); err != nil {
			return nil, nil, err
		}
		for _, d := range input.Refresh.Dependencies {
			for _, v := range d.Versions {
				if v.RecordID != d.RecordID {
					return nil, nil, model.ErrInvalid
				}
				if err := add(facts, v, omittedDependency); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	sorted := func(refs map[string]extractionInputReference) []extractionInputReference {
		out := make([]extractionInputReference, 0, len(refs))
		for _, r := range refs {
			out = append(out, r)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].VersionID < out[j].VersionID })
		return out
	}
	return sorted(facts), sorted(memories), nil
}

func (t *transaction) retainExtractionInputInventory(ctx context.Context, a model.ExtractionAttempt) error {
	facts, memories, err := extractionInputReferences(a)
	if err != nil {
		return err
	}
	q := ledgerdb.New(t.tx)
	for _, r := range facts {
		if err := q.InsertExtractionFactInput(ctx, ledgerdb.InsertExtractionFactInputParams{TenantID: t.tenant, AttemptID: a.ID, RecordID: r.RecordID, VersionID: r.VersionID, PayloadRetained: r.PayloadRetained}); err != nil {
			return err
		}
	}
	for _, r := range memories {
		if err := q.InsertExtractionMemoryInput(ctx, ledgerdb.InsertExtractionMemoryInputParams{TenantID: t.tenant, AttemptID: a.ID, RecordID: r.RecordID, VersionID: r.VersionID, PayloadRetained: r.PayloadRetained}); err != nil {
			return err
		}
	}
	return q.InsertExtractionInputInventory(ctx, ledgerdb.InsertExtractionInputInventoryParams{TenantID: t.tenant, AttemptID: a.ID, RequestDigest: a.RequestDigest, FactCount: int32(len(facts)), MemoryCount: int32(len(memories))})
}
