package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"unicode/utf8"
)

func identityTx(tx model.Tx) (model.IdentityTx, error) {
	t, ok := tx.(model.IdentityTx)
	if !ok {
		return nil, model.ErrPrecondition
	}
	return t, nil
}
func validIdentityText(label, reason string, revision int64) bool {
	return len(label) > 0 && len(label) <= 256 && utf8.ValidString(label) && len(reason) > 0 && len(reason) <= 2048 && utf8.ValidString(reason) && revision >= 0 && revision < 1<<63-1
}
func (s *Service) PutScope(ctx context.Context, c model.Caller, r model.ScopeRequest) (model.CommitResult, error) {
	if !validIdentityText(r.Definition.Label, r.Reason, r.ExpectedRevision) {
		return model.CommitResult{}, model.ErrInvalid
	}
	return s.mutate(ctx, c, r.Definition.ID, "put_scope", "can_manage_identities", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := identityTx(tx)
		if err != nil {
			return err
		}
		v := model.ScopeVersion{Definition: r.Definition, Revision: r.ExpectedRevision + 1, Caller: c, Reason: r.Reason}
		if err = t.StageScope(ctx, v, r.ExpectedRevision); err != nil {
			return err
		}
		out.ID = r.Definition.ID
		out.StreamRevision = v.Revision
		out.ContentDigest = model.Digest(r.Definition)
		return nil
	})
}
func (s *Service) PutSubject(ctx context.Context, c model.Caller, r model.SubjectRequest) (model.CommitResult, error) {
	d := r.Definition
	if !model.ValidID(d.ID) || !model.ValidID(d.Kind) || !validIdentityText(d.Label, r.Reason, r.ExpectedRevision) || len(d.Lookup) > 16 {
		return model.CommitResult{}, model.ErrInvalid
	}
	for namespace, value := range d.Lookup {
		if !model.ValidID(namespace) || !model.ValidID(value) {
			return model.CommitResult{}, model.ErrInvalid
		}
	}
	return s.mutate(ctx, c, d.Scope, "put_subject", "can_manage_identities", r.Command, r, func(tx model.Tx, out *model.CommitResult) error {
		t, err := identityTx(tx)
		if err != nil {
			return err
		}
		v := model.SubjectVersion{Definition: d, Revision: r.ExpectedRevision + 1, Caller: c, Reason: r.Reason}
		if err = t.StageSubject(ctx, v, r.ExpectedRevision); err != nil {
			return err
		}
		out.ID = d.ID
		out.StreamRevision = v.Revision
		out.ContentDigest = model.Digest(d)
		return nil
	})
}
func (s *Service) authorizeIdentityRead(ctx context.Context, c model.Caller, q model.IdentityQuery) error {
	if q.Revision < 0 {
		return model.ErrInvalid
	}
	if _, err := s.authorize(ctx, c, q.Scope, "can_read", model.Command{Purpose: q.Purpose}); err != nil {
		return err
	}
	if q.Revision > 0 {
		_, err := s.authorize(ctx, c, q.Scope, "can_read_history", model.Command{Purpose: q.Purpose})
		return err
	}
	return nil
}
func (s *Service) GetScope(ctx context.Context, c model.Caller, q model.IdentityQuery) (model.ScopeVersion, error) {
	var out model.ScopeVersion
	if q.ID != "" && q.ID != q.Scope {
		return out, model.ErrInvalid
	}
	if err := s.authorizeIdentityRead(ctx, c, q); err != nil {
		return out, err
	}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := identityTx(tx)
		if err != nil {
			return err
		}
		out, err = t.ScopeIdentity(ctx, q.Scope, q.Revision)
		return err
	})
	if err == nil {
		err = s.authorizeIdentityRead(ctx, c, q)
	}
	if err != nil {
		return model.ScopeVersion{}, err
	}
	return out, nil
}
func (s *Service) GetSubject(ctx context.Context, c model.Caller, q model.IdentityQuery) (model.SubjectVersion, error) {
	var out model.SubjectVersion
	if !model.ValidID(q.ID) {
		return out, model.ErrInvalid
	}
	if err := s.authorizeIdentityRead(ctx, c, q); err != nil {
		return out, err
	}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := identityTx(tx)
		if err != nil {
			return err
		}
		out, err = t.SubjectIdentity(ctx, q.Scope, q.ID, q.Revision)
		return err
	})
	if err == nil {
		err = s.authorizeIdentityRead(ctx, c, q)
	}
	if err != nil {
		return model.SubjectVersion{}, err
	}
	return out, nil
}
func (s *Service) LookupSubject(ctx context.Context, c model.Caller, q model.SubjectLookupQuery) (model.SubjectVersion, error) {
	var out model.SubjectVersion
	if !model.ValidID(q.Namespace) || !model.ValidID(q.Value) {
		return out, model.ErrInvalid
	}
	read := model.IdentityQuery{Scope: q.Scope, Purpose: q.Purpose}
	if err := s.authorizeIdentityRead(ctx, c, read); err != nil {
		return out, err
	}
	err := s.ledger.Transact(ctx, c.Tenant, func(tx model.Tx) error {
		t, err := identityTx(tx)
		if err != nil {
			return err
		}
		out, err = t.LookupSubject(ctx, q.Scope, q.Namespace, q.Value)
		return err
	})
	if err == nil {
		err = s.authorizeIdentityRead(ctx, c, read)
	}
	if err != nil {
		return model.SubjectVersion{}, err
	}
	return out, nil
}
