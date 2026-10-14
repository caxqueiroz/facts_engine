package postgres

import (
	"context"
	"sort"

	"github.com/caxqueiroz/facts_engine/gen/ledgerdb"
	"github.com/caxqueiroz/facts_engine/model"
)

func (t *transaction) ScopeIdentity(ctx context.Context, scope string, revision int64) (model.ScopeVersion, error) {
	var v model.ScopeVersion
	b, err := ledgerdb.New(t.tx).GetScopeIdentity(ctx, ledgerdb.GetScopeIdentityParams{TenantID: t.tenant, ScopeID: scope, Revision: revision})
	if err != nil {
		return v, translate(err)
	}
	err = t.decode(b, &v)
	return v, err
}
func (t *transaction) SubjectIdentity(ctx context.Context, scope, id string, revision int64) (model.SubjectVersion, error) {
	var v model.SubjectVersion
	b, err := ledgerdb.New(t.tx).GetSubjectIdentity(ctx, ledgerdb.GetSubjectIdentityParams{TenantID: t.tenant, ScopeID: scope, SubjectID: id, Revision: revision})
	if err != nil {
		return v, translate(err)
	}
	err = t.decode(b, &v)
	return v, err
}
func (t *transaction) identityLookupDigest(scope, namespace, value string) string {
	return model.Digest(struct{ Tenant, Scope, Namespace, Value string }{t.tenant, scope, namespace, value})
}
func (t *transaction) LookupSubject(ctx context.Context, scope, namespace, value string) (model.SubjectVersion, error) {
	id, err := ledgerdb.New(t.tx).LookupSubjectIdentity(ctx, ledgerdb.LookupSubjectIdentityParams{TenantID: t.tenant, ScopeID: scope, Namespace: namespace, ValueDigest: t.identityLookupDigest(scope, namespace, value)})
	if err != nil {
		return model.SubjectVersion{}, err
	}
	return t.SubjectIdentity(ctx, scope, id, 0)
}
func (t *transaction) StageScope(ctx context.Context, v model.ScopeVersion, expected int64) error {
	if err := ledgerdb.New(t.tx).EnsureScopeIdentity(ctx, ledgerdb.EnsureScopeIdentityParams{TenantID: t.tenant, ID: v.Definition.ID}); err != nil {
		return err
	}
	current, err := ledgerdb.New(t.tx).LockScopeIdentity(ctx, ledgerdb.LockScopeIdentityParams{TenantID: t.tenant, ID: v.Definition.ID})
	if err != nil {
		return err
	}
	if current != expected {
		return model.ErrRevision
	}
	if err := ledgerdb.New(t.tx).UpdateScopeIdentity(ctx, ledgerdb.UpdateScopeIdentityParams{TenantID: t.tenant, ID: v.Definition.ID, CurrentRevision: v.Revision}); err != nil {
		return err
	}
	t.pendingScope = &v
	return nil
}
func (t *transaction) StageSubject(ctx context.Context, v model.SubjectVersion, expected int64) error {
	d := v.Definition
	if _, err := t.ScopeIdentity(ctx, d.Scope, 0); err != nil {
		return err
	}
	if err := ledgerdb.New(t.tx).EnsureSubjectIdentity(ctx, ledgerdb.EnsureSubjectIdentityParams{TenantID: t.tenant, ScopeID: d.Scope, ID: d.ID, Kind: d.Kind}); err != nil {
		return err
	}
	locked, err := ledgerdb.New(t.tx).LockSubjectIdentity(ctx, ledgerdb.LockSubjectIdentityParams{TenantID: t.tenant, ScopeID: d.Scope, ID: d.ID})
	if err != nil {
		return err
	}
	if locked.CurrentRevision != expected {
		return model.ErrRevision
	}
	if locked.Kind != d.Kind {
		return model.ErrPrecondition
	}
	if err := ledgerdb.New(t.tx).DeleteSubjectLookups(ctx, ledgerdb.DeleteSubjectLookupsParams{TenantID: t.tenant, ScopeID: d.Scope, SubjectID: d.ID}); err != nil {
		return err
	}
	namespaces := make([]string, 0, len(d.Lookup))
	for ns := range d.Lookup {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)
	for _, ns := range namespaces {
		if err := ledgerdb.New(t.tx).InsertSubjectLookup(ctx, ledgerdb.InsertSubjectLookupParams{TenantID: t.tenant, ScopeID: d.Scope, Namespace: ns, ValueDigest: t.identityLookupDigest(d.Scope, ns, d.Lookup[ns]), SubjectID: d.ID}); err != nil {
			return err
		}
	}
	if err := ledgerdb.New(t.tx).UpdateSubjectIdentity(ctx, ledgerdb.UpdateSubjectIdentityParams{TenantID: t.tenant, ScopeID: d.Scope, ID: d.ID, CurrentRevision: v.Revision}); err != nil {
		return err
	}
	t.pendingSubject = &v
	return nil
}
func (t *transaction) flushIdentities(ctx context.Context, e model.Event) error {
	if t.pendingScope != nil {
		v := *t.pendingScope
		v.LedgerRevision = e.LedgerRevision
		b, err := t.encode(v)
		if err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertScopeVersion(ctx, ledgerdb.InsertScopeVersionParams{TenantID: t.tenant, ScopeID: v.Definition.ID, Revision: v.Revision, LedgerRevision: e.LedgerRevision, Payload: b}); err != nil {
			return err
		}
	}
	if t.pendingSubject != nil {
		v := *t.pendingSubject
		v.LedgerRevision = e.LedgerRevision
		b, err := t.encode(v)
		if err != nil {
			return err
		}
		if err = ledgerdb.New(t.tx).InsertSubjectVersion(ctx, ledgerdb.InsertSubjectVersionParams{TenantID: t.tenant, ScopeID: v.Definition.Scope, SubjectID: v.Definition.ID, Revision: v.Revision, LedgerRevision: e.LedgerRevision, Payload: b}); err != nil {
			return err
		}
	}
	t.pendingScope = nil
	t.pendingSubject = nil
	return nil
}

func (t *transaction) RequireScope(ctx context.Context, scope string) error {
	exists, err := ledgerdb.New(t.tx).ScopeIdentityExists(ctx, ledgerdb.ScopeIdentityExistsParams{TenantID: t.tenant, ID: scope})
	if err != nil {
		return err
	}
	if !exists {
		return model.ErrPrecondition
	}
	return nil
}
func (t *transaction) RequireSubject(ctx context.Context, scope, id string) error {
	exists, err := ledgerdb.New(t.tx).SubjectIdentityExists(ctx, ledgerdb.SubjectIdentityExistsParams{TenantID: t.tenant, ScopeID: scope, ID: id})
	if err != nil {
		return err
	}
	if !exists {
		return model.ErrPrecondition
	}
	return nil
}
