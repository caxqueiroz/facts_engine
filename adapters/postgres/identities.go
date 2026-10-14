package postgres

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"sort"
)

func (t *transaction) ScopeIdentity(ctx context.Context, scope string, revision int64) (model.ScopeVersion, error) {
	var v model.ScopeVersion
	err := t.read(ctx, &v, `SELECT v.payload FROM scope_versions v JOIN scopes s ON s.tenant_id=v.tenant_id AND s.id=v.scope_id WHERE v.tenant_id=$1 AND v.scope_id=$2 AND v.revision=CASE WHEN $3::bigint=0 THEN s.current_revision ELSE $3 END`, t.tenant, scope, revision)
	return v, err
}
func (t *transaction) SubjectIdentity(ctx context.Context, scope, id string, revision int64) (model.SubjectVersion, error) {
	var v model.SubjectVersion
	err := t.read(ctx, &v, `SELECT v.payload FROM subject_versions v JOIN subjects s ON s.tenant_id=v.tenant_id AND s.scope_id=v.scope_id AND s.id=v.subject_id WHERE v.tenant_id=$1 AND v.scope_id=$2 AND v.subject_id=$3 AND v.revision=CASE WHEN $4::bigint=0 THEN s.current_revision ELSE $4 END`, t.tenant, scope, id, revision)
	return v, err
}
func (t *transaction) identityLookupDigest(scope, namespace, value string) string {
	return model.Digest(struct{ Tenant, Scope, Namespace, Value string }{t.tenant, scope, namespace, value})
}
func (t *transaction) LookupSubject(ctx context.Context, scope, namespace, value string) (model.SubjectVersion, error) {
	var id string
	err := t.tx.QueryRow(ctx, `SELECT subject_id FROM subject_lookup WHERE tenant_id=$1 AND scope_id=$2 AND namespace=$3 AND value_digest=$4`, t.tenant, scope, namespace, t.identityLookupDigest(scope, namespace, value)).Scan(&id)
	if err != nil {
		return model.SubjectVersion{}, err
	}
	return t.SubjectIdentity(ctx, scope, id, 0)
}
func (t *transaction) StageScope(ctx context.Context, v model.ScopeVersion, expected int64) error {
	if _, err := t.tx.Exec(ctx, `INSERT INTO scopes VALUES($1,$2,0) ON CONFLICT DO NOTHING`, t.tenant, v.Definition.ID); err != nil {
		return err
	}
	var current int64
	if err := t.tx.QueryRow(ctx, `SELECT current_revision FROM scopes WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, t.tenant, v.Definition.ID).Scan(&current); err != nil {
		return err
	}
	if current != expected {
		return model.ErrRevision
	}
	if _, err := t.tx.Exec(ctx, `UPDATE scopes SET current_revision=$3 WHERE tenant_id=$1 AND id=$2`, t.tenant, v.Definition.ID, v.Revision); err != nil {
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
	if _, err := t.tx.Exec(ctx, `INSERT INTO subjects VALUES($1,$2,$3,$4,0) ON CONFLICT DO NOTHING`, t.tenant, d.Scope, d.ID, d.Kind); err != nil {
		return err
	}
	var current int64
	var kind string
	if err := t.tx.QueryRow(ctx, `SELECT current_revision,kind FROM subjects WHERE tenant_id=$1 AND scope_id=$2 AND id=$3 FOR UPDATE`, t.tenant, d.Scope, d.ID).Scan(&current, &kind); err != nil {
		return err
	}
	if current != expected {
		return model.ErrRevision
	}
	if kind != d.Kind {
		return model.ErrPrecondition
	}
	if _, err := t.tx.Exec(ctx, `DELETE FROM subject_lookup WHERE tenant_id=$1 AND scope_id=$2 AND subject_id=$3`, t.tenant, d.Scope, d.ID); err != nil {
		return err
	}
	namespaces := make([]string, 0, len(d.Lookup))
	for ns := range d.Lookup {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)
	for _, ns := range namespaces {
		if _, err := t.tx.Exec(ctx, `INSERT INTO subject_lookup VALUES($1,$2,$3,$4,$5)`, t.tenant, d.Scope, ns, t.identityLookupDigest(d.Scope, ns, d.Lookup[ns]), d.ID); err != nil {
			return err
		}
	}
	if _, err := t.tx.Exec(ctx, `UPDATE subjects SET current_revision=$4 WHERE tenant_id=$1 AND scope_id=$2 AND id=$3`, t.tenant, d.Scope, d.ID, v.Revision); err != nil {
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
		if _, err = t.tx.Exec(ctx, `INSERT INTO scope_versions VALUES($1,$2,$3,$4,$5)`, t.tenant, v.Definition.ID, v.Revision, e.LedgerRevision, b); err != nil {
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
		if _, err = t.tx.Exec(ctx, `INSERT INTO subject_versions VALUES($1,$2,$3,$4,$5,$6)`, t.tenant, v.Definition.Scope, v.Definition.ID, v.Revision, e.LedgerRevision, b); err != nil {
			return err
		}
	}
	t.pendingScope = nil
	t.pendingSubject = nil
	return nil
}
