package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5"
	"sort"
)

func validGeneration(k model.GenerationKey) bool {
	return model.ValidID(k.Tenant) && model.ValidID(k.Generation)
}
func (t *transaction) generation(ctx context.Context, k model.GenerationKey) (model.RebuildStatus, error) {
	s := model.RebuildStatus{Key: k}
	err := t.tx.QueryRow(ctx, `SELECT state,fence,through_revision,base_epoch,previous_generation,entry_count,digest FROM projection_generations WHERE tenant_id=$1 AND generation=$2`, k.Tenant, k.Generation).Scan(&s.State, &s.Fence, &s.ThroughRevision, &s.BaseEpoch, &s.PreviousGeneration, &s.Count, &s.Digest)
	return s, err
}
func (t *transaction) activeProjection(ctx context.Context) (model.ActiveProjection, error) {
	var a model.ActiveProjection
	err := t.tx.QueryRow(ctx, `SELECT generation,epoch FROM active_projections WHERE tenant_id=$1`, t.tenant).Scan(&a.Generation, &a.Epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, nil
	}
	return a, err
}
func (l *Ledger) ActiveProjection(ctx context.Context, tenant string) (model.ActiveProjection, error) {
	var a model.ActiveProjection
	err := l.Transact(ctx, tenant, func(tx model.Tx) error { var err error; a, err = tx.(*transaction).activeProjection(ctx); return err })
	return a, err
}
func (l *Ledger) RebuildStatus(ctx context.Context, k model.GenerationKey) (model.RebuildStatus, error) {
	var s model.RebuildStatus
	if !validGeneration(k) {
		return s, model.ErrInvalid
	}
	err := l.Transact(ctx, k.Tenant, func(tx model.Tx) error { var err error; s, err = tx.(*transaction).generation(ctx, k); return err })
	return s, err
}
func (l *Ledger) rebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor, fn func(*transaction) (model.RebuildStatus, error)) (model.RebuildStatus, error) {
	var s model.RebuildStatus
	if !validGeneration(k) || !model.ValidID(a.Principal) || a.Purpose == "" || len(a.Purpose) > 1024 {
		return s, model.ErrInvalid
	}
	err := l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		t := tx.(*transaction)
		if _, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, model.Digest([]string{k.Tenant, "projection-management"})); err != nil {
			return err
		}
		var err error
		s, err = fn(t)
		return err
	})
	return s, err
}
func (t *transaction) saveGeneration(ctx context.Context, s model.RebuildStatus, a model.RebuildActor, operation string) error {
	if _, err := t.tx.Exec(ctx, `UPDATE projection_generations SET state=$3,fence=$4,through_revision=$5,base_epoch=$6,previous_generation=$7,entry_count=$8,digest=$9 WHERE tenant_id=$1 AND generation=$2`, s.Key.Tenant, s.Key.Generation, s.State, s.Fence, s.ThroughRevision, s.BaseEpoch, s.PreviousGeneration, s.Count, s.Digest); err != nil {
		return err
	}
	b, err := t.encode(struct {
		Status model.RebuildStatus
		Actor  model.RebuildActor
	}{s, a})
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(ctx, `INSERT INTO projection_rebuild_events(tenant_id,generation,fence,operation,payload) VALUES($1,$2,$3,$4,$5)`, s.Key.Tenant, s.Key.Generation, s.Fence, operation, b)
	return err
}
func (l *Ledger) StartRebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor, maxAttempts int) (model.RebuildStatus, error) {
	if maxAttempts < 1 || maxAttempts > 100 {
		return model.RebuildStatus{}, model.ErrInvalid
	}
	return l.rebuild(ctx, k, a, func(t *transaction) (model.RebuildStatus, error) {
		s, err := t.generation(ctx, k)
		if err == nil {
			var configured int
			if err = t.tx.QueryRow(ctx, `SELECT max_attempts FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, model.ProjectionConsumer(k.Generation)).Scan(&configured); err != nil {
				return s, err
			}
			if configured != maxAttempts {
				return s, model.ErrConflict
			}
			return s, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return s, err
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO tenant_revisions(tenant_id) VALUES($1) ON CONFLICT DO NOTHING`, k.Tenant); err != nil {
			return s, err
		}
		active, err := t.activeProjection(ctx)
		if err != nil {
			return s, err
		}
		s = model.RebuildStatus{Key: k, State: "building", Fence: 1, BaseEpoch: active.Epoch, PreviousGeneration: active.Generation}
		if _, err = t.tx.Exec(ctx, `INSERT INTO consumer_offsets(tenant_id,consumer_id,max_attempts) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, k.Tenant, model.ProjectionConsumer(k.Generation), maxAttempts); err != nil {
			return s, err
		}
		var configured int
		if err = t.tx.QueryRow(ctx, `SELECT max_attempts FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, model.ProjectionConsumer(k.Generation)).Scan(&configured); err != nil {
			return s, err
		}
		if configured != maxAttempts {
			return s, model.ErrConflict
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO projection_generations(tenant_id,generation,state,fence,base_epoch,previous_generation) VALUES($1,$2,'building',1,$3,$4)`, k.Tenant, k.Generation, s.BaseEpoch, s.PreviousGeneration); err != nil {
			return s, err
		}
		return s, t.saveGeneration(ctx, s, a, "start")
	})
}
func (l *Ledger) FreezeRebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor) (model.RebuildStatus, error) {
	return l.rebuild(ctx, k, a, func(t *transaction) (model.RebuildStatus, error) {
		s, err := t.generation(ctx, k)
		if err != nil {
			return s, err
		}
		if s.State == "paused" || s.State == "verified" {
			return s, nil
		}
		if s.State != "building" {
			return s, model.ErrPrecondition
		}
		var state string
		var revision int64
		if err = t.tx.QueryRow(ctx, `SELECT state,acknowledged_revision FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id=$2 FOR UPDATE`, k.Tenant, model.ProjectionConsumer(k.Generation)).Scan(&state, &revision); err != nil {
			return s, err
		}
		var current int64
		if err = t.tx.QueryRow(ctx, `SELECT revision FROM tenant_revisions WHERE tenant_id=$1 FOR UPDATE`, k.Tenant).Scan(&current); err != nil {
			return s, err
		}
		if state != "pending" || revision != current {
			return s, model.ErrPrecondition
		}
		if _, err = t.tx.Exec(ctx, `UPDATE consumer_offsets SET state='paused',fence=fence+1,lease_until=NULL WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, model.ProjectionConsumer(k.Generation)); err != nil {
			return s, err
		}
		s.State = "paused"
		s.Fence++
		s.ThroughRevision = current
		return s, t.saveGeneration(ctx, s, a, "freeze")
	})
}
func (l *Ledger) VerifyRebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor, fence int64, check model.ProjectionCheck) (model.RebuildStatus, error) {
	if check.Count < 0 || len(check.Digest) != 64 {
		return model.RebuildStatus{}, model.ErrInvalid
	}
	return l.rebuild(ctx, k, a, func(t *transaction) (model.RebuildStatus, error) {
		s, err := t.generation(ctx, k)
		if err != nil {
			return s, err
		}
		if s.Fence != fence {
			return s, model.ErrRevision
		}
		if s.State == "verified" {
			if s.Count != check.Count || s.Digest != check.Digest {
				return s, model.ErrConflict
			}
			return s, nil
		}
		if s.State != "paused" {
			return s, model.ErrPrecondition
		}
		s.State = "verified"
		s.Count = check.Count
		s.Digest = check.Digest
		return s, t.saveGeneration(ctx, s, a, "verify")
	})
}
func (l *Ledger) ResumeRebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor, fence int64) (model.RebuildStatus, error) {
	return l.rebuild(ctx, k, a, func(t *transaction) (model.RebuildStatus, error) {
		s, err := t.generation(ctx, k)
		if err != nil {
			return s, err
		}
		if s.Fence == fence+1 && (s.State == "resuming" || s.State == "building") {
			return s, nil
		}
		if s.Fence != fence {
			return s, model.ErrRevision
		}
		if s.State != "paused" && s.State != "verified" {
			return s, model.ErrPrecondition
		}
		s.State = "resuming"
		s.Fence++
		s.Digest = ""
		s.Count = 0
		s.ThroughRevision = 0
		return s, t.saveGeneration(ctx, s, a, "resume")
	})
}
func (l *Ledger) FinishResume(ctx context.Context, k model.GenerationKey, a model.RebuildActor, fence int64) (model.RebuildStatus, error) {
	return l.finishRebuild(ctx, k, a, fence, "resuming", "building", "resume_ready")
}
func (l *Ledger) FinishActivation(ctx context.Context, k model.GenerationKey, a model.RebuildActor, fence int64) (model.RebuildStatus, error) {
	return l.finishRebuild(ctx, k, a, fence, "activating", "active", "activation_ready")
}
func (l *Ledger) finishRebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor, fence int64, from, to, operation string) (model.RebuildStatus, error) {
	return l.rebuild(ctx, k, a, func(t *transaction) (model.RebuildStatus, error) {
		s, err := t.generation(ctx, k)
		if err != nil {
			return s, err
		}
		if s.Fence != fence {
			return s, model.ErrRevision
		}
		if s.State == to {
			return s, nil
		}
		if s.State != from {
			return s, model.ErrPrecondition
		}
		tag, err := t.tx.Exec(ctx, `UPDATE consumer_offsets SET state='pending',available_at=clock_timestamp() WHERE tenant_id=$1 AND consumer_id=$2 AND state='paused'`, k.Tenant, model.ProjectionConsumer(k.Generation))
		if err != nil {
			return s, err
		}
		if tag.RowsAffected() != 1 {
			return s, model.ErrPrecondition
		}
		s.State = to
		return s, t.saveGeneration(ctx, s, a, operation)
	})
}
func (l *Ledger) ActivateRebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor, verifiedFence int64) (model.RebuildStatus, error) {
	return l.rebuild(ctx, k, a, func(t *transaction) (model.RebuildStatus, error) {
		s, err := t.generation(ctx, k)
		if err != nil {
			return s, err
		}
		active, err := t.activeProjection(ctx)
		if err != nil {
			return s, err
		}
		if (s.State == "active" || s.State == "activating") && s.Fence == verifiedFence+1 && active.Generation == k.Generation {
			return s, nil
		}
		if s.State != "verified" || s.Fence != verifiedFence || active.Epoch != s.BaseEpoch || active.Generation != s.PreviousGeneration {
			return s, model.ErrRevision
		}
		// No external calls occur while this ordering barrier is held. A concurrent
		// business commit either precedes this check (and rejects cutover) or follows it.
		var current int64
		if err = t.tx.QueryRow(ctx, `SELECT revision FROM tenant_revisions WHERE tenant_id=$1 FOR UPDATE`, k.Tenant).Scan(&current); err != nil {
			return s, err
		}
		if current != s.ThroughRevision {
			return s, model.ErrPrecondition
		}
		var state string
		var offset int64
		if err = t.tx.QueryRow(ctx, `SELECT state,acknowledged_revision FROM consumer_offsets WHERE tenant_id=$1 AND consumer_id=$2 FOR UPDATE`, k.Tenant, model.ProjectionConsumer(k.Generation)).Scan(&state, &offset); err != nil {
			return s, err
		}
		if state != "paused" || offset != current {
			return s, model.ErrPrecondition
		}
		if active.Generation != "" {
			old, err := t.generation(ctx, model.GenerationKey{Tenant: k.Tenant, Generation: active.Generation})
			if err != nil {
				return s, err
			}
			old.State = "retired"
			old.Fence++
			if _, err = t.tx.Exec(ctx, `UPDATE consumer_offsets SET state='paused',fence=fence+1,lease_until=NULL WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, model.ProjectionConsumer(active.Generation)); err != nil {
				return s, err
			}
			if err = t.saveGeneration(ctx, old, a, "retire"); err != nil {
				return s, err
			}
		}
		if _, err = t.tx.Exec(ctx, `INSERT INTO active_projections VALUES($1,$2,$3) ON CONFLICT(tenant_id) DO UPDATE SET generation=EXCLUDED.generation,epoch=EXCLUDED.epoch`, k.Tenant, k.Generation, active.Epoch+1); err != nil {
			return s, err
		}
		s.State = "activating"
		s.Fence++
		return s, t.saveGeneration(ctx, s, a, "activate")
	})
}

// ProjectionDocuments is a bounded exhaustive acceptance inventory, pinned to
// the frozen source revision. Immutable versions remain stable across reads.
func (l *Ledger) ProjectionDocuments(ctx context.Context, k model.GenerationKey, through int64) ([]model.ProjectionDocument, error) {
	if !validGeneration(k) || through < 0 {
		return nil, model.ErrInvalid
	}
	events := []model.Event{}
	err := l.Transact(ctx, k.Tenant, func(tx model.Tx) error {
		events = nil
		t := tx.(*transaction)
		rows, err := t.tx.Query(ctx, `SELECT DISTINCT ON (r.kind,r.version_id) e.payload FROM ledger_events e JOIN (SELECT tenant_id,event_id,version_id,'fact' AS kind FROM record_events UNION ALL SELECT tenant_id,event_id,version_id,'memory' AS kind FROM memory_record_events) r ON r.tenant_id=e.tenant_id AND r.event_id=e.id WHERE e.tenant_id=$1 AND e.ledger_revision<=$2 AND e.event_type IN ('approve_fact','accept_fact','supersede_fact','approve_memory','accept_memory','supersede_memory') ORDER BY r.kind,r.version_id,e.ledger_revision LIMIT 10001`, k.Tenant, through)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var b []byte
			if err := rows.Scan(&b); err != nil {
				return err
			}
			var e model.Event
			if err := t.decode(b, &e); err != nil {
				return err
			}
			events = append(events, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if len(events) > 10000 {
		return nil, model.ErrPrecondition
	}
	docs := make([]model.ProjectionDocument, 0, len(events))
	size := 0
	for _, e := range events {
		d, err := l.ProjectionDocument(ctx, model.Delivery{Key: model.ConsumerKey{Tenant: k.Tenant}, Event: e}, k.Generation)
		if err != nil {
			return nil, err
		}
		if d == nil {
			return nil, model.ErrPrecondition
		}
		b, err := tallyDocument(*d)
		if err != nil {
			return nil, err
		}
		size += b
		if size > 64<<20 {
			return nil, model.ErrPrecondition
		}
		docs = append(docs, *d)
	}
	sort.Slice(docs, func(i, j int) bool { return model.ProjectionID(docs[i]) < model.ProjectionID(docs[j]) })
	return docs, nil
}

func tallyDocument(d model.ProjectionDocument) (int, error) {
	b, err := json.Marshal(d)
	return len(b), err
}

func (l *Ledger) AbandonRebuild(ctx context.Context, k model.GenerationKey, a model.RebuildActor, fence int64) (model.RebuildStatus, error) {
	return l.rebuild(ctx, k, a, func(t *transaction) (model.RebuildStatus, error) {
		s, err := t.generation(ctx, k)
		if err != nil {
			return s, err
		}
		if s.State == "abandoned" && s.Fence == fence+1 {
			return s, nil
		}
		if s.Fence != fence {
			return s, model.ErrRevision
		}
		if s.State == "active" || s.State == "activating" || s.State == "retired" || s.State == "abandoned" {
			return s, model.ErrPrecondition
		}
		if _, err = t.tx.Exec(ctx, `UPDATE consumer_offsets SET state='paused',fence=fence+1,lease_until=NULL WHERE tenant_id=$1 AND consumer_id=$2`, k.Tenant, model.ProjectionConsumer(k.Generation)); err != nil {
			return s, err
		}
		s.State = "abandoned"
		s.Fence++
		return s, t.saveGeneration(ctx, s, a, "abandon")
	})
}

var _ model.RebuildStore = (*Ledger)(nil)
