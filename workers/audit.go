package workers

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

// AuditExport consumes trusted durable ledger events, never client-supplied
// notifications. The host worker identity is separate from the original requester.
type AuditExport struct {
	runner model.AuditExportRunner
	auth   model.Authorizer
	caller model.Caller
	stream string
}

func NewAuditExport(runner model.AuditExportRunner, auth model.Authorizer, caller model.Caller, stream string) (*AuditExport, error) {
	if runner == nil || auth == nil || !caller.Service || caller.Initiator != "" || !model.ValidID(caller.Tenant) || !model.ValidID(caller.Principal) || !model.ValidID(stream) {
		return nil, model.ErrInvalid
	}
	return &AuditExport{runner: runner, auth: auth, caller: caller, stream: stream}, nil
}

type retryAfterError struct {
	cause error
	delay time.Duration
}

func (e *retryAfterError) Error() string { return "delivery requires retry after job lease" }
func (e *retryAfterError) Unwrap() error { return e.cause }
func (a *AuditExport) Handle(ctx context.Context, d model.Delivery) error {
	if d.Key.Tenant != a.caller.Tenant || d.Key.Consumer != model.AuditExportConsumer(a.stream) || d.Event.Caller.Tenant != a.caller.Tenant {
		return model.ErrDenied
	}
	if d.Event.Type != "start_audit_export" {
		return nil
	}
	if d.Fence < 1 || !model.ValidID(d.Event.ID) || !model.ValidID(d.Event.EntityID) || !model.ValidID(d.Event.Scope) {
		return model.ErrInvalid
	}
	for _, permission := range []string{"can_operate", "can_export_audit"} {
		check, err := a.auth.Check(ctx, model.AuthorizationRequest{Caller: a.caller, Scope: d.Event.Scope, Permission: permission, Purpose: "audit_export_delivery"})
		if err != nil {
			return model.ErrUnavailable
		}
		if !check.Allowed {
			return model.ErrDenied
		}
		if check.ModelID == "" || check.Consistency != "HIGHER_CONSISTENCY" || time.Since(check.CheckedAt) > 30*time.Second || check.CheckedAt.After(time.Now().Add(time.Second)) {
			return model.ErrUnavailable
		}
	}
	// RunAuditExport matches this original caller against the retained job and
	// rechecks current requester permissions before signing or disclosure.
	result, err := a.runner.RunAuditExport(ctx, d.Event.Caller, model.AuditExportQuery{ID: d.Event.EntityID, Scope: d.Event.Scope, Purpose: d.Event.Command.Purpose})
	if err != nil {
		return &retryAfterError{cause: err, delay: model.AuditExportLease}
	}
	if result.ID != d.Event.EntityID || result.State != "completed" {
		return &retryAfterError{cause: model.ErrUnavailable, delay: model.AuditExportLease}
	}
	return nil
}
