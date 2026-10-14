package workers

import (
	"github.com/caxqueiroz/facts_engine/model"
	"time"
)

// validateWorkerAuthorization bounds the lifetime of an observed permission.
// Call again before publication after potentially slow dependency operations.
func validateWorkerAuthorization(decision model.AuthorizationDecision) error {
	if !decision.Allowed {
		return model.ErrDenied
	}
	now := time.Now()
	if decision.ModelID == "" || decision.Consistency != "HIGHER_CONSISTENCY" || now.Sub(decision.CheckedAt) > 30*time.Second || decision.CheckedAt.After(now.Add(time.Second)) {
		return model.ErrUnavailable
	}
	return nil
}
