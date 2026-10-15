// Package pgfailure classifies database availability errors without replaying I/O.
package pgfailure

import (
	"context"
	"errors"
	"net"

	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
)

// Normalize preserves interruption semantics and hides dependency details for
// recognized connection failures. Other errors retain their original identity.
func Normalize(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch pgError.Code {
		case "08000", "08001", "08003", "08004", "08006", "08007", "40003", "53300", "57P01", "57P02", "57P03", "57P05":
			return model.ErrUnavailable
		}
	}
	var connection *pgconn.ConnectError
	var network *net.OpError
	if errors.As(err, &connection) || errors.As(err, &network) || errors.Is(err, pgconn.ErrConnClosed) {
		return model.ErrUnavailable
	}
	return err
}
