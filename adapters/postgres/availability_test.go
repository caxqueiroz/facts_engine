package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/caxqueiroz/facts_engine/model"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDatabaseUnavailableDoesNotRetry(t *testing.T) {
	inputs := []error{&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, pgconn.ErrConnClosed, &pgconn.ConnectError{}}
	for _, code := range []string{"08000", "08001", "08003", "08004", "08006", "08007", "40003", "53300", "57P01", "57P02", "57P03", "57P05"} {
		inputs = append(inputs, &pgconn.PgError{Code: code, Message: "PRIVATE database detail"})
	}
	for i, input := range inputs {
		calls := 0
		// Do not format ConnectError: the synthetic value deliberately has no config.
		err := retryTransaction(context.Background(), func() error { calls++; return input })
		if calls != 1 || err != model.ErrUnavailable {
			t.Fatalf("case %d: attempts=%d result=%v", i, calls, err)
		}
	}
}

func TestDatabaseErrorClassificationBoundaries(t *testing.T) {
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		err := &net.OpError{Op: "read", Net: "tcp", Err: fmt.Errorf("PRIVATE: %w", want)}
		if translate(err) != want {
			t.Fatal("interruption reclassified", translate(err))
		}
	}
	for _, input := range []error{errors.New("application error"), &pgconn.PgError{Code: "42601"}, &pgconn.PgError{Code: "08P01"}} {
		if translate(input) != input {
			t.Fatal("unexpected error concealed", input)
		}
	}
	wrapped := fmt.Errorf("PRIVATE: %w", &pgconn.PgError{Code: "57P01"})
	if translate(wrapped) != model.ErrUnavailable {
		t.Fatal("wrapped outage not recognized")
	}
}
