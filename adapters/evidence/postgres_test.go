package evidence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestCanceledReadDoesNotAccessPool(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	for _, ctx := range []context.Context{canceled, expired} {
		store := &Postgres{}
		b, err := store.Read(ctx, "tenant", model.ObjectRef{})
		if len(b) != 0 || !errors.Is(err, ctx.Err()) {
			t.Fatal(b, err)
		}
	}
}
