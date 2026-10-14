package services

import (
	"context"
	"github.com/caxqueiroz/facts_engine/model"
)

func (s *Service) commitTransaction(ctx context.Context, tenant string, fn func(context.Context, model.Tx) error) (err error) {
	ctx, observation := s.telemetry.Start(ctx, "commit.transaction")
	defer observation.End(&err)
	return s.ledger.Transact(ctx, tenant, func(tx model.Tx) error { return fn(ctx, tx) })
}

// The caller owns the deadline and has durably retained the request before this
// stage. Keep provider payloads and error text out of telemetry.
func (s *Service) callExtractionProvider(ctx context.Context, provider model.ExtractionProvider, request []byte) (response model.ExtractionResponse, err error) {
	ctx, observation := s.telemetry.Start(ctx, "call.extraction_provider")
	defer observation.End(&err)
	response, err = provider.Extract(ctx, append([]byte(nil), request...))
	if err == nil {
		err = ctx.Err()
	}
	return response, err
}
