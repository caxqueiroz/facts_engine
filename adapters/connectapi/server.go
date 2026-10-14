// Package connectapi exposes typed fact operations through Connect, gRPC and
// gRPC-Web. The host owns TLS, authenticated identity verification and lifecycle.
package connectapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectproto"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/gen/memory/v1/memoryv1connect"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// IdentityVerifier must authenticate the peer/session or a signed gateway
// assertion. It must not trust caller-supplied identity headers without proof.
// Any initiator must be bound to a verified delegation by the host.
type IdentityVerifier func(context.Context) (model.Caller, error)

// Limits are required. Timeout includes identity verification and engine work;
// MaxConcurrent is a fail-fast admission bound, not an unbounded waiting queue.
type Limits struct {
	Timeout          time.Duration
	MaxConcurrent    int
	MaxRequestBytes  int
	MaxResponseBytes int
}

// NewHandler registers the fact service without opening sockets or starting
// goroutines. HTTP/2 is required for gRPC. The host must set HTTP server read,
// write and idle deadlines in addition to the per-RPC timeout.
func NewHandler(engine FactEngine, verify IdentityVerifier, limits Limits) (http.Handler, error) {
	if engine == nil || verify == nil || limits.Timeout <= 0 || limits.MaxConcurrent <= 0 || limits.MaxRequestBytes <= 0 || limits.MaxResponseBytes <= 0 {
		return nil, model.ErrInvalid
	}
	validator, err := protovalidate.New(protovalidate.WithMessages(
		&v1.GetFactRequest{}, &v1.ProposeFactRequest{}, &v1.ApproveFactRequest{},
		&v1.AcceptFactRequest{}, &v1.RejectFactRequest{}, &v1.RetractFactRequest{},
		&v1.DisputeFactRequest{}, &v1.ResolveDisputeRequest{}, &v1.SupersedeFactRequest{},
	), protovalidate.WithDisableLazy())
	if err != nil {
		return nil, err
	}
	slots := make(chan struct{}, limits.MaxConcurrent)
	server := connect.NewServer(func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				return connect.NewError(connect.CodeResourceExhausted, "request capacity exhausted")
			}
			ctx, cancel := context.WithTimeout(ctx, limits.Timeout)
			defer cancel()
			caller, err := verify(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return rpcError(ctx.Err())
				}
				return connect.NewError(connect.CodeUnauthenticated, "authentication required")
			}
			if !model.ValidID(caller.Tenant) || !model.ValidID(caller.Principal) || (caller.Initiator != "" && (!caller.Service || !model.ValidID(caller.Initiator))) {
				return connect.NewError(connect.CodeUnauthenticated, "invalid authenticated identity")
			}
			ctx = context.WithValue(ctx, callerKey{}, caller)
			return rpcError(next(ctx, spec, &validatedStream{ServerStream: stream, validator: validator, tenant: caller.Tenant}))
		}
	})
	memoryv1connect.RegisterFactServiceHandler(server, &factService{engine: engine})
	mux := http.NewServeMux()
	connecthttp.Mount(mux, server, connecthttp.WithReadMaxBytes(limits.MaxRequestBytes), connecthttp.WithSendMaxBytes(limits.MaxResponseBytes))
	return mux, nil
}

type callerKey struct{}

func callerFrom(ctx context.Context) model.Caller { return ctx.Value(callerKey{}).(model.Caller) }

type validatedStream struct {
	connect.ServerStream
	validator protovalidate.Validator
	tenant    string
}

func (s *validatedStream) Receive(dst any) error {
	if err := s.ServerStream.Receive(dst); err != nil {
		return err
	}
	message, ok := dst.(proto.Message)
	if !ok {
		return connect.NewError(connect.CodeInternal, "invalid request type")
	}
	// Tenant binding precedes validation and engine access.
	request, ok := dst.(interface{ GetTenant() string })
	if !ok || request.GetTenant() != s.tenant {
		return connect.NewError(connect.CodePermissionDenied, "tenant mismatch")
	}
	if err := validateMessage(s.validator, message); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, "invalid request")
	}
	return nil
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	var rpc *connect.Error
	if errors.As(err, &rpc) {
		return err
	}
	code := connect.CodeInternal
	message := "internal error"
	for _, mapping := range []struct {
		err  error
		code connect.Code
	}{
		{context.Canceled, connect.CodeCanceled}, {context.DeadlineExceeded, connect.CodeDeadlineExceeded},
		{model.ErrInvalid, connect.CodeInvalidArgument}, {model.ErrDenied, connect.CodePermissionDenied},
		{model.ErrNotFound, connect.CodeNotFound}, {model.ErrConflict, connect.CodeAlreadyExists},
		{model.ErrRevision, connect.CodeAborted}, {model.ErrPrecondition, connect.CodeFailedPrecondition},
		{model.ErrQuota, connect.CodeResourceExhausted}, {model.ErrUnavailable, connect.CodeUnavailable},
	} {
		if errors.Is(err, mapping.err) {
			code, message = mapping.code, mapping.err.Error()
			break
		}
	}
	// Dependency errors can contain credentials, SQL or evidence: never expose them.
	result := connect.NewError(code, message)
	var conflict *model.RevisionConflict
	if errors.As(err, &conflict) {
		detail, detailErr := connectproto.NewErrorDetail(&v1.RevisionConflict{RecordId: conflict.RecordID, ExpectedRevision: conflict.ExpectedRevision, CurrentRevision: conflict.CurrentRevision})
		if detailErr == nil {
			result = result.WithDetail(detail)
		}
	}
	return result
}

// CEL treats Timestamp as an instant and can normalize invalid wire components.
// Check the original components before CEL or AsTime can normalize them.
func validateMessage(validator protovalidate.Validator, message proto.Message) error {
	if err := validateTimestamps(message.ProtoReflect()); err != nil {
		return err
	}
	return validator.Validate(message)
}
func validateTimestamps(message protoreflect.Message) error {
	if timestamp, ok := message.Interface().(*timestamppb.Timestamp); ok {
		return timestamp.CheckValid()
	}
	var invalid error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if field.MapValue().Message() != nil {
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					invalid = validateTimestamps(item.Message())
					return invalid == nil
				})
			}
		case field.IsList():
			if field.Message() != nil {
				for i := 0; i < value.List().Len(); i++ {
					if invalid = validateTimestamps(value.List().Get(i).Message()); invalid != nil {
						break
					}
				}
			}
		case field.Message() != nil:
			invalid = validateTimestamps(value.Message())
		}
		return invalid == nil
	})
	return invalid
}
