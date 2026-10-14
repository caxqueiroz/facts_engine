// Package connectapi exposes typed engine operations through Connect, gRPC and
// gRPC-Web. The host owns TLS, authenticated identity verification and lifecycle.
package connectapi

import (
	"context"
	"errors"
	"go.opentelemetry.io/otel/propagation"
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

// NewHandler registers the implemented engine services without opening sockets or starting
// goroutines. HTTP/2 is required for gRPC. The host must set HTTP server read,
// write and idle deadlines in addition to the per-RPC timeout.
func NewHandler(engine Engine, verify IdentityVerifier, limits Limits) (http.Handler, error) {
	return NewHandlerWithOptions(engine, verify, limits, HandlerOptions{})
}

// NewHandlerWithOptions adds injected RPC telemetry and optional traceparent
// extraction without modifying global providers or identity verification.
func NewHandlerWithOptions(engine Engine, verify IdentityVerifier, limits Limits, options HandlerOptions) (http.Handler, error) {
	if engine == nil || verify == nil || limits.Timeout <= 0 || limits.MaxConcurrent <= 0 || limits.MaxRequestBytes <= 0 || limits.MaxResponseBytes <= 0 {
		return nil, model.ErrInvalid
	}
	observer, err := newRPCObserver(options.Telemetry)
	if err != nil {
		return nil, err
	}
	validator, err := protovalidate.New(protovalidate.WithMessages(
		&v1.GetProjectionStatusRequest{}, &v1.RebuildIndexRequest{},
		&v1.PutScopeRequest{}, &v1.PutSubjectRequest{}, &v1.GetScopeRequest{}, &v1.GetSubjectRequest{}, &v1.LookupSubjectRequest{},
		&v1.PrepareContextRequest{}, &v1.GetContextManifestRequest{}, &v1.RecordDecisionRequest{}, &v1.GetDecisionReceiptRequest{},
		&v1.RecallRequest{}, &v1.RecallMemoriesRequest{}, &v1.WaitForProjectionRequest{},
		&v1.RecordCalculationRequest{}, &v1.GetCalculationReceiptRequest{}, &v1.ListChangesRequest{}, &v1.StartAuditExportRequest{}, &v1.GetAuditExportRequest{},
		&v1.IngestConversationRequest{}, &v1.GetConversationRequest{}, &v1.StartExtractionRequest{}, &v1.PlanExtractionChunksRequest{}, &v1.GetJobRequest{}, &v1.RetryJobRequest{}, &v1.GetExtractionRunRequest{}, &v1.StartMemoryRefreshRequest{},
		&v1.RegisterMemoryTypeRequest{}, &v1.GetMemoryRequest{}, &v1.ProposeMemoryRequest{}, &v1.ApproveMemoryRequest{}, &v1.AcceptMemoryRequest{}, &v1.RejectMemoryRequest{}, &v1.RetractMemoryRequest{}, &v1.DisputeMemoryRequest{}, &v1.ResolveMemoryDisputeRequest{}, &v1.SupersedeMemoryRequest{}, &v1.GetMemoryVersionRequest{}, &v1.GetMemoryHistoryRequest{},
		&v1.RegisterAcceptancePolicyRequest{}, &v1.RegisterPredicateVersionRequest{}, &v1.RecordAttestationRequest{}, &v1.RecordDelegatedAttestationRequest{}, &v1.WithdrawAttestationRequest{},
		&v1.RegisterEvidenceRequest{}, &v1.RegisterRedactedEvidenceRequest{}, &v1.GetEvidenceMetadataRequest{}, &v1.GetEvidenceContentRequest{},
		&v1.ListFactsRequest{}, &v1.GetFactHistoryRequest{},
		&v1.CreateFactSnapshotRequest{}, &v1.GetFactSnapshotRequest{},
		&v1.GetFactRequest{}, &v1.ProposeFactRequest{}, &v1.ApproveFactRequest{},
		&v1.AcceptFactRequest{}, &v1.RejectFactRequest{}, &v1.RetractFactRequest{},
		&v1.DisputeFactRequest{}, &v1.ResolveDisputeRequest{}, &v1.SupersedeFactRequest{},
	), protovalidate.WithDisableLazy())
	if err != nil {
		return nil, err
	}
	slots := make(chan struct{}, limits.MaxConcurrent)
	server := connect.NewServer(func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) (rpcErr error) {
			ctx, observation := observer.start(ctx, spec.Procedure)
			defer observation.end(&rpcErr)
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
	memoryv1connect.RegisterSnapshotServiceHandler(server, &snapshotService{engine: engine})
	memoryv1connect.RegisterEvidenceServiceHandler(server, &evidenceService{engine: engine})
	memoryv1connect.RegisterDefinitionServiceHandler(server, &definitionService{engine: engine})
	memoryv1connect.RegisterAttestationServiceHandler(server, &attestationService{engine: engine})
	memoryv1connect.RegisterMemoryServiceHandler(server, &memoryService{engine: engine})
	memoryv1connect.RegisterIngestionServiceHandler(server, &ingestionService{engine: engine})
	memoryv1connect.RegisterExtractionServiceHandler(server, &extractionService{engine: engine})
	memoryv1connect.RegisterCalculationServiceHandler(server, &calculationService{engine: engine})
	memoryv1connect.RegisterAuditServiceHandler(server, &auditService{engine: engine})
	memoryv1connect.RegisterRetrievalServiceHandler(server, &retrievalService{engine: engine})
	memoryv1connect.RegisterContextServiceHandler(server, &contextService{engine: engine})
	memoryv1connect.RegisterIdentityServiceHandler(server, &identityService{engine: engine})
	memoryv1connect.RegisterOperationsServiceHandler(server, &operationsService{engine: engine})
	mux := http.NewServeMux()
	connecthttp.Mount(mux, server, connecthttp.WithReadMaxBytes(limits.MaxRequestBytes), connecthttp.WithSendMaxBytes(limits.MaxResponseBytes))
	if options.AcceptTraceContext {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			values := r.Header.Values("Traceparent")
			if len(values) == 1 && len(values[0]) <= 512 {
				ctx := (propagation.TraceContext{}).Extract(r.Context(), propagation.MapCarrier{"traceparent": values[0]})
				r = r.WithContext(ctx)
			}
			mux.ServeHTTP(w, r)
		}), nil
	}
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
