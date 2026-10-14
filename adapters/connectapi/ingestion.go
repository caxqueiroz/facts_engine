package connectapi

import (
	"context"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
)

type IngestionEngine interface {
	IngestConversation(context.Context, model.Caller, model.ConversationRequest) (model.ConversationRecord, error)
	GetConversation(context.Context, model.Caller, model.ConversationQuery) (model.ConversationRecord, error)
}
type ingestionService struct{ engine IngestionEngine }

func (s *ingestionService) IngestConversation(ctx context.Context, r *v1.IngestConversationRequest) (*v1.IngestConversationResponse, error) {
	input := r.Conversation
	q := model.ConversationRequest{Command: command(input.Command), Scope: input.Scope, SourceSystem: input.SourceSystem, ConversationID: input.ConversationId, Revision: input.Revision, Timezone: input.Timezone}
	for _, m := range input.Messages {
		q.Messages = append(q.Messages, model.ConversationMessage{ID: m.Id, Revision: m.Revision, Speaker: m.Speaker, SpeakerRole: m.SpeakerRole, SourceClass: m.SourceClass, SourceTime: m.SourceTime.AsTime(), MediaType: m.MediaType, Classification: m.Classification, RetentionPolicy: m.RetentionPolicy, Object: objectFromProto(m.Object)})
	}
	out, err := s.engine.IngestConversation(ctx, callerFrom(ctx), q)
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.IngestConversationResponse{Conversation: c.conversation(out)}
	return response, c.err
}
func (s *ingestionService) GetConversation(ctx context.Context, r *v1.GetConversationRequest) (*v1.GetConversationResponse, error) {
	out, err := s.engine.GetConversation(ctx, callerFrom(ctx), model.ConversationQuery{ID: r.Id, Scope: r.Scope, Purpose: r.Purpose})
	if err != nil {
		return nil, err
	}
	c := responseConverter{}
	response := &v1.GetConversationResponse{Conversation: c.conversation(out)}
	return response, c.err
}
func (c *responseConverter) conversation(record model.ConversationRecord) *v1.ConversationRecord {
	r := record.Request
	request := &v1.ConversationInput{Command: &v1.Command{Key: r.Key, Purpose: r.Purpose, CorrelationId: r.CorrelationID}, Scope: r.Scope, SourceSystem: r.SourceSystem, ConversationId: r.ConversationID, Revision: r.Revision, Timezone: r.Timezone}
	for _, m := range r.Messages {
		request.Messages = append(request.Messages, &v1.ConversationMessage{Id: m.ID, Revision: m.Revision, Speaker: m.Speaker, SpeakerRole: m.SpeakerRole, SourceClass: m.SourceClass, SourceTime: c.timestamp(m.SourceTime), MediaType: m.MediaType, Classification: m.Classification, RetentionPolicy: m.RetentionPolicy, Object: objectToProto(m.Object)})
	}
	out := &v1.ConversationRecord{Id: record.ID, Tenant: record.Tenant, Digest: record.Digest, Caller: callerToProto(record.Caller), Request: request, EvidenceIds: record.EvidenceIDs, LedgerRevision: record.LedgerRevision}
	for _, a := range record.Authorization {
		out.Authorization = append(out.Authorization, c.authorization(a))
	}
	return out
}
