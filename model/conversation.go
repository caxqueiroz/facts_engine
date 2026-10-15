package model

import (
	"context"
	"time"
)

// ConversationMessage points at already finalized immutable UTF-8 source bytes.
// SpeakerRole and SourceClass preserve source authority; neither proves truth.
type ConversationMessage struct {
	ID, Revision, Speaker, SpeakerRole, SourceClass string
	SourceTime                                      time.Time
	MediaType, Classification, RetentionPolicy      string
	Object                                          ObjectRef
}
type ConversationRequest struct {
	Command
	Scope, SourceSystem, ConversationID, Revision, Timezone string
	Messages                                                []ConversationMessage
}
type ConversationRecord struct {
	ID, Tenant, Digest string
	Caller             Caller
	Request            ConversationRequest
	EvidenceIDs        []string
	Authorization      []AuthorizationDecision
	LedgerRevision     int64
	PayloadDisposed    bool `json:",omitempty"`
}
type ConversationQuery struct{ ID, Scope, Purpose string }

func ConversationDigest(r ConversationRequest) string {
	r.Command = Command{}
	r.Messages = append([]ConversationMessage(nil), r.Messages...)
	for i := range r.Messages {
		r.Messages[i].SourceTime = r.Messages[i].SourceTime.UTC()
	}
	return Digest(r)
}
func ConversationRecordID(tenant string, r ConversationRequest) string {
	return Digest([]string{"conversation-v1", tenant, r.Scope, r.SourceSystem, r.ConversationID, r.Revision})
}
func MessageEvidence(tenant string, r ConversationRequest, m ConversationMessage) Evidence {
	source := Digest([]string{"conversation-message-v1", r.ConversationID, m.ID})
	return Evidence{ID: Digest([]string{"conversation-evidence-v1", tenant, r.Scope, r.SourceSystem, source, m.Revision}), Scope: r.Scope, SourceSystem: r.SourceSystem, SourceObject: source, SourceRevision: m.Revision, Speaker: m.Speaker, SourceClass: m.SourceClass, SourceTime: m.SourceTime.UTC(), MediaType: m.MediaType, Classification: m.Classification, RetentionPolicy: m.RetentionPolicy, Object: m.Object}
}

type ConversationTx interface {
	Tx
	Conversation(context.Context, string) (ConversationRecord, error)
	StageConversation(context.Context, ConversationRecord) error
}
