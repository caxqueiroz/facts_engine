package model

import (
	"context"
	"time"
)

type RetrievalIdentity struct {
	Generation, EmbeddingID, SearchVersion, RerankerID string
	Dimensions                                         int
}
type RetrievalDescriber interface {
	DescribeRetrieval(context.Context, string, string) (RetrievalIdentity, error)
}
type ContextRequest struct {
	Command
	Scope string
	Query RecallRequest
}
type ContextManifest struct {
	ID, Tenant, Scope                                    string
	Caller                                               Caller
	Request                                              ContextRequest
	CanonicalVersion, RedactionVersion, QueryReference   string
	Retrieval                                            RetrievalIdentity
	Items                                                []RecallItem
	Rendered                                             []byte
	RenderedDigest, Digest                               string
	SelectionRevision, IndexedRevision, CreationRevision int64
	Incomplete                                           bool
	Authorization                                        []AuthorizationDecision
}
type ContextQuery struct{ ID, Purpose string }

func ContextDigest(m ContextManifest) string {
	m.ID = ""
	m.Digest = ""
	m.CreationRevision = 0
	m.Authorization = nil
	m.Request.Key = ""
	m.Request.CorrelationID = ""
	m.Request.Query.ValidAt = m.Request.Query.ValidAt.UTC()
	return Digest(m)
}

type VersionReference struct{ RecordID, VersionID string }
type DecisionReceipt struct {
	Command
	Scope, ApplicationID, ExternalDecisionID                   string
	ContextID, SnapshotID, CalculationID                       string
	SelectedItems, UsedItems                                   []VersionReference
	MethodKind, MethodID, MethodVersion, TransformationVersion string
	Input, Output                                              ArtifactReference
	ModelRequest, ModelResponse, ActionReceipt                 *ArtifactReference
	DecidedAt                                                  time.Time
	Outcome                                                    string
}
type DecisionRecord struct {
	ID, Tenant, Digest, SourceDigest, CalculationDigest string
	Caller                                              Caller
	Receipt                                             DecisionReceipt
	Scopes                                              []string
	LedgerRevision                                      int64
	Authorization                                       []AuthorizationDecision
}
type DecisionQuery struct{ ID, Purpose string }

func DecisionDigest(r DecisionReceipt) string {
	r.Key = ""
	r.CorrelationID = ""
	r.DecidedAt = r.DecidedAt.UTC()
	return Digest(r)
}
func DecisionArtifacts(r DecisionReceipt) map[string]ArtifactReference {
	refs := map[string]ArtifactReference{"input": r.Input, "output": r.Output}
	if r.ModelRequest != nil {
		refs["model_request"] = *r.ModelRequest
	}
	if r.ModelResponse != nil {
		refs["model_response"] = *r.ModelResponse
	}
	if r.ActionReceipt != nil {
		refs["action_receipt"] = *r.ActionReceipt
	}
	return refs
}

type ContextTx interface {
	AuditTx
	ContextManifest(context.Context, string) (ContextManifest, error)
	StageContext(context.Context, ContextManifest) error
	Decision(context.Context, string) (DecisionRecord, error)
	DecisionRun(context.Context, string, string) (DecisionRecord, error)
	StageDecision(context.Context, DecisionRecord, map[VersionReference]string) error
}
