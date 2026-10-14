package model

// ExtractionChunkQuery plans original evidence ranges without creating jobs or
// consuming provider quota. MaxSourceBytes excludes the serialized envelope.
type ExtractionChunkQuery struct {
	RedactedEvidenceIDs                       []string
	Scope, ConversationID, ProfileID, Purpose string
	InputRanges                               []ExtractionInputRange
	MaxSourceBytes                            int
}

type ExtractionChunk struct {
	InputRanges []ExtractionInputRange
	SourceBytes int
}

// ExtractionChunkPlan is an authorized preview, not a persisted job or a quota
// reservation. StartExtraction rechecks authorization and current profile approval.
type ExtractionChunkPlan struct {
	RedactedEvidenceIDs                                        []string `json:",omitempty"`
	Version, Tenant, Scope, ConversationID, ConversationDigest string
	ProfileID, ProfileDigest                                   string
	MaxSourceBytes                                             int
	Chunks                                                     []ExtractionChunk
	ExcludedRanges                                             []ExtractionInputRange
}
