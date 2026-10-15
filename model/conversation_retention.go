package model

import "encoding/hex"

// ConversationTombstone removes message references and object locators while
// retaining the conversation identity, original digest and source inventory.
func ConversationTombstone(r ConversationRecord) ConversationRecord {
	r.PayloadDisposed = true
	r.Request.Messages = nil
	return r
}

func ValidConversationTombstone(r ConversationRecord, tenant, id string) bool {
	digest, err := hex.DecodeString(r.Digest)
	return err == nil && len(digest) == 32 && r.Tenant == tenant && r.ID == id && r.PayloadDisposed && Digest(r) == Digest(ConversationTombstone(r))
}
