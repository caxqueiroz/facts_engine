package model

// CalculationTombstone retains identity, lineage and integrity digests without
// method metadata, runtime configuration identity or execution details.
func CalculationTombstone(r CalculationRecord) CalculationRecord {
	r.PayloadDisposed = true
	old := r.Receipt
	r.Receipt = CalculationReceipt{Command: old.Command, Scope: old.Scope, SnapshotID: old.SnapshotID, ApplicationID: old.ApplicationID, ExternalRunID: old.ExternalRunID, ReferenceSnapshotIDs: append([]string(nil), old.ReferenceSnapshotIDs...), Input: old.Input, Output: old.Output}
	return r
}

// ValidCalculationTombstone rejects content smuggled into a marked envelope.
func ValidCalculationTombstone(r CalculationRecord, tenant, id string) bool {
	return r.PayloadDisposed && r.Tenant == tenant && r.ID == id && Digest(r) == Digest(CalculationTombstone(r))
}
