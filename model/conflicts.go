package model

// RevisionConflict describes the record state observed under the mutation's
// record lock. It is a refresh hint, not a reservation or permission to overwrite.
// Error deliberately contains no record identifiers or revision values.
type RevisionConflict struct {
	RecordID                          string
	ExpectedRevision, CurrentRevision int64
}

func (e *RevisionConflict) Error() string { return ErrRevision.Error() }
func (e *RevisionConflict) Unwrap() error { return ErrRevision }
