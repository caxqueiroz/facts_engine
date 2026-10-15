package model

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"
)

const ProjectionInventoryV2 = "projection-inventory-v2"
const MaxProjectionPageDocuments = 256
const MaxProjectionPageBytes = 8 << 20

type ProjectionCursor struct{ Kind, VersionID string }

func ProjectionDocumentCursor(d ProjectionDocument) ProjectionCursor {
	kind := "fact"
	if d.Memory != nil {
		kind = "memory"
	}
	return ProjectionCursor{Kind: kind, VersionID: d.VersionID}
}
func (c ProjectionCursor) Valid() bool {
	return (c.Kind == "fact" || c.Kind == "memory") && ValidID(c.VersionID)
}
func (c ProjectionCursor) Before(next ProjectionCursor) bool {
	return c.Kind < next.Kind || (c.Kind == next.Kind && c.VersionID < next.VersionID)
}

type ProjectionPage struct {
	Documents []ProjectionDocument
	// Next is the last scanned acceptance, including disposed versions. A page
	// may contain no documents and still advance. Nil means the scan is complete.
	Next *ProjectionCursor
}
type ProjectionPageLoader func(context.Context, ProjectionCursor) (ProjectionPage, error)

// PagedRebuildStore pins every page to one frozen source revision and fence.
// These are trusted administrative ports, not user-facing cursor APIs.
type PagedRebuildStore interface {
	ProjectionDocumentPage(context.Context, GenerationKey, int64, int64, ProjectionCursor, int) (ProjectionPage, error)
}
type PagedRebuildIndex interface {
	VerifyProjectionPages(context.Context, GenerationKey, int64, ProjectionPageLoader) (ProjectionCheck, error)
}

// ProjectionInventory hashes an ordered, length-delimited canonical transcript
// without retaining the whole inventory. Order is kind then version ID, using
// bytewise string comparison. The algorithm version is retained with the check.
type ProjectionInventory struct {
	hash  hash.Hash
	count int
	last  ProjectionCursor
}

func NewProjectionInventory() *ProjectionInventory {
	h := sha256.New()
	_, _ = h.Write([]byte(ProjectionInventoryV2 + "\n"))
	return &ProjectionInventory{hash: h}
}
func (i *ProjectionInventory) Add(d ProjectionDocument) error {
	cursor := ProjectionDocumentCursor(d)
	if !cursor.Valid() || !i.last.Before(cursor) {
		return ErrInvalid
	}
	payload, err := json.Marshal(d)
	if err != nil {
		return err
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(payload)))
	_, _ = i.hash.Write(length[:])
	_, _ = i.hash.Write(payload)
	i.last = cursor
	i.count++
	return nil
}
func (i *ProjectionInventory) Check() ProjectionCheck {
	return ProjectionCheck{Version: ProjectionInventoryV2, Count: i.count, Digest: hex.EncodeToString(i.hash.Sum(nil))}
}

// ValidateProjectionPage checks scan progress independently of the retained
// document inventory. Skipped tombstones may advance the cursor past the last
// document, but no returned document may fall behind a previously scanned row.
func ValidateProjectionPage(page ProjectionPage, after ProjectionCursor) error {
	if len(page.Documents) > MaxProjectionPageDocuments || (after != (ProjectionCursor{}) && !after.Valid()) {
		return ErrInvalid
	}
	last := after
	for _, d := range page.Documents {
		cursor := ProjectionDocumentCursor(d)
		if !cursor.Valid() || !last.Before(cursor) {
			return ErrInvalid
		}
		last = cursor
	}
	if page.Next != nil && (!page.Next.Valid() || !after.Before(*page.Next) || page.Next.Before(last)) {
		return ErrInvalid
	}
	return nil
}
