package model

import (
	"bytes"
	"encoding/json"
	"io"
)

// ValidExtractionTombstoneJSON checks the shape of content-free retained metadata.
// It is not authorization or validation of the original unavailable content.
func ValidExtractionTombstoneJSON(kind string, raw []byte) bool {
	decode := func(v any) bool {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		return d.Decode(v) == nil && d.Decode(new(any)) == io.EOF
	}
	switch kind {
	case "extraction_job":
		var v ExtractionJob
		return decode(&v) && v.PayloadDisposed && Digest(v) == Digest(ExtractionJobTombstone(v))
	case "extraction_attempt":
		var v ExtractionAttempt
		return decode(&v) && v.PayloadDisposed && Digest(v) == Digest(ExtractionAttemptTombstone(v))
	case "extraction_result":
		var v ExtractionResult
		return decode(&v) && v.PayloadDisposed && Digest(v) == Digest(ExtractionResultTombstone(v))
	}
	return false
}
