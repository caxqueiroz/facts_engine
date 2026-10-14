package services

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/caxqueiroz/facts_engine/model"
)

func TestExtractionChunksCoverSelectedBytesExactly(t *testing.T) {
	// Exercise remainders smaller than the next rune, escaped JSON characters,
	// empty sources and multiple original ranges with gaps in a single message.
	sources := [][]byte{[]byte("a私\"😀\n tea"), {}, []byte("end"), []byte("abc OMIT xyz")}
	selections := []model.ExtractionInputRange{{EvidenceID: "a", End: len(sources[0])}, {EvidenceID: "empty"}, {EvidenceID: "b", End: len(sources[2])}, {EvidenceID: "c", End: 3}, {EvidenceID: "c", Start: 9, End: 12}}
	originals := map[string][]byte{"a": sources[0], "empty": sources[1], "b": sources[2], "c": sources[3]}
	for maximum := 4; maximum <= 30; maximum++ {
		var chunks []model.ExtractionChunk
		var expected []byte
		for _, r := range selections {
			b := originals[r.EvidenceID]
			expected = append(expected, b[r.Start:r.End]...)
			if err := appendExtractionChunks(&chunks, r, b, maximum); err != nil {
				t.Fatal(err)
			}
		}
		var actual []byte
		offsets := map[string]int{}
		for _, chunk := range chunks {
			if chunk.SourceBytes < 1 || chunk.SourceBytes > maximum || len(chunk.InputRanges) > 256 {
				t.Fatal(maximum, chunk)
			}
			total := 0
			for _, r := range chunk.InputRanges {
				b := originals[r.EvidenceID][r.Start:r.End]
				if !utf8.Valid(b) || r.Start < offsets[r.EvidenceID] {
					t.Fatal("split rune or overlap", r)
				}
				offsets[r.EvidenceID] = r.End
				actual = append(actual, b...)
				total += len(b)
			}
			if total != chunk.SourceBytes {
				t.Fatal(chunk)
			}
		}
		if !bytes.Equal(actual, expected) {
			t.Fatal("lost or inserted bytes", maximum, string(actual), string(expected))
		}
		var again []model.ExtractionChunk
		for _, r := range selections {
			if err := appendExtractionChunks(&again, r, originals[r.EvidenceID], maximum); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(chunks, again) {
			t.Fatal("nondeterministic boundaries")
		}
	}
}

func TestExtractionChunkBounds(t *testing.T) {
	var chunks []model.ExtractionChunk
	large := []byte(strings.Repeat("x", 4*maximumExtractionChunks+1))
	if err := appendExtractionChunks(&chunks, model.ExtractionInputRange{End: len(large)}, large, 4); err != model.ErrPrecondition {
		t.Fatal("chunk limit", err)
	}
	for _, r := range []model.ExtractionInputRange{{Start: -1, End: 1}, {Start: 1, End: 0}, {End: 4}, {Start: 1, End: 3}} {
		chunks = nil
		if err := appendExtractionChunks(&chunks, r, []byte("私"), 4); err != model.ErrInvalid {
			t.Fatal("invalid range", r, err)
		}
	}
	chunks = nil
	if err := appendExtractionChunks(&chunks, model.ExtractionInputRange{End: 1}, []byte("a"), 3); err != model.ErrInvalid {
		t.Fatal("unsafe byte budget", err)
	}
}
