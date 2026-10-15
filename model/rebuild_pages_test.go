package model

import "testing"

func TestProjectionInventoryRequiresCanonicalOrder(t *testing.T) {
	inventory := NewProjectionInventory()
	fact := ProjectionDocument{VersionID: "a", VersionDigest: "original"}
	memory := ProjectionDocument{VersionID: "a", Memory: &MemoryProjection{Body: "memory"}}
	if err := inventory.Add(fact); err != nil {
		t.Fatal(err)
	}
	if err := inventory.Add(fact); err != ErrInvalid {
		t.Fatal("duplicate", err)
	}
	if err := inventory.Add(memory); err != nil {
		t.Fatal(err)
	}
	if err := inventory.Add(ProjectionDocument{VersionID: "z"}); err != ErrInvalid {
		t.Fatal("kind order", err)
	}
	check := inventory.Check()
	if check.Version != ProjectionInventoryV2 || check.Count != 2 || len(check.Digest) != 64 {
		t.Fatal(check)
	}
	again := NewProjectionInventory()
	if err := again.Add(fact); err != nil {
		t.Fatal(err)
	}
	if err := again.Add(memory); err != nil {
		t.Fatal(err)
	}
	if again.Check() != check {
		t.Fatal("nondeterministic inventory")
	}
	changed := NewProjectionInventory()
	fact.VersionDigest = "changed"
	if err := changed.Add(fact); err != nil {
		t.Fatal(err)
	}
	if err := changed.Add(memory); err != nil {
		t.Fatal(err)
	}
	if changed.Check().Digest == check.Digest {
		t.Fatal("content omitted from proof")
	}
}

func TestProjectionPagesCanAdvanceAcrossDisposedVersions(t *testing.T) {
	cursor := func(id string) *ProjectionCursor { return &ProjectionCursor{Kind: "fact", VersionID: id} }
	doc := ProjectionDocument{VersionID: "b"}
	tests := []struct {
		name  string
		page  ProjectionPage
		after ProjectionCursor
		valid bool
	}{
		{"empty terminal", ProjectionPage{}, ProjectionCursor{}, true},
		{"disposed only", ProjectionPage{Next: cursor("b")}, *cursor("a"), true},
		{"disposed tail", ProjectionPage{Documents: []ProjectionDocument{doc}, Next: cursor("c")}, *cursor("a"), true},
		{"same cursor", ProjectionPage{Next: cursor("a")}, *cursor("a"), false},
		{"backwards cursor", ProjectionPage{Next: cursor("a")}, *cursor("b"), false},
		{"invalid cursor", ProjectionPage{Next: &ProjectionCursor{Kind: "other", VersionID: "b"}}, ProjectionCursor{}, false},
		{"document behind scan", ProjectionPage{Documents: []ProjectionDocument{doc}}, *cursor("c"), false},
		{"cursor behind document", ProjectionPage{Documents: []ProjectionDocument{doc}, Next: cursor("a")}, ProjectionCursor{}, false},
		{"duplicate document", ProjectionPage{Documents: []ProjectionDocument{doc, doc}}, ProjectionCursor{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateProjectionPage(tt.page, tt.after)
			if (err == nil) != tt.valid {
				t.Fatal(err)
			}
		})
	}
}
