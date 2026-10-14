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
