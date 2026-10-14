package model

import (
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }
func TestValues(t *testing.T) {
	cases := []struct {
		name  string
		v     Value
		valid bool
	}{
		{"false", Value{Kind: Boolean, Boolean: ptr(false)}, true}, {"zero", Value{Kind: Integer, Integer: ptr(int64(0))}, true}, {"unknown", Value{Kind: Unknown}, true}, {"mixed", Value{Kind: Text, Text: ptr("x"), Boolean: ptr(false)}, false}, {"wrong tag", Value{Kind: Text, Integer: ptr(int64(1))}, false}, {"decimal", Value{Kind: Decimal, Decimal: ptr("123.45"), Unit: "SGD"}, true}, {"trailing zeros", Value{Kind: Decimal, Decimal: ptr("1.00")}, false}, {"negative zero", Value{Kind: Decimal, Decimal: ptr("-0")}, false}, {"exponent", Value{Kind: Decimal, Decimal: ptr("1e3")}, false}, {"bad date", Value{Kind: Date, Date: ptr("2026-02-30")}, false}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if (c.v.Validate() == nil) != c.valid {
				t.Fatal("unexpected validation")
			}
		})
	}
}
func TestHalfOpenIntervals(t *testing.T) {
	a := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := a.Add(time.Hour)
	c := b.Add(time.Hour)
	i := Interval{&a, &b}
	if i.Contains(b) || !i.Contains(a) || i.Overlaps(Interval{&b, &c}) {
		t.Fatal("half-open boundary")
	}
	if (Interval{}).Contains(a) {
		t.Fatal("unknown start became current")
	}
}
