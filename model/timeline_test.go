package model

import (
	"testing"
	"time"
)

func TestSubtractIntervals(t *testing.T) {
	at := func(hour int) *time.Time { x := time.Date(2026, 1, 1, hour, 0, 0, 0, time.UTC); return &x }
	cases := []struct {
		name          string
		original, cut Interval
		want          []Interval
	}{
		{"middle", Interval{at(1), at(10)}, Interval{at(3), at(7)}, []Interval{{at(1), at(3)}, {at(7), at(10)}}},
		{"future", Interval{at(1), nil}, Interval{at(5), nil}, []Interval{{at(1), at(5)}}},
		{"backdated", Interval{at(5), nil}, Interval{at(1), at(7)}, []Interval{{at(7), nil}}},
		{"touch", Interval{at(1), at(5)}, Interval{at(5), nil}, []Interval{{at(1), at(5)}}},
		{"complete", Interval{at(1), nil}, Interval{at(1), nil}, []Interval{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Subtract(c.original, c.cut)
			if Digest(got) != Digest(c.want) {
				t.Fatal(got, c.want)
			}
		})
	}
}
func TestIntervalRejectsLossyPrecision(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 1, time.UTC)
	if (Interval{From: &at}).Validate() == nil {
		t.Fatal("nanosecond boundary would be rounded by PostgreSQL")
	}
}
