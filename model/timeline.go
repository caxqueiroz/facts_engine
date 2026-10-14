package model

import "sort"

// Subtract returns the unaffected pieces of a known-start interval.
func Subtract(original, cut Interval) []Interval {
	if !original.Overlaps(cut) {
		return []Interval{original}
	}
	out := []Interval{}
	if original.From.Before(*cut.From) {
		end := *cut.From
		out = append(out, Interval{From: original.From, To: &end})
	}
	if cut.To != nil && (original.To == nil || cut.To.Before(*original.To)) {
		start := *cut.To
		out = append(out, Interval{From: &start, To: original.To})
	}
	return out
}
func Intersect(a, b Interval) Interval {
	from := a.From
	if from.Before(*b.From) {
		from = b.From
	}
	to := a.To
	if to == nil || (b.To != nil && b.To.Before(*to)) {
		to = b.To
	}
	return Interval{From: from, To: to}
}
func SortTimeline(entries []Acceptance) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Valid.From.Before(*entries[j].Valid.From) })
}
