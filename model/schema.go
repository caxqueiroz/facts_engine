package model

import (
	"encoding/json"
	"math/big"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// ValueConstraints are immutable, application-owned restrictions. Nil fields
// preserve the base value contract. Bounds are inclusive; text length is runes.
type ValueConstraints struct {
	AllowUnknown               *bool    `json:",omitempty"`
	AllowedValues              []Value  `json:",omitempty"`
	MinTextRunes, MaxTextRunes *int     `json:",omitempty"`
	MinInteger, MaxInteger     *int64   `json:",omitempty"`
	MinDecimal, MaxDecimal     *string  `json:",omitempty"`
	DecimalPlaces              *int     `json:",omitempty"`
	Units                      []string `json:",omitempty"`
}

func (p PredicateVersion) ValidateSchema() error {
	if p.Migration != nil && (!ValidID(p.Migration.FromVersion) || p.Migration.FromVersion == p.Version || p.Migration.Mode != "preserve_history_repropose" || !ValidID(p.Migration.Reason)) {
		return ErrInvalid
	}
	if p.Cardinality != "" && p.Cardinality != "single" {
		return ErrInvalid
	}
	if !slices.Contains([]string{"", "microsecond", "second", "day"}, p.TemporalPrecision) {
		return ErrInvalid
	}
	c := p.Constraints
	if c == nil {
		return nil
	}
	raw, err := json.Marshal(c)
	if err != nil || len(raw) > 64<<10 || len(c.AllowedValues) > 64 || len(c.Units) > 64 {
		return ErrInvalid
	}
	if c.MinTextRunes != nil || c.MaxTextRunes != nil {
		if p.Kind != Text {
			return ErrInvalid
		}
		for _, n := range []*int{c.MinTextRunes, c.MaxTextRunes} {
			if n != nil && (*n < 0 || *n > 65536) {
				return ErrInvalid
			}
		}
		if c.MinTextRunes != nil && c.MaxTextRunes != nil && *c.MinTextRunes > *c.MaxTextRunes {
			return ErrInvalid
		}
	}
	if c.MinInteger != nil || c.MaxInteger != nil {
		if p.Kind != Integer || (c.MinInteger != nil && c.MaxInteger != nil && *c.MinInteger > *c.MaxInteger) {
			return ErrInvalid
		}
	}
	if c.MinDecimal != nil || c.MaxDecimal != nil || c.DecimalPlaces != nil || len(c.Units) > 0 {
		if p.Kind != Decimal {
			return ErrInvalid
		}
		for _, bound := range []*string{c.MinDecimal, c.MaxDecimal} {
			if bound != nil && (Value{Kind: Decimal, Decimal: bound}).Validate() != nil {
				return ErrInvalid
			}
		}
		if c.MinDecimal != nil && c.MaxDecimal != nil && decimalCompare(*c.MinDecimal, *c.MaxDecimal) > 0 {
			return ErrInvalid
		}
		if c.DecimalPlaces != nil && (*c.DecimalPlaces < 0 || *c.DecimalPlaces > 127) {
			return ErrInvalid
		}
		seen := map[string]bool{}
		for _, unit := range c.Units {
			if len(unit) > 32 || !utf8.ValidString(unit) || seen[unit] {
				return ErrInvalid
			}
			seen[unit] = true
		}
	}
	seen := map[string]bool{}
	for _, value := range c.AllowedValues {
		// Unknown is controlled separately and cannot be an enumeration member.
		if value.Kind != p.Kind || value.Validate() != nil || p.validateValue(value, false) != nil {
			return ErrInvalid
		}
		digest := Digest(value)
		if seen[digest] {
			return ErrInvalid
		}
		seen[digest] = true
	}
	return nil
}

func decimalCompare(a, b string) int {
	x, _ := new(big.Rat).SetString(a)
	y, _ := new(big.Rat).SetString(b)
	return x.Cmp(y)
}

func (p PredicateVersion) validateValue(v Value, enumeration bool) error {
	if v.Validate() != nil || (v.Kind != Unknown && v.Kind != p.Kind) {
		return ErrInvalid
	}
	c := p.Constraints
	if c == nil {
		return nil
	}
	if v.Kind == Unknown {
		if c.AllowUnknown != nil && !*c.AllowUnknown {
			return ErrInvalid
		}
		return nil
	}
	if enumeration && len(c.AllowedValues) > 0 {
		found := false
		for _, allowed := range c.AllowedValues {
			if Digest(v) == Digest(allowed) {
				found = true
				break
			}
		}
		if !found {
			return ErrInvalid
		}
	}
	switch v.Kind {
	case Text:
		n := utf8.RuneCountInString(*v.Text)
		if c.MinTextRunes != nil && n < *c.MinTextRunes || c.MaxTextRunes != nil && n > *c.MaxTextRunes {
			return ErrInvalid
		}
	case Integer:
		if c.MinInteger != nil && *v.Integer < *c.MinInteger || c.MaxInteger != nil && *v.Integer > *c.MaxInteger {
			return ErrInvalid
		}
	case Decimal:
		if c.MinDecimal != nil && decimalCompare(*v.Decimal, *c.MinDecimal) < 0 || c.MaxDecimal != nil && decimalCompare(*v.Decimal, *c.MaxDecimal) > 0 {
			return ErrInvalid
		}
		if len(c.Units) > 0 && !slices.Contains(c.Units, v.Unit) {
			return ErrInvalid
		}
		if c.DecimalPlaces != nil {
			_, fraction, _ := strings.Cut(*v.Decimal, ".")
			if len(fraction) > *c.DecimalPlaces {
				return ErrInvalid
			}
		}
	}
	return nil
}

// ValidateFact applies the same deterministic schema to manual and model proposals.
func (p PredicateVersion) ValidateFact(v Value, interval Interval) error {
	if p.ValidateSchema() != nil || p.validateValue(v, true) != nil || interval.Validate() != nil {
		return ErrInvalid
	}
	for _, at := range []*time.Time{interval.From, interval.To, v.Instant} {
		if at == nil {
			continue
		}
		switch p.TemporalPrecision {
		case "microsecond":
			if at.Nanosecond()%1000 != 0 {
				return ErrInvalid
			}
		case "second":
			if at.Nanosecond() != 0 {
				return ErrInvalid
			}
		case "day":
			u := at.UTC()
			if u.Hour() != 0 || u.Minute() != 0 || u.Second() != 0 || u.Nanosecond() != 0 {
				return ErrInvalid
			}
		}
	}
	return nil
}
