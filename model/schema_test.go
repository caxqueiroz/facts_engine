package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func schemaPtr[T any](v T) *T { return &v }
func TestValueConstraints(t *testing.T) {
	cases := []struct {
		name      string
		schema    PredicateVersion
		good, bad Value
	}{
		{"unicode length", PredicateVersion{Kind: Text, Constraints: &ValueConstraints{MinTextRunes: schemaPtr(2), MaxTextRunes: schemaPtr(2)}}, Value{Kind: Text, Text: schemaPtr("茶é")}, Value{Kind: Text, Text: schemaPtr("茶")}},
		{"integer bounds", PredicateVersion{Kind: Integer, Constraints: &ValueConstraints{MinInteger: schemaPtr(int64(-2)), MaxInteger: schemaPtr(int64(3))}}, Value{Kind: Integer, Integer: schemaPtr(int64(3))}, Value{Kind: Integer, Integer: schemaPtr(int64(4))}},
		{"exact decimal", PredicateVersion{Kind: Decimal, Constraints: &ValueConstraints{MaxDecimal: schemaPtr("9007199254740993.01")}}, Value{Kind: Decimal, Decimal: schemaPtr("9007199254740993.01")}, Value{Kind: Decimal, Decimal: schemaPtr("9007199254740993.02")}},
		{"decimal scale", PredicateVersion{Kind: Decimal, Constraints: &ValueConstraints{DecimalPlaces: schemaPtr(2)}}, Value{Kind: Decimal, Decimal: schemaPtr("1.01")}, Value{Kind: Decimal, Decimal: schemaPtr("1.001")}},
		{"unit", PredicateVersion{Kind: Decimal, Constraints: &ValueConstraints{Units: []string{"SGD"}}}, Value{Kind: Decimal, Decimal: schemaPtr("1"), Unit: "SGD"}, Value{Kind: Decimal, Decimal: schemaPtr("1"), Unit: "USD"}},
		{"enumeration", PredicateVersion{Kind: Text, Constraints: &ValueConstraints{AllowedValues: []Value{{Kind: Text, Text: schemaPtr("tea")}}}}, Value{Kind: Text, Text: schemaPtr("tea")}, Value{Kind: Text, Text: schemaPtr("coffee")}},
		{"unknown", PredicateVersion{Kind: Boolean, Constraints: &ValueConstraints{AllowUnknown: schemaPtr(false)}}, Value{Kind: Boolean, Boolean: schemaPtr(false)}, Value{Kind: Unknown}},
	}
	interval := Interval{From: schemaPtr(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.schema.ValidateSchema(); err != nil {
				t.Fatal(err)
			}
			if err := tc.schema.ValidateFact(tc.good, interval); err != nil {
				t.Fatal("valid rejected", err)
			}
			if err := tc.schema.ValidateFact(tc.bad, interval); err == nil {
				t.Fatal("invalid accepted")
			}
		})
	}
	for _, p := range []PredicateVersion{
		{Kind: Text, Constraints: &ValueConstraints{MinInteger: schemaPtr(int64(1))}},
		{Kind: Decimal, Constraints: &ValueConstraints{MinDecimal: schemaPtr("1.0")}},
		{Kind: Integer, Constraints: &ValueConstraints{MinInteger: schemaPtr(int64(2)), MaxInteger: schemaPtr(int64(1))}},
		{Kind: Text, Constraints: &ValueConstraints{AllowedValues: []Value{{Kind: Unknown}}}},
		{Kind: Text, Cardinality: "multiple"}, {Kind: Text, TemporalPrecision: "minute"},
	} {
		if p.ValidateSchema() == nil {
			t.Fatal("invalid schema", p)
		}
	}
}
func TestSchemaPrecisionAndLegacyEncoding(t *testing.T) {
	base := PredicateVersion{Kind: Text}
	b, err := json.Marshal(base)
	if err != nil || strings.Contains(string(b), "Constraints") || strings.Contains(string(b), "Cardinality") || strings.Contains(string(b), "TemporalPrecision") {
		t.Fatal("legacy encoding changed", string(b), err)
	}
	value := Value{Kind: Text, Text: schemaPtr("tea")}
	base.TemporalPrecision = "day"
	midnight := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if base.ValidateFact(value, Interval{From: &midnight}) != nil {
		t.Fatal("UTC midnight rejected")
	}
	later := midnight.Add(time.Hour)
	if base.ValidateFact(value, Interval{From: &later}) == nil {
		t.Fatal("subday boundary accepted")
	}
}
