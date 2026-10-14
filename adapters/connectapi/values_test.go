package connectapi

import (
	"testing"
	"time"

	"buf.build/go/protovalidate"
	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValueRoundTrip(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	values := []*v1.Value{
		{Payload: &v1.Value_Text{Text: ""}},
		{Payload: &v1.Value_Boolean{Boolean: false}},
		{Payload: &v1.Value_Integer{Integer: 0}},
		{Payload: &v1.Value_Decimal{Decimal: &v1.DecimalValue{Amount: "-0.001", Unit: "USD"}}},
		{Payload: &v1.Value_Date{Date: "2024-02-29"}},
		{Payload: &v1.Value_Instant{Instant: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC))}},
		{Payload: &v1.Value_Entity{Entity: "subject-1"}},
		{Payload: &v1.Value_Unknown{Unknown: &v1.UnknownValue{}}},
	}
	for _, value := range values {
		if err := validator.Validate(value); err != nil {
			t.Fatal(err)
		}
		domain, err := valueFromProto(value)
		if err != nil {
			t.Fatal(err)
		}
		got, err := valueToProto(domain)
		if err != nil || !proto.Equal(value, got) {
			t.Fatalf("round trip %v: %v, %v", value, got, err)
		}
	}
	for _, amount := range []string{"", "-0", "00", "1.0", "1e3", "+1", ".1"} {
		if err := validator.Validate(&v1.Value{Payload: &v1.Value_Decimal{Decimal: &v1.DecimalValue{Amount: amount}}}); err == nil {
			t.Errorf("accepted decimal %q", amount)
		}
	}
	for _, value := range []*v1.Value{
		{}, {Payload: &v1.Value_Date{Date: "2025-02-29"}},
		{Payload: &v1.Value_Instant{Instant: &timestamppb.Timestamp{Seconds: 253402300800}}},
	} {
		if _, err := valueFromProto(value); err == nil {
			t.Errorf("accepted invalid value %v", value)
		}
	}
}
func TestIntervalValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		interval *v1.Interval
		valid    bool
	}{
		{"unknown", &v1.Interval{}, true},
		{"epoch", &v1.Interval{From: &timestamppb.Timestamp{}}, true},
		{"microseconds", &v1.Interval{From: &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 123456000}}, true},
		{"nanoseconds", &v1.Interval{From: &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 123456789}}, false},
		{"zero time", &v1.Interval{From: timestamppb.New(time.Time{})}, false},
		{"equal", &v1.Interval{From: &timestamppb.Timestamp{}, To: &timestamppb.Timestamp{}}, false},
		{"reversed", &v1.Interval{From: &timestamppb.Timestamp{Seconds: 1}, To: &timestamppb.Timestamp{}}, false},
		{"invalid nanos", &v1.Interval{From: &timestamppb.Timestamp{Nanos: 1000000000}}, false},
		{"out of range", &v1.Interval{From: &timestamppb.Timestamp{Seconds: 253402300800}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateMessage(validator, tc.interval); (err == nil) != tc.valid {
				t.Fatalf("valid=%v, err=%v", tc.valid, err)
			}
		})
	}
}
