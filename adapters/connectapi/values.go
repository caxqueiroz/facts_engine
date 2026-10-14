package connectapi

import (
	"errors"
	"time"

	v1 "github.com/caxqueiroz/facts_engine/gen/memory/v1"
	"github.com/caxqueiroz/facts_engine/model"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func command(c *v1.Command) model.Command {
	return model.Command{Key: c.Key, Purpose: c.Purpose, CorrelationID: c.CorrelationId}
}
func factKey(k *v1.FactKey) model.FactKey {
	return model.FactKey{Scope: k.Scope, Subject: k.Subject, Predicate: k.Predicate, Discriminator: k.Discriminator}
}
func optionalTime(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	value := t.AsTime()
	return &value
}
func interval(i *v1.Interval) model.Interval {
	return model.Interval{From: optionalTime(i.From), To: optionalTime(i.To)}
}
func commit(c model.CommitResult) *v1.CommitResult {
	return &v1.CommitResult{Id: c.ID, RecordId: c.RecordID, VersionId: c.VersionID, RecordKind: c.RecordKind, ContentDigest: c.ContentDigest, StreamRevision: c.StreamRevision, LedgerRevision: c.LedgerRevision}
}

func valueFromProto(value *v1.Value) (model.Value, error) {
	if value == nil {
		return model.Value{}, model.ErrInvalid
	}
	var out model.Value
	switch v := value.Payload.(type) {
	case *v1.Value_Text:
		out = model.Value{Kind: model.Text, Text: &v.Text}
	case *v1.Value_Boolean:
		out = model.Value{Kind: model.Boolean, Boolean: &v.Boolean}
	case *v1.Value_Integer:
		out = model.Value{Kind: model.Integer, Integer: &v.Integer}
	case *v1.Value_Decimal:
		if v.Decimal == nil {
			return out, model.ErrInvalid
		}
		out = model.Value{Kind: model.Decimal, Decimal: &v.Decimal.Amount, Unit: v.Decimal.Unit}
	case *v1.Value_Date:
		out = model.Value{Kind: model.Date, Date: &v.Date}
	case *v1.Value_Instant:
		if v.Instant.CheckValid() != nil {
			return out, model.ErrInvalid
		}
		out = model.Value{Kind: model.Instant, Instant: optionalTime(v.Instant)}
	case *v1.Value_Entity:
		out = model.Value{Kind: model.Entity, Entity: &v.Entity}
	case *v1.Value_Unknown:
		if v.Unknown == nil {
			return out, model.ErrInvalid
		}
		out = model.Value{Kind: model.Unknown}
	default:
		return out, model.ErrInvalid
	}
	return out, out.Validate()
}
func valueToProto(value model.Value) (*v1.Value, error) {
	// Invalid engine output is an internal error, not a client input error.
	if value.Validate() != nil {
		return nil, errors.New("invalid engine value")
	}
	out := &v1.Value{}
	switch value.Kind {
	case model.Text:
		out.Payload = &v1.Value_Text{Text: *value.Text}
	case model.Boolean:
		out.Payload = &v1.Value_Boolean{Boolean: *value.Boolean}
	case model.Integer:
		out.Payload = &v1.Value_Integer{Integer: *value.Integer}
	case model.Decimal:
		out.Payload = &v1.Value_Decimal{Decimal: &v1.DecimalValue{Amount: *value.Decimal, Unit: value.Unit}}
	case model.Date:
		out.Payload = &v1.Value_Date{Date: *value.Date}
	case model.Instant:
		at := timestamppb.New(*value.Instant)
		if at.CheckValid() != nil {
			return nil, errors.New("engine instant outside protobuf range")
		}
		out.Payload = &v1.Value_Instant{Instant: at}
	case model.Entity:
		out.Payload = &v1.Value_Entity{Entity: *value.Entity}
	case model.Unknown:
		out.Payload = &v1.Value_Unknown{Unknown: &v1.UnknownValue{}}
	}
	return out, nil
}

func sourcesFromProto(sources []*v1.SourceSpan) ([]model.SourceSpan, error) {
	var out []model.SourceSpan
	for _, span := range sources {
		if span.Start > int64(int(^uint(0)>>1)) || span.End > int64(int(^uint(0)>>1)) {
			return nil, model.ErrInvalid
		}
		out = append(out, model.SourceSpan{EvidenceID: span.EvidenceId, Start: int(span.Start), End: int(span.End), Quote: span.Quote})
	}
	return out, nil
}
