package postgres

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func nullableTime(value *time.Time) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *value, Valid: true}
}

func nullableText(value *string) pgtype.Text {
	if value == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *value, Valid: true}
}

func nullableID(id string) pgtype.Text { return pgtype.Text{String: id, Valid: id != ""} }
