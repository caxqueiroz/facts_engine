package model

// Cost units are host-approved integer accounting units, e.g. USD micro-units.
// Amounts are conservative reservations, not provider billing measurements.
type ExtractionCostBudget struct {
	Unit                   string
	DailyLimit, PerAttempt int64
}
type ExtractionReservation struct {
	Day                       string
	Tokens, DailyTokenLimit   int64
	ConcurrentLimit           int
	CostUnit                  string `json:",omitempty"`
	CostUnits, DailyCostLimit int64  `json:",omitempty"`
}
type ExtractionAdmission struct {
	JobStatus
	Reservation ExtractionReservation
}
