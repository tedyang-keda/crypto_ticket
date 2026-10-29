package market

// MinuteAuditTask is one durable range of completed one-minute candles. Times
// in StartMS and EndMS are inclusive candle start keys, not wall-clock bounds.
type MinuteAuditTask struct {
	ID              int64  `json:"id"`
	Exchange        string `json:"exchange"`
	Symbol          string `json:"symbol"`
	MarketType      string `json:"market_type"`
	Kind            string `json:"kind"`
	StartMS         int64  `json:"start_ms"`
	EndMS           int64  `json:"end_ms"`
	ExpectedStartMS int64  `json:"expected_start_ms"`
	ExpectData      bool   `json:"expect_data"`
	Status          string `json:"status"`
	Attempts        int    `json:"attempts"`
	NextAttemptMS   int64  `json:"next_attempt_ms"`
	LeaseUntilMS    int64  `json:"lease_until_ms"`
	LeaseToken      string `json:"lease_token,omitempty"`
	ForceReplay     bool   `json:"force_replay"`
	LastError       string `json:"last_error,omitempty"`
	CreatedAtMS     int64  `json:"created_at_ms"`
	UpdatedAtMS     int64  `json:"updated_at_ms"`
}

// MinuteAuditCursor records how far work was registered, not how far it was
// verified. Completion is represented by durable task outcomes separately.
type MinuteAuditCursor struct {
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	Kind        string `json:"kind"`
	ThroughMS   int64  `json:"through_ms"`
	UpdatedAtMS int64  `json:"updated_at_ms"`
}

// MinuteAuditSummary deliberately distinguishes unverified work from a
// successful most-recent rolling audit.
type MinuteAuditSummary struct {
	Pending         int   `json:"pending"`
	Running         int   `json:"running"`
	Blocked         int   `json:"blocked"`
	Expired         int   `json:"expired"`
	Excluded        int   `json:"excluded"`
	OldestStartMS   int64 `json:"oldest_start_ms"`
	DailyCompleted  int   `json:"daily_completed"`
	DailyTotal      int   `json:"daily_total"`
	DailyDayStartMS int64 `json:"daily_day_start_ms"`
	UpdatedAtMS     int64 `json:"updated_at_ms"`
}
