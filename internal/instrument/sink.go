package instrument

import (
	"context"
	"log"

	"crypto-ticket/internal/market"
)

// LogSink is the default EventSink: it records each corporate-action candidate
// to the standard logger. It is the seam where the factor-derivation stage
// (layer B) will later hook in to compute adjustment factors from the affected
// bars.
type LogSink struct{}

// HandleInstrumentEvent logs the candidate event and never errors.
func (LogSink) HandleInstrumentEvent(_ context.Context, event market.InstrumentChangeEvent) error {
	log.Printf("%s instrument corporate-action event symbol=%s type=%s ts=%d prev=%s cur=%s",
		event.Exchange, event.Symbol, event.EventType, event.EventTSMS,
		event.PreviousHash, event.CurrentHash)
	return nil
}
