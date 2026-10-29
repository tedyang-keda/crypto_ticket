package timeframe

import (
	"errors"
	"fmt"
)

// ErrDisabledTimeframe identifies a known historical interval that is no longer
// offered by the market service. Its calendar arithmetic remains available for
// reading old records, retention and frozen audit reports.
var ErrDisabledTimeframe = errors.New("disabled timeframe")

// ValidateForExchange checks the service's exchange-specific interval policy.
// Normalize intentionally stays exchange-independent for historical tooling.
func ValidateForExchange(exchange, tf string) error {
	if _, err := Normalize(tf); err != nil {
		return err
	}
	if tf == "3D" {
		return fmt.Errorf("%w: %s %s", ErrDisabledTimeframe, exchange, tf)
	}
	return nil
}

// SupportedForExchange returns a fresh list so callers cannot modify Order.
func SupportedForExchange(exchange string) []string {
	out := make([]string, 0, len(Order))
	for _, tf := range Order {
		if ValidateForExchange(exchange, tf) == nil {
			out = append(out, tf)
		}
	}
	return out
}
