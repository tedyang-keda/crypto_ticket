package exchange

import (
	"fmt"
	"strconv"
	"strings"
)

func firstNonEmpty(values ...any) any {
	for _, value := range values {
		if strings.TrimSpace(stringValue(value)) != "" {
			return value
		}
	}
	return nil
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case int:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case jsonNumber:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}

func floatValue(value any) float64 {
	return parseFloat(stringValue(value))
}

func intValue(value any) int64 {
	return parseInt(stringValue(value))
}

func parseFloat(value string) float64 {
	parsed, _ := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return parsed
}

func parseInt(value string) int64 {
	parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return parsed
}

type jsonNumber interface {
	String() string
}
