package simulator

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func nowRFC3339Millis() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func randomUUID(r *rand.Rand) string {
	const hex = "0123456789abcdef"
	b := make([]byte, 36)
	for i := range b {
		b[i] = hex[r.Intn(len(hex))]
	}
	b[8], b[13], b[18], b[23] = '-', '-', '-', '-'
	b[14] = '4'
	b[19] = "89ab"[r.Intn(4)]
	return string(b)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func tickerChan(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }

func simulatedInterval(base time.Duration, speed int) time.Duration {
	return base / time.Duration(maxInt(1, speed))
}

func simulatedSecondsToTicks(totalSeconds, secondsPerTick int) int {
	if secondsPerTick <= 0 {
		secondsPerTick = 1
	}
	if totalSeconds <= 0 {
		return 1
	}
	return maxInt(1, (totalSeconds+secondsPerTick-1)/secondsPerTick)
}

func asInt(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	case json.Number:
		i, err := t.Int64()
		if err == nil {
			return int(i), true
		}
		f, err := t.Float64()
		if err == nil {
			return int(f), true
		}
		return 0, false
	default:
		return 0, false
	}
}
