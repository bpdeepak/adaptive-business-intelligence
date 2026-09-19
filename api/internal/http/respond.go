package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"abi/internal/model"
)

// envelope wraps response data with meta (generated_at, took_ms).
func envelope(data any, start time.Time, now func() time.Time) model.Envelope {
	return model.Envelope{
		Data: data,
		Meta: model.Meta{
			GeneratedAt: now().UTC().Format(time.RFC3339),
			TookMS:      float64(time.Since(start).Microseconds()) / 1000,
		},
	}
}

// writeJSON writes an envelope as JSON with the given status.
func writeJSON(w http.ResponseWriter, status int, env model.Envelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

// writeError is a shorthand for error envelopes.
func writeError(w http.ResponseWriter, status int, msg string, now func() time.Time) {
	writeJSON(w, status, model.Envelope{Error: msg, Meta: newMeta(now)})
}
