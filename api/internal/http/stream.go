package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"abi/internal/model"
)

// handleMetricsStream is the SSE long-poll for live metrics:
//
//	event: metrics
//	data: {snapshot?, current, anomaly?, speed_multiplier, status}
//
// The first frame carries the broadcaster's latest published state (which the
// server seeds with a DB snapshot on startup), then every aggregate flush and
// every fresh anomaly fires a frame.
func (s *Server) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	if s.live == nil {
		writeError(w, http.StatusServiceUnavailable, "realtime stream unavailable (run cmd/server)", s.now)
		return
	}

	// Long-lived response: clear the server's write timeout for this writer.
	if ctrl := http.NewResponseController(w); ctrl != nil {
		_ = ctrl.SetWriteDeadline(time.Time{})
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported", s.now)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Snapshot immediately so clients render without waiting for a flush.
	if last := s.live.Last(); last.Current.BucketStart != "" {
		if !writeSSE(w, flusher, last) {
			return
		}
	}

	sub, unsub := s.live.Subscribe()
	defer unsub()

	clientGone := r.Context().Done()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-clientGone:
			return
		case u, ok := <-sub:
			if !ok {
				return
			}
			if !writeSSE(w, flusher, u) {
				return
			}
		case <-heartbeat.C:
			// Comment-only frame keeps idle proxies from timing the stream out.
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// writeSSE emits one metrics event; returns false when the client went away.
func writeSSE(w io.Writer, flusher http.Flusher, u model.MetricsUpdate) bool {
	raw, err := json.Marshal(u)
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "event: metrics\ndata: %s\n\n", raw); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

// handleRealtimeMetrics returns the most recent one-minute buckets in ascending
// time order — the non-streaming read surface for the live tiles (SSE/gRPC
// stream the same data). `limit` caps the number of buckets (default 60, max
// 720 = 12h at 1-minute granularity).
func (s *Server) handleRealtimeMetrics(w http.ResponseWriter, r *http.Request) {
	n := 60
	if v := r.URL.Query().Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= 720 {
			n = parsed
		}
	}
	start := time.Now()
	buckets, err := s.store.RecentRealtimeMetrics(r.Context(), n)
	if err != nil {
		s.log.Error("realtime metrics", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(map[string]any{
		"buckets": buckets,
	}, start, s.now))
}

// handleAnomalies returns open (undismissed) anomalies, newest first.
func (s *Server) handleAnomalies(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	anoms, err := s.store.OpenAnomalies(r.Context())
	if err != nil {
		s.log.Error("anomalies", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error", s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(anoms, start, s.now))
}

// handleDismissAnomaly marks an anomaly dismissed (dashboard banner clears).
func (s *Server) handleDismissAnomaly(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "anomaly id must be a positive integer", s.now)
		return
	}
	if err := s.store.DismissAnomaly(r.Context(), id); err != nil {
		s.log.Warn("dismiss anomaly", "id", id, "error", err)
		writeError(w, http.StatusNotFound, "anomaly not found or not open", s.now)
		return
	}
	writeJSON(w, http.StatusOK, envelope(map[string]any{
		"id":     id,
		"status": "dismissed",
	}, time.Now(), s.now))
}
