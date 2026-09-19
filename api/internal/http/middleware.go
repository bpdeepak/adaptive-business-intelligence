package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"abi/internal/model"
)

// statusWriter records the response status while guarding against double
// WriteHeader calls (e.g. after a panic recovery).
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.status = code
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

// middleware adds request logging and panic recovery.
func (s *Server) middleware(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()

		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic", "method", r.Method, "path", r.URL.Path, "error", fmt.Sprint(rec))
				writeJSON(sw, http.StatusInternalServerError,
					model.Envelope{Error: "internal error", Meta: newMeta(s.now)})
			}
			s.log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"remote", r.RemoteAddr,
				"status", sw.status,
				"took_ms", float64(time.Since(start).Microseconds())/1000,
			)
		}()

		next(sw, r)
	})
}