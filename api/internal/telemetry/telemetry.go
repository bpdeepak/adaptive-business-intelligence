// Package telemetry is a tiny, dependency-free registry of operational
// pipeline metrics exposed in Prometheus text format at GET /metrics by both
// binaries. It exists so the pipeline is observable before a full
// Prometheus/Grafana stack is wired: producer shedding (ring drop-oldest),
// consumer lag, flush errors and baseline progress are the signals that tell
// an operator the replay is healthy or silently degrading.
package telemetry

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Kind is the Prometheus series type.
type Kind string

const (
	// KindCounter is a monotonic (cumulative) series.
	KindCounter Kind = "counter"
	// KindGauge is a point-in-time value.
	KindGauge Kind = "gauge"
)

// Series is a single labelled metric series. All mutators are nil-safe so
// producers can hold a handle that becomes a no-op when the registry was not
// wired (unit tests, embedded contexts).
type Series struct {
	reg    *Registry
	name   string
	help   string
	kind   Kind
	labels map[string]string
	value  float64
}

// Add increments the series by v (monotonic counters).
func (s *Series) Add(v float64) {
	if s == nil || s.reg == nil {
		return
	}
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	s.value += v
}

// Inc is Add(1).
func (s *Series) Inc() { s.Add(1) }

// Set overwrites the series value (gauges and externally-tracked totals).
func (s *Series) Set(v float64) {
	if s == nil || s.reg == nil {
		return
	}
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	s.value = v
}

// Value returns the current value.
func (s *Series) Value() float64 {
	if s == nil || s.reg == nil {
		return 0
	}
	s.reg.mu.RLock()
	defer s.reg.mu.RUnlock()
	return s.value
}

// Registry owns all series and renders the Prometheus text exposition format.
// Series are keyed by (name, sorted label set): repeated Counter/Gauge calls
// with the same identity return the same backing series.
type Registry struct {
	mu     sync.RWMutex
	byName map[string]*decl
	byKey  map[string]*Series
	series []*Series
}

type decl struct {
	help string
	kind Kind
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]*decl), byKey: make(map[string]*Series)}
}

// Counter returns (or lazily creates) an unlabelled counter series.
func (r *Registry) Counter(name, help string) *Series {
	return r.Series(KindCounter, name, help, nil)
}

// Gauge returns (or lazily creates) an unlabelled gauge series.
func (r *Registry) Gauge(name, help string) *Series {
	return r.Series(KindGauge, name, help, nil)
}

// CounterLabel returns (or lazily creates) a labelled counter series.
func (r *Registry) CounterLabel(name, help string, labels map[string]string) *Series {
	return r.Series(KindCounter, name, help, labels)
}

// GaugeLabel returns (or lazily creates) a labelled gauge series.
func (r *Registry) GaugeLabel(name, help string, labels map[string]string) *Series {
	return r.Series(KindGauge, name, help, labels)
}

// Series returns (or lazily creates) a series with the given kind, name and
// label set (nil label sets become unlabelled). The first call fixes the
// help text; later calls reuse it.
func (r *Registry) Series(kind Kind, name, help string, labels map[string]string) *Series {
	if r == nil {
		return nil
	}
	key := name + "\x00" + sortedLabelKey(labels)
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.byKey[key]; ok {
		return s
	}
	if d, ok := r.byName[name]; ok {
		help = d.help
	} else {
		r.byName[name] = &decl{help: help, kind: kind}
	}
	s := &Series{reg: r, name: name, help: help, kind: kind, labels: labels}
	r.byKey[key] = s
	r.series = append(r.series, s)
	return s
}

// Render returns the full Prometheus text-format payload in deterministic
// (insertion) order: one # HELP / # TYPE header per name, then one line per
// labelled series.
func (r *Registry) Render() []byte {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var b strings.Builder
	seen := make(map[string]bool, len(r.byName))
	for _, s := range r.series {
		if !seen[s.name] {
			seen[s.name] = true
			if d, ok := r.byName[s.name]; ok {
				fmt.Fprintf(&b, "# HELP %s %s\n", s.name, d.help)
				fmt.Fprintf(&b, "# TYPE %s %s\n", s.name, d.kind)
			}
		}
		b.WriteString(s.name)
		if len(s.labels) > 0 {
			keys := make([]string, 0, len(s.labels))
			for k := range s.labels {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			b.WriteString("{")
			for i, k := range keys {
				if i > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, "%s=%q", k, s.labels[k])
			}
			b.WriteString("}")
		}
		fmt.Fprintf(&b, " %s\n", strconv.FormatFloat(s.value, 'g', -1, 64))
	}
	return []byte(b.String())
}

// Handler returns an HTTP handler exposing the registry in Prometheus text
// format. Wire it at GET /metrics on any listener.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(r.Render())
	})
}

func sortedLabelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString("\x01")
		b.WriteString(labels[k])
		b.WriteString("\x02")
	}
	return b.String()
}
