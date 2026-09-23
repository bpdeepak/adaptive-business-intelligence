package scorewriter

import (
	"os"
	"strings"
	"testing"
)

// Phase 3 pin: gold.scorewriter_state is the score-writer's restart-state table
// (velocity/benchmark rings + rate windows). The DDL below is a contract — a
// future refactor must keep the table keyed by text with a jsonb payload, the
// exact shape the snapshot/restore code round-trips.
func TestScorewriterStateDDLPinsPhase3Contract(t *testing.T) {
	norm := strings.Join(strings.Fields(scorewriterStateDDL), " ")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS gold.scorewriter_state",
		"key text PRIMARY KEY",
		"payload jsonb NOT NULL",
		"updated_at timestamptz NOT NULL DEFAULT now()",
	} {
		if !strings.Contains(norm, want) {
			t.Errorf("scorewriter_state DDL missing %q\n%s", want, scorewriterStateDDL)
		}
	}
}

// The model-driven writer must NEVER reuse the statistical detector's insert:
// its anomaly insert hardcodes detector='model' and NULL z_score. This guards
// the scorewriter side so a future merge cannot silently drop the phase-3
// semantics.
func TestModelDetectorDeclaredInServiceInsert(t *testing.T) {
	src, err := os.ReadFile("service.go")
	if err != nil {
		t.Fatalf("read service.go: %v", err)
	}
	if !strings.Contains(string(src), "'model'") {
		t.Error("service.go no longer writes detector='model' in persistModelAnomaly")
	}
	if !strings.Contains(string(src), "NULL") {
		t.Error("service.go no longer inserts NULL z_score for model anomalies")
	}
}