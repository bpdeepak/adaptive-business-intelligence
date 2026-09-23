package realtime

import (
	"os"
	"strings"
	"testing"
)

// Phase 3 pin: gold.anomalies is shared by TWO writer kinds — the statistical
// EWMA/z-score detector (Phase 1) and the model-driven rate-based scorewriter
// (Phase 3). The schema language below is the contract: detector column present
// with default 'statistical', z_score nullable (model anomalies carry none),
// and the idempotent migration present so pre-existing databases converge.
func TestAnomaliesDDLPinsPhase3Contract(t *testing.T) {
	create := ""
	for _, stmt := range schemaDDL {
		if strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS gold.anomalies") {
			create = stmt
			break
		}
	}
	if create == "" {
		t.Fatal("gold.anomalies DDL not found in schemaDDL")
	}
	// The DDL is tab-indented; compare on a whitespace-normalized copy so the
	// pin reads naturally regardless of source formatting.
	norm := strings.Join(strings.Fields(create), " ")
	for _, want := range []string{
		"detector text NOT NULL DEFAULT 'statistical'",
		"z_score double precision,", // nullable: no NOT NULL
		"severity text NOT NULL",
	} {
		if !strings.Contains(norm, want) {
			t.Errorf("gold.anomalies DDL missing %q\n%s", want, create)
		}
	}

	joined := strings.Join(schemaDDL, "\n")
	for _, want := range []string{
		"ALTER TABLE gold.anomalies ADD COLUMN IF NOT EXISTS detector text NOT NULL DEFAULT 'statistical'",
		"ALTER TABLE gold.anomalies ALTER COLUMN z_score DROP NOT NULL",
		"idx_anomalies_detector",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("schemaDDL missing phase-3 migration %q", want)
		}
	}
}

// The model-driven writer must never reuse the statistical detector's insert
// path: the scorewriter's own insert sets detector='model'. This guards the
// aggregator side so a future merge cannot silently drop the column.
func TestStatisticalDetectorDeclaredInAggregatorInsert(t *testing.T) {
	src, err := os.ReadFile("aggregator.go")
	if err != nil {
		t.Fatalf("read aggregator.go: %v", err)
	}
	if !strings.Contains(string(src), "'statistical'") {
		t.Error("aggregator.go no longer writes detector='statistical'")
	}
}