package metrics

import (
	"regexp"
	"strings"
	"testing"
)

// The catalog is the machine-readable semantic layer agents and humans read before
// interpreting a number. These pin its structural contract (it had no tests).

func TestCatalogStructuralContract(t *testing.T) {
	c := Default()
	if !regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(c.Version) {
		t.Errorf("version %q is not semver", c.Version)
	}
	seen := map[string]bool{}
	sources := map[string]bool{SourceBatch: true, SourceLiveReplay: true, SourceModel: true,
		SourceGovernance: true, SourceMonitoring: true}
	for _, m := range c.Metrics {
		if m.Name == "" || m.Label == "" || m.Description == "" || m.Population == "" ||
			m.Aggregation == "" || m.Unit == "" || m.SourceTable == "" {
			t.Errorf("metric %q has an empty required field", m.Name)
		}
		if seen[m.Name] {
			t.Errorf("duplicate metric name %q", m.Name)
		}
		seen[m.Name] = true
		if !sources[m.Source] {
			t.Errorf("metric %q has unknown source %q (every metric carries a required provenance tag)", m.Name, m.Source)
		}
	}
}

// The forecast check was published as "decay" although it only re-reads the
// trainer's own held-out rows (audit C11). The catalog must not claim decay.
func TestCatalogDoesNotClaimDecayForTheBacktestReproductionCheck(t *testing.T) {
	var found bool
	for _, m := range Default().Metrics {
		if m.Name == "model_forecast_decay" {
			t.Fatal("model_forecast_decay was renamed to model_backtest_reproduction (catalog 1.5.0)")
		}
		if m.Name == "model_backtest_reproduction" {
			found = true
			if !strings.Contains(strings.ToLower(m.Description), "does not measure decay") {
				t.Error("the description must say plainly that this is not a decay measurement")
			}
			if !strings.Contains(m.SourceTable, "backtest_repro") {
				t.Errorf("source table %q must name kind='backtest_repro'", m.SourceTable)
			}
		}
	}
	if !found {
		t.Fatal("model_backtest_reproduction missing from the catalog")
	}
}
