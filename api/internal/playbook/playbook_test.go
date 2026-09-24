package playbook

import (
	"os"
	"testing"
)

// TestLoadShippedPlaybooks validates the real policy file the server boots
// with (config/playbooks.yml at the repo root). It skips when the file is not
// present (e.g. the test runs from a different checkout layout) — the server
// itself fails hard on a missing or invalid playbook at startup.
func TestLoadShippedPlaybooks(t *testing.T) {
	paths := []string{"../../config/playbooks.yml", "../config/playbooks.yml"}
	var rules []Rule
	var path string
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			path = p
			break
		}
	}
	if path == "" {
		t.Skip("config/playbooks.yml not found relative to the package — skipping")
	}
	var err error
	rules, err = LoadRules(path)
	if err != nil {
		t.Fatalf("LoadRules(%s): %v", path, err)
	}
	if len(rules) == 0 {
		t.Fatalf("%s defines no rules", path)
	}

	// The shipped file must arm the three live rules + keep the dormant set
	// parseable. Fall out if the plan's rule names drift.
	byName := map[string]Rule{}
	for _, r := range rules {
		byName[r.Name] = r
	}
	for _, live := range []string{"hold-high-fraud-order", "log-midband-bot-session", "retrain-on-critical-drift"} {
		r, ok := byName[live]
		if !ok {
			t.Fatalf("live rule %q missing from %s", live, path)
		}
		if !r.Enabled {
			t.Errorf("live rule %q must be enabled", live)
		}
	}
	for _, dormant := range []string{"propose-retention-offer-high-churn", "nudge-low-risk-churn", "draft-po-on-weak-forecast"} {
		r, ok := byName[dormant]
		if !ok {
			t.Fatalf("dormant rule %q missing from %s", dormant, path)
		}
		if r.Enabled {
			t.Errorf("dormant rule %q must stay disabled until its producer exists", dormant)
		}
	}
}