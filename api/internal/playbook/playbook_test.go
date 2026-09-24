package playbook

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"abi/internal/events"
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
// TestShippedPlaybooksBootTheEngine runs the real policy file through the same
// NewEngine the server uses, so the boot-time checks (condition fields against
// the payload schema, unknown actions, the auto-tier allow-list) are exercised
// on the shipped YAML in CI rather than only at 3am server start.
func TestShippedPlaybooksBootTheEngine(t *testing.T) {
	var rules []Rule
	for _, p := range []string{"../../../config/playbooks.yml", "../../config/playbooks.yml"} {
		if r, err := LoadRules(p); err == nil {
			rules = r
			break
		}
	}
	if len(rules) == 0 {
		t.Skip("config/playbooks.yml not found relative to the package")
	}
	all := &fakeActions{has: map[string]bool{}}
	for _, r := range rules {
		all.has[r.Action] = true
	}
	if _, err := NewEngine(events.New(), all, rules, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("shipped playbooks do not boot: %v", err)
	}
}

func TestLoadRulesRejectsAutoOnNonAllowListedAction(t *testing.T) {
	path := t.TempDir() + "/p.yml"
	yml := "version: 1\nrules:\n  - name: sneaky\n    trigger: drift_computed\n" +
		"    condition: \"drift.status == 'critical'\"\n    action: retrain_model\n" +
		"    risk_tier: auto\n    enabled: false\n"
	if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRules(path); err == nil {
		t.Fatal("LoadRules accepted risk_tier: auto on retrain_model")
	}
}
