// Package playbook is the Phase 4 governance engine: declarative YAML rules
// (config/playbooks.yml) that turn domain events into proposed actions.
// Policy is config, not code — a rule is one YAML block, and the engine only
// ever proposes through the actions registry, which owns the approval queue
// and the immutable audit log.
package playbook

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"abi/internal/actions"
)

// Rule is one declarative policy: when an event of Trigger arrives and
// Condition holds, propose Action with the given RiskTier. Enabled=false
// keeps the rule parseable + validated but inert (dormant producers).
type Rule struct {
	Name        string         `yaml:"name"`
	Trigger     string         `yaml:"trigger"`
	Description string         `yaml:"description"`
	Condition   string         `yaml:"condition"`
	Params      map[string]any `yaml:"params"`
	Action      string         `yaml:"action"`
	RiskTier    string         `yaml:"risk_tier"`
	Enabled     bool           `yaml:"enabled"`
}

// Config is the top-level playbook document.
type Config struct {
	Version int    `yaml:"version"`
	Rules   []Rule `yaml:"rules"`
}

// validateRiskTier enforces the auto-tier allow-list at policy load and engine
// boot: a rule may only claim risk_tier "auto" for an action that
// actions.AutoAllowed lists. Dormant rules are checked too — the policy file
// must never contain a rule that could auto-execute a non-allow-listed action
// the moment someone flips `enabled`.
func validateRiskTier(r Rule) error {
	if r.RiskTier == actions.RiskAuto && !actions.AutoAllowed(r.Action) {
		return fmt.Errorf("playbook %q: action %q is not on the auto-tier allow-list "+
			"(only human-approved risk_tier: approval_required may run it)", r.Name, r.Action)
	}
	return nil
}

// LoadRules reads + parses config/playbooks.yml. Every rule's condition is
// compiled at load time, so a typo in the policy file aborts startup rather
// than silently no-op'ing in production. enabled defaults to true when the
// field is omitted (a missing `enabled:` line must mean "on").
func LoadRules(path string) ([]Rule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read playbooks %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse playbooks %s: %w", path, err)
	}
	for i := range cfg.Rules {
		r := &cfg.Rules[i]
		if r.Name == "" || r.Trigger == "" || r.Condition == "" || r.Action == "" {
			return nil, fmt.Errorf("playbooks %s: rule %d is missing name/trigger/condition/action", path, i+1)
		}
		switch r.RiskTier {
		case "auto", "approval_required":
		case "":
			r.RiskTier = "approval_required"
		default:
			return nil, fmt.Errorf("playbooks %s: rule %q has unknown risk_tier %q", path, r.Name, r.RiskTier)
		}
		if _, err := Parse(r.Condition); err != nil {
			return nil, fmt.Errorf("playbooks %s: rule %q condition invalid: %w", path, r.Name, err)
		}
		if err := validateRiskTier(*r); err != nil {
			return nil, fmt.Errorf("playbooks %s: %w", path, err)
		}
	}
	return cfg.Rules, nil
}