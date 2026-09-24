package playbook

import (
	"context"
	"fmt"
	"log/slog"

	"abi/internal/actions"
	"abi/internal/events"
)

// Engine wires the bus to the actions registry: events in, governed proposals
// out. It is pure decision — the actual effects belong to the action
// executors, and every step writes an audit transition.
type Engine struct {
	bus     *events.Bus
	actions Proposer
	rules   []compiledRule
	log     *slog.Logger
}

// Proposer is the slice of the actions registry the engine drives. Defined as
// an interface so the engine's decision logic is unit-testable without a
// database (the actions.Service implements it in production).
type Proposer interface {
	Has(name string) bool
	Propose(ctx context.Context, p actions.Proposal) (int64, error)
	AutoApproveAndExecute(ctx context.Context, id int64) error
}

type compiledRule struct {
	rule Rule
	cond node // precompiled condition (parsed once at startup)
}

// NewEngine compiles the rules and returns a ready engine. Unknown actions in
// the policy file are rejected here (fail-closed at boot, not at 3am when the
// rule fires). Every rule — including dormant ones, per config/playbooks.yml's
// "kept parseable + validated on purpose" — is parsed and cross-checked
// against the event's declared payload schema (schema.go), so a typo'd field
// name is a boot failure, never a rule that silently never fires.
func NewEngine(bus *events.Bus, svc Proposer, rules []Rule, log *slog.Logger) (*Engine, error) {
	e := &Engine{bus: bus, actions: svc, log: log}
	for _, r := range rules {
		n, err := Parse(r.Condition)
		if err != nil {
			return nil, fmt.Errorf("playbook %q: %w", r.Name, err)
		}
		if err := validateRuleFields(r, n); err != nil {
			return nil, err
		}
		if !r.Enabled {
			e.log.Info("playbook: rule dormant", "rule", r.Name, "reason", "enabled: false")
			continue
		}
		if !svc.Has(r.Action) {
			return nil, fmt.Errorf("playbook %q references unknown action %q", r.Name, r.Action)
		}
		e.log.Info("playbook: rule armed", "rule", r.Name, "trigger", r.Trigger, "tier", r.RiskTier)
		e.rules = append(e.rules, compiledRule{rule: r, cond: n})
	}
	return e, nil
}

// Run consumes the bus until ctx is cancelled. Wire this before any producer
// starts so no event is missed (subscriptions are not replayed).
func (e *Engine) Run(ctx context.Context) error {
	ch, unsub := e.bus.Subscribe()
	defer unsub()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-ch:
			e.Handle(ctx, ev)
		}
	}
}

// Handle evaluates every rule for one event and proposes matches. It returns
// the number of propose attempts that failed at the storage layer, so the
// reconciliation backstop (api/internal/govern) can refuse to advance its
// cursor when a pass did not fully land. The live bus loop ignores the count.
func (e *Engine) Handle(ctx context.Context, ev events.Event) int {
	failures := 0
	for _, c := range e.rules {
		if c.rule.Trigger != string(ev.Type) {
			continue
		}
		ok, err := c.cond.eval(ev.Payload)
		if err != nil {
			// Fail-closed: a rule whose fields are missing (or mistyped) must
			// not fire; it should be fixed in config, not silently skip.
			e.log.Warn("playbook: condition refused to fire",
				"rule", c.rule.Name, "event", ev.Type, "error", err)
			continue
		}
		if b, isBool := ok.(bool); !isBool || !b {
			continue
		}
		if err := e.propose(ctx, c.rule, ev); err != nil {
			failures++
		}
	}
	return failures
}

// propose records the proposal (+ audit 'proposed'), then routes by tier:
// auto allow-list executes immediately via AutoApproveAndExecute; everything
// else waits for a human.
func (e *Engine) propose(ctx context.Context, r Rule, ev events.Event) error {
	id, err := e.actions.Propose(ctx, actions.Proposal{
		Action:   r.Action,
		Entity:   entityOf(ev),
		RiskTier: r.RiskTier,
		Rule:     r.Name,
		DedupKey: dedupKey(r.Name, ev),
		Params:   r.Params,
		Trigger:  triggerEvidence(ev, r),
	})
	if err != nil {
		e.log.Error("playbook: propose failed", "rule", r.Name, "error", err)
		return err
	}
	if id == 0 {
		e.log.Debug("playbook: duplicate trigger, no new proposal", "rule", r.Name)
		return nil
	}
	if r.RiskTier == actions.RiskAuto {
		if err := e.actions.AutoApproveAndExecute(ctx, id); err != nil {
			e.log.Error("playbook: auto action failed", "rule", r.Name, "action_id", id, "error", err)
			return err
		}
		e.log.Info("playbook: allow-list auto action executed", "rule", r.Name, "action_id", id)
		return nil
	}
	e.log.Info("playbook: action awaiting approval", "rule", r.Name, "action_id", id)
	return nil
}

// entityOf resolves the subject of an event for the queue row.
func entityOf(ev events.Event) string {
	switch ev.Type {
	case events.TypeOrderScored, events.TypeSessionScored, events.TypeChurnScored:
		return actions.StringField(ev.Payload, "prediction.entity_id")
	case events.TypeAnomalyDetected:
		return actions.StringField(ev.Payload, "anomaly.metric")
	case events.TypeDriftComputed:
		return actions.StringField(ev.Payload, "drift.model")
	case events.TypeForecastUpdated:
		return actions.StringField(ev.Payload, "forecast.category")
	}
	return ""
}

// dedupKey uniquely identifies (rule, trigger instance) so a replayed or
// re-firing event can never stack duplicate proposals. Scored events scope on
// the entity PLUS the persisted prediction id — each scoring instance is its
// own trigger occurrence, so an entity that was flagged, reviewed and released
// can be legitimately re-flagged when a later scoring crosses the threshold
// again (the first incident never silently vaccinates the entity). Drift and
// anomaly events were already instance-scoped (computed_at / bucket_start).
func dedupKey(rule string, ev events.Event) string {
	scope := ""
	switch ev.Type {
	case events.TypeOrderScored, events.TypeSessionScored, events.TypeChurnScored:
		scope = actions.StringField(ev.Payload, "prediction.entity_id")
		if pid := actions.StringField(ev.Payload, "prediction.id"); pid != "" {
			scope += "|" + pid
		}
	case events.TypeAnomalyDetected:
		scope = actions.StringField(ev.Payload, "anomaly.metric") + "|" +
			actions.StringField(ev.Payload, "anomaly.bucket_start")
	case events.TypeDriftComputed:
		scope = actions.StringField(ev.Payload, "drift.model") + "|" +
			actions.StringField(ev.Payload, "drift.feature") + "|" +
			actions.StringField(ev.Payload, "drift.computed_at")
	case events.TypeForecastUpdated:
		scope = actions.StringField(ev.Payload, "forecast.category") + "|" +
			actions.StringField(ev.Payload, "forecast.week")
	}
	if scope == "" {
		scope = string(ev.Type)
	}
	return rule + "|" + scope
}

// triggerEvidence is the persisted "show your work" record for the proposal:
// the event + the rule + the condition that matched.
func triggerEvidence(ev events.Event, r Rule) map[string]any {
	at := ""
	if !ev.At.IsZero() {
		at = ev.At.UTC().Format("2006-01-02T15:04:05Z")
	}
	return map[string]any{
		"event_type": string(ev.Type),
		"event_at":   at,
		"payload":    ev.Payload,
		"rule":       r.Name,
		"condition":  r.Condition,
	}
}
