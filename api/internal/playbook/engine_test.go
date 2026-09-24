package playbook

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"abi/internal/actions"
	"abi/internal/events"
)

// fakeActions records proposals + auto-executions without a database.
type fakeActions struct {
	mu       sync.Mutex
	has      map[string]bool
	proposed []actions.Proposal
	auto     []int64
}

func (f *fakeActions) Has(name string) bool { return f.has[name] }
func (f *fakeActions) Propose(_ context.Context, p actions.Proposal) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proposed = append(f.proposed, p)
	return int64(len(f.proposed)), nil
}
func (f *fakeActions) AutoApproveAndExecute(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auto = append(f.auto, id)
	return nil
}

func (f *fakeActions) snapshot() ([]actions.Proposal, []int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]actions.Proposal(nil), f.proposed...), append([]int64(nil), f.auto...)
}

func testRules() []Rule {
	return []Rule{
		{Name: "hold-high-fraud-order", Trigger: "order_scored",
			Condition: "prediction.model == 'fraud_risk' && prediction.score >= registry.recommended_threshold",
			Action:    "hold_order_for_review", RiskTier: "approval_required", Enabled: true},
		{Name: "log-midband-bot-session", Trigger: "session_scored",
			Condition: "prediction.model == 'bot_score' && prediction.score >= 0.5 && prediction.score < 0.7",
			Action:    "log_event_note", RiskTier: "auto", Enabled: true,
			Params: map[string]any{"kind": "bot_midband"}},
	}
}

func newTestEngine(t *testing.T, rules []Rule) (*Engine, *fakeActions, *events.Bus) {
	t.Helper()
	bus := events.New()
	fake := &fakeActions{
		has: map[string]bool{
			"hold_order_for_review": true,
			"log_event_note":        true,
			"retrain_model":         true,
		},
	}
	eng, err := NewEngine(bus, fake, rules, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return eng, fake, bus
}

func orderEvent(score float64) events.Event {
	return events.Event{Type: events.TypeOrderScored, At: time.Now(),
		Payload: map[string]any{
			"prediction": map[string]any{
				"model": "fraud_risk", "entity_id": "o-42", "score": score, "threshold": 0.795,
			},
			"registry": map[string]any{"recommended_threshold": 0.795, "positive_rate": 0.011},
		}}
}

func TestEngineProposesApprovalRequired(t *testing.T) {
	eng, fake, _ := newTestEngine(t, testRules())
	eng.handle(context.Background(), orderEvent(0.91))

	proposed, auto := fake.snapshot()
	if len(proposed) != 1 {
		t.Fatalf("proposals = %d, want 1", len(proposed))
	}
	if len(auto) != 0 {
		t.Fatalf("auto-executions = %d, want 0 for an approval_required rule", len(auto))
	}
	p := proposed[0]
	if p.Action != "hold_order_for_review" || p.Rule != "hold-high-fraud-order" {
		t.Errorf("proposal = %+v, want hold_order_for_review under hold-high-fraud-order", p)
	}
	if p.Entity != "o-42" {
		t.Errorf("entity = %q, want o-42", p.Entity)
	}
	if p.RiskTier != "approval_required" {
		t.Errorf("risk_tier = %q", p.RiskTier)
	}
	if want := "hold-high-fraud-order|o-42"; p.DedupKey != want {
		t.Errorf("dedup_key = %q, want %q", p.DedupKey, want)
	}
	// Evidence trail must carry the why (the condition that matched).
	trig := p.Trigger
	if trig["rule"] != "hold-high-fraud-order" || trig["condition"] == "" || trig["event_type"] != "order_scored" {
		t.Errorf("trigger evidence incomplete: %v", trig)
	}
	if _, ok := trig["payload"]; !ok {
		t.Error("trigger evidence is missing the event payload")
	}
}

func TestEngineAutoTierExecutesImmediately(t *testing.T) {
	eng, fake, _ := newTestEngine(t, testRules())
	session := events.Event{Type: events.TypeSessionScored, At: time.Now(),
		Payload: map[string]any{
			"prediction": map[string]any{
				"model": "bot_score", "entity_id": "s-7", "score": 0.6, "threshold": 0.5,
			},
			"registry": map[string]any{"recommended_threshold": 0.5, "positive_rate": 0.02},
		}}
	eng.handle(context.Background(), session)

	proposed, auto := fake.snapshot()
	if len(proposed) != 1 || len(auto) != 1 {
		t.Fatalf("proposals=%d auto=%d, want 1 and 1", len(proposed), len(auto))
	}
	if auto[0] != 1 { // the first proposal's id
		t.Errorf("auto-executed id = %d, want 1", auto[0])
	}
}

func TestEngineDoesNotProposeWhenConditionFalse(t *testing.T) {
	eng, fake, _ := newTestEngine(t, testRules())
	eng.handle(context.Background(), orderEvent(0.5)) // below threshold

	proposed, auto := fake.snapshot()
	if len(proposed) != 0 || len(auto) != 0 {
		t.Fatalf("proposals=%d auto=%d, want none for a below-threshold score", len(proposed), len(auto))
	}
}

func TestEngineFailClosedOnUnknownField(t *testing.T) {
	// score at/above threshold but the condition ALSO references a field the
	// event does not carry → the rule refuses to fire entirely.
	rules := []Rule{
		{Name: "strict", Trigger: "order_scored",
			Condition: "prediction.score >= registry.recommended_threshold && prediction.missing > 0",
			Action:    "hold_order_for_review", RiskTier: "approval_required", Enabled: true},
	}
	eng, fake, _ := newTestEngine(t, rules)
	eng.handle(context.Background(), orderEvent(0.91))
	if proposed, _ := fake.snapshot(); len(proposed) != 0 {
		t.Fatalf("rule with unknown field fired; fail-closed violated: %+v", proposed)
	}
}

func TestEngineRejectsUnknownActionAtBoot(t *testing.T) {
	bus := events.New()
	fake := &fakeActions{has: map[string]bool{}}
	rules := []Rule{
		{Name: "bad", Trigger: "order_scored", Condition: "true",
			Action: "does_not_exist", RiskTier: "approval_required", Enabled: true},
	}
	if _, err := NewEngine(bus, fake, rules, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("engine should refuse to boot with an unknown action")
	}
}

func TestEngineSkipsDormantRules(t *testing.T) {
	rules := append(testRules(), Rule{
		Name: "dormant", Trigger: "order_scored", Condition: "true",
		Action: "retrain_model", RiskTier: "approval_required", Enabled: false,
	})
	eng, fake, _ := newTestEngine(t, rules)
	eng.handle(context.Background(), orderEvent(0.91))
	proposed, _ := fake.snapshot()
	if len(proposed) != 1 {
		t.Fatalf("dormant rule fired; proposals = %d, want 1", len(proposed))
	}
}

func TestEngineRunsOnTheBus(t *testing.T) {
	eng, fake, bus := newTestEngine(t, testRules())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = eng.Run(ctx) }()

	// Let the subscriber attach, then fire an event through the bus.
	time.Sleep(50 * time.Millisecond)
	bus.Publish(orderEvent(0.9))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if proposed, _ := fake.snapshot(); len(proposed) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("engine did not propose within 2s of a bus event")
}

func TestDedupKeyDistinguishesTriggers(t *testing.T) {
	scope := dedupKey("r", events.Event{Type: events.TypeOrderScored,
		Payload: map[string]any{"prediction": map[string]any{"entity_id": "a"}}})
	if scope != "r|a" {
		t.Errorf("dedup = %q", scope)
	}
	drift := dedupKey("r", events.Event{Type: events.TypeDriftComputed,
		Payload: map[string]any{"drift": map[string]any{"model": "m", "feature": "f", "computed_at": "t"}}})
	if drift != "r|m|f|t" {
		t.Errorf("drift dedup = %q", drift)
	}
	if dedupKey("r", events.Event{Type: events.TypeAnomalyDetected,
		Payload: map[string]any{"anomaly": map[string]any{"metric": "revenue"}}}) != "r|revenue|" {
		t.Error("anomaly dedup should incorporate bucket_start even when empty")
	}
}