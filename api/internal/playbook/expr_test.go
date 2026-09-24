package playbook

import "testing"

func ctxWith(payload map[string]any) map[string]any { return payload }

func mustEval(t *testing.T, cond string, ctx map[string]any) bool {
	t.Helper()
	got, err := EvalTrue(cond, ctx)
	if err != nil {
		t.Fatalf("EvalTrue(%q): %v", cond, err)
	}
	return got
}

func TestComparisons(t *testing.T) {
	ctx := ctxWith(map[string]any{
		"prediction": map[string]any{"model": "fraud_risk", "score": 0.91},
		"registry":   map[string]any{"recommended_threshold": 0.795},
	})
	cases := []struct {
		cond string
		want bool
	}{
		{"prediction.score >= registry.recommended_threshold", true},
		{"prediction.score < registry.recommended_threshold", false},
		{"prediction.score == 0.91", true},
		{"prediction.score != 0.91", false},
		{"prediction.model == 'fraud_risk'", true},
		{"prediction.model != 'bot_score'", true},
		{"prediction.model == 'wrong'", false},
	}
	for _, c := range cases {
		if got := mustEval(t, c.cond, ctx); got != c.want {
			t.Errorf("%q = %v, want %v", c.cond, got, c.want)
		}
	}
}

func TestBooleanLogic(t *testing.T) {
	ctx := ctxWith(map[string]any{
		"prediction": map[string]any{"model": "fraud_risk", "score": 0.91, "confidence": 0.7},
	})
	cases := []struct {
		cond string
		want bool
	}{
		{"prediction.model == 'fraud_risk' && prediction.score >= 0.795", true},
		{"prediction.model == 'bot_score' && prediction.score >= 0.795", false},
		{"prediction.model == 'fraud_risk' || prediction.score >= 0.99", true},
		{"prediction.score >= 0.5 && prediction.score < 0.7", false},
		{"!(prediction.score < 0.795)", true},
		{"(prediction.score >= 0.9) && (prediction.confidence >= 0.5)", true},
		{"prediction.score >= 0.9 && prediction.confidence >= 0.99", false},
		{"true", true},
		{"false", false},
		{"prediction.model == 'bot_score' && prediction.score >= 0.9", false}, // short-circuit
	}
	for _, c := range cases {
		if got := mustEval(t, c.cond, ctx); got != c.want {
			t.Errorf("%q = %v, want %v", c.cond, got, c.want)
		}
	}
}

func TestArithmetic(t *testing.T) {
	ctx := ctxWith(map[string]any{
		"forecast": map[string]any{"point_estimate": 120.0, "recent_avg": 100.0},
	})
	if got := mustEval(t, "forecast.point_estimate < forecast.recent_avg * 1.1", ctx); got {
		t.Error("120 < 110 should be false")
	}
	if got := mustEval(t, "forecast.point_estimate > forecast.recent_avg * 1.1", ctx); !got {
		t.Error("120 > 110 should be true")
	}
	if got := mustEval(t, "forecast.point_estimate >= forecast.recent_avg * 1.1", ctx); !got {
		t.Error("120 >= 110 should be true")
	}
	if got := mustEval(t, "forecast.point_estimate == forecast.recent_avg + 20", ctx); !got {
		t.Error("120 == 120 should be true")
	}
	if got := mustEval(t, "forecast.point_estimate - forecast.recent_avg > 10", ctx); !got {
		t.Error("20 > 10 should be true")
	}
}

func TestFailClosedOnUnknownField(t *testing.T) {
	// A rule referencing a field the event does not carry must fail closed:
	// it returns an explicit error (the engine refuses to fire), never a false.
	_, err := EvalTrue("prediction.missing > 1", ctxWith(map[string]any{"prediction": map[string]any{"score": 1.0}}))
	if err == nil {
		t.Fatal("unknown field should error, not evaluate")
	}
}

func TestTypeMismatchFailsClosed(t *testing.T) {
	ctx := ctxWith(map[string]any{"drift": map[string]any{"status": "critical"}})
	// Comparing a string with a number is a type error → fail closed.
	_, err := EvalTrue("drift.status > 1", ctx)
	if err == nil {
		t.Fatal("string/number comparison should error, not evaluate")
	}
	_ = ctx
}

func TestDivisionByZeroFailsClosed(t *testing.T) {
	ctx := ctxWith(map[string]any{"a": map[string]any{"x": 1.0, "y": 0.0}})
	if _, err := EvalTrue("a.x / a.y > 0", ctx); err == nil {
		t.Fatal("division by zero should error")
	}
}

func TestParseErrorsRejected(t *testing.T) {
	for _, bad := range []string{
		"prediction.score >=", // dangling operator
		"&& prediction.score", // leading operator
		"prediction..score",   // dangling dot
		"prediction score",    // missing operator
		"'unterminated",       // bad string
	} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) should fail", bad)
		}
	}
}

func TestConditionMustBeBoolean(t *testing.T) {
	ctx := ctxWith(map[string]any{"prediction": map[string]any{"score": 0.9}})
	// A bare number is not a condition.
	if _, err := EvalTrue("prediction.score", ctx); err == nil {
		t.Fatal("non-boolean condition should error")
	}
}