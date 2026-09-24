package playbook

import (
	"fmt"
	"strings"

	"abi/internal/events"
)

// schema.go — boot-time payload-schema validation for playbook conditions.
//
// The condition evaluator (expr.go) fails closed at EVALUATION time: a field
// the event does not carry makes the rule refuse to fire. That is a good
// second line, but it is not enough on its own: a *typo'd but parse-valid*
// field name (e.g. `prediction.scrore >= 0.7`) compiles fine, boots fine, and
// then silently never fires — forever. The fix below makes the playbook
// compiler cross-check every field path a condition references against a
// declared schema per event type, so a wrong field name aborts startup the
// same way a malformed expression does.

// eventFields declares, per event type, the dotted payload paths a rule may
// legitimately reference. It must stay in sync with the payloads the
// producers publish (score-writer, anomaly aggregator, drift poller) and the
// dormant Phase 5 producers' contract (churn_scored, forecast_updated).
var eventFields = map[string]map[string]bool{
	string(events.TypeOrderScored):   scoredFields,
	string(events.TypeSessionScored): scoredFields,
	string(events.TypeChurnScored):   scoredFields,
	string(events.TypeDriftComputed): {
		"drift.model":         true,
		"drift.model_version": true,
		"drift.status":        true,
		"drift.psi":           true,
		"drift.feature_count": true,
		"drift.features":      true,
		"drift.computed_at":   true,
	},
	string(events.TypeAnomalyDetected): {
		"anomaly.metric":       true,
		"anomaly.detector":     true,
		"anomaly.bucket_start": true,
		"anomaly.observed":     true,
		"anomaly.expected":     true,
		"anomaly.z_score":      true,
		"anomaly.severity":     true,
		"anomaly.status":       true,
		"anomaly.surfaced":     true,
	},
	string(events.TypeForecastUpdated): {
		"forecast.category":       true,
		"forecast.week":           true,
		"forecast.point_estimate": true,
		"forecast.recent_avg":     true,
	},
}

// scoredFields is shared by order_scored / session_scored / churn_scored: the
// prediction + registry blocks the score-writer publishes (and the Phase 5
// churn-scoring job will publish).
var scoredFields = map[string]bool{
	"prediction.model":               true,
	"prediction.model_version":       true,
	"prediction.entity_id":           true,
	"prediction.score":               true,
	"prediction.confidence":          true,
	"prediction.threshold":           true,
	"prediction.id":                  true,
	"registry.recommended_threshold": true,
	"registry.positive_rate":         true,
}

// validateRuleFields rejects any field path a rule's compiled condition
// references that is not declared in the event type's schema. A rule on an
// undeclared event type is itself a policy bug and fails boot.
func validateRuleFields(r Rule, n node) error {
	schema, ok := eventFields[r.Trigger]
	if !ok {
		return fmt.Errorf("playbook %q: unknown trigger %q (no payload schema declared)", r.Name, r.Trigger)
	}
	paths := map[string]bool{}
	collectPaths(n, paths)
	for p := range paths {
		if !schema[p] {
			return fmt.Errorf("playbook %q condition references %q, which the %s event never carries", r.Name, p, r.Trigger)
		}
	}
	return nil
}

// collectPaths walks a compiled condition and records every dotted path it
// touches (recursively, through parentheses and arithmetic).
func collectPaths(n node, out map[string]bool) {
	switch t := n.(type) {
	case pathNode:
		out[strings.Join(t.path, ".")] = true
	case unaryNode:
		collectPaths(t.x, out)
	case binNode:
		collectPaths(t.l, out)
		collectPaths(t.r, out)
	}
}
