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
// same way a malformed expression does. The declared schema and the payload
// constructors every producer uses live together in the events package
// (payloads.go), and events/payloads_test.go pins the two to each other.

// validateRuleFields rejects any field path a rule's compiled condition
// references that is not declared in the event type's schema. A rule on an
// undeclared event type is itself a policy bug and fails boot.
func validateRuleFields(r Rule, n node) error {
	schema, ok := events.DeclaredFields(events.Type(r.Trigger))
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
