// Package playbook is the Phase 4 governance engine: declarative YAML rules
// (config/playbooks.yml) that turn domain events into proposed actions.
package playbook

import (
	"fmt"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// expr.go — the sandboxed condition evaluator.
//
// Conditions are a deliberate, tiny pure-expression language:
//
//	or   := and ("||" and)*
//	and  := cmp ("&&" cmp)*
//	cmp  := add (("==" | "!=" | ">" | ">=" | "<" | "<=") add)?
//	add  := mul (("+" | "-") mul)*
//	mul  := unary (("*" | "/") unary)*
//	unary := "!" unary | primary
//	primary := "(" or ")" | literal | path
//	literal := number | string | "true" | "false"
//	path   := ident ("." ident)*
//
// Why this shape: a playbook must be *auditable and safe*. There are no
// function calls, no assignment, no indexing, no loops — an expression is a
// pure function of the event payload, and one that cannot touch anything else.
// Unknown fields fail evaluation (fail-closed: the rule does not fire) instead
// of silently evaluating to false, so a drift between a rule and the data it
// references shows up, rather than quietly no-op'ing. Values come only from
// the JSON-ish event context (producers pass flat maps; dotted paths resolve
// nested maps).
// ---------------------------------------------------------------------------

// Value is the runtime type of one expression node: float64, string or bool.
type Value = any

// tokenKind enumerates the lexer's token classes.
type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokNumber
	tokString
	tokOp // == != >= <= > < && || + - * / ! ( )
)

type token struct {
	kind tokenKind
	text string
	num  float64
}

func lex(src string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_':
			start := i
			for i < len(src) && (src[i] >= 'a' && src[i] <= 'z' || src[i] >= 'A' && src[i] <= 'Z' || src[i] >= '0' && src[i] <= '9' || src[i] == '_') {
				i++
			}
			toks = append(toks, token{kind: tokIdent, text: src[start:i]})
		case c >= '0' && c <= '9' || c == '.' && i+1 < len(src) && src[i+1] >= '0' && src[i+1] <= '9':
			start := i
			for i < len(src) && (src[i] >= '0' && src[i] <= '9' || src[i] == '.') {
				i++
			}
			f, err := strconv.ParseFloat(src[start:i], 64)
			if err != nil {
				return nil, fmt.Errorf("bad number %q: %w", src[start:i], err)
			}
			toks = append(toks, token{kind: tokNumber, num: f, text: src[start:i]})
		case c == '\'' || c == '"':
			quote := c
			i++
			var sb strings.Builder
			for i < len(src) && src[i] != quote {
				sb.WriteByte(src[i])
				i++
			}
			if i >= len(src) {
				return nil, fmt.Errorf("unterminated string literal")
			}
			i++
			toks = append(toks, token{kind: tokString, text: sb.String()})
		default:
			// Two-char operators first.
			if i+1 < len(src) {
				two := src[i : i+2]
				switch two {
				case "==", "!=", ">=", "<=", "&&", "||":
					toks = append(toks, token{kind: tokOp, text: two})
					i += 2
					continue
				}
			}
			switch c {
			case '>', '<', '+', '-', '*', '/', '!', '(', ')', '.':
				toks = append(toks, token{kind: tokOp, text: string(c)})
				i++
			default:
				return nil, fmt.Errorf("unexpected character %q", string(c))
			}
		}
	}
	toks = append(toks, token{kind: tokEOF})
	return toks, nil
}

// node is one AST node.
type node interface {
	eval(ctx map[string]any) (Value, error)
}

type litNode struct{ v Value }

func (n litNode) eval(map[string]any) (Value, error) { return n.v, nil }

type pathNode struct{ path []string }

func (n pathNode) eval(ctx map[string]any) (Value, error) {
	var cur any = ctx
	for _, seg := range n.path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path %s: %q is not an object", strings.Join(n.path, "."), seg)
		}
		v, ok := m[seg]
		if !ok {
			// Fail-closed: a rule referencing a field the event does not carry
			// must refuse to fire, not silently pass.
			return nil, fmt.Errorf("path %s: field %q not present in event context", strings.Join(n.path, "."), seg)
		}
		cur = v
	}
	return coerceJSON(cur), nil
}

// coerceJSON normalizes numbers decoded from maps so printing/lookups behave.
func coerceJSON(v any) any {
	switch t := v.(type) {
	case float64, string, bool:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case jsonNumber:
		f, _ := t.Float64()
		return f
	default:
		return v
	}
}

// jsonNumber lets path resolution accept json.Number payloads.
type jsonNumber interface{ Float64() (float64, error) }

type unaryNode struct {
	op string
	x  node
}

func (n unaryNode) eval(ctx map[string]any) (Value, error) {
	v, err := n.x.eval(ctx)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case "!":
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("! needs a boolean, got %T", v)
		}
		return !b, nil
	}
	return nil, fmt.Errorf("unknown unary op %q", n.op)
}

type binNode struct {
	op   string
	l, r node
}

func (n binNode) eval(ctx map[string]any) (Value, error) {
	lv, err := n.l.eval(ctx)
	if err != nil {
		return nil, err
	}
	// Short-circuit the boolean operators.
	if n.op == "&&" || n.op == "||" {
		lb, ok := lv.(bool)
		if !ok {
			return nil, fmt.Errorf("%s needs booleans, got %T", n.op, lv)
		}
		if n.op == "&&" && !lb {
			return false, nil
		}
		if n.op == "||" && lb {
			return true, nil
		}
		rv, err := n.r.eval(ctx)
		if err != nil {
			return nil, err
		}
		rb, ok := rv.(bool)
		if !ok {
			return nil, fmt.Errorf("%s needs booleans, got %T", n.op, rv)
		}
		return rb, nil
	}
	rv, err := n.r.eval(ctx)
	if err != nil {
		return nil, err
	}
	return applyOp(n.op, lv, rv)
}

func applyOp(op string, lv, rv Value) (Value, error) {
	switch op {
	case "==":
		return equal(lv, rv), nil
	case "!=":
		return !equal(lv, rv), nil
	}
	// Numeric comparisons + arithmetic.
	lf, lok := num(lv)
	rf, rok := num(rv)
	if !lok || !rok {
		return nil, fmt.Errorf("op %s needs numeric operands, got %T and %T", op, lv, rv)
	}
	switch op {
	case ">":
		return lf > rf, nil
	case ">=":
		return lf >= rf, nil
	case "<":
		return lf < rf, nil
	case "<=":
		return lf <= rf, nil
	case "+":
		return lf + rf, nil
	case "-":
		return lf - rf, nil
	case "*":
		return lf * rf, nil
	case "/":
		if rf == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return lf / rf, nil
	}
	return nil, fmt.Errorf("unknown op %q", op)
}

func equal(lv, rv any) bool {
	lf, lok := num(lv)
	rf, rok := num(rv)
	if lok && rok {
		return lf == rf
	}
	ls, lok2 := lv.(string)
	rs, rok2 := rv.(string)
	if lok2 && rok2 {
		return ls == rs
	}
	lb, lok3 := lv.(bool)
	rb, rok3 := rv.(bool)
	if lok3 && rok3 {
		return lb == rb
	}
	return false
}

func num(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case jsonNumber:
		f, err := t.Float64()
		return f, err == nil
	}
	return 0, false
}

type parser struct {
	toks []token
	pos  int
}

// Parse compiles one condition expression and yields its root node. It is
// purely syntactic — no execution and no context access.
func Parse(src string) (node, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, fmt.Errorf("condition %q: %w", src, err)
	}
	p := &parser{toks: toks}
	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.toks[p.pos].kind != tokEOF {
		return nil, fmt.Errorf("condition %q: trailing tokens after %q", src, p.toks[p.pos].text)
	}
	return n, nil
}

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *parser) parseOr() (node, error) {
	l, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.toks[p.pos].kind == tokOp && p.toks[p.pos].text == "||" {
		p.next()
		r, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l = binNode{op: "||", l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseAnd() (node, error) {
	l, err := p.parseCmp()
	if err != nil {
		return nil, err
	}
	for p.toks[p.pos].kind == tokOp && p.toks[p.pos].text == "&&" {
		p.next()
		r, err := p.parseCmp()
		if err != nil {
			return nil, err
		}
		l = binNode{op: "&&", l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseCmp() (node, error) {
	l, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	if p.toks[p.pos].kind == tokOp {
		switch p.toks[p.pos].text {
		case "==", "!=", ">", ">=", "<", "<=":
			op := p.next().text
			r, err := p.parseAdd()
			if err != nil {
				return nil, err
			}
			return binNode{op: op, l: l, r: r}, nil
		}
	}
	return l, nil
}

func (p *parser) parseAdd() (node, error) {
	l, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for p.toks[p.pos].kind == tokOp && (p.toks[p.pos].text == "+" || p.toks[p.pos].text == "-") {
		op := p.next().text
		r, err := p.parseMul()
		if err != nil {
			return nil, err
		}
		l = binNode{op: op, l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseMul() (node, error) {
	l, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.toks[p.pos].kind == tokOp && (p.toks[p.pos].text == "*" || p.toks[p.pos].text == "/") {
		op := p.next().text
		r, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l = binNode{op: op, l: l, r: r}
	}
	return l, nil
}

func (p *parser) parseUnary() (node, error) {
	if p.toks[p.pos].kind == tokOp && p.toks[p.pos].text == "!" {
		p.next()
		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return unaryNode{op: "!", x: x}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (node, error) {
	t := p.toks[p.pos]
	switch t.kind {
	case tokOp:
		if t.text == "(" {
			p.next()
			n, err := p.parseOr()
			if err != nil {
				return nil, err
			}
			if p.toks[p.pos].kind != tokOp || p.toks[p.pos].text != ")" {
				return nil, fmt.Errorf("expected ')' at %q", p.toks[p.pos].text)
			}
			p.next()
			return n, nil
		}
		return nil, fmt.Errorf("unexpected operator %q", t.text)
	case tokNumber:
		p.next()
		return litNode{v: t.num}, nil
	case tokString:
		p.next()
		return litNode{v: t.text}, nil
	case tokIdent:
		// true/false literals.
		if t.text == "true" || t.text == "false" {
			p.next()
			return litNode{v: t.text == "true"}, nil
		}
		segs := []string{t.text}
		p.next()
		for p.toks[p.pos].kind == tokOp && p.toks[p.pos].text == "." {
			p.next()
			if p.toks[p.pos].kind != tokIdent {
				return nil, fmt.Errorf("expected field name after '.' at %q", p.toks[p.pos].text)
			}
			segs = append(segs, p.next().text)
		}
		if p.toks[p.pos].kind == tokOp && p.toks[p.pos].text == "." {
			return nil, fmt.Errorf("dangling '.' in path")
		}
		return pathNode{path: segs}, nil
	case tokEOF:
		return nil, fmt.Errorf("unexpected end of expression")
	}
	return nil, fmt.Errorf("unexpected token %q", t.text)
}

// EvalTrue compiles the condition once and evaluates it against the context,
// returning true only when it evaluates to a boolean true. Any evaluation
// error (unknown field, type mismatch, division by zero) returns false: the
// rule fails closed.
func EvalTrue(cond string, ctx map[string]any) (bool, error) {
	n, err := Parse(cond)
	if err != nil {
		return false, err
	}
	v, err := n.eval(ctx)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("condition %q evaluated to %T, want bool", cond, v)
	}
	return b, nil
}