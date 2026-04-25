package search

import (
	"math"
	"strings"
	"time"
)

// EvalComputedField parses and evaluates a computed field expression against
// a context map of frontmatter values. Returns the computed value, or nil on
// error. Expressions use the same syntax as DQL WHERE clauses:
// arithmetic (+, -, *, /), comparisons (>, <, =, >=, <=, !=),
// boolean-to-int coercion, and functions len(), days_since().
func EvalComputedField(expr string, ctx map[string]any) any {
	ast, err := parseComputedExpr(expr)
	if err != nil {
		return nil
	}
	return evalExpr(ast, ctx)
}

// parseComputedExpr extends the standard DQL lexer/parser with arithmetic
// operators (+, -, *, /) by using a thin wrapper.
func parseComputedExpr(input string) (computedExpr, error) {
	return parseArithExpr(input)
}

type computedExpr interface{ computedNode() }

type cBinary struct {
	left, right computedExpr
	op          string
}
type cLiteral struct{ value any }
type cFieldRef struct{ path string }
type cFuncCall struct {
	name string
	args []computedExpr
}
type cParen struct{ inner computedExpr }

func (*cBinary) computedNode()   {}
func (*cLiteral) computedNode()  {}
func (*cFieldRef) computedNode() {}
func (*cFuncCall) computedNode() {}
func (*cParen) computedNode()    {}

func parseArithExpr(input string) (computedExpr, error) {
	p := &computedParser{input: input, pos: 0}
	p.skipWS()
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	return expr, nil
}

type computedParser struct {
	input string
	pos   int
}

func (p *computedParser) skipWS() {
	for p.pos < len(p.input) && (p.input[p.pos] == ' ' || p.input[p.pos] == '\t' || p.input[p.pos] == '\n') {
		p.pos++
	}
}

func (p *computedParser) peek() byte {
	p.skipWS()
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *computedParser) parseOr() (computedExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWS()
		if p.matchKeyword("or") {
			right, rerr := p.parseAnd()
			if rerr != nil {
				return nil, rerr
			}
			left = &cBinary{left: left, right: right, op: "||"}
		} else {
			break
		}
	}
	return left, nil
}

func (p *computedParser) parseAnd() (computedExpr, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWS()
		if p.matchKeyword("and") {
			right, rerr := p.parseComparison()
			if rerr != nil {
				return nil, rerr
			}
			left = &cBinary{left: left, right: right, op: "&&"}
		} else {
			break
		}
	}
	return left, nil
}

func (p *computedParser) parseComparison() (computedExpr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}
	p.skipWS()
	if p.pos < len(p.input) {
		switch {
		case p.pos+1 < len(p.input) && p.input[p.pos:p.pos+2] == ">=":
			p.pos += 2
			right, rerr := p.parseAddSub()
			if rerr != nil {
				return nil, rerr
			}
			return &cBinary{left: left, right: right, op: ">="}, nil
		case p.pos+1 < len(p.input) && p.input[p.pos:p.pos+2] == "<=":
			p.pos += 2
			right, rerr := p.parseAddSub()
			if rerr != nil {
				return nil, rerr
			}
			return &cBinary{left: left, right: right, op: "<="}, nil
		case p.pos+1 < len(p.input) && p.input[p.pos:p.pos+2] == "!=":
			p.pos += 2
			right, rerr := p.parseAddSub()
			if rerr != nil {
				return nil, rerr
			}
			return &cBinary{left: left, right: right, op: "!="}, nil
		case p.pos+1 < len(p.input) && p.input[p.pos:p.pos+2] == "==":
			p.pos += 2
			right, rerr := p.parseAddSub()
			if rerr != nil {
				return nil, rerr
			}
			return &cBinary{left: left, right: right, op: "="}, nil
		case p.input[p.pos] == '>':
			p.pos++
			right, rerr := p.parseAddSub()
			if rerr != nil {
				return nil, rerr
			}
			return &cBinary{left: left, right: right, op: ">"}, nil
		case p.input[p.pos] == '<':
			p.pos++
			right, rerr := p.parseAddSub()
			if rerr != nil {
				return nil, rerr
			}
			return &cBinary{left: left, right: right, op: "<"}, nil
		case p.input[p.pos] == '=':
			p.pos++
			right, rerr := p.parseAddSub()
			if rerr != nil {
				return nil, rerr
			}
			return &cBinary{left: left, right: right, op: "="}, nil
		}
	}
	return left, nil
}

func (p *computedParser) parseAddSub() (computedExpr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWS()
		if p.pos >= len(p.input) {
			break
		}
		ch := p.input[p.pos]
		if ch == '+' || ch == '-' {
			p.pos++
			right, rerr := p.parseMulDiv()
			if rerr != nil {
				return nil, rerr
			}
			left = &cBinary{left: left, right: right, op: string(ch)}
		} else {
			break
		}
	}
	return left, nil
}

func (p *computedParser) parseMulDiv() (computedExpr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		p.skipWS()
		if p.pos >= len(p.input) {
			break
		}
		ch := p.input[p.pos]
		if ch == '*' || ch == '/' {
			p.pos++
			right, rerr := p.parseUnary()
			if rerr != nil {
				return nil, rerr
			}
			left = &cBinary{left: left, right: right, op: string(ch)}
		} else {
			break
		}
	}
	return left, nil
}

func (p *computedParser) parseUnary() (computedExpr, error) {
	return p.parseAtom()
}

func (p *computedParser) parseAtom() (computedExpr, error) {
	p.skipWS()
	if p.pos >= len(p.input) {
		return &cLiteral{value: float64(0)}, nil
	}

	ch := p.input[p.pos]

	if ch == '(' {
		p.pos++
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		p.skipWS()
		if p.pos < len(p.input) && p.input[p.pos] == ')' {
			p.pos++
		}
		return &cParen{inner: inner}, nil
	}

	if ch == '"' || ch == '\'' {
		return p.parseString(ch)
	}

	if isDigitByte(ch) || (ch == '-' && p.pos+1 < len(p.input) && isDigitByte(p.input[p.pos+1])) {
		return p.parseNumber()
	}

	if isIdentStartByte(ch) {
		return p.parseIdentOrFunc()
	}

	return &cLiteral{value: float64(0)}, nil
}

func (p *computedParser) parseString(quote byte) (computedExpr, error) {
	p.pos++ // skip opening quote
	start := p.pos
	for p.pos < len(p.input) && p.input[p.pos] != quote {
		p.pos++
	}
	val := p.input[start:p.pos]
	if p.pos < len(p.input) {
		p.pos++ // skip closing quote
	}
	return &cLiteral{value: val}, nil
}

func (p *computedParser) parseNumber() (computedExpr, error) {
	start := p.pos
	if p.pos < len(p.input) && p.input[p.pos] == '-' {
		p.pos++
	}
	for p.pos < len(p.input) && isDigitByte(p.input[p.pos]) {
		p.pos++
	}
	if p.pos < len(p.input) && p.input[p.pos] == '.' {
		p.pos++
		for p.pos < len(p.input) && isDigitByte(p.input[p.pos]) {
			p.pos++
		}
	}
	s := p.input[start:p.pos]
	var val float64
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			continue
		}
		if s[i] == '.' {
			continue
		}
	}
	// use stdlib
	if f, err := parseFloat(s); err == nil {
		val = f
	}
	return &cLiteral{value: val}, nil
}

func (p *computedParser) parseIdentOrFunc() (computedExpr, error) {
	start := p.pos
	for p.pos < len(p.input) && isIdentPartByte(p.input[p.pos]) {
		p.pos++
	}
	// Capture dotted field paths (a.b.c) immediately, before any whitespace skip.
	for p.pos < len(p.input) && p.input[p.pos] == '.' {
		p.pos++
		for p.pos < len(p.input) && isIdentPartByte(p.input[p.pos]) {
			p.pos++
		}
	}
	name := p.input[start:p.pos]

	lower := strings.ToLower(name)
	if lower == "true" {
		return &cLiteral{value: float64(1)}, nil
	}
	if lower == "false" {
		return &cLiteral{value: float64(0)}, nil
	}

	p.skipWS()
	if p.pos < len(p.input) && p.input[p.pos] == '(' {
		p.pos++
		var args []computedExpr
		p.skipWS()
		if p.pos < len(p.input) && p.input[p.pos] != ')' {
			for {
				arg, err := p.parseOr()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
				p.skipWS()
				if p.pos < len(p.input) && p.input[p.pos] == ',' {
					p.pos++
					continue
				}
				break
			}
		}
		p.skipWS()
		if p.pos < len(p.input) && p.input[p.pos] == ')' {
			p.pos++
		}
		return &cFuncCall{name: lower, args: args}, nil
	}

	return &cFieldRef{path: name}, nil
}

func (p *computedParser) matchKeyword(kw string) bool {
	if p.pos+len(kw) > len(p.input) {
		return false
	}
	if !strings.EqualFold(p.input[p.pos:p.pos+len(kw)], kw) {
		return false
	}
	after := p.pos + len(kw)
	if after < len(p.input) && isIdentPartByte(p.input[after]) {
		return false
	}
	p.pos = after
	return true
}

func evalExpr(e computedExpr, ctx map[string]any) any {
	switch n := e.(type) {
	case *cLiteral:
		return n.value
	case *cFieldRef:
		return lookupField(ctx, n.path)
	case *cParen:
		return evalExpr(n.inner, ctx)
	case *cFuncCall:
		return evalFunc(n, ctx)
	case *cBinary:
		return evalBinary(n, ctx)
	default:
		return nil
	}
}

func evalBinary(b *cBinary, ctx map[string]any) any {
	left := toFloat(evalExpr(b.left, ctx))
	right := toFloat(evalExpr(b.right, ctx))

	switch b.op {
	case "+":
		return left + right
	case "-":
		return left - right
	case "*":
		return left * right
	case "/":
		if right == 0 {
			return float64(0)
		}
		return left / right
	case ">":
		return boolToFloat(left > right)
	case "<":
		return boolToFloat(left < right)
	case ">=":
		return boolToFloat(left >= right)
	case "<=":
		return boolToFloat(left <= right)
	case "=", "==":
		return boolToFloat(left == right)
	case "!=":
		return boolToFloat(left != right)
	case "&&":
		return boolToFloat(left != 0 && right != 0)
	case "||":
		return boolToFloat(left != 0 || right != 0)
	}
	return float64(0)
}

func evalFunc(f *cFuncCall, ctx map[string]any) any {
	switch f.name {
	case "len", "length":
		if len(f.args) != 1 {
			return float64(0)
		}
		val := evalExpr(f.args[0], ctx)
		switch v := val.(type) {
		case string:
			return float64(len(v))
		case []any:
			return float64(len(v))
		default:
			return float64(0)
		}
	case "days_since":
		if len(f.args) != 1 {
			return float64(0)
		}
		val := evalExpr(f.args[0], ctx)
		s, ok := val.(string)
		if !ok {
			return float64(0)
		}
		for _, layout := range []string{"2006-01-02", "2006-01-02T15:04:05Z07:00", "2006-01-02T15:04:05Z"} {
			if t, err := time.Parse(layout, s); err == nil {
				return math.Floor(time.Since(t).Hours() / 24)
			}
		}
		return float64(0)
	case "min":
		if len(f.args) < 2 {
			return float64(0)
		}
		m := toFloat(evalExpr(f.args[0], ctx))
		for _, a := range f.args[1:] {
			v := toFloat(evalExpr(a, ctx))
			if v < m {
				m = v
			}
		}
		return m
	case "max":
		if len(f.args) < 2 {
			return float64(0)
		}
		m := toFloat(evalExpr(f.args[0], ctx))
		for _, a := range f.args[1:] {
			v := toFloat(evalExpr(a, ctx))
			if v > m {
				m = v
			}
		}
		return m
	case "abs":
		if len(f.args) != 1 {
			return float64(0)
		}
		return math.Abs(toFloat(evalExpr(f.args[0], ctx)))
	}
	return float64(0)
}

func lookupField(ctx map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = ctx
	for _, key := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current, ok = m[key]
		if !ok {
			// try underscore-prefixed implicit fields
			current, ok = m["_"+key]
			if !ok {
				return nil
			}
		}
	}
	return current
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case bool:
		if n {
			return 1
		}
		return 0
	case string:
		if f, err := parseFloat(n); err == nil {
			return f
		}
		return 0
	case nil:
		return 0
	}
	return 0
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func isDigitByte(b byte) bool     { return b >= '0' && b <= '9' }
func isIdentStartByte(b byte) bool { return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' }
func isIdentPartByte(b byte) bool  { return isIdentStartByte(b) || isDigitByte(b) || b == '-' }

func parseFloat(s string) (float64, error) {
	var neg bool
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	var val float64
	var frac float64
	divisor := 1.0
	seenDot := false
	for _, ch := range s {
		if ch == '.' {
			seenDot = true
			continue
		}
		d := float64(ch - '0')
		if seenDot {
			divisor *= 10
			frac += d / divisor
		} else {
			val = val*10 + d
		}
	}
	val += frac
	if neg {
		val = -val
	}
	return val, nil
}

