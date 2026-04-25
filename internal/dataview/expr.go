package dataview

import (
	"fmt"
	"strconv"
)

// Binding powers define operator precedence.
const (
	bpNone    = 0
	bpOr      = 10
	bpAnd     = 20
	bpNot     = 25
	bpCompare = 30
	bpCall    = 70
)

// ExprParser is a Pratt parser for WHERE clause expressions.
type ExprParser struct {
	tokens []Token
	pos    int
}

func NewExprParser(tokens []Token) *ExprParser {
	return &ExprParser{tokens: tokens}
}

func (p *ExprParser) Parse() (Expr, error) {
	expr, err := p.parseExpr(bpNone)
	if err != nil {
		return nil, err
	}
	if p.peek().Type != TokEOF {
		return nil, fmt.Errorf("unexpected token %q at position %d", p.peek().Value, p.peek().Pos)
	}
	return expr, nil
}

func (p *ExprParser) parseExpr(minBP int) (Expr, error) {
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}
	for {
		bp := p.infixBP()
		if bp <= minBP {
			break
		}
		left, err = p.parseInfix(left, bp)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *ExprParser) parsePrefix() (Expr, error) {
	tok := p.peek()
	switch tok.Type {
	case TokString:
		p.advance()
		return &Literal{Value: tok.Value}, nil
	case TokNumber:
		p.advance()
		return p.parseNumber(tok.Value)
	case TokBool:
		p.advance()
		return &Literal{Value: tok.Value == "true"}, nil
	case TokNil, TokNull:
		p.advance()
		return &Literal{Value: nil}, nil
	case TokNot:
		p.advance()
		expr, err := p.parseExpr(bpNot)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: OpNot, Expr: expr}, nil
	case TokLParen:
		p.advance()
		expr, err := p.parseExpr(bpNone)
		if err != nil {
			return nil, err
		}
		if p.peek().Type != TokRParen {
			return nil, fmt.Errorf("expected ')' at position %d, got %q", p.peek().Pos, p.peek().Value)
		}
		p.advance()
		return expr, nil
	case TokIdent:
		return p.parseIdentOrCall()
	default:
		return nil, fmt.Errorf("unexpected token %q at position %d", tok.Value, tok.Pos)
	}
}

func (p *ExprParser) parseIdentOrCall() (Expr, error) {
	tok := p.peek()
	p.advance()
	name := tok.Value

	// Dotted field access: status.active or mastery.derivatives
	for p.peek().Type == TokDot {
		p.advance() // consume dot
		next := p.peek()
		if next.Type != TokIdent && next.Type != TokNumber {
			return nil, fmt.Errorf("expected field name after '.' at position %d", next.Pos)
		}
		p.advance()
		name += "." + next.Value
	}

	// Function call: name(args...)
	if p.peek().Type == TokLParen {
		p.advance() // consume (
		var args []Expr
		if p.peek().Type != TokRParen {
			for {
				arg, err := p.parseExpr(bpNone)
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
				if p.peek().Type != TokComma {
					break
				}
				p.advance() // consume comma
			}
		}
		if p.peek().Type != TokRParen {
			return nil, fmt.Errorf("expected ')' after function arguments at position %d", p.peek().Pos)
		}
		p.advance()
		return &FuncCall{Name: name, Args: args}, nil
	}

	return &FieldRef{Path: name}, nil
}

func (p *ExprParser) infixBP() int {
	tok := p.peek()
	switch tok.Type {
	case TokOr:
		return bpOr
	case TokAnd:
		return bpAnd
	case TokEq, TokNeq, TokLt, TokGt, TokLte, TokGte:
		return bpCompare
	case TokLike, TokIn, TokBetween, TokIs:
		return bpCompare
	case TokNot:
		// NOT LIKE, NOT IN, NOT BETWEEN
		if p.peekN(1).Type == TokLike || p.peekN(1).Type == TokIn || p.peekN(1).Type == TokBetween {
			return bpCompare
		}
		return bpNone
	default:
		return bpNone
	}
}

var infixOps = map[TokenType]Operator{
	TokEq:  OpEq,
	TokNeq: OpNeq,
	TokLt:  OpLt,
	TokGt:  OpGt,
	TokLte: OpLte,
	TokGte: OpGte,
	TokLike: OpLike,
}

func (p *ExprParser) parseInfix(left Expr, bp int) (Expr, error) {
	tok := p.peek()
	switch tok.Type {
	case TokAnd:
		p.advance()
		right, err := p.parseExpr(bp + 1)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: OpAnd, Right: right}, nil
	case TokOr:
		p.advance()
		right, err := p.parseExpr(bp)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: OpOr, Right: right}, nil
	case TokEq, TokNeq, TokLt, TokGt, TokLte, TokGte, TokLike:
		op := infixOps[tok.Type]
		p.advance()
		right, err := p.parseExpr(bp + 1)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: op, Right: right}, nil
	case TokIn:
		return p.parseIn(left, false)
	case TokBetween:
		return p.parseBetween(left)
	case TokIs:
		return p.parseIsNull(left)
	case TokNot:
		// NOT LIKE, NOT IN, NOT BETWEEN
		p.advance()
		next := p.peek()
		switch next.Type {
		case TokLike:
			p.advance()
			right, err := p.parseExpr(bp + 1)
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Left: left, Op: OpNotLike, Right: right}, nil
		case TokIn:
			return p.parseIn(left, true)
		case TokBetween:
			between, err := p.parseBetween(left)
			if err != nil {
				return nil, err
			}
			return &UnaryExpr{Op: OpNot, Expr: between}, nil
		default:
			return nil, fmt.Errorf("expected LIKE, IN, or BETWEEN after NOT at position %d", next.Pos)
		}
	default:
		return nil, fmt.Errorf("unexpected infix token %q at position %d", tok.Value, tok.Pos)
	}
}

func (p *ExprParser) parseIn(left Expr, negate bool) (Expr, error) {
	p.advance() // consume IN
	if p.peek().Type != TokLParen {
		return nil, fmt.Errorf("expected '(' after IN at position %d", p.peek().Pos)
	}
	p.advance()
	var items []Expr
	for p.peek().Type != TokRParen {
		item, err := p.parseExpr(bpNone)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		if p.peek().Type == TokComma {
			p.advance()
		}
	}
	p.advance() // consume )
	if len(items) == 0 {
		return nil, fmt.Errorf("IN requires at least one value at position %d", p.peek().Pos)
	}
	op := OpIn
	if negate {
		op = OpNotIn
	}
	return &BinaryExpr{Left: left, Op: op, Right: &ListExpr{Items: items}}, nil
}

func (p *ExprParser) parseBetween(left Expr) (Expr, error) {
	p.advance() // consume BETWEEN
	low, err := p.parseExpr(bpCompare + 1)
	if err != nil {
		return nil, err
	}
	if p.peek().Type != TokAnd {
		return nil, fmt.Errorf("expected AND in BETWEEN at position %d", p.peek().Pos)
	}
	p.advance()
	high, err := p.parseExpr(bpCompare + 1)
	if err != nil {
		return nil, err
	}
	return &BetweenExpr{Expr: left, Low: low, High: high}, nil
}

func (p *ExprParser) parseIsNull(left Expr) (Expr, error) {
	p.advance() // consume IS
	negate := false
	if p.peek().Type == TokNot {
		negate = true
		p.advance()
	}
	if p.peek().Type != TokNull && p.peek().Type != TokNil {
		return nil, fmt.Errorf("expected NULL after IS at position %d", p.peek().Pos)
	}
	p.advance()
	return &IsNullExpr{Expr: left, Negate: negate}, nil
}

func (p *ExprParser) parseNumber(s string) (Expr, error) {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &Literal{Value: i}, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q", s)
	}
	return &Literal{Value: f}, nil
}

func (p *ExprParser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokEOF, Pos: -1}
	}
	return p.tokens[p.pos]
}

func (p *ExprParser) peekN(n int) Token {
	i := p.pos + n
	if i >= len(p.tokens) {
		return Token{Type: TokEOF, Pos: -1}
	}
	return p.tokens[i]
}

func (p *ExprParser) advance() Token {
	tok := p.peek()
	p.pos++
	return tok
}

// ParseExpr tokenizes and parses an expression string into an AST.
func ParseExpr(input string) (Expr, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}
	return NewExprParser(tokens).Parse()
}
