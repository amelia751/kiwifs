package dataview

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Token types produced by the lexer.
type TokenType int

const (
	TokEOF TokenType = iota
	TokIdent
	TokString
	TokNumber
	TokBool
	TokNil
	TokAnd
	TokOr
	TokNot
	TokIn
	TokLike
	TokBetween
	TokIs
	TokNull
	TokEq       // =  or ==
	TokNeq      // != or <>
	TokLt       // <
	TokGt       // >
	TokLte      // <=
	TokGte      // >=
	TokLParen   // (
	TokRParen   // )
	TokLBracket // [
	TokRBracket // ]
	TokComma    // ,
	TokDot      // .
	TokBacktick // `quoted ident`
)

var tokenNames = [...]string{
	TokEOF: "EOF", TokIdent: "IDENT", TokString: "STRING", TokNumber: "NUMBER",
	TokBool: "BOOL", TokNil: "NIL", TokAnd: "AND", TokOr: "OR", TokNot: "NOT",
	TokIn: "IN", TokLike: "LIKE", TokBetween: "BETWEEN", TokIs: "IS", TokNull: "NULL",
	TokEq: "=", TokNeq: "!=", TokLt: "<", TokGt: ">", TokLte: "<=", TokGte: ">=",
	TokLParen: "(", TokRParen: ")", TokLBracket: "[", TokRBracket: "]",
	TokComma: ",", TokDot: ".", TokBacktick: "`",
}

func (t TokenType) String() string {
	if int(t) < len(tokenNames) && tokenNames[t] != "" {
		return tokenNames[t]
	}
	return "?"
}

// Token is a single lexical unit.
type Token struct {
	Type  TokenType
	Value string
	Pos   int // byte offset in the source
}

// Lexer tokenizes a DQL WHERE clause expression.
type Lexer struct {
	input string
	pos   int
	tokens []Token
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

func (l *Lexer) Tokenize() ([]Token, error) {
	for {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			l.tokens = append(l.tokens, Token{Type: TokEOF, Pos: l.pos})
			return l.tokens, nil
		}
		ch, size := utf8.DecodeRuneInString(l.input[l.pos:])
		switch {
		case ch == '(':
			l.emit(TokLParen, "(")
		case ch == ')':
			l.emit(TokRParen, ")")
		case ch == '[':
			l.emit(TokLBracket, "[")
		case ch == ']':
			l.emit(TokRBracket, "]")
		case ch == ',':
			l.emit(TokComma, ",")
		case ch == '.':
			l.emit(TokDot, ".")
		case ch == '=' && l.peekChar(1) == '=':
			l.emitN(TokEq, "==", 2)
		case ch == '=':
			l.emit(TokEq, "=")
		case ch == '!' && l.peekChar(1) == '=':
			l.emitN(TokNeq, "!=", 2)
		case ch == '<' && l.peekChar(1) == '>':
			l.emitN(TokNeq, "<>", 2)
		case ch == '<' && l.peekChar(1) == '=':
			l.emitN(TokLte, "<=", 2)
		case ch == '<':
			l.emit(TokLt, "<")
		case ch == '>' && l.peekChar(1) == '=':
			l.emitN(TokGte, ">=", 2)
		case ch == '>':
			l.emit(TokGt, ">")
		case ch == '&' && l.peekChar(1) == '&':
			l.emitN(TokAnd, "&&", 2)
		case ch == '|' && l.peekChar(1) == '|':
			l.emitN(TokOr, "||", 2)
		case ch == '`':
			if err := l.scanBacktick(); err != nil {
				return nil, err
			}
		case ch == '"' || ch == '\'':
			if err := l.scanString(ch); err != nil {
				return nil, err
			}
		case isDigit(ch) || (ch == '-' && l.pos+size < len(l.input) && isDigit(peekRune(l.input[l.pos+size:]))):
			l.scanNumber()
		case isIdentStart(ch):
			l.scanIdent()
		default:
			return nil, fmt.Errorf("unexpected character %q at position %d", ch, l.pos)
		}
	}
}

func (l *Lexer) emit(typ TokenType, val string) {
	l.tokens = append(l.tokens, Token{Type: typ, Value: val, Pos: l.pos})
	l.pos += len(val)
}

func (l *Lexer) emitN(typ TokenType, val string, n int) {
	l.tokens = append(l.tokens, Token{Type: typ, Value: val, Pos: l.pos})
	l.pos += n
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if !unicode.IsSpace(r) {
			break
		}
		l.pos += size
	}
}

func (l *Lexer) peekChar(offset int) byte {
	i := l.pos + offset
	if i >= len(l.input) {
		return 0
	}
	return l.input[i]
}

func (l *Lexer) scanString(quote rune) error {
	start := l.pos
	l.pos++ // skip opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if ch == '\\' && l.pos+size < len(l.input) {
			next, nsize := utf8.DecodeRuneInString(l.input[l.pos+size:])
			switch next {
			case '"', '\'', '\\':
				sb.WriteRune(next)
				l.pos += size + nsize
				continue
			case 'n':
				sb.WriteByte('\n')
				l.pos += size + nsize
				continue
			case 't':
				sb.WriteByte('\t')
				l.pos += size + nsize
				continue
			}
		}
		if ch == quote {
			l.pos++ // skip closing quote
			l.tokens = append(l.tokens, Token{Type: TokString, Value: sb.String(), Pos: start})
			return nil
		}
		sb.WriteRune(ch)
		l.pos += size
	}
	return fmt.Errorf("unterminated string starting at position %d", start)
}

func (l *Lexer) scanNumber() {
	start := l.pos
	if l.pos < len(l.input) && l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) && isDigit(rune(l.input[l.pos])) {
		l.pos++
	}
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.pos++
		for l.pos < len(l.input) && isDigit(rune(l.input[l.pos])) {
			l.pos++
		}
	}
	l.tokens = append(l.tokens, Token{Type: TokNumber, Value: l.input[start:l.pos], Pos: start})
}

var keywords = map[string]TokenType{
	"and":     TokAnd,
	"or":      TokOr,
	"not":     TokNot,
	"in":      TokIn,
	"like":    TokLike,
	"between": TokBetween,
	"is":      TokIs,
	"null":    TokNull,
	"nil":     TokNil,
	"true":    TokBool,
	"false":   TokBool,
}

func (l *Lexer) scanIdent() {
	start := l.pos
	for l.pos < len(l.input) {
		ch, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if !isIdentPart(ch) {
			break
		}
		l.pos += size
	}
	word := l.input[start:l.pos]
	lower := strings.ToLower(word)
	if typ, ok := keywords[lower]; ok {
		l.tokens = append(l.tokens, Token{Type: typ, Value: lower, Pos: start})
	} else {
		l.tokens = append(l.tokens, Token{Type: TokIdent, Value: word, Pos: start})
	}
}

func (l *Lexer) scanBacktick() error {
	start := l.pos
	l.pos++ // skip opening backtick
	for l.pos < len(l.input) {
		if l.input[l.pos] == '`' {
			word := l.input[start+1 : l.pos]
			l.pos++ // skip closing backtick
			l.tokens = append(l.tokens, Token{Type: TokIdent, Value: word, Pos: start})
			return nil
		}
		l.pos++
	}
	return fmt.Errorf("unterminated backtick identifier starting at position %d", start)
}

func isDigit(r rune) bool       { return r >= '0' && r <= '9' }
func isIdentStart(r rune) bool  { return unicode.IsLetter(r) || r == '_' }
func isIdentPart(r rune) bool   { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' }

func peekRune(s string) rune {
	r, _ := utf8.DecodeRuneInString(s)
	return r
}
