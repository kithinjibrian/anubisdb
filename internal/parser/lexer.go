package parser

import (
	"strings"
	"unicode"
)

type TokenType int

const (
	EOF TokenType = iota
	IDENTIFIER
	KEYWORD
	NUMBER
	STRING
	OPERATOR
	COMMA
	SEMICOLON
	LPAREN
	RPAREN
	ASTERISK
)

type Token struct {
	Type    TokenType
	Value   string
	Literal string
}

type Lexer struct {
	input   string
	pos     int
	readPos int
	ch      byte
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	pos := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readNumber() string {
	pos := l.pos
	for isDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readString() string {
	quote := l.ch
	l.readChar()
	pos := l.pos
	for l.ch != quote && l.ch != 0 {
		l.readChar()
	}
	result := l.input[pos:l.pos]
	l.readChar()
	return result
}

func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case 0:
		tok = Token{Type: EOF, Literal: ""}
	case '*':
		tok = Token{Type: ASTERISK, Literal: string(l.ch)}
		l.readChar()
	case ',':
		tok = Token{Type: COMMA, Literal: string(l.ch)}
		l.readChar()
	case ';':
		tok = Token{Type: SEMICOLON, Literal: string(l.ch)}
		l.readChar()
	case '(':
		tok = Token{Type: LPAREN, Literal: string(l.ch)}
		l.readChar()
	case ')':
		tok = Token{Type: RPAREN, Literal: string(l.ch)}
		l.readChar()
	case '=', '!', '<', '>':
		op := string(l.ch)
		if l.peekChar() == '=' {
			l.readChar()
			op += string(l.ch)
		}
		tok = Token{Type: OPERATOR, Literal: op}
		l.readChar()
	case '\'', '"':
		tok = Token{Type: STRING, Literal: l.readString()}
	default:
		if isLetter(l.ch) {
			literal := l.readIdentifier()
			tok = Token{Literal: literal}
			if isKeyword(literal) {
				tok.Type = KEYWORD
				tok.Value = strings.ToUpper(literal)
			} else {
				tok.Type = IDENTIFIER
				tok.Value = literal
			}
			return tok
		} else if isDigit(l.ch) {
			tok = Token{Type: NUMBER, Literal: l.readNumber()}
			return tok
		} else {
			tok = Token{Type: EOF, Literal: string(l.ch)}
			l.readChar()
		}
	}

	return tok
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
}

func isKeyword(s string) bool {
	keywords := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE", "AND", "OR", "ORDER", "BY",
		"LIMIT", "JOIN", "ON", "AS", "CREATE", "TABLE", "DROP",
		"INT", "PRIMARY KEY", "VARCHAR",
	}
	upper := strings.ToUpper(s)
	for _, kw := range keywords {
		if upper == kw {
			return true
		}
	}
	return false
}
