package lexer

import (
	"fmt"
	"strings"
)

type TokenType int

const (
	TOKEN_SELECT TokenType = iota
	TOKEN_INSERT
	TOKEN_DELETE
	TOKEN_CREATE
	TOKEN_DROP
	TOKEN_TABLE
	TOKEN_INTO
	TOKEN_VALUES
	TOKEN_FROM
	TOKEN_WHERE
	TOKEN_AND

	TOKEN_STAR
	TOKEN_COMMA
	TOKEN_LPAREN
	TOKEN_RPAREN
	TOKEN_EQ
	TOKEN_GTE
	TOKEN_LTE
	TOKEN_GT
	TOKEN_LT
	TOKEN_SEMICOLON

	TOKEN_IDENT
	TOKEN_STRING
	TOKEN_EOF
)

type Token struct {
	Type    TokenType
	Literal string
}

type Lexer struct {
	input string
	pos   int
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input, pos: 0}
}

func (lx *Lexer) current() byte {
	if lx.pos >= len(lx.input) {
		return 0
	}
	return lx.input[lx.pos]
}

func (lx *Lexer) advance() {
	lx.pos++
}

func (lx *Lexer) skipWhitespace() {
	for lx.pos < len(lx.input) && (lx.current() == ' ' || lx.current() == '\t' || lx.current() == '\n' || lx.current() == '\r') {
		lx.advance()
	}
}

func (lx *Lexer) readIdent() string {
	start := lx.pos
	for lx.pos < len(lx.input) {
		c := lx.current()
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':' {
			lx.advance()
		} else {
			break
		}
	}
	return lx.input[start:lx.pos]
}

func (lx *Lexer) readString() (string, error) {
	lx.advance()
	start := lx.pos
	for lx.pos < len(lx.input) {
		if lx.current() == '\'' {
			s := lx.input[start:lx.pos]
			lx.advance()
			return s, nil
		}
		lx.advance()
	}
	return "", fmt.Errorf("unterminated string")
}

var keywords = map[string]TokenType{
	"SELECT": TOKEN_SELECT,
	"INSERT": TOKEN_INSERT,
	"DELETE": TOKEN_DELETE,
	"CREATE": TOKEN_CREATE,
	"DROP":   TOKEN_DROP,
	"TABLE":  TOKEN_TABLE,
	"INTO":   TOKEN_INTO,
	"VALUES": TOKEN_VALUES,
	"FROM":   TOKEN_FROM,
	"WHERE":  TOKEN_WHERE,
	"AND":    TOKEN_AND,
}

func (lx *Lexer) NextToken() (Token, error) {
	lx.skipWhitespace()

	if lx.pos >= len(lx.input) {
		return Token{Type: TOKEN_EOF, Literal: ""}, nil
	}

	c := lx.current()

	switch c {
	case '*':
		lx.advance()
		return Token{Type: TOKEN_STAR, Literal: "*"}, nil
	case ',':
		lx.advance()
		return Token{Type: TOKEN_COMMA, Literal: ","}, nil
	case '(':
		lx.advance()
		return Token{Type: TOKEN_LPAREN, Literal: "("}, nil
	case ')':
		lx.advance()
		return Token{Type: TOKEN_RPAREN, Literal: ")"}, nil
	case ';':
		lx.advance()
		return Token{Type: TOKEN_SEMICOLON, Literal: ";"}, nil
	case '=':
		lx.advance()
		return Token{Type: TOKEN_EQ, Literal: "="}, nil
	case '>':
		lx.advance()
		if lx.current() == '=' {
			lx.advance()
			return Token{Type: TOKEN_GTE, Literal: ">="}, nil
		}
		return Token{Type: TOKEN_GT, Literal: ">"}, nil
	case '<':
		lx.advance()
		if lx.current() == '=' {
			lx.advance()
			return Token{Type: TOKEN_LTE, Literal: "<="}, nil
		}
		return Token{Type: TOKEN_LT, Literal: "<"}, nil
	case '\'':
		s, err := lx.readString()
		if err != nil {
			return Token{}, err
		}
		return Token{Type: TOKEN_STRING, Literal: s}, nil
	}

	if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
		ident := lx.readIdent()
		if tt, ok := keywords[strings.ToUpper(ident)]; ok {
			return Token{Type: tt, Literal: ident}, nil
		}
		return Token{Type: TOKEN_IDENT, Literal: ident}, nil
	}

	return Token{}, fmt.Errorf("unexpected character: %c", c)
}

func (lx *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token
	for {
		tok, err := lx.NextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.Type == TOKEN_EOF {
			break
		}
	}
	return tokens, nil
}
