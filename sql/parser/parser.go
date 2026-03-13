package parser

import "github.com/ish4n10/miniaturedb/sql/lexer"

type Parser struct {
	tokens []lexer.Token
	pos    int
}

func NewParser(tokens []lexer.Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

// func (p *Parser) Parse() (interface{}, error) {

// }

func (p *Parser) current() lexer.Token {
	if p.pos > len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF, Literal: ""}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() lexer.Token {
	if p.pos+1 > len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF, Literal: ""}
	}
	p.pos++
	return p.current()
}

// func (p *Parser) expect(tt lexer.TokenType) (lexer.Token, error) {

// }
