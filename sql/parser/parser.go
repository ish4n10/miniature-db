package parser

import (
	"fmt"

	"github.com/ish4n10/miniaturedb/sql/lexer"
)

type Statement interface{ statementNode() }

func (s *CreateTableStmt) statementNode() {}
func (s *DropTableStmt) statementNode()   {}
func (s *InsertStmt) statementNode()      {}
func (s *DeleteStmt) statementNode()      {}
func (s *SelectStmt) statementNode()      {}

type Parser struct {
	tokens []lexer.Token
	pos    int
}

func NewParser(tokens []lexer.Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

func (p *Parser) current() lexer.Token {
	if p.pos >= len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) peek() lexer.Token {
	if p.pos+1 >= len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF}
	}
	return p.tokens[p.pos+1]
}

func (p *Parser) advance() lexer.Token {
	tok := p.current()
	p.pos++
	return tok
}

func (p *Parser) expect(tt lexer.TokenType) (lexer.Token, error) {
	tok := p.current()
	if tok.Type != tt {
		return lexer.Token{}, fmt.Errorf("expected token %d got %d (%q)", tt, tok.Type, tok.Literal)
	}
	p.advance()
	return tok, nil
}

func (p *Parser) Parse() (Statement, error) {
	tok := p.current()
	switch tok.Type {
	case lexer.TOKEN_SELECT:
		return p.parseSelect()
	case lexer.TOKEN_INSERT:
		return p.parseInsert()
	case lexer.TOKEN_DELETE:
		return p.parseDelete()
	case lexer.TOKEN_CREATE:
		return p.parseCreate()
	case lexer.TOKEN_DROP:
		return p.parseDrop()
	default:
		return nil, fmt.Errorf("unexpected token %q", tok.Literal)
	}
}

func (p *Parser) parseCreate() (*CreateTableStmt, error) {
	p.advance() // consume CREATE
	if _, err := p.expect(lexer.TOKEN_TABLE); err != nil {
		return nil, err
	}
	name, err := p.expect(lexer.TOKEN_IDENT)
	if err != nil {
		return nil, err
	}
	p.expect(lexer.TOKEN_SEMICOLON)
	return &CreateTableStmt{TableName: name.Literal}, nil
}

func (p *Parser) parseDrop() (*DropTableStmt, error) {
	p.advance()
	if _, err := p.expect(lexer.TOKEN_TABLE); err != nil {
		return nil, err
	}
	name, err := p.expect(lexer.TOKEN_IDENT)
	if err != nil {
		return nil, err
	}
	p.expect(lexer.TOKEN_SEMICOLON)
	return &DropTableStmt{TableName: name.Literal}, nil
}

func (p *Parser) parseInsert() (*InsertStmt, error) {
	p.advance()
	if _, err := p.expect(lexer.TOKEN_INTO); err != nil {
		return nil, err
	}
	table, err := p.expect(lexer.TOKEN_IDENT)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_LPAREN); err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_IDENT); err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_COMMA); err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_IDENT); err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_RPAREN); err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_VALUES); err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_LPAREN); err != nil {
		return nil, err
	}
	key, err := p.expect(lexer.TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_COMMA); err != nil {
		return nil, err
	}
	value, err := p.expect(lexer.TOKEN_STRING)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_RPAREN); err != nil {
		return nil, err
	}
	p.expect(lexer.TOKEN_SEMICOLON)
	return &InsertStmt{TableName: table.Literal, Key: key.Literal, Value: value.Literal}, nil
}

func (p *Parser) parseDelete() (*DeleteStmt, error) {
	p.advance()
	if _, err := p.expect(lexer.TOKEN_FROM); err != nil {
		return nil, err
	}
	table, err := p.expect(lexer.TOKEN_IDENT)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_WHERE); err != nil {
		return nil, err
	}
	where, err := p.parseWhere()
	if err != nil {
		return nil, err
	}
	p.expect(lexer.TOKEN_SEMICOLON)
	return &DeleteStmt{TableName: table.Literal, Where: &where}, nil
}

func (p *Parser) parseSelect() (*SelectStmt, error) {
	p.advance()
	if _, err := p.expect(lexer.TOKEN_STAR); err != nil {
		return nil, err
	}
	if _, err := p.expect(lexer.TOKEN_FROM); err != nil {
		return nil, err
	}
	table, err := p.expect(lexer.TOKEN_IDENT)
	if err != nil {
		return nil, err
	}

	if p.current().Type == lexer.TOKEN_WHERE {
		p.advance()
		where, err := p.parseWhere()
		if err != nil {
			return nil, err
		}
		p.expect(lexer.TOKEN_SEMICOLON)
		return &SelectStmt{TableName: table.Literal, Where: &where}, nil
	}

	p.expect(lexer.TOKEN_SEMICOLON)
	return &SelectStmt{TableName: table.Literal, Where: nil}, nil
}

func (p *Parser) parseWhere() (WhereClause, error) {
	key, err := p.expect(lexer.TOKEN_IDENT)
	if err != nil {
		return WhereClause{}, err
	}

	op := p.current()
	switch op.Type {
	case lexer.TOKEN_EQ, lexer.TOKEN_GTE, lexer.TOKEN_GT, lexer.TOKEN_LTE, lexer.TOKEN_LT:
		p.advance()
	default:
		return WhereClause{}, fmt.Errorf("expected comparison operator, got %q", op.Literal)
	}

	val, err := p.expect(lexer.TOKEN_STRING)
	if err != nil {
		return WhereClause{}, err
	}

	return WhereClause{Key: key.Literal, Op: op.Type, Value: val.Literal}, nil
}
