package parser

import "github.com/ish4n10/miniaturedb/sql/lexer"

type Parser struct {
	tokens []lexer.Token
	pos    int
}
