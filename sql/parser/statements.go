package parser

import "github.com/ish4n10/miniaturedb/sql/lexer"

type CreateTableStmt struct {
	TableName string
}

type DropTableStmt struct {
	TableName string
}

type InsertStmt struct {
	TableName string
	Key       string
	Value     string
}

type DeleteStmt struct {
	TableName string
	Where     *WhereClause
}

type SelectStmt struct {
	TableName string
	Where     *WhereClause
}

type WhereClause struct {
	Key   string
	Op    lexer.TokenType
	Value string
}
