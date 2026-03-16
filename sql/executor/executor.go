package executor

import (
	parser "github.com/ish4n10/miniaturedb/sql/parser"
	db "github.com/ish4n10/miniaturedb/storage"
)

func NewExecutor(db *db.DB) *Executor {
	return &Executor{db: db}
}

func (e *Executor) Execute(stmt parser.Statement) (*Result, error) {

}

func (e *Executor) executeCreateTable(stmt *parser.CreateTableStmt) (*Result, error) {
	err := e.db.CreateTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	return &Result{}, nil
}
