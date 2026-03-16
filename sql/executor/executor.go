package executor

import (
	"fmt"

	"github.com/ish4n10/miniaturedb/sql/lexer"
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

func (e *Executor) executeDropTable(stmt *parser.DropTableStmt) (*Result, error) {
	err := e.db.DropTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	return &Result{}, nil
}

func (e *Executor) executeInsert(stmt *parser.InsertStmt) (*Result, error) {
	err := e.db.Put(stmt.TableName, []byte(stmt.Key), []byte(stmt.Value))
	if err != nil {
		return nil, err
	}
	return &Result{Affected: 1}, nil
}

func (e *Executor) executeDelete(stmt *parser.DeleteStmt) (*Result, error) {

	if stmt.Where == nil {
		return nil, fmt.Errorf("DELETE needs a WHERE")
	}
	if stmt.Where.Op != lexer.TOKEN_EQ {
		return nil, fmt.Errorf("DELETE needs a =")
	}
	err := e.db.Delete(stmt.TableName, []byte(stmt.Where.Value))
	if err != nil {
		return nil, err
	}
	return &Result{Affected: 1}, nil
}

func (e *Executor) executeSelect(s *parser.SelectStmt) (*Result, error) {
	if s.Where == nil {
		return e.scanAll(s.TableName, nil)
	}

	switch s.Where.Op {
	case lexer.TOKEN_EQ:
		val, err := e.db.Get(s.TableName, []byte(s.Where.Value))
		if err != nil {
			return nil, err
		}
		return &Result{
			Rows: []Row{{Key: s.Where.Value, Value: string(val)}},
		}, nil

	case lexer.TOKEN_GTE:
		return e.scanAll(s.TableName, &scanOpts{
			startKey: s.Where.Value,
			include:  true,
		})

	case lexer.TOKEN_GT:
		return e.scanAll(s.TableName, &scanOpts{
			startKey: s.Where.Value,
			include:  false,
		})

	case lexer.TOKEN_LTE:
		return e.scanUntil(s.TableName, s.Where.Value, true)

	case lexer.TOKEN_LT:
		return e.scanUntil(s.TableName, s.Where.Value, false)

	default:
		return nil, fmt.Errorf("unsupported operator in WHERE clause")
	}
}

type scanOpts struct {
	startKey string
	include  bool
}

func (e *Executor) scanAll(table string, opts *scanOpts) (*Result, error) {
	startKey := []byte("")
	if opts != nil {
		startKey = []byte(opts.startKey)
	}

	cursor, err := e.db.Scan(table, startKey)
	if err != nil {
		return nil, err
	}

	var rows []Row
	for cursor.Valid() {
		key := string(cursor.Key())

		if opts != nil && !opts.include && key == opts.startKey {
			cursor.Next()
			continue
		}

		rows = append(rows, Row{Key: key, Value: string(cursor.Value())})
		cursor.Next()
	}

	return &Result{Rows: rows}, nil
}

func (e *Executor) scanUntil(table string, endKey string, include bool) (*Result, error) {
	cursor, err := e.db.Scan(table, []byte(""))
	if err != nil {
		return nil, err
	}

	var rows []Row
	for cursor.Valid() {
		key := string(cursor.Key())

		if include && key > endKey {
			break
		}
		if !include && key >= endKey {
			break
		}

		rows = append(rows, Row{Key: key, Value: string(cursor.Value())})
		cursor.Next()
	}

	return &Result{Rows: rows}, nil
}
