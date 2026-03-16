package executor

import db "github.com/ish4n10/miniaturedb/storage"

func NewExecutor(db *db.DB) *Executor {
	return &Executor{db: db}
}
