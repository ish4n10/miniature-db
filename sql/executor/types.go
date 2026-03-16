package executor

import db "github.com/ish4n10/miniaturedb/storage"

type Result struct {
	Rows     []Row
	Affected int
}

type Row struct {
	Key   string
	Value string
}

type Executor struct {
	db *db.DB
}
