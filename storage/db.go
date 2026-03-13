package db

import (
	"bytes"
	"encoding/binary"
	"fmt"

	btree "github.com/ish4n10/miniaturedb/storage/btree"
	cache "github.com/ish4n10/miniaturedb/storage/cache"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
)

const cacheSize = 64

type DB struct {
	dm      *diskmanager.DiskManager
	cache   *cache.Cache
	catalog *btree.Btree
	tables  map[string]*btree.Btree
}

func Open(path string) (*DB, error) {
	dm, err := diskmanager.InitDiskManager(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	c := cache.NewCache(cacheSize, dm)

	catalogRootPageID, err := dm.ReadDescriptor()
	if err != nil {
		dm.CloseFile()
		return nil, fmt.Errorf("open: read descriptor: %w", err)
	}

	var catalog *btree.Btree
	if catalogRootPageID == 0 {
		catalog, err = btree.NewBtree(c, dm, bytes.Compare)
		if err != nil {
			dm.CloseFile()
			return nil, fmt.Errorf("open: create catalog: %w", err)
		}
		if err := dm.FlushDescriptor(catalog.RootPageID()); err != nil {
			dm.CloseFile()
			return nil, fmt.Errorf("open: flush descriptor: %w", err)
		}
	} else {
		catalog = btree.OpenBtree(c, dm, bytes.Compare, catalogRootPageID)
	}

	return &DB{
		dm:      dm,
		cache:   c,
		catalog: catalog,
		tables:  make(map[string]*btree.Btree),
	}, nil
}

func (db *DB) Close() error {
	if err := db.cache.FlushAll(); err != nil {
		return err
	}
	return db.dm.CloseFile()
}

func (db *DB) CreateTable(name string) error {
	_, err := db.catalog.Search([]byte(name))
	if err == nil {
		return fmt.Errorf("table %q already exists", name)
	}

	t, err := btree.NewBtree(db.cache, db.dm, bytes.Compare)
	if err != nil {
		return fmt.Errorf("create table %q: %w", name, err)
	}

	rootIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(rootIDBytes, t.RootPageID())
	if err := db.catalog.Insert([]byte(name), rootIDBytes); err != nil {
		return fmt.Errorf("create table %q: catalog insert: %w", name, err)
	}

	db.tables[name] = t
	return nil
}

func (db *DB) DropTable(name string) error {
	if err := db.catalog.Delete([]byte(name)); err != nil {
		return fmt.Errorf("drop table %q: %w", name, err)
	}
	delete(db.tables, name)
	return nil
}

func (db *DB) getTable(name string) (*btree.Btree, error) {
	if t, ok := db.tables[name]; ok {
		return t, nil
	}

	rootIDBytes, err := db.catalog.Search([]byte(name))
	if err != nil {
		return nil, fmt.Errorf("table %q not found", name)
	}

	rootPageID := binary.LittleEndian.Uint32(rootIDBytes)
	t := btree.OpenBtree(db.cache, db.dm, bytes.Compare, rootPageID)
	db.tables[name] = t
	return t, nil
}

func (db *DB) Put(table string, key []byte, value []byte) error {
	t, err := db.getTable(table)
	if err != nil {
		return err
	}
	return t.Insert(key, value)
}

func (db *DB) Get(table string, key []byte) ([]byte, error) {
	t, err := db.getTable(table)
	if err != nil {
		return nil, err
	}
	return t.Search(key)
}

func (db *DB) Delete(table string, key []byte) error {
	t, err := db.getTable(table)
	if err != nil {
		return err
	}
	return t.Delete(key)
}

func (db *DB) Scan(table string, startKey []byte) (*btree.Cursor, error) {
	t, err := db.getTable(table)
	if err != nil {
		return nil, err
	}
	return t.SeekTo(startKey)
}
