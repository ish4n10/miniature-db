package test

import (
	"bytes"
	"testing"

	"github.com/ish4n10/miniaturedb/storage/btree"
	"github.com/ish4n10/miniaturedb/storage/cache"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
)

func setupBtree(t *testing.T) *btree.Btree {
	t.Helper()
	dm, err := diskmanager.InitDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("init dm failed: %v", err)
	}
	t.Cleanup(func() { dm.CloseFile() })

	c := cache.NewCache(10, dm)
	bt, err := btree.NewBtree(c, dm, bytes.Compare)
	if err != nil {
		t.Fatalf("init btree failed: %v", err)
	}
	return bt
}

func TestNewBtree(t *testing.T) {
	bt := setupBtree(t)
	if bt == nil {
		t.Fatal("expected btree, got nil")
	}
}

func TestSearch_KeyNotFound(t *testing.T) {
	bt := setupBtree(t)

	_, err := bt.Search([]byte("user:1"))
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestSearch_EmptyTree(t *testing.T) {
	bt := setupBtree(t)

	_, err := bt.Search([]byte("anything"))
	if err == nil {
		t.Fatal("expected error on empty tree")
	}
}
