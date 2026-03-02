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

func TestInsert_Single(t *testing.T) {
	bt := setupBtree(t)

	err := bt.Insert([]byte("user:1"), []byte(`{"name":"alice"}`))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	val, err := bt.Search([]byte("user:1"))
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if string(val) != `{"name":"alice"}` {
		t.Fatalf("wrong value: %s", val)
	}
}

func TestInsert_Multiple(t *testing.T) {
	bt := setupBtree(t)

	docs := []struct{ key, val string }{
		{"user:1", `{"name":"alice"}`},
		{"user:2", `{"name":"bob"}`},
		{"user:3", `{"name":"charlie"}`},
	}

	for _, d := range docs {
		if err := bt.Insert([]byte(d.key), []byte(d.val)); err != nil {
			t.Fatalf("insert %s failed: %v", d.key, err)
		}
	}

	for _, d := range docs {
		val, err := bt.Search([]byte(d.key))
		if err != nil {
			t.Fatalf("search %s failed: %v", d.key, err)
		}
		if string(val) != d.val {
			t.Fatalf("wrong value for %s: got %s", d.key, val)
		}
	}
}

func TestInsert_NotFound(t *testing.T) {
	bt := setupBtree(t)

	bt.Insert([]byte("user:1"), []byte(`{"name":"alice"}`))

	_, err := bt.Search([]byte("user:99"))
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestInsert_OverwriteKey(t *testing.T) {
	bt := setupBtree(t)

	bt.Insert([]byte("user:1"), []byte(`{"name":"alice"}`))
	bt.Insert([]byte("user:1"), []byte(`{"name":"alice-updated"}`))

	val, err := bt.Search([]byte("user:1"))
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	// last write wins
	if string(val) != `{"name":"alice-updated"}` {
		t.Fatalf("wrong value: %s", val)
	}
}
