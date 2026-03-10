package test

import (
	"bytes"
	"fmt"
	"testing"

	btree "github.com/ish4n10/miniaturedb/storage/btree"
	cache "github.com/ish4n10/miniaturedb/storage/cache"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
)

func setupBtree(t *testing.T) *btree.Btree {
	t.Helper()
	dm, err := diskmanager.InitDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to init disk manager: %v", err)
	}
	t.Cleanup(func() { dm.CloseFile() })
	c := cache.NewCache(50, dm)
	bt, err := btree.NewBtree(c, dm, bytes.Compare)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}
	return bt
}

func TestInsert_Single(t *testing.T) {
	bt := setupBtree(t)
	if err := bt.Insert([]byte("user:1"), []byte(`{"name":"alice"}`)); err != nil {
		t.Fatal(err)
	}
	bt.PrintTree(t)
	val, err := bt.Search([]byte("user:1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != `{"name":"alice"}` {
		t.Fatalf("wrong value: %s", val)
	}
}

func TestInsert_Multiple(t *testing.T) {
	bt := setupBtree(t)
	bt.Insert([]byte("user:1"), []byte(`{"name":"alice"}`))
	bt.Insert([]byte("user:2"), []byte(`{"name":"bob"}`))
	bt.Insert([]byte("user:3"), []byte(`{"name":"charlie"}`))
	bt.PrintTree(t)

	for _, tc := range []struct{ key, val string }{
		{"user:1", `{"name":"alice"}`},
		{"user:2", `{"name":"bob"}`},
		{"user:3", `{"name":"charlie"}`},
	} {
		v, err := bt.Search([]byte(tc.key))
		if err != nil {
			t.Fatalf("search %s: %v", tc.key, err)
		}
		if string(v) != tc.val {
			t.Fatalf("wrong value for %s: got %s", tc.key, v)
		}
	}
}

func TestInsert_NotFound(t *testing.T) {
	bt := setupBtree(t)
	bt.Insert([]byte("user:1"), []byte(`{"name":"alice"}`))
	bt.PrintTree(t)
	_, err := bt.Search([]byte("user:999"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestInsert_OverwriteKey(t *testing.T) {
	bt := setupBtree(t)
	bt.Insert([]byte("user:1"), []byte(`{"name":"alice"}`))
	t.Log("--- after first insert ---")
	bt.PrintTree(t)

	bt.Insert([]byte("user:1"), []byte(`{"name":"alice-updated"}`))
	t.Log("--- after overwrite ---")
	bt.PrintTree(t)

	val, err := bt.Search([]byte("user:1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != `{"name":"alice-updated"}` {
		t.Fatalf("wrong value: %s", val)
	}
}

func TestInsert_PageFull(t *testing.T) {
	bt := setupBtree(t)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("user:%03d", i)
		val := fmt.Sprintf(`{"id":%d}`, i)
		if err := bt.Insert([]byte(key), []byte(val)); err != nil {
			t.Logf("page full at i=%d: %v", i, err)
			break
		}
	}
	bt.PrintTree(t)
}

func TestInsert_Split(t *testing.T) {
	bt := setupBtree(t)

	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("user:%03d", i)
		val := fmt.Sprintf(`{"id":%d}`, i)
		if err := bt.Insert([]byte(key), []byte(val)); err != nil {
			t.Fatalf("insert %s failed: %v", key, err)
		}
	}

	bt.PrintTree(t)
	height, leafPages, internalPages := bt.Stats()
	t.Logf("height=%d leafPages=%d internalPages=%d", height, leafPages, internalPages)

	if leafPages <= 1 {
		t.Fatal("expected split to have happened")
	}

	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("user:%03d", i)
		expected := fmt.Sprintf(`{"id":%d}`, i)
		val, err := bt.Search([]byte(key))
		if err != nil {
			t.Fatalf("search %s failed: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("wrong value for %s: got %s want %s", key, val, expected)
		}
	}
}

func TestInsert_SplitReverseOrder(t *testing.T) {
	bt := setupBtree(t)

	for i := 29; i >= 0; i-- {
		key := fmt.Sprintf("user:%03d", i)
		val := fmt.Sprintf(`{"id":%d}`, i)
		if err := bt.Insert([]byte(key), []byte(val)); err != nil {
			t.Fatalf("insert %s failed: %v", key, err)
		}
	}

	bt.PrintTree(t)
	height, leafPages, internalPages := bt.Stats()
	t.Logf("height=%d leafPages=%d internalPages=%d", height, leafPages, internalPages)

	if leafPages <= 1 {
		t.Fatal("expected split to have happened")
	}

	for i := 29; i >= 0; i-- {
		key := fmt.Sprintf("user:%03d", i)
		expected := fmt.Sprintf(`{"id":%d}`, i)
		val, err := bt.Search([]byte(key))
		if err != nil {
			t.Fatalf("search %s failed: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("wrong value for %s: got %s want %s", key, val, expected)
		}
	}
}

func TestInsert_SplitPreservesData(t *testing.T) {
	bt := setupBtree(t)

	bt.Insert([]byte("user:001"), []byte(`{"name":"alice"}`))
	bt.Insert([]byte("user:002"), []byte(`{"name":"bob"}`))

	for i := 3; i <= 30; i++ {
		bt.Insert([]byte(fmt.Sprintf("user:%03d", i)), []byte(`{"name":"x"}`))
	}

	bt.PrintTree(t)

	for _, tc := range []struct{ key, val string }{
		{"user:001", `{"name":"alice"}`},
		{"user:002", `{"name":"bob"}`},
	} {
		v, err := bt.Search([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s not found: %v", tc.key, err)
		}
		if string(v) != tc.val {
			t.Fatalf("wrong value for %s: got %s want %s", tc.key, v, tc.val)
		}
	}
}

func TestInsert_SplitRandomOrder(t *testing.T) {
	bt := setupBtree(t)

	keys := []int{15, 3, 27, 8, 19, 1, 22, 11, 5, 29, 0, 17, 9, 25, 13, 6, 21, 2, 14, 18, 7, 24, 10, 28, 4, 16, 20, 12, 26, 23}

	for _, i := range keys {
		key := fmt.Sprintf("user:%03d", i)
		val := fmt.Sprintf(`{"id":%d}`, i)
		if err := bt.Insert([]byte(key), []byte(val)); err != nil {
			t.Fatalf("insert %s failed: %v", key, err)
		}
	}

	bt.PrintTree(t)
	height, leafPages, internalPages := bt.Stats()
	t.Logf("height=%d leafPages=%d internalPages=%d", height, leafPages, internalPages)

	for _, i := range keys {
		key := fmt.Sprintf("user:%03d", i)
		expected := fmt.Sprintf(`{"id":%d}`, i)
		val, err := bt.Search([]byte(key))
		if err != nil {
			t.Fatalf("search %s failed: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("wrong value for %s: got %s want %s", key, val, expected)
		}
	}
}

func TestInsert_LargeDataset(t *testing.T) {
	bt := setupBtree(t)

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("user:%05d", i)
		val := fmt.Sprintf(`{"id":%d}`, i)
		if err := bt.Insert([]byte(key), []byte(val)); err != nil {
			t.Fatalf("insert %s failed: %v", key, err)
		}
	}

	height, leafPages, internalPages := bt.Stats()
	t.Logf("height=%d leafPages=%d internalPages=%d", height, leafPages, internalPages)

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("user:%05d", i)
		expected := fmt.Sprintf(`{"id":%d}`, i)
		val, err := bt.Search([]byte(key))
		if err != nil {
			t.Fatalf("search %s failed: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("wrong value for %s: got %s want %s", key, val, expected)
		}
	}
}
