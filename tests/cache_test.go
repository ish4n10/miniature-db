package test

import (
	"testing"

	"github.com/ish4n10/miniaturedb/storage/cache"
	constants "github.com/ish4n10/miniaturedb/storage/common"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
	page "github.com/ish4n10/miniaturedb/storage/page"
)

func setupCache(t *testing.T, maxPages int) (*cache.Cache, *diskmanager.DiskManager) {
	t.Helper()
	dm, err := diskmanager.InitDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	t.Cleanup(func() { dm.CloseFile() })
	return cache.NewCache(maxPages, dm), dm
}

func writePage(t *testing.T, dm *diskmanager.DiskManager, recno uint64) uint32 {
	t.Helper()
	p := page.NewPage()
	page.InitPage(p, recno, page.PageTypeRowLeaf)
	id := dm.AllocatePage()
	if err := dm.WritePage(id, p.Data); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	return id
}

func TestFetchPage(t *testing.T) {
	c, dm := setupCache(t, 5)
	id := writePage(t, dm, 1)

	p, err := c.FetchPage(id)
	if err != nil || p == nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if p.PageHeader.Recno != 1 {
		t.Fatalf("wrong recno: %d", p.PageHeader.Recno)
	}
}

func TestFetchPage_CacheHit(t *testing.T) {
	c, dm := setupCache(t, 5)
	id := writePage(t, dm, 1)

	p1, _ := c.FetchPage(id)
	p2, _ := c.FetchPage(id)
	if p1 != p2 {
		t.Fatal("expected same pointer on cache hit")
	}
}

func TestUnpinPage(t *testing.T) {
	c, dm := setupCache(t, 5)
	id := writePage(t, dm, 1)

	c.FetchPage(id)
	if err := c.UnpinPage(id, false); err != nil {
		t.Fatalf("unpin failed: %v", err)
	}
}

func TestUnpinPage_Errors(t *testing.T) {
	c, dm := setupCache(t, 5)
	id := writePage(t, dm, 1)

	if err := c.UnpinPage(99, false); err == nil {
		t.Fatal("expected error: page not in cache")
	}

	c.FetchPage(id)
	c.UnpinPage(id, false)
	if err := c.UnpinPage(id, false); err == nil {
		t.Fatal("expected error: already unpinned")
	}
}

func TestFlushPage(t *testing.T) {
	c, dm := setupCache(t, 5)
	id := writePage(t, dm, 1)

	p, _ := c.FetchPage(id)
	p.Data[constants.PageHeaderSize] = 0xAB
	c.UnpinPage(id, true)
	c.FlushPage(id)

	buf := make([]byte, constants.PageSize)
	dm.ReadPage(id, buf)
	if buf[constants.PageHeaderSize] != 0xAB {
		t.Fatal("data not flushed to disk")
	}
}

func TestFlushAll(t *testing.T) {
	c, dm := setupCache(t, 5)

	ids := []uint32{writePage(t, dm, 1), writePage(t, dm, 2), writePage(t, dm, 3)}
	for _, id := range ids {
		p, _ := c.FetchPage(id)
		p.Data[constants.PageHeaderSize] = 0xFF
		c.UnpinPage(id, true)
	}

	if err := c.FlushAll(); err != nil {
		t.Fatalf("flush all failed: %v", err)
	}

	for _, id := range ids {
		buf := make([]byte, constants.PageSize)
		dm.ReadPage(id, buf)
		if buf[constants.PageHeaderSize] != 0xFF {
			t.Fatalf("page %d not flushed", id)
		}
	}
}

func TestEviction_TriggersWhenFull(t *testing.T) {
	c, dm := setupCache(t, 2)

	id1, id2 := writePage(t, dm, 1), writePage(t, dm, 2)
	c.FetchPage(id1)
	c.UnpinPage(id1, false)
	c.FetchPage(id2)
	c.UnpinPage(id2, false)

	id3 := writePage(t, dm, 3)
	if _, err := c.FetchPage(id3); err != nil {
		t.Fatalf("eviction failed: %v", err)
	}
}

func TestEviction_DirtyPageFlushed(t *testing.T) {
	c, dm := setupCache(t, 2)

	id1, id2 := writePage(t, dm, 1), writePage(t, dm, 2)

	p1, _ := c.FetchPage(id1)
	p1.Data[constants.PageHeaderSize] = 0xBE
	c.UnpinPage(id1, true)

	c.FetchPage(id2)
	c.UnpinPage(id2, false)

	id3 := writePage(t, dm, 3)
	c.FetchPage(id3)

	buf := make([]byte, constants.PageSize)
	dm.ReadPage(id1, buf)
	if buf[constants.PageHeaderSize] != 0xBE {
		t.Fatal("dirty page not flushed before eviction")
	}
}

func TestEviction_AllPinned(t *testing.T) {
	c, dm := setupCache(t, 2)

	c.FetchPage(writePage(t, dm, 1))
	c.FetchPage(writePage(t, dm, 2))

	if _, err := c.FetchPage(writePage(t, dm, 3)); err == nil {
		t.Fatal("expected error: all pages pinned")
	}
}
