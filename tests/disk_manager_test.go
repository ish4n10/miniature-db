package test

import (
	"os"
	"testing"

	constants "github.com/ish4n10/miniaturedb/storage/common"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
)

func newDM(t *testing.T) *diskmanager.DiskManager {
	t.Helper()
	dm, err := diskmanager.InitDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("failed to init disk manager: %v", err)
	}
	t.Cleanup(func() { dm.CloseFile() })
	return dm
}

func TestInitDiskManager(t *testing.T) {
	t.Run("creates new file", func(t *testing.T) {
		path := t.TempDir() + "/test.db"
		dm, err := diskmanager.InitDiskManager(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer dm.CloseFile()

		if _, err := os.Stat(path); err != nil {
			t.Fatal("file was not created on disk")
		}
	})

	t.Run("reopens existing file", func(t *testing.T) {
		path := t.TempDir() + "/test.db"

		dm, err := diskmanager.InitDiskManager(path)
		if err != nil {
			t.Fatalf("first open failed: %v", err)
		}
		dm.CloseFile()

		dm2, err := diskmanager.InitDiskManager(path)
		if err != nil {
			t.Fatalf("reopen failed: %v", err)
		}
		dm2.CloseFile()
	})

	t.Run("rejects corrupt file", func(t *testing.T) {
		path := t.TempDir() + "/test.db"
		os.WriteFile(path, make([]byte, constants.PageSize), 0644)

		_, err := diskmanager.InitDiskManager(path)
		if err == nil {
			t.Fatal("expected error for corrupt file")
		}
	})
}

func TestAllocatePage(t *testing.T) {
	dm := newDM(t)

	first := dm.AllocatePage()
	if first != 1 {
		t.Fatalf("first page should be 1, got %d", first)
	}

	for i := uint32(2); i <= 5; i++ {
		if id := dm.AllocatePage(); id != i {
			t.Fatalf("expected page %d, got %d", i, id)
		}
	}
}

func TestWriteAndReadPage(t *testing.T) {
	dm := newDM(t)
	pageID := dm.AllocatePage()

	buf := make([]byte, constants.PageSize)
	buf[40] = 0xAB
	buf[41] = 0xCD
	buf[100] = 0xFF

	if err := dm.WritePage(pageID, buf); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	got := make([]byte, constants.PageSize)
	if err := dm.ReadPage(pageID, got); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if got[40] != 0xAB || got[41] != 0xCD || got[100] != 0xFF {
		t.Fatal("data mismatch after read")
	}
}

func TestChecksumMismatch(t *testing.T) {
	path := t.TempDir() + "/test.db"

	dm, err := diskmanager.InitDiskManager(path)
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}

	pageID := dm.AllocatePage()
	buf := make([]byte, constants.PageSize)
	buf[40] = 0x01
	dm.WritePage(pageID, buf)
	dm.CloseFile()

	// corrupt the page directly on disk
	f, _ := os.OpenFile(path, os.O_RDWR, 0644)
	f.WriteAt([]byte{0xFF, 0xFF}, int64(pageID)*int64(constants.PageSize)+40)
	f.Close()

	dm2, err := diskmanager.InitDiskManager(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer dm2.CloseFile()

	if err := dm2.ReadPage(pageID, make([]byte, constants.PageSize)); err == nil {
		t.Fatal("expected checksum error on corrupted page")
	}
}

func TestInvalidOperations(t *testing.T) {
	dm := newDM(t)
	pageID := dm.AllocatePage()

	t.Run("write wrong buffer size", func(t *testing.T) {
		if err := dm.WritePage(pageID, make([]byte, 100)); err == nil {
			t.Fatal("expected error for wrong buffer size")
		}
	})

	t.Run("read wrong buffer size", func(t *testing.T) {
		if err := dm.ReadPage(pageID, make([]byte, 100)); err == nil {
			t.Fatal("expected error for wrong buffer size")
		}
	})

	t.Run("write to page 0", func(t *testing.T) {
		if err := dm.WritePage(0, make([]byte, constants.PageSize)); err == nil {
			t.Fatal("expected error writing to reserved page 0")
		}
	})

	t.Run("read from page 0", func(t *testing.T) {
		if err := dm.ReadPage(0, make([]byte, constants.PageSize)); err == nil {
			t.Fatal("expected error reading from reserved page 0")
		}
	})
}

func TestFlush(t *testing.T) {
	dm := newDM(t)
	if err := dm.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
}
