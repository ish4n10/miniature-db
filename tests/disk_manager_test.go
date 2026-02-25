package test

import (
	"os"
	"testing"

	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
)

func TestDiskManager(t *testing.T) {
	path := "test.db"
	defer os.Remove(path)

	dm, err := diskmanager.InitDiskManager(path)
	if err != nil {
		t.Fatalf("NewDiskManager failed: %v", err)
	}
	defer dm.CloseFile()

	data := make([]byte, 5)
	copy(data, []byte("ishan"))

	if err := dm.WritePage(1, data); err != nil {
		t.Fatalf("WritePage failed: %v", err)
	}
	if err := dm.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	newData := make([]byte, 5)
	err = dm.ReadPage(1, newData)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}
	t.Logf("read data: %q", newData)
	if string(newData) != "ishan" {
		t.Fatalf("unexpected read: %q", newData[:5])
	}
}
