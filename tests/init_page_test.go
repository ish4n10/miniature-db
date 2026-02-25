package test

import (
	"testing"

	constants "github.com/ish4n10/miniaturedb/storage/common"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
	page "github.com/ish4n10/miniaturedb/storage/page"
)

func TestNewPage(t *testing.T) {
	p := page.NewPage()

	if len(p.Data) != constants.PageSize {
		t.Fatalf("expected page size %d, got %d", constants.PageSize, len(p.Data))
	}
	if p.PageHeader == nil || p.BlockHeader == nil {
		t.Fatal("headers should not be nil after NewPage")
	}
}

func TestInitPage(t *testing.T) {
	p := page.NewPage()

	for i := range p.Data {
		p.Data[i] = 0xFF
	}

	page.InitPage(p, 42, page.PageTypeRowLeaf)

	if p.PageHeader.Type != page.PageTypeRowLeaf {
		t.Fatalf("wrong page type: got %d", p.PageHeader.Type)
	}
	if p.PageHeader.Recno != 42 {
		t.Fatalf("wrong recno: got %d", p.PageHeader.Recno)
	}
	if p.PageHeader.Entries != 0 {
		t.Fatalf("entries should be 0 on fresh page, got %d", p.PageHeader.Entries)
	}
	if p.PageHeader.Version != 1 {
		t.Fatalf("expected version 1, got %d", p.PageHeader.Version)
	}
	if p.BlockHeader.DiskSize != uint32(constants.PageSize) {
		t.Fatalf("wrong disk size: got %d", p.BlockHeader.DiskSize)
	}

	for i := constants.PageHeaderSize; i < constants.PageSize; i++ {
		if p.Data[i] != 0 {
			t.Fatalf("byte %d should be 0 after init, got %d", i, p.Data[i])
		}
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	p := page.NewPage()
	page.InitPage(p, 99, page.PageTypeRowInternal)

	p2 := page.NewPage()
	copy(p2.Data, p.Data)
	p2.ReadHeaders()

	if p2.PageHeader.Recno != 99 {
		t.Fatalf("recno mismatch: got %d", p2.PageHeader.Recno)
	}
	if p2.PageHeader.Type != page.PageTypeRowInternal {
		t.Fatalf("page type mismatch: got %d", p2.PageHeader.Type)
	}
	if p2.BlockHeader.DiskSize != uint32(constants.PageSize) {
		t.Fatalf("disk size mismatch: got %d", p2.BlockHeader.DiskSize)
	}
}

func TestPageDiskRoundTrip(t *testing.T) {
	dm, err := diskmanager.InitDiskManager(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer dm.CloseFile()

	p := page.NewPage()
	page.InitPage(p, 1, page.PageTypeRowLeaf)
	p.Data[constants.PageHeaderSize] = 0xDE
	p.Data[constants.PageHeaderSize+1] = 0xAD

	pageID := dm.AllocatePage()
	if err := dm.WritePage(pageID, p.Data); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	p2 := page.NewPage()
	if err := dm.ReadPage(pageID, p2.Data); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	p2.ReadHeaders()

	if p2.PageHeader.Type != page.PageTypeRowLeaf {
		t.Fatal("page type mismatch after disk round trip")
	}
	if p2.Data[constants.PageHeaderSize] != 0xDE || p2.Data[constants.PageHeaderSize+1] != 0xAD {
		t.Fatal("data mismatch after disk round trip")
	}
}
