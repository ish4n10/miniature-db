package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	cache "github.com/ish4n10/miniaturedb/storage/cache"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
	page "github.com/ish4n10/miniaturedb/storage/page"
)

type Btree struct {
	rootPageID uint32
	c          *cache.Cache
	dm         *diskmanager.DiskManager
	compare    func(a, b []byte) int
}

func NewBtree(c *cache.Cache, dm *diskmanager.DiskManager, compare func(a, b []byte) int) (*Btree, error) {
	pageID := dm.AllocatePage()

	p := page.NewPage()

	page.InitPage(p, 0, page.PageTypeRowLeaf)

	err := dm.WritePage(pageID, p.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to write root page: %w", err)
	}

	return &Btree{rootPageID: pageID, c: c, dm: dm, compare: compare}, nil
}

func (bt *Btree) Search(key []byte) ([]byte, error) {
	currentPageID := bt.rootPageID

	for {
		p, err := bt.c.FetchPage(currentPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch page %d: %w", currentPageID, err)
		}

		cells, err := p.ReadCells()
		if err != nil {
			bt.c.UnpinPage(currentPageID, false)
			return nil, err
		}

		pageType := p.PageHeader.Type
		bt.c.UnpinPage(currentPageID, false)

		switch pageType {

		case page.PageTypeRowLeaf:
			{
				for i := 0; i+1 < len(cells); i += 2 {
					if bt.compare(cells[i].Data, key) == 0 {
						value := make([]byte, len(cells[i+1].Data))
						copy(value, cells[i+1].Data)
						return value, nil
					}
				}
				return nil, errors.New("key not found")
			}
		case page.PageTypeRowInternal:
			{
				if len(cells) < 2 {
					return nil, errors.New("corrupt internal page")
				}

				// default to first child
				childPageID := binary.LittleEndian.Uint32(cells[1].Data)

				// find last entry where key >= cells[i] (minimum key of subtree)
				for i := 0; i+1 < len(cells); i += 2 {
					if bt.compare(key, cells[i].Data) >= 0 {
						childPageID = binary.LittleEndian.Uint32(cells[i+1].Data)
					}
				}

				currentPageID = childPageID
			}
		default:
			return nil, fmt.Errorf("unknown page type: %d", pageType)
		}
	}
}

func (bt *Btree) Insert(key []byte, value []byte) error {
	currentPageID := bt.rootPageID

	// traverse to leaf
	for {
		p, err := bt.c.FetchPage(currentPageID)
		if err != nil {
			return fmt.Errorf("failed to fetch page %d: %w", currentPageID, err)
		}

		pageType := p.PageHeader.Type

		if pageType == page.PageTypeRowLeaf {
			bt.c.UnpinPage(currentPageID, false)
			break
		}

		cells, err := p.ReadCells()
		if err != nil {
			bt.c.UnpinPage(currentPageID, false)
			return err
		}

		childPageID := binary.LittleEndian.Uint32(cells[1].Data)
		for i := 0; i+1 < len(cells); i += 2 {
			if bt.compare(key, cells[i].Data) >= 0 {
				childPageID = binary.LittleEndian.Uint32(cells[i+1].Data)
			}
		}

		bt.c.UnpinPage(currentPageID, false)
		currentPageID = childPageID
	}

	// insert on leaf
	p, err := bt.c.FetchPage(currentPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch leaf: %w", err)
	}

	found, err := p.FindAndUpdate(key, value, bt.compare)
	if err != nil {
		bt.c.UnpinPage(currentPageID, false)
		return err
	}
	if found {
		bt.c.UnpinPage(currentPageID, true)
		return nil
	}

	err = p.AppendKeyValue(key, value)
	if err != nil {
		bt.c.UnpinPage(currentPageID, false)
		return fmt.Errorf("page full: %w", err)
	}

	bt.c.UnpinPage(currentPageID, true)
	return nil
}

func (bt *Btree) PrintTree(t interface{ Logf(string, ...any) }) {
	t.Logf("=== TREE (root=%d) ===", bt.rootPageID)
	bt.printPage(t, bt.rootPageID, 0)
}

func (bt *Btree) printPage(t interface{ Logf(string, ...any) }, pageID uint32, depth int) {
	indent := strings.Repeat("  ", depth)

	p, err := bt.c.FetchPage(pageID)
	if err != nil {
		t.Logf("%sERROR fetching page %d: %v", indent, pageID, err)
		return
	}

	cells, _ := p.ReadCells()
	pageType := p.PageHeader.Type
	bt.c.UnpinPage(pageID, false)

	if pageType == page.PageTypeRowLeaf {
		t.Logf("%sLEAF(page=%d) keys=%d", indent, pageID, len(cells)/2)
		for i := 0; i+1 < len(cells); i += 2 {
			t.Logf("%s  [%s] = %s", indent, cells[i].Data, cells[i+1].Data)
		}
		return
	}

	t.Logf("%sINTERNAL(page=%d) keys=%d", indent, pageID, len(cells)/2)
	for i := 0; i+1 < len(cells); i += 2 {
		childID := binary.LittleEndian.Uint32(cells[i+1].Data)
		t.Logf("%s  [%s] → page=%d", indent, cells[i].Data, childID)
		bt.printPage(t, childID, depth+1)
	}
}
