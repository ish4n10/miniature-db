package btree

import (
	"encoding/binary"
	"errors"
	"fmt"

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
				// default
				childPageID := binary.LittleEndian.Uint32(cells[len(cells)-1].Data)

				for i := 0; i+1 < len(cells); i += 2 {
					if bt.compare(key, cells[i].Data) < 0 {

						childPageID = binary.LittleEndian.Uint32(cells[i+1].Data)
						break
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
	path := []uint32{}
	currentPageID := bt.rootPageID

	// phase 1: traverse to leaf
	for {
		p, err := bt.c.FetchPage(currentPageID)
		if err != nil {
			return fmt.Errorf("failed to fetch page %d: %w", currentPageID, err)
		}

		pageType := p.PageHeader.Type

		if pageType == page.PageTypeRowLeaf {
			bt.c.UnpinPage(currentPageID, false)
			break // found our leaf, stop traversal
		}

		// find correct child
		cells, err := p.ReadCells()
		if err != nil {
			bt.c.UnpinPage(currentPageID, false)
			return err
		}

		// default to rightmost child
		childPageID := binary.LittleEndian.Uint32(cells[len(cells)-1].Data)
		for i := 0; i+1 < len(cells); i += 2 {
			if bt.compare(key, cells[i].Data) < 0 {
				childPageID = binary.LittleEndian.Uint32(cells[i+1].Data)
				break
			}
		}

		path = append(path, currentPageID)
		bt.c.UnpinPage(currentPageID, false)
		currentPageID = childPageID
	}

	// phase 2: insert on leaf
	p, err := bt.c.FetchPage(currentPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch leaf page %d: %w", currentPageID, err)
	}

	err = p.AppendKeyValue(key, value)
	if err != nil {
		bt.c.UnpinPage(currentPageID, false)
		// page is full → need to split (phase 3, coming next)
		return fmt.Errorf("page full, split not implemented yet: %w", err)
	}

	bt.c.UnpinPage(currentPageID, true) // dirty = true, we modified it
	return nil
}
