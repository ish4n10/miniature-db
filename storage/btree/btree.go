package btree

import (
	"encoding/binary"
	"errors"
	"fmt"

	cache "github.com/ish4n10/miniaturedb/storage/cache"
	cell "github.com/ish4n10/miniaturedb/storage/cell"
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
	if err := dm.WritePage(pageID, p.Data); err != nil {
		return nil, fmt.Errorf("failed to write root page: %w", err)
	}
	return &Btree{rootPageID: pageID, c: c, dm: dm, compare: compare}, nil
}

func (bt *Btree) Search(key []byte) ([]byte, error) {
	currentPageID := bt.rootPageID

	for {
		p, err := bt.c.FetchPage(currentPageID)
		if err != nil {
			return nil, err
		}

		cells, err := p.ReadCells()
		pageType := p.PageHeader.Type
		bt.c.UnpinPage(currentPageID, false)
		if err != nil {
			return nil, err
		}

		switch pageType {
		case page.PageTypeRowLeaf:
			for i := 0; i+1 < len(cells); i += 2 {
				if bt.compare(cells[i].Data, key) == 0 {
					if cells[i+1].Type == cell.CellTypeDeleted {
						return nil, errors.New("key not found")
					}
					val := make([]byte, len(cells[i+1].Data))
					copy(val, cells[i+1].Data)
					return val, nil
				}
			}
			return nil, errors.New("key not found")

		case page.PageTypeRowInternal:
			// layout: [addr(P0)][key(K1)][addr(P1)][key(K2)][addr(P2)]
			childPageID := binary.LittleEndian.Uint32(cells[0].Data)
			for i := 1; i+1 < len(cells); i += 2 {
				if bt.compare(key, cells[i].Data) >= 0 {
					childPageID = binary.LittleEndian.Uint32(cells[i+1].Data)
				}
			}
			currentPageID = childPageID

		default:
			return nil, fmt.Errorf("unknown page type: %d", pageType)
		}
	}
}

func (bt *Btree) Insert(key []byte, value []byte) error {
	var path []uint32
	currentPageID := bt.rootPageID

	// traverse to leaf
	for {
		p, err := bt.c.FetchPage(currentPageID)
		if err != nil {
			return err
		}

		pageType := p.PageHeader.Type
		if pageType == page.PageTypeRowLeaf {
			bt.c.UnpinPage(currentPageID, false)
			break
		}

		cells, err := p.ReadCells()
		bt.c.UnpinPage(currentPageID, false)
		if err != nil {
			return err
		}

		childPageID := binary.LittleEndian.Uint32(cells[0].Data)
		for i := 1; i+1 < len(cells); i += 2 {
			if bt.compare(key, cells[i].Data) >= 0 {
				childPageID = binary.LittleEndian.Uint32(cells[i+1].Data)
			}
		}

		path = append(path, currentPageID)
		currentPageID = childPageID
	}

	// try insert on leaf
	p, err := bt.c.FetchPage(currentPageID)
	if err != nil {
		return err
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

	err = p.InsertSorted(key, value, bt.compare)
	if err == nil {
		bt.c.UnpinPage(currentPageID, true)
		return nil
	}
	if err.Error() != "page is full" {
		bt.c.UnpinPage(currentPageID, false)
		return err
	}

	// page full split
	cells, _ := p.ReadCells()
	bt.c.UnpinPage(currentPageID, true)

	// merge new key into cells sorted
	insertAt := len(cells)
	for i := 0; i+1 < len(cells); i += 2 {
		if bt.compare(key, cells[i].Data) < 0 {
			insertAt = i
			break
		}
	}
	merged := make([]*cell.Cell, 0, len(cells)+2)
	merged = append(merged, cells[:insertAt]...)
	merged = append(merged, &cell.Cell{Type: cell.CellTypeKey, Data: key}, &cell.Cell{Type: cell.CellTypeValue, Data: value})
	merged = append(merged, cells[insertAt:]...)

	left, right, rightMinKey := splitCells(merged)
	if left == nil {
		return errors.New("page too small to split")
	}

	// rewrite left into current page

	lp, err := bt.c.FetchPage(currentPageID)
	if err != nil {
		return err
	}
	oldNextPageID := lp.PageHeader.NextPageID

	// create right page first so we know its ID
	rightPageID, err := bt.createNewLeafPage(right, oldNextPageID)
	if err != nil {
		bt.c.UnpinPage(currentPageID, false)
		return err
	}

	// rewrite left with NextPageID  right
	if err := bt.writeLeafPage(lp, currentPageID, left, rightPageID); err != nil {
		bt.c.UnpinPage(currentPageID, false)
		return err
	}
	bt.c.UnpinPage(currentPageID, true)

	if len(path) == 0 {
		return bt.createNewRoot(currentPageID, rightMinKey, rightPageID)
	}
	return bt.insertIntoParent(path[len(path)-1], rightMinKey, rightPageID, path[:len(path)-1])
}

func (bt *Btree) Delete(key []byte) error {
	var path []uint32
	currentPageID := bt.rootPageID

	// traverse to leaf
	for {
		p, err := bt.c.FetchPage(currentPageID)
		if err != nil {
			return err
		}

		pageType := p.PageHeader.Type
		if pageType == page.PageTypeRowLeaf {
			bt.c.UnpinPage(currentPageID, false)
			break
		}

		cells, err := p.ReadCells()
		bt.c.UnpinPage(currentPageID, false)
		if err != nil {
			return err
		}

		childPageID := binary.LittleEndian.Uint32(cells[0].Data)
		for i := 1; i+1 < len(cells); i += 2 {
			if bt.compare(key, cells[i].Data) >= 0 {
				childPageID = binary.LittleEndian.Uint32(cells[i+1].Data)
			}
		}

		path = append(path, currentPageID)
		currentPageID = childPageID
	}

	p, err := bt.c.FetchPage(currentPageID)
	if err != nil {
		return err
	}

	found, err := p.MarkDeleted(key, bt.compare)
	if err != nil {
		bt.c.UnpinPage(currentPageID, false)
		return err
	}
	if !found {
		bt.c.UnpinPage(currentPageID, false)
		return errors.New("key not found")
	}

	bt.c.UnpinPage(currentPageID, true)
	return nil
}

func (bt *Btree) RootPageID() uint32 {
	return bt.rootPageID
}

// OpenBtree reopens an existing B-tree at a known root pageID
func OpenBtree(c *cache.Cache, dm *diskmanager.DiskManager, compare func(a, b []byte) int, rootPageID uint32) *Btree {
	return &Btree{rootPageID: rootPageID, c: c, dm: dm, compare: compare}
}
