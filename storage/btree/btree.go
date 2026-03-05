package btree

import (
	"encoding/binary"
	"errors"
	"fmt"

	cache "github.com/ish4n10/miniaturedb/storage/cache"
	"github.com/ish4n10/miniaturedb/storage/cell"
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
	if err == nil {
		bt.c.UnpinPage(currentPageID, true)
		return nil
	}
	// leaf full now

	cells, _ := p.ReadCells()
	totalPairs := len(cells) / 2
	mid := totalPairs / 2

	leftCells := cells[:mid*2]
	rightCells := cells[mid*2:]

	middleKey := make([]byte, len(rightCells[0].Data))

	copy(middleKey, rightCells[0].Data)

	recno := p.PageHeader.Recno
	clear(p.Data)
	page.InitPage(p, recno, page.PageTypeRowLeaf)

	for i := 0; i+1 < len(leftCells); i += 2 {
		p.AppendKeyValue(leftCells[i].Data, leftCells[i+1].Data)
	}

	newPageID := bt.dm.AllocatePage()
	newPage := page.NewPage()
	page.InitPage(newPage, 0, page.PageTypeRowLeaf)
	for i := 0; i+1 < len(rightCells); i += 2 {
		newPage.AppendKeyValue(rightCells[i].Data, rightCells[i+1].Data)
	}

	if bt.compare(key, middleKey) < 0 {
		p.AppendKeyValue(key, value)
		bt.c.UnpinPage(currentPageID, true)
		bt.dm.WritePage(newPageID, newPage.Data)
	} else {
		newPage.AppendKeyValue(key, value)
		bt.c.UnpinPage(currentPageID, true)
		bt.dm.WritePage(newPageID, newPage.Data)
	}

	// promote middle key to parent
	if len(path) == 0 {
		newRootID := bt.dm.AllocatePage()
		newRoot := page.NewPage()
		page.InitPage(newRoot, 0, page.PageTypeRowInternal)
		newRoot.AppendKeyAddr(middleKey, currentPageID)
		newRoot.AppendKeyAddr(middleKey, newPageID)
		if err := bt.dm.WritePage(newRootID, newRoot.Data); err != nil {
			return fmt.Errorf("failed to write new root: %w", err)
		}
		bt.rootPageID = newRootID
		return nil
	}

	// insert middle key into existing parent
	parentPageID := path[len(path)-1]
	parentPage, err := bt.c.FetchPage(parentPageID)
	if err != nil {
		return err
	}

	parentCells, _ := parentPage.ReadCells()
	pageIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(pageIDBytes, newPageID)

	newKeyCell := &cell.Cell{Type: cell.CellTypeKey, Data: middleKey}
	newAddrCell := &cell.Cell{Type: cell.CellTypeAddr, Data: pageIDBytes}

	insertAt := len(parentCells)
	for i := 0; i < len(parentCells); i += 2 {
		if bt.compare(middleKey, parentCells[i].Data) < 0 {
			insertAt = i
			break
		}
	}

	newCells := make([]*cell.Cell, 0, len(parentCells)+2)
	newCells = append(newCells, parentCells[:insertAt]...)
	newCells = append(newCells, newKeyCell, newAddrCell)
	newCells = append(newCells, parentCells[insertAt:]...)

	clear(parentPage.Data)
	page.InitPage(parentPage, parentPage.PageHeader.Recno, page.PageTypeRowInternal)
	for i := 0; i+1 < len(newCells); i += 2 {
		parentPage.AppendKeyAddr(newCells[i].Data, binary.LittleEndian.Uint32(newCells[i+1].Data))
	}

	bt.c.UnpinPage(parentPageID, true)
	return nil
}
