package btree

import (
	"errors"

	cell "github.com/ish4n10/miniaturedb/storage/cell"
	page "github.com/ish4n10/miniaturedb/storage/page"
)

type Cursor struct {
	bt     *Btree
	pageID uint32
	index  int
	cells  []*cell.Cell
	valid  bool
}

func (bt *Btree) SeekTo(key []byte) (*Cursor, error) {
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

		if pageType == page.PageTypeRowLeaf {
			cur := &Cursor{bt: bt, pageID: currentPageID, cells: cells, valid: false}
			for i := 0; i+1 < len(cells); i += 2 {
				if bt.compare(cells[i].Data, key) >= 0 && cells[i+1].Type != cell.CellTypeDeleted {
					cur.index = i
					cur.valid = true
					return cur, nil
				}
			}
			if err := cur.advanceToNextLeaf(); err != nil {
				return cur, nil
			}
			return cur, nil
		}

		childPageID := uint32(0)
		if len(cells) > 0 {
			childPageID = btUint32(cells[0].Data)
		}
		for i := 1; i+1 < len(cells); i += 2 {
			if bt.compare(key, cells[i].Data) >= 0 {
				childPageID = btUint32(cells[i+1].Data)
			}
		}
		currentPageID = childPageID
	}
}

func (c *Cursor) Key() []byte {
	if !c.valid {
		return nil
	}
	out := make([]byte, len(c.cells[c.index].Data))
	copy(out, c.cells[c.index].Data)
	return out
}

func (c *Cursor) Value() []byte {
	if !c.valid {
		return nil
	}
	out := make([]byte, len(c.cells[c.index+1].Data))
	copy(out, c.cells[c.index+1].Data)
	return out
}

func (c *Cursor) Next() error {
	if !c.valid {
		return errors.New("cursor exhausted")
	}

	for i := c.index + 2; i+1 < len(c.cells); i += 2 {
		if c.cells[i+1].Type != cell.CellTypeDeleted {
			c.index = i
			return nil
		}
	}

	return c.advanceToNextLeaf()
}

func (c *Cursor) advanceToNextLeaf() error {
	for {
		p, err := c.bt.c.FetchPage(c.pageID)
		if err != nil {
			c.valid = false
			return err
		}
		nextPageID := p.PageHeader.NextPageID

		c.bt.c.UnpinPage(c.pageID, false)

		if nextPageID == 0 {
			c.valid = false
			return nil
		}

		np, err := c.bt.c.FetchPage(nextPageID)
		if err != nil {
			c.valid = false
			return err
		}
		cells, err := np.ReadCells()
		c.bt.c.UnpinPage(nextPageID, false)
		if err != nil {
			c.valid = false
			return err
		}

		c.pageID = nextPageID
		c.cells = cells

		for i := 0; i+1 < len(cells); i += 2 {
			if cells[i+1].Type != cell.CellTypeDeleted {
				c.index = i
				c.valid = true
				return nil
			}
		}
	}
}

func btUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func (c *Cursor) Valid() bool {
	return c.valid
}
