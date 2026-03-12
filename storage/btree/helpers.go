package btree

import (
	"encoding/binary"
	"strings"

	cell "github.com/ish4n10/miniaturedb/storage/cell"
	page "github.com/ish4n10/miniaturedb/storage/page"
)

func splitCells(cells []*cell.Cell) (left []*cell.Cell, right []*cell.Cell, rightMinKey []byte) {
	// compact tombstones out before splitting
	live := make([]*cell.Cell, 0, len(cells))
	for i := 0; i+1 < len(cells); i += 2 {
		if cells[i+1].Type != cell.CellTypeDeleted {
			live = append(live, cells[i], cells[i+1])
		}
	}
	cells = live

	n := len(cells)
	if n < 4 {
		return nil, nil, nil
	}

	mid := (n / 4) * 2
	if mid < 2 {
		mid = 2
	}
	if mid >= n {
		mid = n - 2
	}

	left = cells[:mid]
	right = cells[mid:]

	rightMinKey = make([]byte, len(right[0].Data))
	copy(rightMinKey, right[0].Data)
	return
}

func (bt *Btree) writeLeafPage(p *page.Page, pageID uint32, cells []*cell.Cell) error {
	recno := p.PageHeader.Recno
	clear(p.Data)
	page.InitPage(p, recno, page.PageTypeRowLeaf)

	for i := 0; i+1 < len(cells); i += 2 {
		if err := p.AppendKeyValue(cells[i].Data, cells[i+1].Data); err != nil {
			return err
		}
	}

	return bt.dm.WritePage(pageID, p.Data)
}

func (bt *Btree) createNewLeafPage(cells []*cell.Cell) (uint32, error) {
	newPageID := bt.dm.AllocatePage()
	p := page.NewPage()
	page.InitPage(p, 0, page.PageTypeRowLeaf)

	if err := bt.writeLeafPage(p, newPageID, cells); err != nil {
		return 0, err
	}
	return newPageID, nil
}

func (bt *Btree) createNewRoot(leftPageID uint32, rightSeparator []byte, rightPageID uint32) error {
	rootPageID := bt.dm.AllocatePage()
	root := page.NewPage()
	page.InitPage(root, 0, page.PageTypeRowInternal)

	if err := root.AppendAddr(leftPageID); err != nil {
		return err
	}
	if err := root.AppendKeyAddr(rightSeparator, rightPageID); err != nil {
		return err
	}
	if err := bt.dm.WritePage(rootPageID, root.Data); err != nil {
		return err
	}

	bt.rootPageID = rootPageID
	return nil
}

func (bt *Btree) insertIntoParent(parentPageID uint32, separator []byte, rightPageID uint32, path []uint32) error {
	parent, err := bt.c.FetchPage(parentPageID)
	if err != nil {
		return err
	}

	cells, err := parent.ReadCells()
	if err != nil {
		bt.c.UnpinPage(parentPageID, false)
		return err
	}

	pageIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(pageIDBytes, rightPageID)

	newKey := &cell.Cell{Type: cell.CellTypeKey, Data: separator}
	newAddr := &cell.Cell{Type: cell.CellTypeAddr, Data: pageIDBytes}

	insertAt := len(cells)
	for i := 1; i < len(cells); i += 2 {
		if bt.compare(separator, cells[i].Data) < 0 {
			insertAt = i
			break
		}
	}

	newCells := make([]*cell.Cell, 0, len(cells)+2)
	newCells = append(newCells, cells[:insertAt]...)
	newCells = append(newCells, newKey, newAddr)
	newCells = append(newCells, cells[insertAt:]...)

	// try writing updated internal page
	clear(parent.Data)
	page.InitPage(parent, parent.PageHeader.Recno, page.PageTypeRowInternal)

	if err := parent.AppendAddr(binary.LittleEndian.Uint32(newCells[0].Data)); err != nil {
		bt.c.UnpinPage(parentPageID, false)
		return err
	}
	fitErr := error(nil)
	for i := 1; i+1 < len(newCells); i += 2 {
		if err := parent.AppendKeyAddr(newCells[i].Data, binary.LittleEndian.Uint32(newCells[i+1].Data)); err != nil {
			fitErr = err
			break
		}
	}

	if fitErr == nil {
		bt.c.UnpinPage(parentPageID, true)
		return nil
	}

	// internal page full then split internal page
	bt.c.UnpinPage(parentPageID, false)

	// split newCells into left and right halves
	// newCells layout: [addr][key][addr][key][addr]...
	// count pairs: (len-1)/2 pairs of [key][addr] plus one leading addr
	// split at midpoint of [key][addr] pairs
	pairCount := (len(newCells) - 1) / 2
	midPair := pairCount / 2

	// left gets: [addr][key][addr]...[key][addr] (midPair pairs)
	// promote: newCells[midPair*2+1] (the middle key)
	// right gets: [addr][key][addr]... starting after promoted key

	splitIdx := 1 + midPair*2 // index of promoted key in newCells
	promotedKey := make([]byte, len(newCells[splitIdx].Data))
	copy(promotedKey, newCells[splitIdx].Data)

	leftCells := newCells[:splitIdx]    // [addr][key][addr]...[key][addr]
	rightCells := newCells[splitIdx+1:] // starts with [addr][key][addr]...

	// rewrite left into parentPageID
	lp, err := bt.c.FetchPage(parentPageID)
	if err != nil {
		return err
	}
	clear(lp.Data)
	page.InitPage(lp, lp.PageHeader.Recno, page.PageTypeRowInternal)
	lp.AppendAddr(binary.LittleEndian.Uint32(leftCells[0].Data))
	for i := 1; i+1 < len(leftCells); i += 2 {
		lp.AppendKeyAddr(leftCells[i].Data, binary.LittleEndian.Uint32(leftCells[i+1].Data))
	}
	bt.dm.WritePage(parentPageID, lp.Data)
	bt.c.UnpinPage(parentPageID, true)

	// create new internal page for right
	newInternalID := bt.dm.AllocatePage()
	rp := page.NewPage()
	page.InitPage(rp, 0, page.PageTypeRowInternal)
	rp.AppendAddr(binary.LittleEndian.Uint32(rightCells[0].Data))
	for i := 1; i+1 < len(rightCells); i += 2 {
		rp.AppendKeyAddr(rightCells[i].Data, binary.LittleEndian.Uint32(rightCells[i+1].Data))
	}
	bt.dm.WritePage(newInternalID, rp.Data)

	// promote to grandparent
	if len(path) == 0 {
		return bt.createNewRoot(parentPageID, promotedKey, newInternalID)
	}
	return bt.insertIntoParent(path[len(path)-1], promotedKey, newInternalID, path[:len(path)-1])
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

	t.Logf("%sINTERNAL(page=%d)", indent, pageID)
	if len(cells) > 0 {
		leftmostID := binary.LittleEndian.Uint32(cells[0].Data)
		t.Logf("%s  [leftmost] → page=%d", indent, leftmostID)
		bt.printPage(t, leftmostID, depth+1)
	}
	for i := 1; i+1 < len(cells); i += 2 {
		childID := binary.LittleEndian.Uint32(cells[i+1].Data)
		t.Logf("%s  [>=%s] → page=%d", indent, cells[i].Data, childID)
		bt.printPage(t, childID, depth+1)
	}
}

func (bt *Btree) Stats() (height int, leafPages int, internalPages int) {
	queue := []uint32{bt.rootPageID}
	visited := map[uint32]bool{}

	for len(queue) > 0 {
		pageID := queue[0]
		queue = queue[1:]
		if visited[pageID] {
			continue
		}
		visited[pageID] = true

		p, err := bt.c.FetchPage(pageID)
		if err != nil {
			continue
		}

		pageType := p.PageHeader.Type
		cells, _ := p.ReadCells()
		bt.c.UnpinPage(pageID, false)

		if pageType == page.PageTypeRowLeaf {
			leafPages++
		} else {
			internalPages++
			if len(cells) > 0 {
				queue = append(queue, binary.LittleEndian.Uint32(cells[0].Data))
			}
			for i := 2; i < len(cells); i += 2 {
				queue = append(queue, binary.LittleEndian.Uint32(cells[i].Data))
			}
		}
	}

	height = 1
	currentID := bt.rootPageID
	for {
		p, _ := bt.c.FetchPage(currentID)
		t := p.PageHeader.Type
		cells, _ := p.ReadCells()
		bt.c.UnpinPage(currentID, false)
		if t == page.PageTypeRowLeaf {
			break
		}
		currentID = binary.LittleEndian.Uint32(cells[0].Data)
		height++
	}
	return
}
