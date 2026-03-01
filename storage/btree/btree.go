package btree

import (
	"fmt"

	cache "github.com/ish4n10/miniaturedb/storage/cache"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
	page "github.com/ish4n10/miniaturedb/storage/page"
)

type Btree struct {
	rootPageID uint32
	c          *cache.Cache
	dm         *diskmanager.DiskManager
}

func NewBtree(c *cache.Cache, dm *diskmanager.DiskManager) (*Btree, error) {
	pageID := dm.AllocatePage()

	p := page.NewPage()

	page.InitPage(p, 0, page.PageTypeRowLeaf)

	err := dm.WritePage(pageID, p.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to write root page: %w", err)
	}

	return &Btree{rootPageID: pageID, c: c, dm: dm}, nil
}

// func (bt *Btree) Search(key []byte) ([]byte, error) {
// 	p, err := bt.c.FetchPage(bt.rootPageID)
// 	defer bt.c.UnpinPage(bt.rootPageID, false)
// 	if err != nil {
// 		return nil, errors.New("Could not fetch root page")
// 	}

// 	for {
// 		cells, err := cell.ReadAll(p.Data, 0)
// 		if err != nil {
// 			return nil, fmt.Errorf("Error in fetching all cells")
// 		}

// 	}
// }
