package cache

import (
	"sync"

	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
	page "github.com/ish4n10/miniaturedb/storage/page"
	ref "github.com/ish4n10/miniaturedb/storage/ref"
)

const (
	evictTarget  = 0.80
	evictTrigger = 0.95
)

type Cache struct {
	refs       []*ref.Ref
	maxPages   int
	byteInMem  uint64
	bytesDirty uint64
	dm         *diskmanager.DiskManager
	mu         sync.Mutex
}

func (c *Cache) usagePercentage() float64 {
	used := 0
	for _, r := range c.refs {
		if r.State != ref.RefStateDisk {
			used++
		}
	}
	return float64(used) / float64(c.maxPages)
}

func NewCache(dm *diskmanager.DiskManager, maxPages int) *Cache {
	refs := make([]*ref.Ref, maxPages)

	for i := range refs {
		refs[i] = &ref.Ref{
			State:    ref.RefStateDisk,
			PageID:   0,
			Page:     nil,
			PinCount: 0,
			LastUsed: 0,
		}
	}

	return &Cache{refs: refs, maxPages: maxPages, byteInMem: 0, bytesDirty: 0, dm: dm}
}

func (c *Cache) evict() (*ref.Ref, error) {

}
func FetchPage(pageID uint32) (*page.Page, error) {

}
