package cache

import (
	"errors"
	"fmt"
	"sync"
	"time"

	constants "github.com/ish4n10/miniaturedb/storage/common"
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
	bytesInMem uint64
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

	return &Cache{refs: refs, maxPages: maxPages, bytesInMem: 0, bytesDirty: 0, dm: dm}
}

func (c *Cache) evict() (*ref.Ref, error) {
	var evictableRef []*ref.Ref

	for i := range c.refs {
		currentRef := c.refs[i]
		if currentRef.State != ref.RefStateDisk && currentRef.PinCount == 0 {
			evictableRef = append(evictableRef, currentRef)
		}
	}

	if len(evictableRef) == 0 {
		return nil, errors.New("cache full: all pages are pinned")
	}

	oldest := evictableRef[0]
	for i := range evictableRef {
		currentRef := evictableRef[i]
		if currentRef.LastUsed < oldest.LastUsed {
			oldest = currentRef
		}
	}

	if oldest.State == ref.RefStateDirty {
		err := c.dm.WritePage(oldest.PageID, oldest.Page.Data)

		if err != nil {
			return nil, fmt.Errorf("could not write the dirty page to memory PageID %d", oldest.PageID)
		}
		c.bytesDirty -= constants.PageSize
	}

	oldest.Page = nil
	oldest.PageID = 0
	oldest.State = ref.RefStateDisk
	oldest.PinCount = 0
	oldest.LastUsed = 0
	c.bytesInMem -= constants.PageSize

	return oldest, nil
}

func (c *Cache) FetchPage(pageID uint32) (*page.Page, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.refs {
		r := c.refs[i]
		if r.State != ref.RefStateDisk && r.PageID == pageID {
			r.PinCount++
			r.LastUsed = time.Now().UnixNano()
			return r.Page, nil
		}
	}

	var slot *ref.Ref
	for i := range c.refs {
		if c.refs[i].State == ref.RefStateDisk {
			slot = c.refs[i]
			break
		}
	}

	if slot == nil {
		var err error
		slot, err = c.evict()
		if err != nil {
			return nil, err
		}
	}

	slot.State = ref.RefStateLocked
	slot.PageID = pageID

	p := page.NewPage()
	err := c.dm.ReadPage(pageID, p.Data)
	if err != nil {
		slot.State = ref.RefStateDisk
		slot.PageID = 0
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	p.ReadHeaders()
	slot.Page = p
	slot.State = ref.RefStateMem
	slot.PinCount = 1
	slot.LastUsed = time.Now().UnixNano()
	c.bytesInMem += constants.PageSize

	return slot.Page, nil
}
