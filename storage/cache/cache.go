package cache

import (
	"sync"

	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
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
