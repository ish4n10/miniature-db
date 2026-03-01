package btree

import (
	cache "github.com/ish4n10/miniaturedb/storage/cache"
	diskmanager "github.com/ish4n10/miniaturedb/storage/disk_manager"
)

type Btree struct {
	RootPageID uint32
	c          *cache.Cache
	dm         *diskmanager.DiskManager
}
