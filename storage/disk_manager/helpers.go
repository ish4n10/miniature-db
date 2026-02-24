package disk_manager

import (
	"github.com/ish4n10/miniaturedb/common"
)

func GetPageOffset(pageID uint32) int64 {
	return int64(pageID) * int64(common.PageSize)
}
