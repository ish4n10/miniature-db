package storage

import (
	"encoding/binary"

	storage "github.com/ish4n10/miniaturedb/storage/common"
)

const (
	PageTypeMeta storage.PageTypeT = iota
	PageTypeData
	PageTypeIndex
)

const (
	PageLevelNone storage.PageLevelT = iota
	PageLevelLeaf
	PageLevelInternal
)

type PageHeader struct {
	PageID       uint32
	PageType     storage.PageTypeT
	PageLevel    storage.PageLevelT
	Flags        uint16
	CellCount    uint16
	FreeStart    uint16
	FreeEnd      uint16
	ParentPageID uint32
	LSN          uint64
	Reserved     [4]byte
}

func (h *PageHeader) WritePageHeader(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], h.PageID)

}
