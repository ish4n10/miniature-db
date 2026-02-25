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

func (ph *PageHeader) WritePageHeader(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], ph.PageID)
	buf[4] = byte(ph.PageType)
	buf[5] = byte(ph.PageLevel)
	binary.LittleEndian.PutUint16(buf[6:8], ph.Flags)
	binary.LittleEndian.PutUint16(buf[8:10], ph.CellCount)
	binary.LittleEndian.PutUint16(buf[10:12], ph.FreeStart)
	binary.LittleEndian.PutUint16(buf[12:14], ph.FreeEnd)
	binary.LittleEndian.PutUint32(buf[14:18], ph.ParentPageID)
	binary.LittleEndian.PutUint64(buf[18:26], ph.LSN)
	copy(buf[26:32], ph.Reserved[:])
}

func ReadPageHeader(buf []byte) *PageHeader {
	return &PageHeader{
		PageID:       binary.LittleEndian.Uint32(buf[0:4]),
		PageType:     storage.PageTypeT(buf[4]),
		PageLevel:    storage.PageLevelT(buf[5]),
		Flags:        binary.LittleEndian.Uint16(buf[6:8]),
		CellCount:    binary.LittleEndian.Uint16(buf[8:10]),
		FreeStart:    binary.LittleEndian.Uint16(buf[10:12]),
		FreeEnd:      binary.LittleEndian.Uint16(buf[12:14]),
		ParentPageID: binary.LittleEndian.Uint32(buf[14:18]),
		LSN:          binary.LittleEndian.Uint64(buf[18:26]),
	}
}
