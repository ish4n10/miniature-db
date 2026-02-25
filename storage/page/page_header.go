package storage

import (
	"encoding/binary"

	constants "github.com/ish4n10/miniaturedb/storage/common"
)

const (
	PageTypeRowInternal constants.PageTypeT = 0x01
	PageTypeRowLeaf     constants.PageTypeT = 0x02
	PageTypeOverflow    constants.PageTypeT = 0x03
	PageTypeMeta        constants.PageTypeT = 0x04
)

type PageHeader struct {
	Recno    uint64
	WriteGen uint64
	MemSize  uint32
	Entries  uint32
	Type     constants.PageTypeT
	Flags    uint8
	Unused   uint8
	Version  uint8
}

type BlockHeader struct {
	DiskSize uint32
	Checksum uint32
	Flags    uint8
	Unused   [3]byte
}

func (ph *PageHeader) Write(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], ph.Recno)
	binary.LittleEndian.PutUint64(buf[8:16], ph.WriteGen)
	binary.LittleEndian.PutUint32(buf[16:20], ph.MemSize)
	binary.LittleEndian.PutUint32(buf[20:24], ph.Entries)
	buf[24] = byte(ph.Type)
	buf[25] = ph.Flags
	buf[26] = 0
	buf[27] = ph.Version
}

func ReadPageHeader(buf []byte) *PageHeader {
	return &PageHeader{
		Recno:    binary.LittleEndian.Uint64(buf[0:8]),
		WriteGen: binary.LittleEndian.Uint64(buf[8:16]),
		MemSize:  binary.LittleEndian.Uint32(buf[16:20]),
		Entries:  binary.LittleEndian.Uint32(buf[20:24]),
		Type:     constants.PageTypeT(buf[24]),
		Flags:    buf[25],
		Version:  buf[27],
	}
}

func (bh *BlockHeader) Write(buf []byte) {
	binary.LittleEndian.PutUint32(buf[28:32], bh.DiskSize)
	binary.LittleEndian.PutUint32(buf[32:36], bh.Checksum)
	buf[36] = bh.Flags
	buf[37] = 0
	buf[38] = 0
	buf[39] = 0
}

func ReadBlockHeader(buf []byte) *BlockHeader {
	return &BlockHeader{
		DiskSize: binary.LittleEndian.Uint32(buf[28:32]),
		Checksum: binary.LittleEndian.Uint32(buf[32:36]),
		Flags:    buf[36],
	}
}
