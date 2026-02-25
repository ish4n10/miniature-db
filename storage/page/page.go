package storage

import (
	constants "github.com/ish4n10/miniaturedb/storage/common"
)

type Page struct {
	PageHeader  *PageHeader
	BlockHeader *BlockHeader
	Data        []byte
}

func NewPage() *Page {
	return &Page{
		PageHeader:  &PageHeader{},
		BlockHeader: &BlockHeader{},
		Data:        make([]byte, constants.PageSize),
	}
}

func (p *Page) WriteHeaders() {
	p.PageHeader.Write(p.Data)
	p.BlockHeader.Write(p.Data)
}

func (p *Page) ReadHeaders() {
	p.PageHeader = ReadPageHeader(p.Data)
	p.BlockHeader = ReadBlockHeader(p.Data)
}

func InitPage(p *Page, recno uint64, pageType constants.PageTypeT) {
	clear(p.Data)

	p.PageHeader = &PageHeader{
		Recno:    recno,
		WriteGen: 0,
		MemSize:  uint32(constants.PageSize),
		Entries:  0,
		Type:     pageType,
		Flags:    0,
		Version:  1,
	}

	p.BlockHeader = &BlockHeader{
		DiskSize: uint32(constants.PageSize),
		Checksum: 0,
		Flags:    0,
	}

	p.WriteHeaders()
}
