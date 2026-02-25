package storage

import (
	storage "github.com/ish4n10/miniaturedb/storage/common"
)

type Page struct {
	Data []byte
}

func NewPage() *Page {
	return &Page{Data: make([]byte, storage.PageSize)}
}

func (p *Page) GetHeader() *PageHeader {
	return ReadPageHeader(p.Data)
}

func (p *Page) WriteHeader(ph *PageHeader) {
	ph.WritePageHeader(p.Data)
}
