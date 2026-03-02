package storage

import (
	"encoding/binary"
	"errors"

	cell "github.com/ish4n10/miniaturedb/storage/cell"
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

func (p *Page) nextFreeOffset() (int, error) {
	offset := constants.PageHeaderSize

	for offset < constants.PageSize {
		if p.Data[offset] == 0x00 {
			return offset, nil
		}

		_, newOffset, err := cell.Read(p.Data, offset)

		if err != nil {
			return offset, err
		}

		offset = newOffset
	}

	return offset, nil
}

func (p *Page) AppendKeyValue(key []byte, value []byte) error {
	keyCell := &cell.Cell{Type: cell.CellTypeKey, Data: key}
	valueCell := &cell.Cell{Type: cell.CellTypeValue, Data: value}

	neededSize := keyCell.EncodedSize() + valueCell.EncodedSize()

	offset, err := p.nextFreeOffset()
	if err != nil {
		return err
	}
	if offset+neededSize > constants.PageSize {
		return errors.New("page is full")
	}

	offset, err = cell.Write(p.Data, offset, keyCell)

	if err != nil {
		return err
	}

	offset, err = cell.Write(p.Data, offset, valueCell)

	if err != nil {
		return err
	}

	p.PageHeader.Entries += 2
	p.WriteHeaders()
	return nil
}

func (p *Page) AppendKeyAddr(key []byte, pageID uint32) error {

	valueData := make([]byte, 4)
	binary.LittleEndian.PutUint32(valueData, pageID)
	keyCell := &cell.Cell{Type: cell.CellTypeKey, Data: key}
	addrCell := &cell.Cell{Type: cell.CellTypeAddr, Data: valueData}

	neededSize := keyCell.EncodedSize() + addrCell.EncodedSize()

	offset, err := p.nextFreeOffset()
	if err != nil {
		return err
	}
	if offset+neededSize > constants.PageSize {
		return errors.New("page is full")
	}

	offset, err = cell.Write(p.Data, offset, keyCell)

	if err != nil {
		return err
	}

	offset, err = cell.Write(p.Data, offset, addrCell)

	if err != nil {
		return err
	}

	p.PageHeader.Entries += 2
	p.WriteHeaders()
	return nil
}

func (p *Page) AppendAddr(pageID uint32) error {
	pageIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(pageIDBytes, pageID)

	addrCell := &cell.Cell{Type: cell.CellTypeAddr, Data: pageIDBytes}

	offset, err := p.nextFreeOffset()
	if err != nil {
		return err
	}
	if offset+addrCell.EncodedSize() > constants.PageSize {
		return errors.New("page is full")
	}

	_, err = cell.Write(p.Data, offset, addrCell)
	if err != nil {
		return err
	}

	p.PageHeader.Entries++
	p.WriteHeaders()
	return nil
}

func (p *Page) AppendDeleted(key []byte) error {
	keyCell := &cell.Cell{Type: cell.CellTypeKey, Data: key}
	delCell := &cell.Cell{Type: cell.CellTypeDeleted, Data: []byte{}}

	needed := keyCell.EncodedSize() + delCell.EncodedSize()

	offset, err := p.nextFreeOffset()
	if err != nil {
		return err
	}

	if offset+needed > constants.PageSize {
		return errors.New("page is full")
	}

	offset, err = cell.Write(p.Data, offset, keyCell)
	if err != nil {
		return err
	}

	_, err = cell.Write(p.Data, offset, delCell)
	if err != nil {
		return err
	}

	p.PageHeader.Entries += 2
	p.WriteHeaders()
	return nil
}

func (p *Page) ReadCells() ([]*cell.Cell, error) {
	return cell.ReadAll(p.Data, constants.PageHeaderSize)
}
