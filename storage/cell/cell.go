package cell

import (
	"encoding/binary"
	"errors"
)

type CellType uint8

const (
	CellTypeKey     CellType = 0
	CellTypeValue   CellType = 1
	CellTypeDeleted CellType = 2
	CellTypeAddr    CellType = 3
)

const shortFlag = uint8(0x08)

type Cell struct {
	Type CellType
	Data []byte
}

func (c *Cell) EncodedSize() int {
	n := len(c.Data)

	if n <= 0xff {
		return 1 + 1 + n
	}
	return 1 + 4 + n
}

func Write(buf []byte, offset int, c *Cell) (int, error) {

	dataLength := len(c.Data)

	if offset+c.EncodedSize() > len(buf) {
		return offset, errors.New("no space for buffer")
	}

	if dataLength <= 0xff {
		buf[offset] = (uint8(c.Type << 4)) | shortFlag

		offset++
		buf[offset] = uint8(dataLength)
		offset++
	} else {
		buf[offset] = uint8(c.Type << 4)
		offset++

		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(dataLength))
		offset += 4
	}

	copy(buf[offset:], c.Data)

	return offset + dataLength, nil
}

func Read(buf []byte, offset int) (*Cell, int, error) {

	if offset >= len(buf) {
		return nil, offset, errors.New("out of bound read")
	}

	descriptor := buf[offset]
	offset++

	cellType := CellType(descriptor >> 4)

	isShort := (descriptor & shortFlag) != 0

	var dataLength int

	if isShort {
		if offset > len(buf) {
			return nil, offset, errors.New("buffer size small while reading short")
		}

		dataLength = int(buf[offset])
		offset++
	} else {
		if offset+4 > len(buf) {
			return nil, offset, errors.New("buffer size small while reading")
		}
		dataLength = int(binary.LittleEndian.Uint32(buf[offset : offset+4]))
		offset += 4
	}
	if offset+dataLength > len(buf) {
		return nil, offset, errors.New("buffer too small for cell data")
	}

	data := make([]byte, dataLength)

	copy(data, buf[offset:offset+dataLength])

	return &Cell{Type: cellType, Data: data}, offset + dataLength, nil
}

func ReadAll(buf []byte, startOffset int) ([]*Cell, error) {
	var cells []*Cell
	offset := startOffset

	for offset < len(buf) {

		if buf[offset] == 0 {
			break
		}

		c, newOffset, err := Read(buf, offset)

		if err != nil {
			return cells, err
		}

		cells = append(cells, c)
		offset = newOffset
	}

	return cells, nil
}
