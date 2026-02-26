package cell

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

	if n < 0xff {
		return 1 + 1 + n
	}
	return 1 + 4 + n
}

// func
