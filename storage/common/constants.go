package common

const (
	PageSize       = 4096
	PageHeaderSize = 40

	MagicNumber uint32 = 0xdeadbeef
	Version     uint8  = 1
)

type PageTypeT uint8
