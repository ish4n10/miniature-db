package ref

import (
	page "github.com/ish4n10/miniaturedb/storage/page"
)

type RefState uint8

const (
	RefStateDisk   RefState = 0
	RefStateMem    RefState = 1
	RefStateDirty  RefState = 2
	RefStateLocked RefState = 3
)

type Ref struct {
	Page     *page.Page
	PageID   uint32
	State    RefState
	PinCount int
	LastUsed int
}
