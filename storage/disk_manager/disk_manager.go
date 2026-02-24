package disk_manager

import (
	"fmt"
	"os"
	"sync"

	"github.com/ish4n10/miniaturedb/common"
)

type DiskManager struct {
	file  *os.File
	mutex sync.Mutex
}

func InitDiskManager(path string) (*DiskManager, error) {
	file, error := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)

	if error != nil {
		return nil, error
	}

	dm := DiskManager{file: file}
	return &dm, error
}

func (dm *DiskManager) ReadPage(pageID uint32, buffer []byte) error {
	if len(buffer) > common.PageSize {
		return fmt.Errorf("buffer must be %f bytes", common.PageSize)
	}

	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	_, err := dm.file.Seek(GetPageOffset(pageID), 0)
	if err != nil {
		return err
	}

	bytes_read, err := dm.file.Read(buffer)
	if err != nil {
		return err
	}

	for i := bytes_read; i < common.PageSize; i++ {
		buffer[i] = 0
	}
	return nil
}

func (dm *DiskManager) WritePage(pageID uint32, buffer []byte) error {
	if len(buffer) > common.PageSize {
		return fmt.Errorf("buffer must be %f bytes", common.PageSize)
	}

	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	_, err := dm.file.Seek(GetPageOffset(pageID), 0)
	if err != nil {
		return err
	}

	_, err = dm.file.Write(buffer)

	return err
}

func (dm *DiskManager) CloseFile() error {
	return dm.file.Close()
}

func (dm *DiskManager) Flush() error {
	return dm.file.Sync()
}
