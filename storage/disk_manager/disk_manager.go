package diskmanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	constants "github.com/ish4n10/miniaturedb/storage/common"
)

type DiskManager struct {
	file     *os.File
	mutex    sync.Mutex
	nextPage uint32
}

func InitDiskManager(path string) (*DiskManager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	dm := &DiskManager{file: file}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		err = dm.writeDescriptor()
		if err != nil {
			file.Close()
			return nil, err
		}
		dm.nextPage = 1
	} else {
		err = dm.verifyDescriptor()
		if err != nil {
			file.Close()
			return nil, err
		}
		dm.nextPage = uint32(info.Size()) / uint32(constants.PageSize)
	}

	return dm, nil
}

func (dm *DiskManager) writeDescriptor() error {
	buf := make([]byte, constants.PageSize)
	WriteDescriptorBlock(buf)

	_, err := dm.file.WriteAt(buf, 0)
	return err
}

func (dm *DiskManager) verifyDescriptor() error {
	buf := make([]byte, constants.PageSize)
	_, err := dm.file.ReadAt(buf, 0)
	if err != nil {
		return err
	}
	return ReadAndVerifyDescriptorBlock(buf)
}

func (dm *DiskManager) AllocatePage() uint32 {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()
	id := dm.nextPage
	dm.nextPage++
	return id
}

func (dm *DiskManager) ReadPage(pageID uint32, buffer []byte) error {
	if len(buffer) != constants.PageSize {
		return fmt.Errorf("buffer must be %d bytes", constants.PageSize)
	}
	if pageID == 0 {
		return errors.New("page 0 is reserved for descriptor block")
	}

	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	offset := int64(pageID) * int64(constants.PageSize)
	_, err := dm.file.ReadAt(buffer, offset)
	if err != nil {
		return err
	}

	stored := binary.LittleEndian.Uint32(buffer[32:36])
	buffer[32] = 0
	buffer[33] = 0
	buffer[34] = 0
	buffer[35] = 0
	computed := ComputeChecksum(buffer)
	binary.LittleEndian.PutUint32(buffer[32:36], stored)

	if stored != computed {
		return fmt.Errorf("page %d checksum mismatch: file may be corrupt", pageID)
	}

	return nil
}

func (dm *DiskManager) WritePage(pageID uint32, buffer []byte) error {
	if len(buffer) != constants.PageSize {
		return fmt.Errorf("buffer must be %d bytes", constants.PageSize)
	}
	if pageID == 0 {
		return errors.New("page 0 is reserved for descriptor block")
	}

	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	buffer[32] = 0
	buffer[33] = 0
	buffer[34] = 0
	buffer[35] = 0
	checksum := ComputeChecksum(buffer)
	binary.LittleEndian.PutUint32(buffer[32:36], checksum)

	offset := int64(pageID) * int64(constants.PageSize)
	_, err := dm.file.WriteAt(buffer, offset)
	return err
}

func (dm *DiskManager) Flush() error {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()
	return dm.file.Sync()
}

func (dm *DiskManager) CloseFile() error {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()
	return dm.file.Close()
}
