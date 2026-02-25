package diskmanager

import (
	"encoding/binary"
	"errors"
	"fmt"

	constants "github.com/ish4n10/miniaturedb/storage/common"
)

type DescriptorBlock struct {
	Magic    uint32
	Version  uint8
	Checksum uint32
}

func WriteDescriptorBlock(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], constants.MagicNumber)
	buf[4] = constants.Version

	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	buf[8] = 0

	checksum := ComputeChecksum(buf[:constants.PageSize])
	binary.LittleEndian.PutUint32(buf[5:9], checksum)
}

func ReadAndVerifyDescriptorBlock(buf []byte) error {

	magic := binary.LittleEndian.Uint32(buf[0:4])
	fmt.Println("The magic numbers are ", magic, constants.MagicNumber)
	if magic != constants.MagicNumber {
		return errors.New("invalid file: magic number mismatch")
	}

	version := buf[4]
	if version != constants.Version {
		return errors.New("incompatible file version")
	}

	storedChecksum := binary.LittleEndian.Uint32(buf[5:9])

	buf[5] = 0
	buf[6] = 0
	buf[7] = 0
	buf[8] = 0

	computed := ComputeChecksum(buf[:constants.PageSize])

	binary.LittleEndian.PutUint32(buf[5:9], storedChecksum)

	if storedChecksum != computed {
		return errors.New("descriptor block checksum mismatch: file may be corrupt")
	}

	return nil
}
