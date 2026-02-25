package diskmanager

import "encoding/binary"

func ComputeChecksum(buf []byte) uint32 {
	var checksum uint32
	// use len(buf)-3 to never go out of bounds
	for i := 0; i <= len(buf)-4; i += 4 {
		checksum ^= binary.LittleEndian.Uint32(buf[i : i+4])
	}
	return checksum
}
