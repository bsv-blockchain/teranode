package miner

import (
	"encoding/binary"
)

// BuildBlockHeader builds the block header byte array from the specific fields in the header.
func BuildBlockHeader(version uint32, previousBlockHash []byte, merkleRoot []byte, time []byte, bits []byte, nonce []byte) []byte {
	v := make([]byte, 4)
	binary.LittleEndian.PutUint32(v, version)

	a := []byte{}
	a = append(a, v...)
	a = append(a, previousBlockHash...)
	a = append(a, merkleRoot...)
	a = append(a, time...)
	a = append(a, bits...)
	a = append(a, nonce...)
	return a
}
