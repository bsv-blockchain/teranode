package packedsql

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

const (
	slotHashSize  = 32
	slotSpendSize = 36

	blockRefSize = 12

	flagCoinbase     = int16(1)
	flagFrozen       = int16(2)
	flagConflicting  = int16(4)
	flagLocked       = int16(8)
	flagHasOverrides = int16(16)

	guardFlagsMask = flagFrozen | flagConflicting | flagLocked | flagHasOverrides
)

func packSpendingData(sd *spend.SpendingData) []byte {
	b := make([]byte, slotSpendSize)
	if sd == nil || sd.TxID == nil {
		return b
	}

	copy(b, sd.TxID[:])
	binary.LittleEndian.PutUint32(b[slotHashSize:], uint32(sd.Vin)) //nolint:gosec

	return b
}

func unpackSpendingData(b []byte) *spend.SpendingData {
	if len(b) < slotSpendSize {
		return nil
	}

	allZero := true

	for _, c := range b[:slotSpendSize] {
		if c != 0 {
			allZero = false
			break
		}
	}

	if allZero {
		return nil
	}

	txid, err := chainhash.NewHash(b[:slotHashSize])
	if err != nil {
		return nil
	}

	return &spend.SpendingData{
		TxID: txid,
		Vin:  int(binary.LittleEndian.Uint32(b[slotHashSize:slotSpendSize])),
	}
}

func packOffsetBlob(items [][]byte) []byte {
	n := len(items)
	headerLen := 4 + 4*(n+1)
	total := headerLen

	for _, item := range items {
		total += len(item)
	}

	blob := make([]byte, headerLen, total)
	binary.LittleEndian.PutUint32(blob, uint32(n)) //nolint:gosec

	offset := uint32(headerLen) //nolint:gosec

	for i, item := range items {
		binary.LittleEndian.PutUint32(blob[4+4*i:], offset)
		offset += uint32(len(item)) //nolint:gosec
		blob = append(blob, item...)
	}

	binary.LittleEndian.PutUint32(blob[4+4*n:], offset)

	return blob
}

func offsetBlobCount(blob []byte) int {
	if len(blob) < 4 {
		return 0
	}

	return int(binary.LittleEndian.Uint32(blob))
}

func offsetBlobRange(blob []byte, i int) (uint32, uint32, error) {
	n := offsetBlobCount(blob)
	if i < 0 || i >= n {
		return 0, 0, errors.NewProcessingError("offset blob index %d out of range (count %d)", i, n)
	}

	start := binary.LittleEndian.Uint32(blob[4+4*i:])
	end := binary.LittleEndian.Uint32(blob[4+4*(i+1):])

	if end < start || int(end) > len(blob) {
		return 0, 0, errors.NewProcessingError("offset blob corrupt at index %d", i)
	}

	return start + 1, end - start, nil
}

func offsetBlobItem(blob []byte, i int) ([]byte, error) {
	start, length, err := offsetBlobRange(blob, i)
	if err != nil {
		return nil, err
	}

	return blob[start-1 : start-1+length], nil
}

func packBlockRefs(infos []utxo.MinedBlockInfo) []byte {
	b := make([]byte, 0, len(infos)*blockRefSize)

	for _, info := range infos {
		b = appendBlockRef(b, info)
	}

	return b
}

func unpackBlockRefs(b []byte) ([]uint32, []uint32, []int) {
	n := len(b) / blockRefSize
	ids := make([]uint32, 0, n)
	heights := make([]uint32, 0, n)
	subtreeIdxs := make([]int, 0, n)

	for i := 0; i < n; i++ {
		off := i * blockRefSize
		ids = append(ids, binary.LittleEndian.Uint32(b[off:]))
		heights = append(heights, binary.LittleEndian.Uint32(b[off+4:]))
		subtreeIdxs = append(subtreeIdxs, int(binary.LittleEndian.Uint32(b[off+8:])))
	}

	return ids, heights, subtreeIdxs
}

func appendBlockRef(b []byte, info utxo.MinedBlockInfo) []byte {
	for i := 0; i+blockRefSize <= len(b); i += blockRefSize {
		if binary.LittleEndian.Uint32(b[i:]) == info.BlockID {
			return b
		}
	}

	ref := make([]byte, blockRefSize)
	binary.LittleEndian.PutUint32(ref, info.BlockID)
	binary.LittleEndian.PutUint32(ref[4:], info.BlockHeight)
	binary.LittleEndian.PutUint32(ref[8:], uint32(info.SubtreeIdx)) //nolint:gosec

	return append(b, ref...)
}

func removeBlockRef(b []byte, blockID uint32) []byte {
	out := make([]byte, 0, len(b))

	for i := 0; i+blockRefSize <= len(b); i += blockRefSize {
		if binary.LittleEndian.Uint32(b[i:]) != blockID {
			out = append(out, b[i:i+blockRefSize]...)
		}
	}

	return out
}

func pageOfVout(vout, pageSize uint32) uint32 {
	return vout / pageSize
}

func slotOfVout(vout, pageSize uint32) uint32 {
	return vout % pageSize
}
