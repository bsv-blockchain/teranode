// Package pebble is a SPIKE implementation of the utxo.Store interface over an
// embedded Pebble LSM. The slot/blob codec is duplicated from stores/utxo/packedsql
// while both implementations are in flight; extraction into a shared package is a
// promotion-time task, not a spike task.
package pebble

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

	flagCoinbase     = uint16(1)
	flagFrozen       = uint16(2)
	flagConflicting  = uint16(4)
	flagLocked       = uint16(8)
	flagHasOverrides = uint16(16)
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

func offsetBlobItem(blob []byte, i int) ([]byte, error) {
	n := offsetBlobCount(blob)
	if i < 0 || i >= n {
		return nil, errors.NewProcessingError("pebble: offset blob index %d out of range (count %d)", i, n)
	}

	start := binary.LittleEndian.Uint32(blob[4+4*i:])
	end := binary.LittleEndian.Uint32(blob[4+4*(i+1):])

	if end < start || int(end) > len(blob) {
		return nil, errors.NewProcessingError("pebble: offset blob corrupt at index %d", i)
	}

	return blob[start:end], nil
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

type masterRecord struct {
	flags                  uint16
	coinbaseSpendingHeight uint32
	outputCount            uint32
	totalCount             uint32
	page0Count             uint32
	spentCount             uint32
	pagesTotal             uint32
	pagesSpent             uint32
	version                uint32
	lockTime               uint32
	fee                    uint64
	sizeInBytes            uint64
	createdAt              int64
	deleteAtHeight         int64
	unminedSince           int64
	preserveUntil          int64
	blockRefs              []byte
	spends                 []byte
}

const masterFixedLen = 2 + 4*9 + 8*2 + 8*4 + 2

func encodeMaster(m *masterRecord) []byte {
	b := make([]byte, masterFixedLen, masterFixedLen+len(m.blockRefs)+len(m.spends))

	binary.LittleEndian.PutUint16(b[0:], m.flags)
	binary.LittleEndian.PutUint32(b[2:], m.coinbaseSpendingHeight)
	binary.LittleEndian.PutUint32(b[6:], m.outputCount)
	binary.LittleEndian.PutUint32(b[10:], m.totalCount)
	binary.LittleEndian.PutUint32(b[14:], m.spentCount)
	binary.LittleEndian.PutUint32(b[18:], m.page0Count)
	binary.LittleEndian.PutUint32(b[22:], m.pagesTotal)
	binary.LittleEndian.PutUint32(b[26:], m.pagesSpent)
	binary.LittleEndian.PutUint32(b[30:], m.version)
	binary.LittleEndian.PutUint32(b[34:], m.lockTime)
	binary.LittleEndian.PutUint64(b[38:], m.fee)
	binary.LittleEndian.PutUint64(b[46:], m.sizeInBytes)
	binary.LittleEndian.PutUint64(b[54:], uint64(m.createdAt))      //nolint:gosec
	binary.LittleEndian.PutUint64(b[62:], uint64(m.deleteAtHeight)) //nolint:gosec
	binary.LittleEndian.PutUint64(b[70:], uint64(m.unminedSince))   //nolint:gosec
	binary.LittleEndian.PutUint64(b[78:], uint64(m.preserveUntil))  //nolint:gosec
	binary.LittleEndian.PutUint16(b[86:], uint16(len(m.blockRefs))) //nolint:gosec

	b = append(b, m.blockRefs...)
	b = append(b, m.spends...)

	return b
}

func decodeMaster(b []byte) (*masterRecord, error) {
	if len(b) < masterFixedLen {
		return nil, errors.NewStorageError("pebble: master record too short (%d bytes)", len(b))
	}

	m := &masterRecord{
		flags:                  binary.LittleEndian.Uint16(b[0:]),
		coinbaseSpendingHeight: binary.LittleEndian.Uint32(b[2:]),
		outputCount:            binary.LittleEndian.Uint32(b[6:]),
		totalCount:             binary.LittleEndian.Uint32(b[10:]),
		spentCount:             binary.LittleEndian.Uint32(b[14:]),
		page0Count:             binary.LittleEndian.Uint32(b[18:]),
		pagesTotal:             binary.LittleEndian.Uint32(b[22:]),
		pagesSpent:             binary.LittleEndian.Uint32(b[26:]),
		version:                binary.LittleEndian.Uint32(b[30:]),
		lockTime:               binary.LittleEndian.Uint32(b[34:]),
		fee:                    binary.LittleEndian.Uint64(b[38:]),
		sizeInBytes:            binary.LittleEndian.Uint64(b[46:]),
		createdAt:              int64(binary.LittleEndian.Uint64(b[54:])), //nolint:gosec
		deleteAtHeight:         int64(binary.LittleEndian.Uint64(b[62:])), //nolint:gosec
		unminedSince:           int64(binary.LittleEndian.Uint64(b[70:])), //nolint:gosec
		preserveUntil:          int64(binary.LittleEndian.Uint64(b[78:])), //nolint:gosec
	}

	refsLen := int(binary.LittleEndian.Uint16(b[86:]))
	if masterFixedLen+refsLen > len(b) {
		return nil, errors.NewStorageError("pebble: master record block refs overflow")
	}

	m.blockRefs = append([]byte(nil), b[masterFixedLen:masterFixedLen+refsLen]...)
	m.spends = append([]byte(nil), b[masterFixedLen+refsLen:]...)

	return m, nil
}

type pageRecord struct {
	spendableCount uint32
	spends         []byte
}

func encodePage(p *pageRecord) []byte {
	b := make([]byte, 4, 4+len(p.spends))
	binary.LittleEndian.PutUint32(b, p.spendableCount)

	return append(b, p.spends...)
}

func decodePage(b []byte) (*pageRecord, error) {
	if len(b) < 4 {
		return nil, errors.NewStorageError("pebble: page record too short")
	}

	return &pageRecord{
		spendableCount: binary.LittleEndian.Uint32(b),
		spends:         append([]byte(nil), b[4:]...),
	}, nil
}

type overrideRecord struct {
	frozen         bool
	spendableIn    int64
	reassignedHash []byte
}

func encodeOverride(o *overrideRecord) []byte {
	b := make([]byte, 9, 9+len(o.reassignedHash))

	if o.frozen {
		b[0] = 1
	}

	binary.LittleEndian.PutUint64(b[1:], uint64(o.spendableIn)) //nolint:gosec

	return append(b, o.reassignedHash...)
}

func decodeOverride(b []byte) (*overrideRecord, error) {
	if len(b) < 9 {
		return nil, errors.NewStorageError("pebble: override record too short")
	}

	o := &overrideRecord{
		frozen:      b[0] == 1,
		spendableIn: int64(binary.LittleEndian.Uint64(b[1:])), //nolint:gosec
	}

	if len(b) > 9 {
		o.reassignedHash = append([]byte(nil), b[9:]...)
	}

	return o, nil
}
