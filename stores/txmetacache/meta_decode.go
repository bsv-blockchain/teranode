package txmetacache

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// CachedMeta is the part of a cached txmeta entry that subtree validation
// reads. Unlike *meta.Data it is a value: GetMetaCachedInto fills a caller's
// slice of them without allocating per entry.
type CachedMeta struct {
	Fee         uint64
	SizeInBytes uint64
	IsCoinbase  bool
	Frozen      bool
	Conflicting bool
	Locked      bool
	InBlock     bool
	TxInpoints  subtree.TxInpoints
}

// metaHeaderBytes is fee (8) + size (8) + flags (1) + parent count (4).
const metaHeaderBytes = 21

// metaArenaNeed returns upper bounds on the parent hashes and vout words that
// decoding b needs. They come from b's length, not from its counts, so a
// corrupt count cannot make the caller allocate more than b could hold.
func metaArenaNeed(b []byte) (parents, vouts int) {
	if len(b) < metaHeaderBytes {
		return 0, 0
	}

	rest := len(b) - metaHeaderBytes
	parents = min(int(binary.LittleEndian.Uint32(b[17:21])), rest/chainhash.HashSize)

	return parents, (rest - parents*chainhash.HashSize) / 4
}

// decodeCachedMeta decodes a MetaBytes encoding (trailing bytes ignored) into
// a CachedMeta. Parent hashes and vout words are copied into the caller's
// parents and vouts, which must be at least as large as metaArenaNeed(b)
// reports; it returns how many of each it used. The decoded TxInpoints
// reference those slices, capped so an append cannot run into a neighbour's
// data.
//
// It accepts and rejects exactly what meta.NewMetaDataFromBytes does.
func decodeCachedMeta(b []byte, parents []chainhash.Hash, vouts []uint32) (CachedMeta, int, int, error) {
	if len(b) < metaHeaderBytes {
		return CachedMeta{}, 0, 0, errors.NewProcessingError("txmeta entry too short: %d bytes", len(b))
	}

	flags := b[16]

	m := CachedMeta{
		Fee:         binary.LittleEndian.Uint64(b[0:8]),
		SizeInBytes: binary.LittleEndian.Uint64(b[8:16]),
		IsCoinbase:  flags&0b1 != 0,
		Frozen:      flags&0b10 != 0,
		Conflicting: flags&0b100 != 0,
		Locked:      flags&0b1000 != 0,
		InBlock:     flags&0b10000 != 0,
	}

	n := uint64(binary.LittleEndian.Uint32(b[17:21]))
	if n == 0 {
		return m, 0, 0, nil
	}

	pos := uint64(metaHeaderBytes)
	size := uint64(len(b))

	if n*chainhash.HashSize > size-pos {
		return CachedMeta{}, 0, 0, errors.NewProcessingError("txmeta entry truncated in parent hashes")
	}

	if n > uint64(len(parents)) {
		return CachedMeta{}, 0, 0, errors.NewProcessingError("txmeta decode arena too small for %d parents", n)
	}

	for i := uint64(0); i < n; i++ {
		copy(parents[i][:], b[pos:pos+chainhash.HashSize])
		pos += chainhash.HashSize
	}

	used := uint64(0)
	words := uint64(len(vouts))

	for i := uint64(0); i < n; i++ {
		if size-pos < 4 || used >= words {
			return CachedMeta{}, 0, 0, errors.NewProcessingError("txmeta entry truncated in vout counts")
		}

		count := uint64(binary.LittleEndian.Uint32(b[pos:]))
		pos += 4

		if count*4 > size-pos || count > words-used-1 {
			return CachedMeta{}, 0, 0, errors.NewProcessingError("txmeta entry truncated in vouts")
		}

		vouts[used] = uint32(count) //nolint:gosec // read from a uint32
		used++

		for j := uint64(0); j < count; j++ {
			vouts[used] = binary.LittleEndian.Uint32(b[pos:])
			used++
			pos += 4
		}
	}

	m.TxInpoints = subtree.NewTxInpointsFromPacked(parents[:n:n], vouts[:used:used])

	return m, int(n), int(used), nil //nolint:gosec // bounded by len(b)
}
