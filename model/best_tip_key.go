package model

import (
	"encoding/binary"
	"math"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// BestTipLowWaterStateKey is the state key under which the blockchain store records the lowest
// place the best tip moved down to. Only a write that makes the best tip sort worse records it
// (invalidating or deleting a block), and block assembly clears it once it has replayed from it.
const BestTipLowWaterStateKey = "BestTipLowWater"

// bestTipKeyVersion is the first byte of an encoded BestTipKey. Version 2 is the order by chain
// work and id. Version 1, written by binaries that broke equal-work ties by the relaying peer's
// id first, carried the peer id between the chain work and the id; it is still read from stored
// state, see readBestTipKey. A binary that knows only version 1 rejects version 2, so neither
// side reads a key of the other order as one of its own.
const (
	bestTipKeyVersion       = 2
	bestTipKeyLegacyVersion = 1
)

// BestTipKey is a block's place in the order the blockchain store picks its best tip by: most
// chain work first, then the lowest block id. Above the checkpoints a block's id is taken when
// StoreBlock writes its row, after validation, so between blocks of equal work the one this node
// stored first wins. That is close to SV Node's rule of keeping the block it received first, but
// not the same: a sibling that arrives later and validates faster can take the lower id. The
// order is total, and without an invalidation the best tip only ever moves to a block that sorts
// better, so a reader that keeps the key of the last block it handled finds every later best tip
// among the valid blocks that sort better than that key.
//
// ChainWork is the stored cumulative work, 32 bytes big-endian, so comparing it bytewise
// compares the work. Hash identifies the block and plays no part in the order.
type BestTipKey struct {
	ChainWork []byte
	ID        uint32
	Hash      chainhash.Hash
}

// NewBestTipKey returns the place of the block with this header and metadata.
func NewBestTipKey(header *BlockHeader, meta *BlockHeaderMeta) BestTipKey {
	return BestTipKey{
		ChainWork: append([]byte(nil), meta.ChainWork...),
		ID:        meta.ID,
		Hash:      *header.Hash(),
	}
}

// Bytes serialises the key: a version byte, the chain work behind a 4-byte length, the id, and
// the hash.
func (k BestTipKey) Bytes() []byte {
	b := make([]byte, 0, 1+4+len(k.ChainWork)+4+chainhash.HashSize)
	b = append(b, bestTipKeyVersion)
	b = appendBestTipBytes(b, k.ChainWork)
	b = binary.LittleEndian.AppendUint32(b, k.ID)

	return append(b, k.Hash[:]...)
}

// NewBestTipKeyFromBytes parses a key from stored state: one written by Bytes, or a version 1
// key an older binary saved, placed as readBestTipKey describes. It fails on anything else, an
// empty or truncated value included, rather than guessing.
func NewBestTipKeyFromBytes(b []byte) (BestTipKey, error) {
	k, rest, err := readBestTipKey(b)
	if err != nil {
		return BestTipKey{}, err
	}

	if len(rest) != 0 {
		return BestTipKey{}, errors.NewProcessingError("invalid best tip key: %d trailing bytes", len(rest))
	}

	return k, nil
}

// NewCurrentBestTipKeyFromBytes parses a key a caller sends in a request, which must be one
// written by Bytes. A version 1 key comes from a block assembly that still orders by peer id: it
// would read the replies in its own order and clear a low-water mark it cannot parse, so refusing
// it makes that block assembly follow the best header instead, as it does against any blockchain
// service that does not serve its order.
func NewCurrentBestTipKeyFromBytes(b []byte) (BestTipKey, error) {
	if len(b) > 0 && b[0] != bestTipKeyVersion {
		return BestTipKey{}, errors.NewProcessingError("invalid best tip key: version %d, expected %d", b[0], bestTipKeyVersion)
	}

	return NewBestTipKeyFromBytes(b)
}

// BestTipLowWater is the low-water mark of the best tip: the key of the best tip right after it
// moved down, the lowest such key since the mark was last cleared. Generation grows on every
// write, including one that keeps the old key because it was lower, so a reader that clears the
// mark only when it still holds the bytes it read never clears a drop it has not seen.
//
// DroppedTips lists the blocks that were the best tip when it dropped, each once, in the order
// they dropped. A block that was the best tip and was invalidated before block assembly applied
// it is not among the valid blocks the replay reads, so this list is the only record that it
// existed: its block-only transactions reach the pool through a reset. The list keeps at most
// MaxBestTipLowWaterDroppedTips hashes. A drop past that, or one merged into a mark this binary
// could not read, sets DroppedTipsOverflow instead, which tells the reader to reset whatever
// the list says, so no drop is ever lost.
type BestTipLowWater struct {
	Key                 BestTipKey
	Generation          uint64
	DroppedTips         []chainhash.Hash
	DroppedTipsOverflow bool
}

// MaxBestTipLowWaterDroppedTips bounds DroppedTips. Block assembly clears the mark on its next
// replay, so the list only grows while it is down or failing, and one reset covers any number of
// drops.
const MaxBestTipLowWaterDroppedTips = 64

// bestTipLowWaterTipsVersion marks the section after the generation that carries the dropped
// tips. A mark with nothing after the generation is the first encoding, which had no tips.
const bestTipLowWaterTipsVersion = 2

// AddDroppedTip records that hash was the best tip when it dropped. A hash already listed is not
// listed twice. Past MaxBestTipLowWaterDroppedTips the hash is not kept and DroppedTipsOverflow
// is set.
func (m *BestTipLowWater) AddDroppedTip(hash chainhash.Hash) {
	for i := range m.DroppedTips {
		if m.DroppedTips[i] == hash {
			return
		}
	}

	if len(m.DroppedTips) >= MaxBestTipLowWaterDroppedTips {
		m.DroppedTipsOverflow = true
		return
	}

	m.DroppedTips = append(m.DroppedTips, hash)
}

// Bytes serialises the mark: the key, the generation, then a version byte, a flags byte (bit 0
// is DroppedTipsOverflow), the number of dropped tips as 4 bytes and the hashes.
func (m BestTipLowWater) Bytes() []byte {
	b := binary.LittleEndian.AppendUint64(m.Key.Bytes(), m.Generation)
	b = append(b, bestTipLowWaterTipsVersion)

	var flags byte
	if m.DroppedTipsOverflow {
		flags |= 1
	}

	b = append(b, flags)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.DroppedTips))) //nolint:gosec // bounded by MaxBestTipLowWaterDroppedTips

	for i := range m.DroppedTips {
		b = append(b, m.DroppedTips[i][:]...)
	}

	return b
}

// NewBestTipLowWaterFromBytes parses a mark written by Bytes, or by the first encoding, which
// ended at the generation and is read as a mark without dropped tips.
func NewBestTipLowWaterFromBytes(b []byte) (BestTipLowWater, error) {
	k, rest, err := readBestTipKey(b)
	if err != nil {
		return BestTipLowWater{}, err
	}

	if len(rest) < 8 {
		return BestTipLowWater{}, errors.NewProcessingError("invalid best tip low-water mark: expected 8 bytes of generation, got %d", len(rest))
	}

	mark := BestTipLowWater{Key: k, Generation: binary.LittleEndian.Uint64(rest)}
	rest = rest[8:]

	if len(rest) == 0 {
		return mark, nil
	}

	if rest[0] != bestTipLowWaterTipsVersion {
		return BestTipLowWater{}, errors.NewProcessingError("invalid best tip low-water mark: unknown version %d", rest[0])
	}

	if len(rest) < 6 {
		return BestTipLowWater{}, errors.NewProcessingError("invalid best tip low-water mark: truncated dropped tips header")
	}

	flags := rest[1]
	if flags&^1 != 0 {
		return BestTipLowWater{}, errors.NewProcessingError("invalid best tip low-water mark: unknown flags %#x", flags)
	}

	mark.DroppedTipsOverflow = flags&1 != 0

	n := binary.LittleEndian.Uint32(rest[2:])
	rest = rest[6:]

	if n > MaxBestTipLowWaterDroppedTips || uint64(n)*chainhash.HashSize != uint64(len(rest)) {
		return BestTipLowWater{}, errors.NewProcessingError("invalid best tip low-water mark: %d dropped tips in %d bytes", n, len(rest))
	}

	if n > 0 {
		mark.DroppedTips = make([]chainhash.Hash, n)
		for i := range mark.DroppedTips {
			copy(mark.DroppedTips[i][:], rest[i*chainhash.HashSize:])
		}
	}

	return mark, nil
}

func appendBestTipBytes(b, v []byte) []byte {
	// Chain work is 32 bytes; it comes nowhere near 4 GiB.
	b = binary.LittleEndian.AppendUint32(b, uint32(len(v))) //nolint:gosec // see above

	return append(b, v...)
}

func readBestTipBytes(b []byte) ([]byte, []byte, error) {
	if len(b) < 4 {
		return nil, nil, errors.NewProcessingError("invalid best tip key: truncated length")
	}

	n := binary.LittleEndian.Uint32(b)
	b = b[4:]

	if uint64(n) > uint64(len(b)) {
		return nil, nil, errors.NewProcessingError("invalid best tip key: field of %d bytes, %d left", n, len(b))
	}

	return append([]byte(nil), b[:n]...), b[n:], nil
}

// readBestTipKey reads a key in either version. A version 1 key held its place in the order by
// chain work, peer id and id, so among blocks of equal work it does not have the same place by
// chain work and id: a sibling with a higher id and a lower peer id sorted better than it and now
// sorts worse, and a replay from its id would skip that sibling. A version 1 key with a peer id is
// therefore read with the highest id, last among blocks of its work, so a replay from it visits
// every sibling of equal work again; a block it already holds is passed over. One with an empty
// peer id sorted first among its equal-work siblings in the old order, so its id already places
// it at least as early, and it is read as it is.
func readBestTipKey(b []byte) (BestTipKey, []byte, error) {
	if len(b) == 0 || (b[0] != bestTipKeyVersion && b[0] != bestTipKeyLegacyVersion) {
		return BestTipKey{}, nil, errors.NewProcessingError("invalid best tip key: unknown version")
	}

	var (
		k    BestTipKey
		peer []byte
		err  error
	)

	legacy := b[0] == bestTipKeyLegacyVersion
	rest := b[1:]

	if k.ChainWork, rest, err = readBestTipBytes(rest); err != nil {
		return BestTipKey{}, nil, err
	}

	if legacy {
		if peer, rest, err = readBestTipBytes(rest); err != nil {
			return BestTipKey{}, nil, err
		}
	}

	if len(rest) < 4+chainhash.HashSize {
		return BestTipKey{}, nil, errors.NewProcessingError("invalid best tip key: truncated id or hash")
	}

	k.ID = binary.LittleEndian.Uint32(rest)
	copy(k.Hash[:], rest[4:4+chainhash.HashSize])

	if len(peer) > 0 {
		k.ID = math.MaxUint32
	}

	return k, rest[4+chainhash.HashSize:], nil
}
