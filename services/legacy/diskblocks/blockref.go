// Package diskblocks reads blocks directly from a stopped SV Node (bitcoind)
// data directory: the blocks/index LevelDB and the blocks/blk*.dat files.
// It is used by the legacy service's disk-sync mode for network-isolated IBD.
package diskblocks

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// Block index status flags, mirroring Bitcoin Core's BlockStatus bits.
const (
	BlockValidReserved     uint64 = 1
	BlockValidTree         uint64 = 2
	BlockValidTransactions uint64 = 3
	BlockValidChain        uint64 = 4
	BlockValidScripts      uint64 = 5
	BlockValidMask                = BlockValidReserved | BlockValidTree | BlockValidTransactions | BlockValidChain | BlockValidScripts

	BlockHaveData uint64 = 8
	BlockHaveUndo uint64 = 16
)

// BlockRef locates one block in an SV Node datadir and carries the chain links
// needed to order the blocks without consulting the chainstate database.
type BlockRef struct {
	Hash     chainhash.Hash
	PrevHash chainhash.Hash
	Height   uint32
	TxCount  uint64
	NFile    uint32
	NDataPos uint32
	HaveData bool
}

// readVarInt decodes one Bitcoin Core base-128 "ReadVarInt" value and returns
// it with the number of bytes consumed. On empty input it returns (0, 0); the
// caller is responsible for detecting a truncated record (e.g. via the
// remaining-length check before reading the 80-byte header).
func readVarInt(data []byte) (uint64, int) {
	var n uint64
	i := 0
	for i < len(data) {
		ch := data[i]
		i++
		n = (n << 7) | uint64(ch&0x7f)
		if ch&0x80 != 0 {
			n++
		} else {
			break
		}
	}
	return n, i
}

// parseBlockIndexRecord decodes one blocks/index value into a BlockRef.
// It returns an error for records without on-disk block data or with a header
// shorter than 80 bytes; such records are skipped by the caller.
func parseBlockIndexRecord(data []byte) (*BlockRef, error) {
	pos := 0
	_, i := readVarInt(data[pos:]) // client version (discarded)
	pos += i

	height, i := readVarInt(data[pos:])
	pos += i

	status, i := readVarInt(data[pos:])
	pos += i

	txs, i := readVarInt(data[pos:])
	pos += i

	if status&BlockHaveData == 0 {
		return nil, errors.NewBlockInvalidError("block index record has no on-disk data")
	}

	var nFile, nDataPos uint64
	if status&(BlockHaveData|BlockHaveUndo) != 0 {
		nFile, i = readVarInt(data[pos:])
		pos += i
	}
	if status&BlockHaveData != 0 {
		nDataPos, i = readVarInt(data[pos:])
		pos += i
	}
	if status&BlockHaveUndo != 0 {
		_, i = readVarInt(data[pos:]) // nUndoPos (discarded)
		pos += i
	}

	if len(data[pos:]) < 80 {
		return nil, errors.NewProcessingError("block index header is shorter than 80 bytes")
	}

	bh, err := model.NewBlockHeaderFromBytes(data[pos : pos+80])
	if err != nil {
		return nil, err
	}

	return &BlockRef{
		Hash:     *bh.Hash(),
		PrevHash: *bh.HashPrevBlock,
		Height:   uint32(height),
		TxCount:  txs,
		NFile:    uint32(nFile),
		NDataPos: uint32(nDataPos),
		HaveData: true,
	}, nil
}
