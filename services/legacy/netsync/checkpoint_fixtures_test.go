package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	legacychain "github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/stretchr/testify/require"
)

func makeDuplicateTxidBlock(height int32) *bsvutil.Block {
	makeCoinbase := func(h int32) *wire.MsgTx {
		cb := wire.NewMsgTx(1)
		cb.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
			SignatureScript:  []byte{byte(h & 0xff), 0x01}, //nolint:gosec // test coinbase script
			Sequence:         0xffffffff,
		})
		cb.AddTxOut(&wire.TxOut{Value: 5000, PkScript: []byte{0x76, 0xa9, 0x14}})
		return cb
	}

	// A single regular tx, added to the block twice → identical txid twice.
	external := chainhash.Hash{0xde, 0xad, 0x42}
	tx := wire.NewMsgTx(1)
	tx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: external, Index: 0},
		SignatureScript:  []byte{0x00, 0x42},
		Sequence:         0xffffffff,
	})
	tx.AddTxOut(&wire.TxOut{Value: 1000, PkScript: []byte{0x76, 0xa9, 0x14, 0x42}})

	msgBlock := &wire.MsgBlock{
		Header:       wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
		Transactions: []*wire.MsgTx{makeCoinbase(height), tx, tx}, // tx duplicated
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(height)

	return block
}
func bodyCommitment(t *testing.T, block *bsvutil.Block) *model.Block {
	t.Helper()
	commitment, err := model.NewBlockFromMsgBlock(block.MsgBlock(), nil)
	require.NoError(t, err)
	commitment.Subtrees = nil
	commitment.SubtreeSlices = nil
	return commitment
}
func setBodyMerkleRoot(block *wire.MsgBlock) {
	roots := legacychain.BuildMerkleTreeStore(bsvutil.NewBlock(block).Transactions())
	block.Header.MerkleRoot = *roots[len(roots)-1]
}
