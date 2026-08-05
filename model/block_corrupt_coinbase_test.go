package model

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestBlockValid_CorruptWhenFirstTxNotCoinbase covers the body-integrity classification at
// model/Block.go step 4 (bitcoin-sv/teranode#4692): a first transaction that is not a valid coinbase
// is an unbound-body defect, so block.Valid returns ERR_BLOCK_CORRUPT (re-download + strike), never
// a header verdict. Asserts the classification, not just that validation fails.
func TestBlockValid_CorruptWhenFirstTxNotCoinbase(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	// A transaction spending a real (non-zero) previous outpoint is NOT a coinbase.
	notCoinbase := bt.NewTx()
	require.NoError(t, notCoinbase.From(
		"1111111111111111111111111111111111111111111111111111111111111111", 0,
		"76a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac", 1000))
	require.False(t, notCoinbase.IsCoinbase(), "test fixture must not be a coinbase")

	block := &Block{
		Header:           blockHeader,
		CoinbaseTx:       notCoinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees:         []*chainhash.Hash{},
	}

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, nil, createTestUTXOStore(t),
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)

	require.False(t, valid)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a non-coinbase first tx is a corrupt body, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a corrupt body must not be classified invalid")
	require.Contains(t, err.Error(), "not a valid coinbase")
}

// TestBlockValid_CorruptWhenFirstSubtreeHasNoNodes covers the empty-first-subtree body check in
// block.Valid (bitcoin-sv/teranode#4692): a subtree that carries no nodes is a body-integrity defect,
// classified corrupt (re-download), not invalid. The subtree slice is pre-populated so
// GetAndValidateSubtrees takes its "already loaded" fast path and the empty-subtree guard runs.
func TestBlockValid_CorruptWhenFirstSubtreeHasNoNodes(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	empty, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.Equal(t, 0, empty.Length(), "test fixture: the first subtree must have no nodes")

	block := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees:         []*chainhash.Hash{{0x01}},
		// Pre-set (non-nil, len matches Subtrees) so GetAndValidateSubtrees skips loading and the
		// empty-subtree guard is reached.
		SubtreeSlices: []*subtreepkg.Subtree{empty},
	}

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, &mockSubtreeStore{}, createTestUTXOStore(t),
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)

	require.False(t, valid)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an empty first subtree is a corrupt body, got: %v", err)
	require.Contains(t, err.Error(), "first subtree has no nodes")
}
