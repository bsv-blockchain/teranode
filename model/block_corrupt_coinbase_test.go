package model

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
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

// TestBlockValid_TransientWhenFirstSubtreeEmptied covers the emptied-first-subtree case in
// block.Valid (bitcoin-sv/teranode#4692). A first subtree that was present and non-empty when
// CheckBlockSubtrees admitted it but whose Nodes slice was emptied afterwards (concurrent node
// release) is a transient LOCAL condition, not peer corruption: on every peer-striking path
// CheckBlockSubtrees runs first and rejects a zero-node subtree upstream, so this line is
// unreachable with peer-supplied data. block.Valid must return a retryable processing error, never
// a corrupt verdict that would strike an innocent peer. The empty subtree is served from the store
// (a pre-set 0-node SubtreeSlices entry is treated as "not loaded" by GetAndValidateSubtrees, so it
// must come from the store to reach the guard).
func TestBlockValid_TransientWhenFirstSubtreeEmptied(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	// A present-but-empty subtree blob standing in for one whose nodes were released after admission.
	// Serialized layout (go-subtree):
	// rootHash[32] | fees[8] | sizeInBytes[8] | numNodes[8]=0 | numConflictingNodes[8]=0.
	key := chainhash.Hash{0x01}
	emptySubtreeBlob := make([]byte, 64)
	copy(emptySubtreeBlob[:32], key[:]) // rootHash field (cosmetic; not re-checked on load)

	blobStore := blobmemory.New()
	require.NoError(t, blobStore.Set(context.Background(), key[:], fileformat.FileTypeSubtree, emptySubtreeBlob))

	block := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees:         []*chainhash.Hash{&key},
		// SubtreeSlices left nil so GetAndValidateSubtrees loads the (empty) subtree from the store.
	}

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, blobStore, createTestUTXOStore(t),
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)

	require.False(t, valid)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "an emptied first subtree is a transient local error, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "an emptied first subtree must not be classified corrupt (would strike an innocent peer)")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an emptied first subtree must never poison the hash")
	require.Contains(t, err.Error(), "first subtree emptied (released) during validation")
}

// TestBlockValid_CorruptWhenBodyHasNoSubtrees is the positive control for the reclassification: a
// reloaded body carrying NO subtrees at all remains genuine body corruption
// (bitcoin-sv/teranode#4692). A multi-transaction hash served with an emptied subtree list fails the
// coinbase-only merkle binding and stays ERR_BLOCK_CORRUPT (re-download + strike) — it must NOT be
// reclassified to a transient local error like the emptied-first-subtree case.
func TestBlockValid_CorruptWhenBodyHasNoSubtrees(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	blobStore := blobmemory.New()

	// No subtrees, but the header merkle root does not equal the coinbase txid (block1Header's
	// merkle root is a real multi-tx root), so the coinbase-only binding fails → corrupt body.
	block := &Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      123,
		Subtrees:         []*chainhash.Hash{},
	}

	valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, blobStore, createTestUTXOStore(t),
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)

	require.False(t, valid)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a body with no subtrees is corrupt, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrProcessing), "a no-subtrees body must not be reclassified transient")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a corrupt body must never poison the hash")
	require.Contains(t, err.Error(), "body carries no subtrees")
}
