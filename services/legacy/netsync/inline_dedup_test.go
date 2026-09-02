package netsync

// CVE-2012-2459 duplicate-transaction dedup floor on the INLINE (non-unified)
// below-checkpoint path.
//
// Background: CheckSubtreeSlicesForDuplicateTxs is the explicit dedup guard for
// any path that holds the block's subtree slices in memory without running the
// full Block.Valid duplicate-tx scan. Before this task it ran only inside the
// `if preparedSubtreeSlices != nil` block in HandleBlockDirect  — and prepareSubtrees only returns non-nil
// preparedSubtreeSlices on the UNIFIED route (see the `if sm.legacyUnified(...)`
// guard at the bottom of prepareSubtrees). On the inline route the returned
// slices are nil, so HandleBlockDirect's guard is skipped and the CVE check
// never ran on that path.
//
// createTxMap uses txMap.Set, which SILENTLY OVERWRITES a duplicate txid, so the
// map itself cannot catch the duplicate. But createSubtrees iterates the
// block-order txOrder (which retains both copies of the duplicated hash) and
// AddNodes the duplicate twice into the locally-built slices. The fix runs
// CheckSubtreeSlicesForDuplicateTxs on those locally-built slices inside
// prepareSubtrees, unconditionally, right after createSubtrees — so every path
// (inline, unified, non-quick) gets the floor from one place.

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// makeDuplicateTxidBlock builds a below-checkpoint wire block whose transaction
// list contains a duplicated non-coinbase txid: [coinbase, txX, txX]. This is
// the CVE-2012-2459 mutation shape at the tx-list level — the merkle root of
// such a block is preserved by the duplicate-last-when-odd rule, so the merkle
// check alone would pass, but the block still contains a repeated hash.
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

// TestLegacyInline_DuplicateTxid_Rejected proves that a below-checkpoint block
// routed through the INLINE path (prepareSubtrees,
// LegacyUnifiedBelowCheckpoint=false) carrying a duplicated txid is rejected
// with a BlockInvalidError before any UTXO commit. Previously this block would
// slip past the dedup guard (which only fired on the unified route) and reach
// ValidateTransactionsLegacyMode.
func TestLegacyInline_DuplicateTxid_Rejected(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const checkpointHeight = int32(1000)
	const height = int32(500)

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = false

	logger := ulogger.TestLogger{}

	storeURL, err := url.Parse("sqlitememory:///inline_dedup")
	require.NoError(t, err)
	store, err := utxosql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	mockBC := &blockchain.Mock{}
	mockBC.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(101), nil).Maybe()

	sm := &SyncManager{
		settings:         tSettings,
		chainParams:      params,
		logger:           logger,
		utxoStore:        store,
		blockchainClient: mockBC,
		validationClient: makeSpendValidator(store),
		subtreeStore:     memory.New(),
		ctx:              ctx,
	}

	// Sanity: the below-checkpoint inline gate engages for this block.
	require.True(t, sm.legacyOutpointOnly(uint32(height)), "below-checkpoint outpoint-only gate must engage for the test block")
	require.False(t, sm.legacyUnified(uint32(height)), "test must exercise the inline (non-unified) route")

	block := makeDuplicateTxidBlock(height)

	_, _, _, prepErr := sm.prepareSubtrees(ctx, block)
	require.Error(t, prepErr, "a duplicated txid must be rejected on the inline path")
	require.True(t, errors.Is(prepErr, errors.ErrBlockInvalid),
		"duplicate-txid rejection must be a BlockInvalidError (CVE-2012-2459), got: %v", prepErr)
}
