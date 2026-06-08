package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// h is a tiny helper to build a deterministic, distinct hash from a label.
func h(label string) chainhash.Hash {
	return chainhash.HashH([]byte(label))
}

// assertDisjoint fails if any hash appears in both a and b.
func assertDisjoint(t *testing.T, a, b []chainhash.Hash) {
	t.Helper()
	set := make(map[chainhash.Hash]struct{}, len(a))
	for _, x := range a {
		set[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := set[x]; ok {
			t.Fatalf("sets are not disjoint: hash %s appears in both", x.String())
		}
	}
}

// assertNoDuplicates fails if a contains the same hash more than once.
func assertNoDuplicates(t *testing.T, a []chainhash.Hash) {
	t.Helper()
	seen := make(map[chainhash.Hash]struct{}, len(a))
	for _, x := range a {
		if _, ok := seen[x]; ok {
			t.Fatalf("slice contains duplicate hash %s", x.String())
		}
		seen[x] = struct{}{}
	}
}

// TestPartitionLongestChainMarks exhaustively exercises the pure partitioning logic that
// guarantees the mark(true) and mark(false) sets are disjoint before they are written
// concurrently to the UTXO store during a reorg.
func TestPartitionLongestChainMarks(t *testing.T) {
	a, b, c, d, e := h("a"), h("b"), h("c"), h("d"), h("e")
	coinbase := subtreepkg.CoinbasePlaceholderHashValue

	tests := []struct {
		name            string
		markOnLongest   []chainhash.Hash
		losing          []chainhash.Hash
		assembly        []chainhash.Hash
		expectMarkTrue  []chainhash.Hash
		expectMarkFalse []chainhash.Hash
	}{
		{
			name:            "basic disjoint inputs",
			markOnLongest:   []chainhash.Hash{a, b},
			losing:          []chainhash.Hash{c},
			assembly:        []chainhash.Hash{d},
			expectMarkTrue:  []chainhash.Hash{a, b},
			expectMarkFalse: []chainhash.Hash{c, d},
		},
		{
			// The exact bug the fix addresses: a winning tx is also still in assembly.
			// It must end up only in mark(false), never in both.
			name:            "winning tx also in assembly -> false wins",
			markOnLongest:   []chainhash.Hash{a, b},
			losing:          nil,
			assembly:        []chainhash.Hash{b},
			expectMarkTrue:  []chainhash.Hash{a},
			expectMarkFalse: []chainhash.Hash{b},
		},
		{
			name:            "winning tx also in losing set -> false wins",
			markOnLongest:   []chainhash.Hash{a, b},
			losing:          []chainhash.Hash{a},
			assembly:        nil,
			expectMarkTrue:  []chainhash.Hash{b},
			expectMarkFalse: []chainhash.Hash{a},
		},
		{
			name:            "duplicate assembly hashes are de-duplicated",
			markOnLongest:   nil,
			losing:          nil,
			assembly:        []chainhash.Hash{d, d, e},
			expectMarkTrue:  []chainhash.Hash{},
			expectMarkFalse: []chainhash.Hash{d, e},
		},
		{
			name:            "losing and assembly overlap are de-duplicated",
			markOnLongest:   nil,
			losing:          []chainhash.Hash{c},
			assembly:        []chainhash.Hash{c, d},
			expectMarkTrue:  []chainhash.Hash{},
			expectMarkFalse: []chainhash.Hash{c, d},
		},
		{
			name:            "coinbase placeholder is stripped from mark(false)",
			markOnLongest:   nil,
			losing:          nil,
			assembly:        []chainhash.Hash{coinbase, d},
			expectMarkTrue:  []chainhash.Hash{},
			expectMarkFalse: []chainhash.Hash{d},
		},
		{
			name:            "empty inputs",
			markOnLongest:   nil,
			losing:          nil,
			assembly:        nil,
			expectMarkTrue:  []chainhash.Hash{},
			expectMarkFalse: []chainhash.Hash{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			markTrue, markFalse := partitionLongestChainMarks(tc.markOnLongest, tc.losing, tc.assembly)

			require.ElementsMatch(t, tc.expectMarkTrue, markTrue, "mark(true) set mismatch")
			require.ElementsMatch(t, tc.expectMarkFalse, markFalse, "mark(false) set mismatch")

			// Core invariants the fix must always uphold.
			assertDisjoint(t, markTrue, markFalse)
			assertNoDuplicates(t, markFalse)
		})
	}
}

// TestPartitionLongestChainMarks_DisjointUnderHeavyOverlap stress-tests the disjointness
// invariant with large, heavily overlapping inputs.
func TestPartitionLongestChainMarks_DisjointUnderHeavyOverlap(t *testing.T) {
	const n = 5000

	markOnLongest := make([]chainhash.Hash, 0, n)
	losing := make([]chainhash.Hash, 0, n)
	assembly := make([]chainhash.Hash, 0, n)

	for i := 0; i < n; i++ {
		hash := h(string(rune(i%256)) + "-overlap-" + chainhash.HashH([]byte{byte(i), byte(i >> 8)}).String())
		// Deliberately place every hash in markOnLongest AND in one of the mark-false
		// sources, so a naive implementation would double-write every single record.
		markOnLongest = append(markOnLongest, hash)
		if i%2 == 0 {
			losing = append(losing, hash)
		} else {
			assembly = append(assembly, hash)
		}
	}

	markTrue, markFalse := partitionLongestChainMarks(markOnLongest, losing, assembly)

	assertDisjoint(t, markTrue, markFalse)
	assertNoDuplicates(t, markFalse)
	// Every markOnLongest hash overlapped a mark-false source, so mark(true) must be empty.
	require.Empty(t, markTrue, "all winning hashes overlapped mark(false) sources, expected empty mark(true)")
}

// newCheckMarkTestProcessor builds a SubtreeProcessor backed by a real sqlitememory UTXO
// store and a mocked blockchain client, suitable for exercising checkMarkNotOnLongestChain.
func newCheckMarkTestProcessor(t *testing.T, mockBC *blockchain.Mock) (*SubtreeProcessor, utxostore.Store) {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	// checkMarkNotOnLongestChain caps its errgroup at
	// max(MaxMinedRoutines, GetBatcherSize*2); ensure that is strictly positive so the
	// group can actually start goroutines (a zero limit would deadlock).
	if tSettings.UtxoStore.MaxMinedRoutines < 4 {
		tSettings.UtxoStore.MaxMinedRoutines = 4
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blob_memory.New(), mockBC, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	return stp, utxoStore
}

// createMinedTx creates a transaction spending coinbaseTx output `vout` and records it in
// the UTXO store as mined into block `blockID`. It returns the tx hash.
func createMinedTx(t *testing.T, ctx context.Context, store utxostore.Store, vout uint32, blockID uint32) chainhash.Hash {
	t.Helper()

	tx := bt.NewTx()
	err := tx.From(coinbaseTx.TxIDChainHash().String(), vout,
		coinbaseTx.Outputs[vout].LockingScript.String(), uint64(coinbaseTx.Outputs[vout].Satoshis))
	require.NoError(t, err)
	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100000000))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, err = store.Create(ctx, tx, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{
		BlockID:        blockID,
		BlockHeight:    1,
		OnLongestChain: true,
	}))
	require.NoError(t, err)

	return *tx.TxIDChainHash()
}

// TestCheckMarkNotOnLongestChain covers each decision branch of the invalidation-time
// "should this tx be marked not-on-longest-chain?" check that this PR introduces.
func TestCheckMarkNotOnLongestChain(t *testing.T) {
	ctx := context.Background()

	const (
		invalidBlockID = uint32(100)
		recentBlockID  = uint32(5)  // present in the last-1000-headers set
		otherChainID   = uint32(50) // not recent; resolved via CheckBlockIsInCurrentChain
		forkedChainID  = uint32(60) // not recent; CheckBlockIsInCurrentChain -> false
	)

	mockBC := &blockchain.Mock{}
	// Last 1000 headers behind the invalid block contain IDs 1..5.
	recentMetas := []*model.BlockHeaderMeta{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: recentBlockID}}
	mockBC.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, recentMetas, nil)
	// A tx whose block is not in the recent window is resolved via the chain check.
	mockBC.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{otherChainID}).Return(true, nil)
	mockBC.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{forkedChainID}).Return(false, nil)

	stp, store := newCheckMarkTestProcessor(t, mockBC)

	// Build one tx per branch (each spends a distinct coinbase output).
	txOnlyInvalid := createMinedTx(t, ctx, store, 0, invalidBlockID) // only in the invalid block -> mark false
	txRecent := createMinedTx(t, ctx, store, 1, recentBlockID)       // still in a recent block -> keep
	txOtherChain := createMinedTx(t, ctx, store, 2, otherChainID)    // on current chain -> keep
	txForked := createMinedTx(t, ctx, store, 3, forkedChainID)       // not on current chain -> mark false

	invalidBlock := &model.Block{
		ID:     invalidBlockID,
		Height: 1,
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           model.NBit{},
			Nonce:          1,
		},
	}

	input := []chainhash.Hash{txOnlyInvalid, txRecent, txOtherChain, txForked}
	result, err := stp.checkMarkNotOnLongestChain(ctx, invalidBlock, input)
	require.NoError(t, err)

	// Only the tx unique to the invalid block and the tx on a forked (non-current) chain
	// should be marked as not on the longest chain.
	require.ElementsMatch(t, []chainhash.Hash{txOnlyInvalid, txForked}, result)
}

// TestCheckMarkNotOnLongestChain_TxNotFound verifies the error path when a candidate tx is
// missing from the UTXO store.
func TestCheckMarkNotOnLongestChain_TxNotFound(t *testing.T) {
	ctx := context.Background()

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{{ID: 1}}, nil)

	stp, _ := newCheckMarkTestProcessor(t, mockBC)

	invalidBlock := &model.Block{
		ID:     7,
		Height: 1,
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
		},
	}

	missing := h("never-created-tx")
	_, err := stp.checkMarkNotOnLongestChain(ctx, invalidBlock, []chainhash.Hash{missing})
	require.Error(t, err)
}

// TestMarkNotOnLongestChain_InvalidGating verifies that the invalidation-aware filtering is
// only triggered for the single-moveBack / zero-moveForward case AND only when the moved-back
// block is actually flagged invalid.
func TestMarkNotOnLongestChain_InvalidGating(t *testing.T) {
	ctx := context.Background()

	t.Run("non-invalid block skips the per-tx chain check", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Invalid: false}, nil)

		stp, store := newCheckMarkTestProcessor(t, mockBC)
		tx := createMinedTx(t, ctx, store, 0, 42)

		block := &model.Block{
			ID:     42,
			Height: 1,
			Header: &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
		}

		err := stp.markNotOnLongestChain(ctx, []*model.Block{block}, nil, []chainhash.Hash{tx})
		require.NoError(t, err)
		// GetBlockHeaders is only called from checkMarkNotOnLongestChain, which must be
		// skipped when the block is not invalid.
		mockBC.AssertNotCalled(t, "GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("invalid block triggers the per-tx chain check", func(t *testing.T) {
		mockBC := &blockchain.Mock{}
		mockBC.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Invalid: true}, nil)
		mockBC.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{}, []*model.BlockHeaderMeta{{ID: 1}}, nil)

		stp, store := newCheckMarkTestProcessor(t, mockBC)
		tx := createMinedTx(t, ctx, store, 0, 42) // only in the invalid block -> will be marked false

		block := &model.Block{
			ID:     42,
			Height: 1,
			Header: &model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}},
		}

		err := stp.markNotOnLongestChain(ctx, []*model.Block{block}, nil, []chainhash.Hash{tx})
		require.NoError(t, err)
		mockBC.AssertCalled(t, "GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything)
	})
}
