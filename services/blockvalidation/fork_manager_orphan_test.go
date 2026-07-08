package blockvalidation

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// orphanTestBlock builds a block whose parent is unknown to the store, forcing
// DetermineForkID down the orphan path. nonce/prev vary the full block hash.
func orphanTestBlock(t *testing.T, nonce uint32) *model.Block {
	t.Helper()

	prev := &chainhash.Hash{}
	prev[0] = byte(nonce)
	prev[1] = 0xAB

	merkleRoot := &chainhash.Hash{}
	merkleRoot[0] = byte(nonce)

	return &model.Block{
		Height: 100,
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prev,
			HashMerkleRoot: merkleRoot,
			Timestamp:      1700000000,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          nonce,
		},
	}
}

// TestDetermineForkID_OrphanUsesFullHash verifies that the orphan fork ID is
// derived from the full block hash (256 bits) rather than a truncated suffix.
// Truncating to the last 8 hex chars (32 bits) allowed two distinct orphan
// blocks to alias onto a single ForkBranch and corrupt its bookkeeping. See
// issue #4663.
func TestDetermineForkID_OrphanUsesFullHash(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10

	// A best block that no orphan parent matches, so DetermineForkID never
	// short-circuits to "main".
	store := blockchain_store.NewMockStore()
	store.BestBlock = &model.Block{
		Height: 1,
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{0xFF},
			Timestamp:      1700000000,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          1,
		},
	}

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	fm := NewForkManager(logger, tSettings)

	block := orphanTestBlock(t, 1)
	forkID, err := fm.DetermineForkID(ctx, block, client)
	require.NoError(t, err)

	fullHash := block.Hash().String()

	// The fork ID must carry the full hash, not a truncated suffix.
	require.Equal(t, "orphan-"+fullHash, forkID)
	require.True(t, strings.Contains(forkID, fullHash),
		"orphan fork ID must embed the full block hash to avoid collisions")
	require.Greater(t, len(forkID), len("orphan-")+8,
		"orphan fork ID must use more than the truncated 8-char suffix")

	// The block and its parent are mapped to this fork.
	fm.mu.RLock()
	require.Equal(t, forkID, fm.blockToFork[*block.Hash()])
	require.Equal(t, forkID, fm.blockToFork[*block.Header.HashPrevBlock])
	_, ok := fm.forks[forkID]
	fm.mu.RUnlock()
	require.True(t, ok, "fork must be registered")
}

// TestDetermineForkID_DistinctOrphansDistinctForks verifies that two distinct
// orphan blocks produce two distinct ForkBranches, each tracking its own block.
func TestDetermineForkID_DistinctOrphansDistinctForks(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10

	store := blockchain_store.NewMockStore()
	store.BestBlock = &model.Block{
		Height: 1,
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{0xFF},
			Timestamp:      1700000000,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          1,
		},
	}

	client, err := blockchain.NewLocalClient(logger, tSettings, store, nil, nil)
	require.NoError(t, err)

	fm := NewForkManager(logger, tSettings)

	blockA := orphanTestBlock(t, 1)
	blockB := orphanTestBlock(t, 2)
	require.False(t, blockA.Hash().IsEqual(blockB.Hash()), "test blocks must differ")

	forkA, err := fm.DetermineForkID(ctx, blockA, client)
	require.NoError(t, err)

	forkB, err := fm.DetermineForkID(ctx, blockB, client)
	require.NoError(t, err)

	require.NotEqual(t, forkA, forkB, "distinct orphan blocks must not alias onto one fork")

	fm.mu.RLock()
	require.Equal(t, forkA, fm.blockToFork[*blockA.Hash()])
	require.Equal(t, forkB, fm.blockToFork[*blockB.Hash()])
	require.Len(t, fm.forks, 2, "each distinct orphan must create its own fork")
	fm.mu.RUnlock()
}
