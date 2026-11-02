package blockvalidation

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_RejectsForkBelowCheckpoint pins bitcoin-sv's bad-fork-prior-to-checkpoint
// on the direct (non-catchup) path. A side block below a checkpoint the tip has passed is
// validated at its own height, so below Genesis or Chronicle it gets the older script rules and
// its txs can reach block assembly (issue 4940).
func TestValidateBlock_RejectsForkBelowCheckpoint(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	countingClient := &countingSubtreeValidationClient{Interface: subtreeValidationClient}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	// Checkpoint at 1000, block at height 1.
	block, mockBlockchain, _ := belowCheckpointPoWFixture(t, tSettings)

	// The fixture's header deliberately fails its own target; make it pass so the block
	// reaches the checkpoint rules.
	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	block.Header.Bits = *nBits
	for ok, _, _ := block.Header.HasMetTargetDifficulty(); !ok; ok, _, _ = block.Header.HasMetTargetDifficulty() {
		block.Header.Nonce++
	}

	// The tip has passed the checkpoint.
	for _, call := range mockBlockchain.ExpectedCalls {
		if call.Method == "GetBestBlockHeader" {
			call.ReturnArguments[1] = &model.BlockHeaderMeta{Height: 1500}
		}
	}
	mockBlockchain.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(false, nil).Maybe()
	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, countingClient)

	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a fork below the checkpoint must surface as BlockInvalid, got: %v", err)
	require.Contains(t, err.Error(), "bad-fork-prior-to-checkpoint")

	require.Equal(t, int32(0), countingClient.checkBlockSubtreesCalls.Load(),
		"a fork below the checkpoint must be rejected before its transactions are validated")
	mockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestValidateBlock_RejectsForkBelowCheckpointPassedDuringValidation pins the re-check
// before the block is stored: the tip is below the checkpoint when validation starts and
// passes it while the block's subtrees are checked, so the block must not be stored.
func TestValidateBlock_RejectsForkBelowCheckpointPassedDuringValidation(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()

	utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
	defer cleanup()

	passingClient := &passingSubtreeValidationClient{Interface: subtreeValidationClient}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	// Checkpoint at 1000, block at height 1.
	block, mockBlockchain, subtree := belowCheckpointPoWFixture(t, tSettings)

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	subtreeMetaBytes, err := subtreepkg.NewSubtreeMeta(subtree).Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes))

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(block.CoinbaseTx.TxIDChainHash(), 0, uint64(block.CoinbaseTx.Size())) //nolint:gosec
	block.Header.HashMerkleRoot = replicated.RootHash()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)
	block.Header.Bits = *nBits
	for ok, _, _ := block.Header.HasMetTargetDifficulty(); !ok; ok, _, _ = block.Header.HasMetTargetDifficulty() {
		block.Header.Nonce++
	}

	// The first tip lookup is below the checkpoint, every later one has passed it.
	for _, call := range mockBlockchain.ExpectedCalls {
		if call.Method == "GetBestBlockHeader" {
			call.ReturnArguments[1] = &model.BlockHeaderMeta{Height: 900}
			call.Once()
		}
	}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(block.Header, &model.BlockHeaderMeta{Height: 1500}, nil)
	mockBlockchain.On("GetNextWorkRequired", mock.Anything, mock.Anything, mock.Anything).Return(nBits, nil).Maybe()
	mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil).Maybe()
	mockBlockchain.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).Return(nil).Maybe()
	mockBlockchain.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("no main-chain header")).Maybe()
	mockBlockchain.On("GetBlockExists", mock.Anything, block.Header.Hash()).Return(false, nil).Maybe()
	mockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, passingClient)

	err = bv.ValidateBlock(ctx, block, "test")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a fork below the checkpoint must surface as BlockInvalid, got: %v", err)
	require.Contains(t, err.Error(), "bad-fork-prior-to-checkpoint")
	require.Equal(t, int32(1), passingClient.checkBlockSubtreesCalls.Load(), "the block must reach subtree validation before the tip passes the checkpoint")
	mockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// passingSubtreeValidationClient reports every block's subtrees valid and counts the calls.
type passingSubtreeValidationClient struct {
	subtreevalidation.Interface
	checkBlockSubtreesCalls atomic.Int32
}

func (c *passingSubtreeValidationClient) CheckBlockSubtrees(context.Context, *model.Block, string, string, bool) error {
	c.checkBlockSubtreesCalls.Add(1)
	return nil
}

// TestValidateBlock_StoredForkBelowCheckpoint pins the checkpoint fork rule on a block that
// is already stored as valid: one that is not on the main chain below a checkpoint the tip
// has passed is refused, while a main-chain block (an ancestor of the checkpoint) still
// returns as already validated.
func TestValidateBlock_StoredForkBelowCheckpoint(t *testing.T) {
	for _, onMainChain := range []bool{false, true} {
		t.Run(fmt.Sprintf("onMainChain=%v", onMainChain), func(t *testing.T) {
			initPrometheusMetrics()

			ctx := context.Background()

			utxoStore, subtreeValidationClient, _, txStore, subtreeStore, cleanup := setup(t)
			defer cleanup()

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.OptimisticMining = false

			// Checkpoint at 1000, block at height 1, tip at 1500.
			block, mockBlockchain, _ := belowCheckpointPoWFixture(t, tSettings)

			for _, call := range mockBlockchain.ExpectedCalls {
				switch call.Method {
				case "GetBlockExists":
					call.ReturnArguments = mock.Arguments{true, nil}
				case "GetBestBlockHeader":
					call.ReturnArguments[1] = &model.BlockHeaderMeta{Height: 1500}
				}
			}

			mockBlockchain.On("GetBlockHeader", mock.Anything, block.Header.Hash()).
				Return(block.Header, &model.BlockHeaderMeta{Height: block.Height}, nil)

			// A stale sibling at the same height is what a lookup by height would find.
			mockBlockchain.On("GetBlockHeadersFromHeight", mock.Anything, mock.Anything, mock.Anything).
				Return([]*model.BlockHeader{{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{0x02}}}, []*model.BlockHeaderMeta{{Height: block.Height}}, nil).Maybe()
			mockBlockchain.On("GetBlockHeaderIDs", mock.Anything, block.Header.Hash(), uint64(1)).Return([]uint32{7}, nil)
			mockBlockchain.On("CheckBlockIsInCurrentChain", mock.Anything, []uint32{7}).Return(onMainChain, nil)

			bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, mockBlockchain, subtreeStore, txStore, utxoStore, nil, subtreeValidationClient)

			err := bv.ValidateBlock(ctx, block, "test")
			if onMainChain {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
			require.Contains(t, err.Error(), "bad-fork-prior-to-checkpoint")
		})
	}
}
