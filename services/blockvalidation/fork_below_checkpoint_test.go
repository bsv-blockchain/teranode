package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
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
}
