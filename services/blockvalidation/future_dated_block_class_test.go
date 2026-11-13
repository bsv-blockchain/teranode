//go:build bvgaps

package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_FutureDatedBlock_IsNotClassifiedInvalid checks the class block validation gives a
// block stamped more than two hours ahead of the local clock, with and without optimistic mining.
//
// Spec: BV-HEADER-006 and the CLOCK-DEFERRED row of BV-VERDICT-001 (tracked as GAP-BV-050).
//
// Scenario: Given a coinbase-only block whose header is mined with the right difficulty and stamped
// three hours after now. When ValidateBlockWithOptions runs it, with optimistic mining off and on,
// Then it is rejected and nothing is persisted, but the error is not ERR_BLOCK_INVALID: the outcome
// depends on the local clock, so it is not a verdict on the block.
//
// Today both paths return ERR_BLOCK_INVALID. TestValidateBlock_Difficulty1Header_IsNotPersisted's
// "contextual header failure under optimistic mining" subtest and
// TestBlockValidation_OptimisticMining_RejectsFutureTimestampSynchronously used to assert that class.
// That no invalid-block notification is published and the peer is not struck is proven on the
// docs/blockvalidation-spec branch by TestKnownDefect_FutureDatedBlock_IsNotPersistedNotifiedNorStruck.
func TestKnownDefect_FutureDatedBlock_IsNotClassifiedInvalid(t *testing.T) {
	const (
		aheadOfClock = 3 * time.Hour
		blockHeight  = uint32(1)
	)

	modes := []struct {
		name             string
		optimisticMining bool
	}{
		{"optimistic mining off", false},
		{"optimistic mining on", true},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.ChainCfgParams.Checkpoints = nil
			tSettings.BlockValidation.OptimisticMining = mode.optimisticMining
			tSettings.BlockValidation.OptimisticMiningPeerBlocks = mode.optimisticMining

			bv, client := newNoPersistHarness(ctx, t, tSettings)

			timestamp := uint32(time.Now().Add(aheadOfClock).Unix()) //nolint:gosec

			bits, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
			require.NoError(t, err)

			coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
			header := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *bits, timestamp)

			block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0) //nolint:gosec
			require.NoError(t, err)

			err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})

			require.Error(t, err)
			requireNothingPersisted(ctx, t, client, block.Hash())
			require.False(t, errors.Is(err, errors.ErrBlockInvalid),
				"BV-HEADER-006: a block ahead of the local clock is CLOCK-DEFERRED, not a verdict on the block, got: %v", err)
		})
	}
}
