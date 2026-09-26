package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleConvertedBlock_GateFailureOnWindowRouteIsALocalFault proves that when the
// block-assembly gate fails for a block on the quick-window route, the error
// HandleConvertedBlock returns is a local fault (errors.IsTransientLocalError), not
// a bare passthrough of WaitForBlockAssemblyReady's error — so a parked gate
// throttles re-delivery through the local-fault path instead of churning the
// sync peer as if the block itself were bad.
//
// Ported from the deleted HandleBlockDirect route: HandleConvertedBlock carries
// the identical windowRoute wrap (handle_block.go), so the same gate-failure
// classification is exercised here through the converted-record route instead.
func TestHandleConvertedBlock_GateFailureOnWindowRouteIsALocalFault(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)
	const parentHeight = uint32(499)
	const blockHeight = parentHeight + 1 // 500, below checkpointHeight

	tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
	tSettings.BlockValidation.LegacyUnifiedBelowCheckpoint = true
	tSettings.BlockValidation.QuickWindowBlocks = 2

	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: parentHeight}, nil)

	// Block assembly reports a height far behind the block under test, so the
	// gate never opens. The bound on how long the test waits for that to give
	// up comes from the short context below, not from these mocked calls —
	// WaitForBlockAssemblyReady retries up to 100 times on a linear backoff.
	blockAssembly := blockassembly.NewMock()
	blockAssembly.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{CurrentHeight: 0}, nil)

	sm := &SyncManager{
		settings:         tSettings,
		logger:           ulogger.TestLogger{},
		chainParams:      params,
		blockchainClient: blockchainClient,
		blockAssembly:    blockAssembly,
		utxoStore:        &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}},
	}

	require.True(t, sm.windowRoute(blockHeight), "precondition: this block must take the quick-window route")

	header := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{0x02},
		HashMerkleRoot: &chainhash.Hash{0x03},
	}
	blk, err := model.NewBlock(header, coinbaseTx(t), nil, 1, 0, 0, 0)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err = sm.HandleConvertedBlock(ctx, nil, *header.Hash(), blk)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "a parked gate on the window route must be a local fault, got: %v", err)
}
