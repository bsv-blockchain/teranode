package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The startup load must fail on a DiskTxMap storage error; reset's reload
// runs after reset has committed and must only report it. isReload selects
// which subtree processor entry point each load path uses, so every path is
// pinned for both values: the mock only accepts the expected method.
func TestLoadUnminedTransactions_IsReloadSelectsEntryPoint(t *testing.T) {
	initPrometheusMetrics()

	paths := []struct {
		name      string
		configure func(t *testing.T, ba *BlockAssembler)
		batched   bool
	}{
		{
			name: "batched",
			configure: func(_ *testing.T, ba *BlockAssembler) {
				ba.settings.BlockAssembly.UnminedLoadingBatchSize = 1024
			},
			batched: true,
		},
		{
			name: "sequential",
			configure: func(_ *testing.T, ba *BlockAssembler) {
				ba.settings.BlockAssembly.UnminedLoadingBatchSize = 0
			},
		},
		{
			name: "disk sort",
			configure: func(t *testing.T, ba *BlockAssembler) {
				ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = true
				ba.settings.BlockAssembly.UnminedTxDiskSortPaths = []string{t.TempDir()}
			},
			// The disk-sorted load adds the merged runs in batches.
			batched: true,
		},
	}

	for _, p := range paths {
		for _, isReload := range []bool{false, true} {
			t.Run(p.name+map[bool]string{false: "/startup", true: "/reload"}[isReload], func(t *testing.T) {
				ctx := context.Background()

				settings := test.CreateBaseTestSettings(t)
				settings.BlockAssembly.OnRestartValidateParentChain = false
				settings.BlockAssembly.UnminedTxDiskSortEnabled = false

				txs := []*utxo.UnminedTransaction{{
					Node:       &subtree.Node{Hash: chainhash.DoubleHashH([]byte("reload-routing-tx")), Fee: 1000, SizeInBytes: 250},
					TxInpoints: &subtree.TxInpoints{},
					CreatedAt:  1000,
				}}

				mockIterator := new(utxo.MockUnminedTxIterator)
				mockIterator.On("Next", mock.Anything).Return(txs, nil).Once()
				mockIterator.On("Next", mock.Anything).Return(nil, nil).Maybe()

				mockStore := new(utxo.MockUtxostore)
				mockStore.On("GetUnminedTxIterator").Return(mockIterator, nil).Once()

				// Only the expected entry point is set up; any other call fails
				// the mock.
				stp := &subtreeprocessor.MockSubtreeProcessor{}
				stp.On("GetCurrentBlockHeader").Return(blockHeader1, nil).Maybe()

				var method string

				switch {
				case p.batched && isReload:
					method = "AddNodesDirectlyReportOnly"
				case p.batched:
					method = "AddNodesDirectly"
				case isReload:
					method = "AddDirectlyReportOnly"
				default:
					method = "AddDirectly"
				}

				if p.batched {
					stp.On(method, mock.Anything, true).Return(nil).Once()
				} else {
					stp.On(method, mock.Anything, mock.Anything, true).Return(nil).Once()
				}

				stp.On("FlushDiskTxMapForLoad", mock.Anything, isReload).Return(nil).Once()

				blockchainClient := &blockchain.Mock{}
				blockchainClient.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)

				ba := &BlockAssembler{
					utxoStore:        mockStore,
					logger:           ulogger.TestLogger{},
					settings:         settings,
					subtreeProcessor: stp,
					blockchainClient: blockchainClient,
				}
				ba.setBestBlockHeader(nil, 100)
				p.configure(t, ba)

				require.NoError(t, ba.loadUnminedTransactions(ctx, isReload))
				stp.AssertExpectations(t)
			})
		}
	}
}
