package blockvalidation

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestKeepBlockAssemblyHandoff pins issue 2067's rule: a catchup keeps handing unseen
// transactions to block assembly only when it starts from RUNNING and fetches no more than
// CatchupKeepBlockAssemblyMaxBlocks blocks. A long run, a run from any other state, a failed
// state read and a bound of zero all keep the hand-off off, as before the rule existed.
func TestKeepBlockAssemblyHandoff(t *testing.T) {
	const bound = 6

	cases := []struct {
		name     string
		state    blockchain.FSMStateType
		stateErr error
		blocks   int
		bound    int
		want     bool
	}{
		{name: "short run from RUNNING", state: blockchain.FSMStateRUNNING, blocks: 2, bound: bound, want: true},
		{name: "run of exactly the bound from RUNNING", state: blockchain.FSMStateRUNNING, blocks: bound, bound: bound, want: true},
		{name: "run above the bound from RUNNING", state: blockchain.FSMStateRUNNING, blocks: bound + 1, bound: bound, want: false},
		{name: "short run from IDLE", state: blockchain.FSMStateIDLE, blocks: 2, bound: bound, want: false},
		{name: "short run already in CATCHINGBLOCKS", state: blockchain.FSMStateCATCHINGBLOCKS, blocks: 2, bound: bound, want: false},
		{name: "state read fails", stateErr: errors.NewServiceError("blockchain unavailable"), blocks: 2, bound: bound, want: false},
		{name: "bound of zero turns the rule off", state: blockchain.FSMStateRUNNING, blocks: 1, bound: 0, want: false},
	}

	blockUpTo := testhelpers.CreateTestBlockChain(t, 2)[1]

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.CatchupKeepBlockAssemblyMaxBlocks = tc.bound

			bc := &blockchain.Mock{}
			if tc.stateErr != nil {
				bc.On("GetFSMCurrentState", mock.Anything).Return((*blockchain.FSMStateType)(nil), tc.stateErr).Maybe()
			} else {
				state := tc.state
				bc.On("GetFSMCurrentState", mock.Anything).Return(&state, nil).Maybe()
			}

			srv := &Server{logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: bc}

			got := srv.keepBlockAssemblyHandoff(context.Background(), &CatchupContext{blockUpTo: blockUpTo}, tc.blocks)
			require.Equal(t, tc.want, got, fmt.Sprintf("state=%s blocks=%d bound=%d", tc.state, tc.blocks, tc.bound))
		})
	}
}

// handoffRecordingClient records the service-boundary flag while allowing the real
// subtree validation and sqlitememory-backed block validation to finish.
type handoffRecordingClient struct {
	subtreevalidation.Interface
	handoffs []bool
}

func (c *handoffRecordingClient) CheckBlockSubtrees(ctx context.Context, block *model.Block, peerID, baseURL string, keep bool) error {
	c.handoffs = append(c.handoffs, keep)
	return c.Interface.CheckBlockSubtrees(ctx, block, peerID, baseURL, keep)
}

// TestValidateBlocksOnChannel_ForwardsBlockAssemblyHandoff exercises the options
// chain from the catch-up consumer through full validation to CheckBlockSubtrees.
// It also checks the accepted block in the real store, so an early exit cannot
// satisfy the flag assertion without completing validation.
func TestValidateBlocksOnChannel_ForwardsBlockAssemblyHandoff(t *testing.T) {
	initPrometheusMetrics()
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep=%t", keep), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			utxoStore, subtreeClient, localClient, txStore, subtreeStore, cleanup := setup(t)
			defer cleanup()
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.OptimisticMining = false
			recorder := &handoffRecordingClient{Interface: subtreeClient}

			// A coinbase-only subtree still crosses CheckBlockSubtrees, while keeping
			// the fixture focused on block validation rather than transaction policy.
			block := testhelpers.CreateTestBlocksWithPrev(t, 1, tSettings.ChainCfgParams.GenesisHash)[0]
			block.Height = 1
			block.TransactionCount = 1
			block.SizeInBytes = uint64(block.CoinbaseTx.Size())
			subtree, err := subtreepkg.NewTreeByLeafCount(2)
			require.NoError(t, err)
			require.NoError(t, subtree.AddCoinbaseNode())
			block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
			subtreeBytes, err := subtree.Serialize()
			require.NoError(t, err)
			require.NoError(t, subtreeStore.Set(ctx, block.Subtrees[0][:], fileformat.FileTypeSubtree, subtreeBytes))
			subtreeMeta := subtreepkg.NewSubtreeMeta(subtree)
			metaBytes, err := subtreeMeta.Serialize()
			require.NoError(t, err)
			require.NoError(t, subtreeStore.Set(ctx, block.Subtrees[0][:], fileformat.FileTypeSubtreeMeta, metaBytes))

			bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, localClient, subtreeStore, txStore, utxoStore, nil, recorder, NoSetMined{})
			defer bv.StopCaches()
			srv := &Server{
				logger: ulogger.TestLogger{}, settings: tSettings, blockchainClient: localClient,
				blockValidation: bv, headerChainCache: catchup.NewHeaderChainCache(ulogger.TestLogger{}),
			}
			catchupCtx := &CatchupContext{blockUpTo: block, baseURL: "http://peer", peerID: "peer", keepBlockAssemblyHandoff: keep}
			input := make(chan blockForValidation, 1)
			input <- blockForValidation{block: block}
			close(input)
			var size atomic.Int64
			size.Store(1)
			require.NoError(t, srv.validateBlocksOnChannel(input, ctx, catchupCtx, &size, nil))
			require.Equal(t, []bool{keep}, recorder.handoffs)
			require.Zero(t, size.Load())
			stored, err := localClient.GetBlock(ctx, block.Hash())
			require.NoError(t, err)
			require.Equal(t, block.Hash(), stored.Hash())
			require.Equal(t, block.Subtrees, stored.Subtrees)
		})
	}
}
