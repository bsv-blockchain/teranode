package blockassembly

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/mining"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

// addBlockOptionsSpyClient wraps a real blockchain.ClientI and records the
// StoreBlockOptions passed to AddBlock, so the test can assert on exactly what
// block assembly asked the store to do rather than inferring it from the
// store's state afterwards (which SetBlockSubtreesSet would also leave true,
// masking a regression that dropped the option from AddBlock itself).
type addBlockOptionsSpyClient struct {
	blockchain.ClientI

	mu   sync.Mutex
	opts *options.StoreBlockOptions
}

func (c *addBlockOptionsSpyClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	c.mu.Lock()
	c.opts = options.ProcessStoreBlockOptions(opts...)
	c.mu.Unlock()

	return c.ClientI.AddBlock(ctx, block, peerID, opts...)
}

// TestSubmitMiningSolution_AddsBlockWithSubtreesSetTrue pins the p2p
// announce-after-subtrees-set fix's block assembly side: a locally mined
// block's own subtrees are already built and validated by block.Valid()
// before AddBlock runs, so AddBlock must be called with
// options.WithSubtreesSet(true). Without it, p2p would defer announcing a
// locally mined block until the later SetBlockSubtreesSet call's own
// notification, adding a needless round trip to the fastest, always-safe
// announcement case.
func TestSubmitMiningSolution_AddsBlockWithSubtreesSetTrue(t *testing.T) {
	initPrometheusMetrics()

	common := testutil.NewCommonTestSetup(t)

	const subtreeSize = 4
	common.Settings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	common.Settings.BlockAssembly.MinimumMerkleItemsPerSubtree = subtreeSize
	common.Settings.BlockAssembly.SubmitMiningSolutionWaitForResponse = true

	subtreeStore := memory.New()

	ctx, cancel := context.WithCancel(common.Ctx)

	realBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	spyClient := &addBlockOptionsSpyClient{ClientI: realBlockchainClient}

	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, spyClient)
	server.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, server.Init(ctx))

	require.NoError(t, realBlockchainClient.Run(ctx, "blockassembly-test"))

	t.Cleanup(func() {
		cancel()
		_ = server.Stop(context.Background())
		if server.blockAssembler != nil {
			server.blockAssembler.Wait()
		}
	})

	require.NoError(t, server.blockAssembler.Start(ctx))

	require.Eventually(t, func() bool {
		return server.blockAssembler.GetCurrentRunningState() == StateRunning
	}, 5*time.Second, 50*time.Millisecond, "block assembler did not reach Running state")

	parentHash := chainhash.HashH([]byte("mined-block-subtrees-set-parent"))
	for i := range subtreeSize - 1 {
		txHash := chainhash.HashH(fmt.Appendf(nil, "mined-block-subtrees-set-tx-%d", i))
		require.True(t, server.blockAssembler.AddTxBatchIfRoom(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(1000 + i), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{singleParentInpointsPtr(parentHash, uint32(i))},
		))
	}

	var (
		candidate *model.MiningCandidate
		err       error
	)

	require.Eventually(t, func() bool {
		candidate, err = server.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
		if err != nil || candidate == nil || len(candidate.SubtreeHashes) == 0 {
			return false
		}
		for _, subtreeHash := range candidate.SubtreeHashes {
			if _, getErr := subtreeStore.Get(ctx, subtreeHash, fileformat.FileTypeSubtree); getErr != nil {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "completed subtree was not persisted to the subtree store")

	require.NotEmpty(t, candidate.Id)

	solution, err := mining.Mine(ctx, server.settings, candidate, nil)
	require.NoError(t, err)
	require.NotNil(t, solution)

	resp, err := server.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         candidate.Id,
		Nonce:      solution.Nonce,
		Time:       solution.Time,
		Version:    solution.Version,
		CoinbaseTx: solution.Coinbase,
	})
	require.NoError(t, err, "mining solution submission must succeed")
	require.NotNil(t, resp)
	require.True(t, resp.Ok)

	spyClient.mu.Lock()
	capturedOpts := spyClient.opts
	spyClient.mu.Unlock()

	require.NotNil(t, capturedOpts, "AddBlock must have been called")
	require.True(t, capturedOpts.SubtreesSet, "AddBlock for a locally mined block must be called with WithSubtreesSet(true)")

	// Cross-check against the store itself (sqlitememory-backed, not a mock):
	// the option must actually have been persisted, not merely passed.
	// GetBestBlockHeader's meta does not populate SubtreesSet (it does not
	// select that column at all, unrelated to this fix), so look the mined
	// block up by hash instead, via GetBlockHeader, which does.
	bestHeader, _, err := realBlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	_, meta, err := realBlockchainClient.GetBlockHeader(ctx, bestHeader.Hash())
	require.NoError(t, err)
	require.True(t, meta.SubtreesSet, "the stored block must have subtrees_set true")
}
