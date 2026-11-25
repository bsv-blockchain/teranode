package blockassembly

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/mining"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/bsv-blockchain/teranode/util/testutil/blobfault"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 3, end to end through Init: with blockassembly_storedPrefixTemplate on, a
// mining candidate is cut at the stored prefix by the one set the store workers fill and the
// subtree processor reads.

// startStoredPrefixServer runs a started block assembly with subtrees of 4 leaves, whose subtree
// writes wait on the returned channel until it is closed.
func startStoredPrefixServer(t *testing.T, storedPrefix bool) (server *BlockAssembly, release func()) {
	t.Helper()

	initPrometheusMetrics()

	common := testutil.NewCommonTestSetup(t)
	common.Settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	common.Settings.BlockAssembly.MinimumMerkleItemsPerSubtree = 4
	common.Settings.BlockAssembly.SubmitMiningSolutionWaitForResponse = true
	common.Settings.BlockAssembly.StoredPrefixTemplate = storedPrefix

	ctx, cancel := context.WithCancel(common.Ctx)
	realBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	server = New(common.Logger, common.Settings, nil, utxoStore, memory.New(), realBlockchainClient)
	server.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, server.Init(ctx))
	require.NoError(t, realBlockchainClient.Run(ctx, "blockassembly-test"))

	gate := make(chan struct{})
	fault := blobfault.New(server.subtreeStore)
	fault.OnSetAny(fileformat.FileTypeSubtree, blobfault.Rule{Block: gate})
	server.subtreeStore = fault

	var released bool

	release = func() {
		if !released {
			released = true

			close(gate)
		}
	}

	t.Cleanup(func() {
		release()
		cancel()
		_ = server.Stop(context.Background())

		if server.blockAssembler != nil {
			server.blockAssembler.Wait()
		}
	})

	require.NoError(t, server.blockAssembler.Start(ctx))
	require.Eventually(t, func() bool {
		return server.blockAssembler.GetCurrentRunningState() == StateRunning
	}, 5*time.Second, 50*time.Millisecond)

	return server, release
}

// addCandidateTxs adds n transactions: with 4 leaves per subtree (the first holds the coinbase), 10
// complete three subtrees and leave the open one with a transaction in it.
func addCandidateTxs(t *testing.T, server *BlockAssembly, n int) {
	t.Helper()

	parentHash := chainhash.HashH([]byte("stored-prefix-template-parent"))
	for i := range n {
		txHash := chainhash.HashH(fmt.Appendf(nil, "stored-prefix-template-tx-%d", i))
		require.Equal(t, subtreeprocessor.Admitted, server.blockAssembler.AddTxBatchBounded(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(1000 + i), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{singleParentInpointsPtr(parentHash, uint32(i))},
			false,
			nil,
		))
	}
}

func candidateSubtreeCount(t *testing.T, server *BlockAssembly) uint32 {
	t.Helper()

	candidate, err := server.GetMiningCandidate(t.Context(), &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
	require.NoError(t, err)

	return candidate.SubtreeCount
}

func TestStoredPrefixTemplate_NothingStoredIsAnEmptyBlockAndTheCandidateGrowsAsSubtreesStore(t *testing.T) {
	server, release := startStoredPrefixServer(t, true)
	require.NotNil(t, server.storedSet, "Init builds the set when the setting is on")

	addCandidateTxs(t, server, 12)

	require.Eventually(t, func() bool { return len(server.blockAssembler.subtreeProcessor.GetChainedSubtrees()) >= 3 },
		10*time.Second, 20*time.Millisecond, "subtrees completed")

	// no subtree write has finished, and the open subtree holds transactions: the candidate is an
	// empty block, not the open subtree alone
	require.Zero(t, candidateSubtreeCount(t, server), "nothing stored")

	release()

	require.Eventually(t, func() bool { return candidateSubtreeCount(t, server) >= 3 },
		10*time.Second, 50*time.Millisecond, "the candidate grows as the stores finish")
}

func TestStoredPrefixTemplate_AJobWhoseSubtreesAreAllStoredSubmits(t *testing.T) {
	server, release := startStoredPrefixServer(t, true)
	release()

	addCandidateTxs(t, server, 12)

	var candidate *model.MiningCandidate

	require.Eventually(t, func() bool {
		var err error

		candidate, err = server.GetMiningCandidate(t.Context(), &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})

		return err == nil && candidate != nil && candidate.SubtreeCount >= 3
	}, 10*time.Second, 50*time.Millisecond)

	solution, err := mining.Mine(t.Context(), server.settings, candidate, nil)
	require.NoError(t, err)

	resp, err := server.SubmitMiningSolution(t.Context(), &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         candidate.Id,
		Nonce:      solution.Nonce,
		Time:       solution.Time,
		Version:    solution.Version,
		CoinbaseTx: solution.Coinbase,
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

func TestStoredPrefixTemplate_OffByDefaultBuildsTheTemplateFromEverySubtree(t *testing.T) {
	server, _ := startStoredPrefixServer(t, false)
	require.Nil(t, server.storedSet)

	addCandidateTxs(t, server, 12)

	// the writes are held, and the template does not wait for them
	require.Eventually(t, func() bool { return candidateSubtreeCount(t, server) >= 3 },
		10*time.Second, 50*time.Millisecond)
}
