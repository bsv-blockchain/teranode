package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/mining"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

func TestMiningCandidateSurvivesSameTipMmapReset(t *testing.T) {
	common := testutil.NewCommonTestSetup(t)
	common.Settings.BlockAssembly.SubtreeMmapDir = t.TempDir()
	common.Settings.BlockAssembly.InitialMerkleItemsPerSubtree = 2
	common.Settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	ctx, cancel := context.WithCancel(common.Ctx)
	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	subtreeStore := testutil.NewMemoryBlobStore()
	server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
	server.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, server.Init(ctx))
	t.Cleanup(func() {
		cancel()
		require.NoError(t, server.Stop(context.Background()))
		server.blockAssembler.Wait()
	})
	require.NoError(t, server.blockAssembler.Start(ctx))
	tx := newTx(73)
	_, _, err := utxoStore.SpendAndCreate(ctx, tx, 0, utxo.WithCreateOnly())
	require.NoError(t, err)
	txID := *tx.TxIDChainHash()
	require.True(t, server.blockAssembler.AddTxBatchIfRoom([]subtreepkg.Node{{Hash: txID, Fee: 17, SizeInBytes: uint64(tx.Size())}}, []*subtreepkg.TxInpoints{{}}))
	require.Eventually(t, func() bool {
		data := server.blockAssembler.subtreeProcessor.GetPrecomputedMiningData()
		if data == nil {
			return false
		}
		defer data.Lease.Release()
		return len(data.Subtrees) == 1 && server.blockAssembler.GetCurrentRunningState() == StateRunning
	}, time.Second, time.Millisecond)
	var candidate *model.MiningCandidate
	require.Eventually(t, func() bool {
		value, candidateErr := server.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{})
		if candidateErr != nil || value.NumTxs != 1 {
			return false
		}
		candidate = value
		return true
	}, time.Second, time.Millisecond, "wait for the completed-subtree job after startup reconciliation")
	id, err := chainhash.NewHash(candidate.Id)
	require.NoError(t, err)
	job := server.jobStore.Get(*id).Value()
	require.NotNil(t, job.Lease)
	require.True(t, job.Subtrees[0].IsMmapBacked())
	before, err := server.GetCandidateBlock(ctx, &blockassembly_api.GetCandidateBlockRequest{Id: candidate.Id})
	require.NoError(t, err)
	header, _ := server.blockAssembler.CurrentBlock()
	err = server.blockAssembler.subtreeProcessor.RecoverUnmined(ctx, header, nil, func(_ context.Context, hashes []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxo.UnminedTransaction, error) {
		require.Contains(t, hashes, txID)
		require.True(t, accepted(txID))
		return []*utxo.UnminedTransaction{{Node: &subtreepkg.Node{Hash: txID, Fee: 17, SizeInBytes: uint64(tx.Size())}, TxInpoints: &subtreepkg.TxInpoints{}}}, nil
	})
	require.NoError(t, err)
	after, err := server.GetCandidateBlock(ctx, &blockassembly_api.GetCandidateBlockRequest{Id: candidate.Id})
	require.NoError(t, err)
	require.Equal(t, before.TransactionCount, after.TransactionCount)
	require.Equal(t, before.SubtreeHashes, after.SubtreeHashes)
	require.Equal(t, txID, job.Subtrees[0].Nodes[1].Hash)
	solution, err := mining.Mine(ctx, common.Settings, candidate, nil)
	require.NoError(t, err)
	response, err := server.submitMiningSolution(ctx, &BlockSubmissionRequest{SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
		Id: solution.Id, Nonce: solution.Nonce, CoinbaseTx: solution.Coinbase, Time: solution.Time, Version: solution.Version,
	}})
	require.NoError(t, err, "a valid issued mining job must remain submittable after same-tip recovery")
	require.True(t, response.Ok)
	require.Eventually(t, func() bool {
		lease, retained := job.Lease.Retain()
		lease.Release()
		return !retained
	}, time.Second, time.Millisecond, "job eviction must release ownership")
}
