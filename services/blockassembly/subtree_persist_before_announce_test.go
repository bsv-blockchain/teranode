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
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

// addBlockSpyClient wraps a real blockchain.ClientI and, at the instant AddBlock
// is invoked (i.e. the block is announced to the rest of the network), inspects
// the Subtree Store to record any subtree referenced by the block that is NOT
// yet retrievable. It then delegates to the wrapped client so the submission
// completes normally.
//
// Interface embedding promotes every blockchain.ClientI method; only AddBlock is
// overridden.
type addBlockSpyClient struct {
	blockchain.ClientI

	store blob.Store

	mu             sync.Mutex
	addBlockCalled bool
	missing        []string // string form of subtree hashes missing at announce time
	referenced     int      // number of subtrees referenced by the announced block
}

func (c *addBlockSpyClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	c.mu.Lock()
	c.addBlockCalled = true
	c.referenced = len(block.Subtrees)
	for _, subtreeHash := range block.Subtrees {
		// BA-SUBTREE-007: the subtree must already be persisted in the Subtree
		// Store at the moment the block is announced.
		if _, err := c.store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree); err != nil {
			c.missing = append(c.missing, subtreeHash.String())
		}
	}
	c.mu.Unlock()

	return c.ClientI.AddBlock(ctx, block, peerID, opts...)
}

// TestSubtreesPersistedBeforeBlockAnnounced pins BA-SUBTREE-007 and its
// acceptance criterion AC-BA-SUBTREE-007.1: for any block this node submits,
// every subtree referenced by that block MUST be persisted in (retrievable from)
// the Subtree Store before the block itself is announced.
//
// The test drives a full mining-solution submission for a block that references a
// real, completed subtree, and asserts — via a blockchain client that snapshots
// the Subtree Store at the exact moment AddBlock fires — that no referenced
// subtree was missing from the store when the block was announced.
func TestSubtreesPersistedBeforeBlockAnnounced(t *testing.T) {
	initPrometheusMetrics()

	common := testutil.NewCommonTestSetup(t)

	// Small subtrees so a single batch of synthetic transactions completes a
	// subtree (which the assembly pipeline persists) without needing millions of
	// txs. The first subtree reserves index 0 for the coinbase placeholder, so a
	// size-4 subtree completes after 3 real transactions.
	const subtreeSize = 4
	common.Settings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	common.Settings.BlockAssembly.MinimumMerkleItemsPerSubtree = subtreeSize
	// Block until the submission has actually been processed so we can assert on
	// what the AddBlock spy observed.
	common.Settings.BlockAssembly.SubmitMiningSolutionWaitForResponse = true

	subtreeStore := memory.New()

	ctx, cancel := context.WithCancel(common.Ctx)

	realBlockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	spyClient := &addBlockSpyClient{ClientI: realBlockchainClient, store: subtreeStore}

	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	// txStore nil: the coinbase-tx persistence step is skipped, keeping the test
	// focused on the subtree-store / announce ordering.
	server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, spyClient)
	server.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, server.Init(ctx))

	// GetMiningCandidate (and therefore the whole mine→submit path) requires the
	// blockchain FSM to be in the RUNNING state.
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

	// Add enough transactions to complete the first subtree. With the coinbase
	// placeholder occupying index 0, subtreeSize-1 real txs fill it.
	parentHash := chainhash.HashH([]byte("persist-before-announce-parent"))
	for i := range subtreeSize - 1 {
		txHash := chainhash.HashH(fmt.Appendf(nil, "persist-before-announce-tx-%d", i))
		server.blockAssembler.AddTxBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: uint64(1000 + i), SizeInBytes: 250}},
			[]*subtreepkg.TxInpoints{singleParentInpointsPtr(parentHash, uint32(i))},
		)
	}

	// Wait for the completed subtree to be both reflected in a mining candidate
	// and persisted to the store. GetMiningCandidate reads in-memory precomputed
	// data, so the candidate can appear before the async storage listener writes
	// the subtree; we therefore wait until the referenced subtree is retrievable.
	var (
		candidate *model.MiningCandidate
		err       error
	)

	require.Eventually(t, func() bool {
		// The gRPC GetMiningCandidate registers the candidate as a job (required
		// by SubmitMiningSolution) and, with IncludeSubtrees, returns the
		// referenced subtree hashes so we can confirm they are persisted.
		candidate, err = server.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{IncludeSubtrees: true})
		if err != nil || candidate == nil || len(candidate.SubtreeHashes) == 0 {
			return false
		}
		// Every subtree the candidate references must be retrievable from the
		// store before we attempt to submit (block.Valid would otherwise reject).
		for _, subtreeHash := range candidate.SubtreeHashes {
			if _, getErr := subtreeStore.Get(ctx, subtreeHash, fileformat.FileTypeSubtree); getErr != nil {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "completed subtree was not persisted to the subtree store")

	require.NotEmpty(t, candidate.Id)
	require.GreaterOrEqual(t, len(candidate.SubtreeHashes), 1, "expected at least one completed subtree in the candidate")

	// Mine a valid solution for the candidate (regtest difficulty is trivial).
	solution, err := mining.Mine(ctx, server.settings, candidate, nil)
	require.NoError(t, err)
	require.NotNil(t, solution)

	// Submit the solution. With SubmitMiningSolutionWaitForResponse=true this
	// blocks until submitMiningSolution (and thus AddBlock) has run.
	// Pass the exact coinbase the nonce was mined against. Without it the server
	// recreates and mutates the coinbase, changing the merkle root so the nonce
	// would no longer meet the target.
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

	// BA-SUBTREE-007 assertions: the block was announced, it referenced at least
	// one subtree, and none of its subtrees were missing from the store at the
	// moment of announcement.
	spyClient.mu.Lock()
	defer spyClient.mu.Unlock()
	require.True(t, spyClient.addBlockCalled, "the block must have been announced via AddBlock")
	require.GreaterOrEqual(t, spyClient.referenced, 1, "the announced block must reference at least one subtree")
	require.Empty(t, spyClient.missing,
		"every subtree referenced by the announced block must be retrievable from the Subtree Store at announce time")
}
