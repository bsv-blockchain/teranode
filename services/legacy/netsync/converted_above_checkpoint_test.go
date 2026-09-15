package netsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestHandleConvertedBlock_CommitsAboveTheCheckpointWithoutTheUnifiedFlag is
// task 13's required end-to-end test (task-13-brief.md Step 2), driving a
// block above the chain's final checkpoint through the real pipelineBlockSink
// and the real HandleConvertedBlock, with NEITHER
// BlockValidation.LegacyUnifiedBelowCheckpoint NOR
// BlockValidation.OutpointOnlyBelowCheckpoint set — unlike every other test in
// handle_converted_block_test.go, which sets both so their below-checkpoint
// fixtures are eligible for the (now-removed) legacyUnified gate. This is the
// one case that gate used to refuse outright, and it must now commit exactly
// like the below-checkpoint tests beside it.
//
// The checkpoint is newPipelineManager's own default (height 1000), left
// untouched, and the fixture's parent is stubbed at height 1000 so the block
// under test resolves to height 1001 — genuinely above a real, positive,
// configured checkpoint. An earlier version of this test instead emptied the
// checkpoint out to height 0, which HighestCheckpointHeight
// (model/checkpoint.go) reads as "no checkpoints configured" rather than
// "above the highest one": behaviourally identical for quickValidationAllowed
// (both read false), but not the shape a reader should be shown as "above the
// checkpoint". Reaching height 1000 with 1000 real, chained, committed blocks
// is not practical in a unit test, so the parent is stubbed directly on
// blockchainClient instead of built.
//
// Infrastructure is otherwise the same real stack pipeline_sink_test.go and
// handle_converted_block_test.go already use: a real subtree blob store and a
// real blockPark over an in-memory blob store. blockValidation is
// convertedRouteSpyValidation, the same spy every other test in
// handle_converted_block_test.go uses, and blockchainClient is a
// blockchain2.Mock rather than newPipelineManager's real sqlitememory-backed
// client, stubbed only for the two calls this route actually makes
// (GetBlockExists, GetBlockHeader) — swapped in after construction, so the
// subtree store and park underneath it are still the real thing this test
// checks bytes against.
//
// HONEST LIMIT, stated here and in this task's commit message: this does NOT
// reach real validation or a real Kafka round trip. No test in this package
// constructs a real blockvalidation.Server — every one of them, this one
// included, stands in a spy for it, because driving a block through the real
// server needs a live SV Node / the full service wiring this package's tests
// do not build (see TestHandleConvertedBlock_CommitsWithoutTheBlock's own doc
// comment, which states the identical limit for the below-checkpoint case).
// What this test actually proves: a converted record built above the
// checkpoint, carrying blockID 0, reaches blockValidation.ProcessBlock with
// the correct height and content, through the real routing and height
// re-derivation HandleConvertedBlock performs — the class of change that
// wedged mainnet at block 386,817 on 2026-05-31 was a downstream consumer
// nobody had listed, which a spy standing in for that exact consumer cannot
// surface any more than the below-checkpoint tests already in this file can.
// The honest fallback for that gap is a soak on a live node, not a unit test.
func TestHandleConvertedBlock_CommitsAboveTheCheckpointWithoutTheUnifiedFlag(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	// Left off deliberately, to say plainly that this test does not depend on
	// them: quickValidationAllowed alone (BelowCheckpoint) is what decides
	// eligibility for conversion now, not legacyUnified.
	sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint = false
	sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint = false
	sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

	blk := wireBlockWithTxs(t, 6, false)
	blk.MsgBlock().Header.Bits = 0x207fffff
	pipelineHeaderFixture(t, sm, blk) // sets a genesis parent and a correct merkle root

	// Re-parent onto a stand-in for a real block at height 1000, the manager's
	// own default checkpoint (newPipelineManager). Neither the merkle root
	// (computed from the transactions above, not the parent) nor the mined
	// nonce below depends on which parent hash this is — this must simply be
	// a hash that is not genesis, so the stub below is unambiguously what
	// answers it.
	parent := chainhash.HashH([]byte("task13-above-checkpoint-parent-at-height-1000"))
	blk.MsgBlock().Header.PrevBlock = parent

	mineRegtestPoW(t, blk)
	body := blockBodyBytes(t, blk)

	// blockchainClient stubbed for every call this route makes: GetBlockExists
	// (HandleConvertedBlock's own first check), GetBlockHeader for the
	// stand-in parent (both pipelineParentHeight, at conversion time, and
	// HandleConvertedBlock's own re-derivation, at commit time, ask the
	// identical question of the identical hash), and GetBlockIsMined — needed
	// here for a reason specific to this test: needsParentMinedWait
	// (handle_block.go) skips the wait only on the below-checkpoint
	// outpoint-only fast path, which this test deliberately leaves off, so
	// above the checkpoint the wait runs exactly as it does on the ordinary
	// route (see the corrected comment at its call site).
	mockClient := &blockchain2.Mock{}
	mockClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	mockClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1000}, nil)
	mockClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	sm.blockchainClient = mockClient

	spy := &convertedRouteSpyValidation{}
	sm.blockValidation = spy
	// sm.blockAssembly stays nil: WaitForBlockAssemblyReady treats a nil
	// client as "skip" (util/blockassemblyutil), the same as every other test
	// in this file.

	converted, err := sm.pipelineBlockSink(*blk.Hash(), &blk.MsgBlock().Header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block above the checkpoint must convert cleanly, not be refused")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing")

	// Confirm the premise directly rather than trusting the checkpoint
	// arithmetic: this record must actually be above the checkpoint (not
	// eligible for quick validation), or this test is not exercising the case
	// it claims to.
	require.False(t, sm.quickValidationAllowed(headerProven, 1001), "sanity: height 1001 must read as above the checkpoint (1000) for this test to mean anything")

	record, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, uint32(1001), record.Height, "sanity: the stubbed parent is at height 1000, so the resolved height must be 1001")

	for _, h := range record.Subtrees {
		toCheck, existsErr := store.Exists(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, existsErr)
		require.True(t, toCheck, "above the checkpoint the structure file must be FileTypeSubtreeToCheck")
	}

	err = sm.HandleConvertedBlock(ctx, nil, *blk.Hash(), record)
	require.NoError(t, err, "a valid converted record above the checkpoint must commit, with neither legacyUnified conjunct set")

	require.Equal(t, 1, spy.callCount(), "HandleConvertedBlock must hand the record to blockValidation exactly once")

	call := spy.lastCall()
	require.Equal(t, record.Header.Hash().String(), call.block.Header.Hash().String(), "the committed block must be the record's own")
	require.Equal(t, record.Height, call.blockHeight, "the committed height must be the record's own height, re-verified against the chain")
	require.Equal(t, uint32(0), call.blockID, "blockID must be 0 above the checkpoint too: the universal assign-server-side convention full validation reads (Step 1 of task 13)")
	require.Equal(t, uint64(6), call.block.TransactionCount, "the record's transaction count must reach the committer unchanged")
	require.Equal(t, len(record.Subtrees), len(call.block.Subtrees), "the record's subtree list must reach the committer unchanged")
}
