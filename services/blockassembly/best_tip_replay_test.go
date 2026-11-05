package blockassembly

import (
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobopts "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// recordedMove is one Reorg call on the subtree processor: a catch-up has no moveBack.
type recordedMove struct {
	back    []string
	forward []string
}

// recordMoves replaces the subtree processor with a mock that accepts every Reorg and
// MoveForwardBlock and records them, in order, as moves.
func recordMoves(t *testing.T, items *baTestItems) *[]recordedMove {
	t.Helper()

	moves := &[]recordedMove{}

	hashes := func(blocks []*model.Block) []string {
		out := make([]string, len(blocks))
		for i, b := range blocks {
			out[i] = b.Header.Hash().String()
		}

		return out
	}

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Reorg", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*moves = append(*moves, recordedMove{
			back:    hashes(args.Get(0).([]*model.Block)),
			forward: hashes(args.Get(1).([]*model.Block)),
		})
	})
	mockStp.On("MoveForwardBlock", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*moves = append(*moves, recordedMove{forward: hashes([]*model.Block{args.Get(0).(*model.Block)})})
	})
	injectMockStp(t, items, mockStp)

	return moves
}

func headerHashes(headers ...*model.BlockHeader) []string {
	out := make([]string, len(headers))
	for i, h := range headers {
		out[i] = h.Hash().String()
	}

	return out
}

// startReplayAt puts block assembly on tip with the replay position on tip, as if it had
// handled every block up to it.
func startReplayAt(t *testing.T, items *baTestItems, tip *model.BlockHeader, height uint32) model.BestTipKey {
	t.Helper()

	items.blockAssembler.setBestBlockHeader(tip, height)

	pos := bestTipKeyOf(t, items, tip)

	items.blockAssembler.setBestTipPos(pos)
	items.blockAssembler.bestTipPosKnown.Store(true)

	return pos
}

// bestTipKeyOf reads the place of a stored block in the best-tip order.
func bestTipKeyOf(t *testing.T, items *baTestItems, header *model.BlockHeader) model.BestTipKey {
	t.Helper()

	h, meta, err := items.blockchainClient.GetBlockHeader(t.Context(), header.Hash())
	require.NoError(t, err)

	return model.NewBestTipKey(h, meta)
}

// requireReplayPosition asserts the replay position is on want.
func requireReplayPosition(t *testing.T, items *baTestItems, want *model.BlockHeader) {
	t.Helper()

	pos := items.blockAssembler.bestTipPos.Load()
	require.NotNil(t, pos)
	require.Equal(t, want.Hash().String(), pos.Hash.String(), "replay position")
}

// requireNoLowWater asserts the blockchain store holds no low-water mark of the best tip.
func requireNoLowWater(t *testing.T, items *baTestItems) {
	t.Helper()

	_, err := items.blockchainClient.GetState(t.Context(), model.BestTipLowWaterStateKey)
	require.Error(t, err, "the low-water mark is cleared once the replay has passed it")
}

func requireBlockAssemblyTip(t *testing.T, items *baTestItems, want *model.BlockHeader) {
	t.Helper()

	tip, _ := items.blockAssembler.CurrentBlock()
	require.Equal(t, want.Hash().String(), tip.Hash().String())
}

// TestReplayBestTipChanges_FlipFlop is the scenario of issue 2063: branch B becomes the best
// chain and loses it to branch A again before block assembly looks. Following the best header
// alone catches up from A1 to A3 and never touches B. The replay moves forward to B2 and then
// reorgs back to A3, so B's transactions go forward and back through the subtree processor.
func TestReplayBestTipChanges_FlipFlop(t *testing.T) {
	initPrometheusMetrics()

	// A realistic maturity: the test settings use 1, under which moving back over any block
	// of the best chain could delete a confirmed spend of its coinbase, and the replay
	// restamps the branch instead of moving over it.
	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 1000)[0]
	addChain(t, items, []*model.BlockHeader{a1})

	startReplayAt(t, items, a1, 1)
	moves := recordMoves(t, items)

	// B1 ties A1 and loses the tiebreak; B2 makes B the best chain.
	b := buildChain(genesis, 2, 2000)
	addChain(t, items, b)

	// A2 ties B2 and loses; A3 makes A the best chain again.
	a := buildChain(a1, 2, 3000)
	addChain(t, items, a)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Equal(t, []recordedMove{
		{back: headerHashes(a1), forward: headerHashes(b[0], b[1])},
		{back: headerHashes(b[1], b[0]), forward: headerHashes(a1, a[0], a[1])},
	}, *moves)

	requireBlockAssemblyTip(t, items, a[1])
	requireReplayPosition(t, items, a[1])

	_, err := items.blockchainClient.GetState(t.Context(), BestTipPositionStateKey)
	require.Error(t, err, "the position is not saved while block assembly is still processing")

	items.blockAssembler.returnToRunning(t.Context())

	saved, err := items.blockchainClient.GetState(t.Context(), BestTipPositionStateKey)
	require.NoError(t, err)

	savedPos, err := model.NewBestTipKeyFromBytes(saved)
	require.NoError(t, err)
	require.Equal(t, bestTipKeyOf(t, items, a[1]), savedPos, "the position is persisted for a restart")
}

// TestReplayBestTipChanges_ExtensionsCollapse checks that a run of extensions is one catch-up,
// not one move per block.
func TestReplayBestTipChanges_ExtensionsCollapse(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	startReplayAt(t, items, genesis, 0)
	moves := recordMoves(t, items)

	chain := buildChain(genesis, 100, 4000)
	addChain(t, items, chain)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Len(t, *moves, 1)
	require.Empty(t, (*moves)[0].back)
	require.Len(t, (*moves)[0].forward, 100)
	requireBlockAssemblyTip(t, items, chain[99])
}

// TestReplayBestTipChanges_OwnLineageIsANoOp checks that a change block assembly already passed,
// for example through a reset that went straight to a later tip, moves nothing back, and that
// the position still advances past it.
func TestReplayBestTipChanges_OwnLineageIsANoOp(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	startReplayAt(t, items, genesis, 0)

	chain := buildChain(genesis, 3, 5000)
	addChain(t, items, chain)

	// Block assembly is already on the latest block, but its position is before all three.
	items.blockAssembler.setBestBlockHeader(chain[2], 3)
	moves := recordMoves(t, items)

	// The oldest change, two blocks below the tip, goes through the ancestry check.
	_, meta, err := items.blockchainClient.GetBlockHeader(t.Context(), chain[0].Hash())
	require.NoError(t, err)
	require.NoError(t, items.blockAssembler.applyBestTipChange(t.Context(), chain[0], meta, false))
	require.Empty(t, *moves)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Empty(t, *moves)
	requireBlockAssemblyTip(t, items, chain[2])
	requireReplayPosition(t, items, chain[2])
}

// TestReplayBestTipChanges_LatestChangeOnOwnChainMovesBack checks that when the best tip drops to
// an ancestor of block assembly's tip, block assembly follows. Invalidating block assembly's tip
// records its parent in the low-water mark, which sorts below the replay position.
func TestReplayBestTipChanges_LatestChangeOnOwnChainMovesBack(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 3, 5500)
	addChain(t, items, chain)

	startReplayAt(t, items, chain[2], 3)
	moves := recordMoves(t, items)

	// A reorg off an invalid block is followed by a reset, which the mock lets succeed.
	mockStp := items.blockAssembler.subtreeProcessor.(*subtreeprocessor.MockSubtreeProcessor)
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)

	resets := 0

	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{}).Run(func(mock.Arguments) { resets++ })

	_, err := items.blockchainClient.InvalidateBlock(t.Context(), chain[2].Hash())
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.SetBlockMinedSet(t.Context(), chain[2].Hash()))

	// The mark names chain[2] as the tip that dropped. Block assembly applied it, so the reorg
	// off it is the one reset; the dropped tip must not add a reset of its own.
	markData, err := items.blockchainClient.GetState(t.Context(), model.BestTipLowWaterStateKey)
	require.NoError(t, err)
	mark, err := model.NewBestTipLowWaterFromBytes(markData)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*chain[2].Hash()}, mark.DroppedTips)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Equal(t, []recordedMove{{back: headerHashes(chain[2]), forward: []string{}}}, *moves)
	require.Equal(t, 1, resets, "moving off an invalid block resets block assembly, once")
	requireBlockAssemblyTip(t, items, chain[1])
	requireReplayPosition(t, items, chain[1])
	requireNoLowWater(t, items)

	saved, err := items.blockchainClient.GetState(t.Context(), BestTipPositionStateKey)
	require.NoError(t, err)
	require.Equal(t, bestTipKeyOf(t, items, chain[1]).Bytes(), saved, "the position is persisted before the mark is cleared")
}

// TestReplayBestTipChanges_NoOpWhenCaughtUp checks a wake-up with no new change moves nothing.
func TestReplayBestTipChanges_NoOpWhenCaughtUp(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 2, 6000)
	addChain(t, items, chain)

	startReplayAt(t, items, chain[1], 2)
	moves := recordMoves(t, items)

	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Empty(t, *moves)
	requireReplayPosition(t, items, chain[1])
}

// TestReplayBestTipChanges_InvalidNeverAppliedResets covers a branch that became the best tip and
// was invalidated before block assembly applied it. Its block-only transactions reach the pool
// only through a reset, which the replay runs once block validation has processed the block.
func TestReplayBestTipChanges_InvalidNeverAppliedResets(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 7000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1)

	b := buildChain(genesis, 2, 8000)
	addChain(t, items, b)

	_, err := items.blockchainClient.InvalidateBlock(t.Context(), b[0].Hash())
	require.NoError(t, err)

	// Block validation has processed the invalidation.
	require.NoError(t, items.blockchainClient.SetBlockMinedSet(t.Context(), b[1].Hash()))

	_, bMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), b[1].Hash())
	require.NoError(t, err)
	require.True(t, bMeta.Invalid)

	err = items.blockAssembler.applyBestTipChange(t.Context(), b[1], bMeta, true)
	require.ErrorIs(t, err, errors.ErrBlockAssemblyReset)
	requireBlockAssemblyTip(t, items, a1)
}

// TestReplayBestTipChanges_InvalidOnOwnChainIsLeftToTheNextChange checks that an invalid block
// block assembly's chain contains is not reset for: the change that follows its invalidation
// moves block assembly off it through the reorg's reset rule.
func TestReplayBestTipChanges_InvalidOnOwnChainIsLeftToTheNextChange(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 2, 9000)
	addChain(t, items, chain)
	startReplayAt(t, items, chain[1], 2)

	_, err := items.blockchainClient.InvalidateBlock(t.Context(), chain[0].Hash())
	require.NoError(t, err)

	_, meta, err := items.blockchainClient.GetBlockHeader(t.Context(), chain[0].Hash())
	require.NoError(t, err)

	moves := recordMoves(t, items)

	require.NoError(t, items.blockAssembler.applyBestTipChange(t.Context(), chain[0], meta, false))
	require.Empty(t, *moves)
	requireBlockAssemblyTip(t, items, chain[1])
}

// TestDroppedTipNeedsReset checks which dropped best tips the replay resets for: an invalid one
// off block assembly's chain or one that is gone does, while one on block assembly's chain (the
// move off it resets) or one valid again (the replay moves to it) does not.
func TestDroppedTipNeedsReset(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 3, 9500)
	addChain(t, items, chain)
	fork := buildChain(genesis, 1, 9600)
	addChain(t, items, fork)
	valid := buildChain(genesis, 1, 9700)
	addChain(t, items, valid)
	startReplayAt(t, items, chain[2], 3)

	_, err := items.blockchainClient.InvalidateBlock(t.Context(), chain[1].Hash())
	require.NoError(t, err)
	_, err = items.blockchainClient.InvalidateBlock(t.Context(), fork[0].Hash())
	require.NoError(t, err)

	ba := items.blockAssembler

	for name, tc := range map[string]struct {
		hash chainhash.Hash
		want bool
	}{
		"block assembly's tip":              {*chain[2].Hash(), false},
		"invalid on block assembly's chain": {*chain[1].Hash(), false},
		"invalid off it":                    {*fork[0].Hash(), true},
		"valid again":                       {*valid[0].Hash(), false},
		"deleted":                           {chainhash.Hash{0xde, 0xad}, true},
	} {
		got, err := ba.droppedTipNeedsReset(t.Context(), tc.hash)
		require.NoError(t, err, name)
		require.Equal(t, tc.want, got, name)
	}
}

// TestReplayBestTipChanges_RestartResumesFromPersistedPosition checks that changes made while
// block assembly was down are replayed from the persisted position.
func TestReplayBestTipChanges_RestartResumesFromPersistedPosition(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 10000)[0]
	addChain(t, items, []*model.BlockHeader{a1})

	pos := startReplayAt(t, items, a1, 1)
	items.blockAssembler.bestTipPosSaved.Store(nil)
	items.blockAssembler.saveBestTipPosition(t.Context())

	// The flip happens while block assembly is down.
	b := buildChain(genesis, 2, 11000)
	addChain(t, items, b)
	a := buildChain(a1, 2, 12000)
	addChain(t, items, a)

	// A restart reads the position back.
	items.blockAssembler.bestTipPos.Store(nil)
	items.blockAssembler.bestTipPosKnown.Store(false)
	items.blockAssembler.loadBestTipPosition(t.Context())
	require.True(t, items.blockAssembler.bestTipPosKnown.Load())
	require.Equal(t, pos, *items.blockAssembler.bestTipPos.Load())

	moves := recordMoves(t, items)
	items.blockAssembler.processNewBlockAnnouncement(t.Context())

	require.Len(t, *moves, 2, "the downtime's flip is replayed")
	requireBlockAssemblyTip(t, items, a[1])
}

// TestReplayBestTipChanges_MockWithoutOrderFollowsBestHeader checks block assembly against a
// blockchain client that does not serve the best-tip order: it follows the best header as before.
func TestReplayBestTipChanges_MockWithoutOrderFollowsBestHeader(t *testing.T) {
	m := &blockchain.Mock{}

	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}}

	require.True(t, ba.followBestTipChanges(t.Context()))
	require.False(t, ba.bestTipPosKnown.Load())
}

// TestReplayBestTipChanges_RepeatedFailureFollowsBestHeader checks that a replay that keeps
// failing gives way to the best-header path after maxBestTipReplayFailures passes.
func TestReplayBestTipChanges_RepeatedFailureFollowsBestHeader(t *testing.T) {
	initPrometheusMetrics()

	best := &model.BlockHeader{Version: 2, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}
	bestMeta := &model.BlockHeaderMeta{ID: 42, PeerID: "peer", Height: 11, ChainWork: []byte{1}}

	m := &blockchain.Mock{}
	m.On("BlocksBetterThan", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader(nil), []*model.BlockHeaderMeta(nil), errors.NewServiceError("down"))
	m.On("GetState", mock.Anything, model.BestTipLowWaterStateKey).Return([]byte(nil), errors.NewNotFoundError("no mark"))
	// Over gRPC the best header's metadata has no block id or peer id; the block's own header
	// metadata has both.
	m.On("GetBestBlockHeader", mock.Anything).Return(best, withoutBestTipIdentity(bestMeta), nil)
	m.On("GetBlockHeader", mock.Anything, best.Hash()).Return(best, bestMeta, nil)
	// Giving the history up owes a mark of the fork blocks in it, recorded first.
	m.On("SetState", mock.Anything, SkippedForkMarkStateKey, []byte{1}).Return(nil).Once()

	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}, settings: createTestSettings(t)}
	ba.bestTipPosKnown.Store(true)
	ba.setBestTipPos(model.BestTipKey{ChainWork: []byte{0}})
	ba.setBestBlockHeader(&model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}, 10)

	for i := 1; i < maxBestTipReplayFailures; i++ {
		require.False(t, ba.followBestTipChanges(t.Context()), "pass %d retries", i)
	}

	require.True(t, ba.followBestTipChanges(t.Context()))
	require.Equal(t, model.NewBestTipKey(best, bestMeta), *ba.bestTipPos.Load(), "the position jumps to the best tip, with its id and peer id")
	require.True(t, ba.skippedForkMarkPending, "the mark the history owes is pending")
	m.AssertExpectations(t)
}

// withoutBestTipIdentity returns a copy of meta as GetBestBlockHeader returns it over gRPC: without
// the block id and peer id, which place a block among others of equal work.
func withoutBestTipIdentity(meta *model.BlockHeaderMeta) *model.BlockHeaderMeta {
	stripped := *meta
	stripped.ID = 0
	stripped.PeerID = ""

	return &stripped
}

// grpcShapedBestHeaderClient serves GetBestBlockHeader as the gRPC client does, without the block
// id and peer id.
type grpcShapedBestHeaderClient struct {
	blockchain.ClientI
}

func (c *grpcShapedBestHeaderClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	header, meta, err := c.ClientI.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, nil, err
	}

	return header, withoutBestTipIdentity(meta), nil
}

// TestReturnToRunning_SavesAfterLeavingProcessingState checks that the replay position is written
// only once block assembly is running again, so the write does not lengthen the time a mining
// candidate comes back empty.
func TestReturnToRunning_SavesAfterLeavingProcessingState(t *testing.T) {
	initPrometheusMetrics()

	m := &blockchain.Mock{}
	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}}

	var stateAtSave State

	pos := model.BestTipKey{ChainWork: []byte{7}, PeerID: "p", ID: 7}

	m.On("SetState", mock.Anything, BestTipPositionStateKey, pos.Bytes()).
		Run(func(mock.Arguments) { stateAtSave = ba.GetCurrentRunningState() }).
		Return(nil).Once()

	ba.bestTipPosKnown.Store(true)
	ba.setBestTipPos(pos)
	ba.setCurrentRunningState(StateReorging)

	ba.returnToRunning(t.Context())

	m.AssertExpectations(t)
	require.Equal(t, StateRunning, stateAtSave)
	require.Equal(t, pos, *ba.bestTipPosSaved.Load())

	// Nothing moved, so the next return writes nothing.
	ba.returnToRunning(t.Context())
	m.AssertNumberOfCalls(t, "SetState", 1)

	// A moved position waits for bestTipPositionSaveInterval, so a pass per block during
	// initial sync does not cost a write per block.
	moved := model.BestTipKey{ChainWork: []byte{8}, PeerID: "p", ID: 8}
	m.On("SetState", mock.Anything, BestTipPositionStateKey, moved.Bytes()).Return(nil).Once()
	ba.setBestTipPos(moved)
	ba.returnToRunning(t.Context())
	m.AssertNumberOfCalls(t, "SetState", 1)

	ba.lastBestTipPosSave.Store(time.Now().Add(-bestTipPositionSaveInterval).UnixNano())
	ba.returnToRunning(t.Context())
	m.AssertNumberOfCalls(t, "SetState", 2)
	require.Equal(t, moved, *ba.bestTipPosSaved.Load())
}

// syncCallCounter counts the calls to the blockchain service that a replay pass can make on top of
// moving block assembly.
type syncCallCounter struct {
	blockchain.ClientI
	betterThan, bestHeader, getState, positionSaves int
}

func (c *syncCallCounter) BlocksBetterThan(ctx context.Context, after model.BestTipKey, from *model.BestTipKey, floor, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, bool, error) {
	c.betterThan++
	return c.ClientI.BlocksBetterThan(ctx, after, from, floor, limit)
}

func (c *syncCallCounter) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.bestHeader++
	return c.ClientI.GetBestBlockHeader(ctx)
}

func (c *syncCallCounter) GetState(ctx context.Context, key string) ([]byte, error) {
	c.getState++
	return c.ClientI.GetState(ctx, key)
}

func (c *syncCallCounter) SetState(ctx context.Context, key string, data []byte) error {
	if key == BestTipPositionStateKey {
		c.positionSaves++
	}

	return c.ClientI.SetState(ctx, key, data)
}

// TestReplayBestTipChanges_InitialSyncCallsPerBlock pins what the replay costs per block during
// initial sync, where a pass runs for every one of many small blocks: one BlocksBetterThan read,
// which takes the place of the best header read that following the best header made, and no
// read of the low-water mark or save of the position.
func TestReplayBestTipChanges_InitialSyncCallsPerBlock(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)
	startReplayAt(t, items, genesis, 0)
	recordMoves(t, items)

	ba := items.blockAssembler
	chain := buildChain(genesis, 51, 35000)

	pass := func() {
		ba.setCurrentRunningState(StateBlockchainSubscription)
		ba.processNewBlockAnnouncement(t.Context())
		ba.returnToRunning(t.Context())
	}

	// The first pass reads the mark and saves the position.
	addChain(t, items, chain[:1])
	pass()

	counter := &syncCallCounter{ClientI: items.blockchainClient}
	ba.blockchainClient = counter

	for _, h := range chain[1:] {
		addChain(t, items, []*model.BlockHeader{h})
		pass()
	}

	requireBlockAssemblyTip(t, items, chain[50])
	require.Equal(t, 50, counter.betterThan)
	require.Zero(t, counter.bestHeader)
	require.Zero(t, counter.getState)
	require.Zero(t, counter.positionSaves)
}

// TestTakeBlockNotifications_CoalescesQueued checks that the notifications already queued behind
// the one received are taken with it, so one replay pass covers them all, and that a
// BlockMinedUnset among them makes the next pass read the low-water mark.
func TestTakeBlockNotifications_CoalescesQueued(t *testing.T) {
	ba := &BlockAssembler{blockchainSubscriptionCh: make(chan *blockchain.Notification, 8)}
	ba.bestTipLowWaterQuiet.Store(true)

	ba.blockchainSubscriptionCh <- &blockchain.Notification{Type: model.NotificationType_Block}
	ba.blockchainSubscriptionCh <- nil
	ba.blockchainSubscriptionCh <- &blockchain.Notification{Type: model.NotificationType_BlockMinedUnset}
	ba.blockchainSubscriptionCh <- &blockchain.Notification{Type: model.NotificationType_Block}

	require.True(t, ba.takeBlockNotifications(&blockchain.Notification{Type: model.NotificationType_Block}))
	require.Empty(t, ba.blockchainSubscriptionCh)
	require.False(t, ba.bestTipLowWaterQuiet.Load(), "an invalidation may have set the mark")

	// Notifications of other kinds need no pass.
	ba.blockchainSubscriptionCh <- &blockchain.Notification{Type: model.NotificationType_Subtree}
	require.False(t, ba.takeBlockNotifications(&blockchain.Notification{Type: model.NotificationType_PING}))
	require.Empty(t, ba.blockchainSubscriptionCh)

	// A BlockMinedUnset alone calls for a pass: its Block notification may have been handled
	// before the mark could be read.
	ba.bestTipLowWaterQuiet.Store(true)
	require.True(t, ba.takeBlockNotifications(&blockchain.Notification{Type: model.NotificationType_BlockMinedUnset}))
	require.False(t, ba.bestTipLowWaterQuiet.Load())
}

// TestReadBestTipLowWaterIfDue_ReadsOnlyWhenTheMarkCanHaveChanged checks that once a read found no
// mark, the replay reads it again only after a BlockMinedUnset notification or once
// blockassembly_reconcileInterval has passed, and keeps reading while a mark is set.
func TestReadBestTipLowWaterIfDue_ReadsOnlyWhenTheMarkCanHaveChanged(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler
	ctx := t.Context()

	mark, _, err := ba.readBestTipLowWaterIfDue(ctx)
	require.NoError(t, err)
	require.Nil(t, mark)
	require.True(t, ba.bestTipLowWaterQuiet.Load())

	// A drop recorded now is not read on the next pass.
	genesis := genesisHeader(t, items)
	chain := buildChain(genesis, 2, 34000)
	addChain(t, items, chain)
	_, err = items.blockchainClient.InvalidateBlock(ctx, chain[1].Hash())
	require.NoError(t, err)

	mark, _, err = ba.readBestTipLowWaterIfDue(ctx)
	require.NoError(t, err)
	require.Nil(t, mark, "no notification and no interval, so the mark is not read")

	// The invalidation's BlockMinedUnset notification makes the next pass read it.
	require.True(t, ba.takeBlockNotifications(&blockchain.Notification{Type: model.NotificationType_BlockMinedUnset}))
	mark, data, err := ba.readBestTipLowWaterIfDue(ctx)
	require.NoError(t, err)
	require.NotNil(t, mark)

	// A mark is read on every pass until it is cleared.
	mark, _, err = ba.readBestTipLowWaterIfDue(ctx)
	require.NoError(t, err)
	require.NotNil(t, mark)

	require.NoError(t, items.blockchainClient.ClearBestTipLowWater(ctx, data))
	mark, _, err = ba.readBestTipLowWaterIfDue(ctx)
	require.NoError(t, err)
	require.Nil(t, mark)
	require.True(t, ba.bestTipLowWaterQuiet.Load())

	// A lost notification is covered once the interval has passed.
	_, err = items.blockchainClient.InvalidateBlock(ctx, chain[0].Hash())
	require.NoError(t, err)
	mark, _, err = ba.readBestTipLowWaterIfDue(ctx)
	require.NoError(t, err)
	require.Nil(t, mark)

	ba.lastBestTipLowWaterRead.Store(time.Now().Add(-ba.settings.BlockAssembly.ReconcileInterval).UnixNano())
	mark, _, err = ba.readBestTipLowWaterIfDue(ctx)
	require.NoError(t, err)
	require.NotNil(t, mark, "the interval read finds the mark the lost notification would have flagged")
}

// TestReplayBestTipChanges_ProbeIsRateLimited checks that against a blockchain service without
// the best-tip order, block assembly asks again only after bestTipProbeInterval, so a
// mixed-version deploy does not cost an RPC and a warning on every wake-up.
func TestReplayBestTipChanges_ProbeIsRateLimited(t *testing.T) {
	m := &blockchain.Mock{}
	m.On("BlocksBetterThan", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*model.BlockHeader(nil), []*model.BlockHeaderMeta(nil), errors.NewServiceUnavailableError("not implemented"))

	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}}

	require.True(t, ba.followBestTipChanges(t.Context()))
	require.True(t, ba.followBestTipChanges(t.Context()))
	m.AssertNumberOfCalls(t, "BlocksBetterThan", 1)
	require.True(t, ba.bestTipUnsupportedLogged.Load())

	ba.lastBestTipProbe.Store(time.Now().Add(-bestTipProbeInterval).UnixNano())
	require.True(t, ba.followBestTipChanges(t.Context()))
	m.AssertNumberOfCalls(t, "BlocksBetterThan", 2)
}

// TestLoadBestTipPosition_NoPersistedPosition checks the start without a persisted position: on
// the best tip, block assembly starts there, so it is not moved through the stale branches
// before it; off the best tip, it starts at its own tip and replays what sorts better.
func TestLoadBestTipPosition_NoPersistedPosition(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 13000)[0]
	addChain(t, items, []*model.BlockHeader{a1})

	// A stale branch that was the best chain for a while.
	b := buildChain(genesis, 2, 14000)
	addChain(t, items, b)
	a := buildChain(a1, 2, 15000)
	addChain(t, items, a)

	ba := items.blockAssembler
	local := ba.blockchainClient
	ba.blockchainClient = &grpcShapedBestHeaderClient{ClientI: local}

	ba.setBestBlockHeader(a[1], 3)
	ba.bestTipPosKnown.Store(false)
	ba.loadBestTipPosition(t.Context())
	require.True(t, ba.bestTipPosKnown.Load())
	requireReplayPosition(t, items, a[1])
	// The whole key, not only the hash: without the id and peer id the position would sort
	// better than the best block and hide a block of equal work.
	require.Equal(t, bestTipKeyOf(t, items, a[1]), *ba.bestTipPos.Load(), "the position carries the best block's id and peer id")
	ba.blockchainClient = local

	moves := recordMoves(t, items)
	ba.processNewBlockAnnouncement(t.Context())
	require.Empty(t, *moves, "the stale branch is not replayed")

	ba.setBestBlockHeader(a1, 1)
	ba.bestTipPosKnown.Store(false)
	ba.loadBestTipPosition(t.Context())
	require.True(t, ba.bestTipPosKnown.Load())
	requireReplayPosition(t, items, a1)

	// A persisted value from the build that numbered tip changes is under another key, and an
	// unreadable value under this key is ignored rather than misparsed.
	require.NoError(t, items.blockchainClient.SetState(t.Context(), "BlockAssemblerBestTipSeq", []byte{1, 0, 0, 0, 0, 0, 0, 0}))
	require.NoError(t, items.blockchainClient.SetState(t.Context(), BestTipPositionStateKey, []byte{1, 0, 0, 0, 0, 0, 0, 0}))
	ba.bestTipPosKnown.Store(false)
	ba.loadBestTipPosition(t.Context())
	require.True(t, ba.bestTipPosKnown.Load())
	requireReplayPosition(t, items, a1)
}

// TestIdleReconcileNeeded checks that the idle tick starts a replay only when the tip moved:
// a check that finds nothing keeps block assembly out of StateReconciling, where a mining
// candidate is an empty one.
func TestIdleReconcileNeeded(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)

	chain := buildChain(genesis, 2, 16000)
	addChain(t, items, chain[:1])

	ba := items.blockAssembler
	startReplayAt(t, items, chain[0], 1)

	ba.lastBestTipReplay.Store(0)
	require.False(t, ba.idleReconcileNeeded(t.Context()), "nothing moved")
	require.NotZero(t, ba.lastBestTipReplay.Load(), "the check waits a full interval before the next one")

	// A block whose notification was lost.
	addChain(t, items, chain[1:])
	require.True(t, ba.idleReconcileNeeded(t.Context()), "a block sorts better than the position")

	// Against a blockchain service without the best-tip order, the best header decides.
	ba.bestTipPosKnown.Store(false)
	require.True(t, ba.idleReconcileNeeded(t.Context()), "block assembly is behind the best header")

	ba.setBestBlockHeader(chain[1], 2)
	require.False(t, ba.idleReconcileNeeded(t.Context()), "block assembly is on the best header")

	ba.skippedForkMarkPending = true
	require.True(t, ba.idleReconcileNeeded(t.Context()), "an unfinished mark of skipped fork transactions must not wait for a new block")

	ba.skippedForkMarkRetryAt = time.Now().Add(time.Hour)
	require.False(t, ba.idleReconcileNeeded(t.Context()), "a mark inside its back-off does not wake the idle reconcile")
}

func TestReconcileDue(t *testing.T) {
	ba := &BlockAssembler{settings: createTestSettings(t)}
	ba.settings.BlockAssembly.ReconcileInterval = 30 * time.Second

	now := time.Now()
	ba.lastBestTipReplay.Store(now.Add(-29 * time.Second).UnixNano())
	require.False(t, ba.reconcileDue(now))

	ba.lastBestTipReplay.Store(now.Add(-30 * time.Second).UnixNano())
	require.True(t, ba.reconcileDue(now))

	ba.settings.BlockAssembly.ReconcileInterval = 0
	require.False(t, ba.reconcileDue(now), "0 turns the timer off")
	ba.bestTipResetReplayPending.Store(true)
	require.True(t, ba.reconcileDue(now), "failed reset recovery is retried even with the timer off")
	require.True(t, ba.idleReconcileNeeded(t.Context()), "pending reset recovery must not wait for a new block")

	ba.bestTipResetReplayPending.Store(false)
	ba.skippedForkMarkPending = true
	require.True(t, ba.reconcileDue(now), "an unfinished mark of skipped fork transactions is retried even with the timer off")
}

func TestBestTipFloor(t *testing.T) {
	ba := &BlockAssembler{settings: createTestSettings(t)}
	retention := ba.settings.GetUtxoStoreBlockHeightRetention()
	require.NotZero(t, retention)

	require.Equal(t, uint32(0), ba.bestTipFloor(retention))
	require.Equal(t, uint32(10), ba.bestTipFloor(retention+10))
}

// historicalWinnerSetup puts block assembly on A1 at height 1002 with its position there, then
// adds branch B of bLen blocks off A1's parent and branch A of aLen blocks on A1, so B is the
// best chain for a while and A wins it back. It records every Reorg and Reset.
func historicalWinnerSetup(t *testing.T, retention uint32, bLen, aLen int) (*baTestItems, *model.BlockHeader, []*model.BlockHeader, []*model.BlockHeader, *[]recordedMove, *[]recordedMove) {
	t.Helper()

	items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
		s.GlobalBlockHeightRetention = retention
		s.UtxoStore.BlockHeightRetentionAdjustment = 0
	}, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)
	prefix := buildChain(genesis, 1001, 18000)
	addChain(t, items, prefix)
	common := prefix[len(prefix)-1]
	a1 := buildChain(common, 1, 15000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1002)
	moves := recordMoves(t, items)
	mockStp := items.blockAssembler.subtreeProcessor.(*subtreeprocessor.MockSubtreeProcessor)
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	resetMoves := &[]recordedMove{}
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{}).Run(func(args mock.Arguments) {
		m := recordedMove{}
		for _, block := range args.Get(1).([]*model.Block) {
			m.back = append(m.back, block.Hash().String())
		}
		for _, block := range args.Get(2).([]*model.Block) {
			m.forward = append(m.forward, block.Hash().String())
		}
		*resetMoves = append(*resetMoves, m)
	})
	b := buildChain(common, bLen, 16000)
	addChain(t, items, b)
	a := buildChain(a1, aLen, 17000)
	addChain(t, items, a)

	return items, a1, a, b, moves, resetMoves
}

// movesTouch reports whether any recorded move takes hash back, and whether any takes it forward.
func movesTouch(hash string, moves ...[]recordedMove) (back, forward bool) {
	for _, ms := range moves {
		for _, m := range ms {
			for _, h := range m.back {
				back = back || h == hash
			}
			for _, h := range m.forward {
				forward = forward || h == hash
			}
		}
	}

	return back, forward
}

// Regression proof: a historical tip that takes reset must still be traversed. B is longer
// than the UTXO retention, so moving to it is a reset, while the best tip stays less than
// coinbase maturity above A1, so moving A1 back cannot touch a confirmed coinbase spend.
func TestReplayBestTipChanges_ResetVisitsHistoricalWinner(t *testing.T) {
	initPrometheusMetrics()
	items, _, a, b, moves, resetMoves := historicalWinnerSetup(t, 50, 60, 61)

	_, err := items.blockAssembler.replayBestTipChanges(t.Context())
	require.NoError(t, err)
	requireBlockAssemblyTip(t, items, a[len(a)-1])
	requireReplayPosition(t, items, a[len(a)-1])
	require.NotEmpty(t, *resetMoves)
	_, forward := movesTouch(b[len(b)-1].Hash().String(), *moves, *resetMoves)
	require.True(t, forward, "B became best within the height floor, but replay checkpointed it without passing B to the subtree processor")
}

// TestReplayBestTipChanges_DeepHistoricalWinnerIsRestamped is the coinbase case: the best tip is
// coinbase maturity blocks above A1, so a spend of A1's coinbase can already be confirmed on A.
// Moving A1 back to visit B would delete that spend for good, so the replay never moves A1 back.
// It restamps B where it lies and reloads the pool on block assembly's own tip instead.
func TestReplayBestTipChanges_DeepHistoricalWinnerIsRestamped(t *testing.T) {
	initPrometheusMetrics()
	items, a1, a, b, moves, resetMoves := historicalWinnerSetup(t, 288, 100, 100)

	_, err := items.blockAssembler.replayBestTipChanges(t.Context())
	require.NoError(t, err)
	requireBlockAssemblyTip(t, items, a[len(a)-1])
	requireReplayPosition(t, items, a[len(a)-1])

	back, _ := movesTouch(a1.Hash().String(), *moves, *resetMoves)
	require.False(t, back, "A1 is on the best chain and its coinbase may be spent there: it must never be moved back")
	_, forward := movesTouch(b[len(b)-1].Hash().String(), *moves, *resetMoves)
	require.False(t, forward, "B is restamped where it lies, not moved to")
	require.Contains(t, *resetMoves, recordedMove{}, "the restamp reloads the pool on block assembly's own tip")
}

// Regression proof: a historical tip that takes reset must still be traversed.
func TestReplayBestTipChanges_ExternalResetRetainsPendingHistory(t *testing.T) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 15000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1)
	moves := recordMoves(t, items)
	mockStp := items.blockAssembler.subtreeProcessor.(*subtreeprocessor.MockSubtreeProcessor)
	mockStp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	resetMoves := []recordedMove{}
	mockStp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(subtreeprocessor.ResetResponse{}).Run(func(args mock.Arguments) {
		m := recordedMove{}
		for _, block := range args.Get(1).([]*model.Block) {
			m.back = append(m.back, block.Hash().String())
		}
		for _, block := range args.Get(2).([]*model.Block) {
			m.forward = append(m.forward, block.Hash().String())
		}
		resetMoves = append(resetMoves, m)
	})
	b := buildChain(genesis, 2, 16000)
	addChain(t, items, b)
	a := buildChain(a1, 2, 17000)
	addChain(t, items, a)
	items.blockAssembler.handleResetRequests(t.Context(), resetRequest{})
	read, err := items.blockAssembler.replayBestTipChanges(t.Context())
	require.NoError(t, err)
	require.Zero(t, read, "reset must synchronously apply its pending history")
	requireBlockAssemblyTip(t, items, a[len(a)-1])
	requireReplayPosition(t, items, a[len(a)-1])
	t.Logf("incremental moves=%d resets=%d first reset back=%d forward=%d", len(*moves), len(resetMoves), len(resetMoves[0].back), len(resetMoves[0].forward))
	appliedHistorical := false
	for _, m := range append(*moves, resetMoves...) {
		for _, hash := range append(m.back, m.forward...) {
			if hash == b[len(b)-1].Hash().String() {
				appliedHistorical = true
			}
		}
	}
	require.True(t, appliedHistorical, "B2 became best within the height floor, but replay checkpointed it without passing either B block to the subtree processor")
}

// cappedTipClient keeps real blockchain storage while emulating an RPC server's
// smaller response cap (or a temporary transport failure).
type cappedTipClient struct {
	blockchain.ClientI
	cap  uint32
	fail bool
}

func (c *cappedTipClient) BlocksBetterThan(ctx context.Context, after model.BestTipKey, from *model.BestTipKey, floor, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, bool, error) {
	if c.fail {
		return nil, nil, false, errors.NewStorageError("temporary replay read failure")
	}
	return c.ClientI.BlocksBetterThan(ctx, after, from, floor, min(limit, c.cap))
}

func TestReplayBestTipChanges_CappedPagesSkipIntermediateAncestors(t *testing.T) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)
	chain := buildChain(genesis, 3, 25000)
	startReplayAt(t, items, genesis, 0)
	addChain(t, items, chain)
	items.blockAssembler.setBestBlockHeader(chain[2], 3)
	moves := recordMoves(t, items)
	items.blockAssembler.blockchainClient = &cappedTipClient{ClientI: items.blockchainClient, cap: 1}
	n, err := items.blockAssembler.replayBestTipChanges(t.Context())
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Empty(t, *moves, "a short capped page must not turn an intermediate ancestor into a final rewind")
	requireBlockAssemblyTip(t, items, chain[2])
}

func TestReplayBestTipChanges_FailedResetRetainsPosition(t *testing.T) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
		s.GlobalBlockHeightRetention = 2
		s.UtxoStore.BlockHeightRetentionAdjustment = 0
	})
	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 26000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1)
	recordMoves(t, items)
	stp := items.blockAssembler.subtreeProcessor.(*subtreeprocessor.MockSubtreeProcessor)
	stp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	failure := errors.NewStorageError("reset failed")
	stp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(subtreeprocessor.ResetResponse{Err: failure})
	stp.On("GetCurrentBlockHeader").Return(a1)
	b := buildChain(genesis, 2, 27000)
	addChain(t, items, b)
	a := buildChain(a1, 2, 28000)
	addChain(t, items, a)
	_, err := items.blockAssembler.replayBestTipChanges(t.Context())
	require.ErrorIs(t, err, failure)
	requireReplayPosition(t, items, a1)
}

func TestReplayBestTipChanges_ExternalResetFailureGatesCandidates(t *testing.T) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 29000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1)
	recordMoves(t, items)
	stp := items.blockAssembler.subtreeProcessor.(*subtreeprocessor.MockSubtreeProcessor)
	stp.On("WaitForPendingBlocks", mock.Anything).Return(nil)
	stp.On("Reset", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(subtreeprocessor.ResetResponse{})
	b := buildChain(genesis, 2, 30000)
	addChain(t, items, b)
	a := buildChain(a1, 2, 31000)
	addChain(t, items, a)
	client := &cappedTipClient{ClientI: items.blockchainClient, cap: 1, fail: true}
	ba := items.blockAssembler
	ba.blockchainClient = client
	answer := make(chan error, 1)
	ba.handleResetRequests(t.Context(), resetRequest{ErrCh: answer})
	require.NoError(t, <-answer, "the reset itself succeeded, so its sender is not asked to reset again")
	require.True(t, ba.bestTipResetReplayPending.Load())
	require.Len(t, ba.reconcileCh, 1, "the failed replay is retried on the next pass")
	ba.setCurrentRunningState(StateRunning)
	candidate, _, err := ba.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	require.Zero(t, candidate.NumTxs, "current tip must not expose child-only pool while replay is pending")
	for i := 1; i < maxBestTipReplayFailures; i++ {
		require.False(t, ba.followBestTipChanges(t.Context()), "pass %d retries the pending history", i)
		require.True(t, ba.bestTipResetReplayPending.Load())
	}
	require.True(t, ba.followBestTipChanges(t.Context()), "the last failure gives the history up and follows the best header")
	require.False(t, ba.bestTipResetReplayPending.Load(), "a replay that keeps failing must not hold candidates empty for ever")
	client.fail = false
	ba.processNewBlockAnnouncement(t.Context())
	require.False(t, ba.bestTipResetReplayPending.Load())
	requireBlockAssemblyTip(t, items, a[1])
}

// noBestTipOrderClient is a blockchain client whose BlocksBetterThan fails, as a blockchain
// service that does not serve the best-tip order yet, or one the replay keeps failing on. With
// failMarkSave it refuses writes of SkippedForkMarkStateKey. afterList runs once, after the first
// GetBlockHeadersFromHeight. fail names a method that fails while it is set (GetState only for
// SkippedForkMarkStateKey). staleMinedSet makes header reads report mined_set unset, as their
// cache can after SetBlockMinedSet. headerIDsCalls counts GetBlockHeaderIDs calls, one per load
// of the unmined transactions, and onIsMined runs on every GetBlockIsMined.
type noBestTipOrderClient struct {
	blockchain.ClientI
	failMarkSave bool
	// failListSave refuses a write of SkippedForkMarkStateKey that carries held transactions.
	failListSave   bool
	afterList      func()
	fail           string
	staleMinedSet  bool
	headerIDsCalls int
	onIsMined      func()
	// maxHeaders rejects a GetBlockHeadersFromHeight of more heights, as the blockchain service
	// does above blockchain_maxBlockHeadersPerRequest; only the mark reads it, and its best-chain
	// read shares the page size. emptyHeaders makes GetBlockHeaders return
	// none. blockHeaderCalls counts GetBlockHeader calls. onBestCall runs before each
	// GetBestBlockHeader with its number, counting from 1, and failBestCall fails that call.
	onSave           func(value byte)
	maxHeaders       int
	maxHeadersAll    int
	mismatchMetas    bool
	emptyHeaders     bool
	blockHeaderCalls int
	bestCalls        int
	onBestCall       func(n int)
	failBestCall     int
}

func (c *noBestTipOrderClient) failing(method string) error {
	if c.fail == method {
		return errors.NewServiceError("%s failed", method)
	}

	return nil
}

func (c *noBestTipOrderClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.bestCalls++

	if c.onBestCall != nil {
		c.onBestCall(c.bestCalls)
	}

	if c.failBestCall == c.bestCalls {
		return nil, nil, errors.NewServiceError("GetBestBlockHeader failed")
	}

	if err := c.failing("GetBestBlockHeader"); err != nil {
		return nil, nil, err
	}

	return c.ClientI.GetBestBlockHeader(ctx)
}

func (c *noBestTipOrderClient) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if err := c.failing("GetBlockHeaders"); err != nil {
		return nil, nil, err
	}

	if c.maxHeadersAll > 0 && n > uint64(c.maxHeadersAll) {
		return nil, nil, errors.NewInvalidArgumentError("%d headers requested, maximum is %d", n, c.maxHeadersAll)
	}

	if c.emptyHeaders {
		return nil, nil, nil
	}

	if c.mismatchMetas {
		headers, metas, err := c.ClientI.GetBlockHeaders(ctx, hash, n)
		if err != nil || len(metas) == 0 {
			return headers, metas, err
		}

		return headers, metas[:len(metas)-1], nil
	}

	headers, metas, err := c.ClientI.GetBlockHeaders(ctx, hash, n)
	if err != nil || !c.staleMinedSet {
		return headers, metas, err
	}

	stale := make([]*model.BlockHeaderMeta, len(metas))
	for i, m := range metas {
		copied := *m
		copied.MinedSet = false
		stale[i] = &copied
	}

	return headers, stale, nil
}

func (c *noBestTipOrderClient) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.blockHeaderCalls++

	if err := c.failing("GetBlockHeader"); err != nil {
		return nil, nil, err
	}

	header, meta, err := c.ClientI.GetBlockHeader(ctx, hash)
	if err != nil || !c.staleMinedSet {
		return header, meta, err
	}

	stale := *meta
	stale.MinedSet = false

	return header, &stale, nil
}

func (c *noBestTipOrderClient) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if err := c.failing("GetBlockHeadersFromHeight"); err != nil {
		return nil, nil, err
	}

	if c.maxHeaders > 0 && limit > uint32(c.maxHeaders) { //nolint:gosec // a small test limit
		return nil, nil, errors.NewInvalidArgumentError("invalid count %d, maximum is %d", limit, c.maxHeaders)
	}

	headers, metas, err := c.ClientI.GetBlockHeadersFromHeight(ctx, height, limit)

	if c.afterList != nil {
		hook := c.afterList
		c.afterList = nil
		hook()
	}

	return headers, metas, err
}

func (c *noBestTipOrderClient) GetState(ctx context.Context, key string) ([]byte, error) {
	if key == SkippedForkMarkStateKey {
		if err := c.failing("GetState"); err != nil {
			return nil, err
		}
	}

	return c.ClientI.GetState(ctx, key)
}

// unreachableSubtreeStore is a subtree store that cannot be reached for a file it does not have:
// it reports a storage error, not that the file is missing, as a store that is down does.
type unreachableSubtreeStore struct {
	blob.Store
}

func (s *unreachableSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	r, err := s.Store.GetIoReader(ctx, key, fileType, opts...)
	if errors.Is(err, errors.ErrNotFound) {
		return nil, errors.NewStorageError("subtree store unreachable")
	}

	return r, err
}

// Exists cannot say a file is not there either.
func (s *unreachableSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (bool, error) {
	exists, err := s.Store.Exists(ctx, key, fileType, opts...)
	if err == nil && !exists {
		return false, errors.NewStorageError("subtree store unreachable")
	}

	return exists, err
}

// failingUtxoStore is a UTXO store whose BatchDecorate or MarkTransactionsOnLongestChain fails
// while fail names it.
type failingUtxoStore struct {
	utxo.Store
	fail string
}

func (s *failingUtxoStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	if s.fail == "BatchDecorate" {
		return errors.NewStorageError("BatchDecorate failed")
	}

	return s.Store.BatchDecorate(ctx, items, f...)
}

func (s *failingUtxoStore) MarkTransactionsOnLongestChain(ctx context.Context, hashes []chainhash.Hash, onLongestChain bool) error {
	if s.fail == "MarkTransactionsOnLongestChain" || (onLongestChain && s.fail == "MarkTransactionsOnLongestChain(true)") {
		return errors.NewStorageError("MarkTransactionsOnLongestChain failed")
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, hashes, onLongestChain)
}

func (c *noBestTipOrderClient) GetBlockHeaderIDs(ctx context.Context, hash *chainhash.Hash, n uint64) ([]uint32, error) {
	c.headerIDsCalls++

	if err := c.failing("GetBlockHeaderIDs"); err != nil {
		return nil, err
	}

	return c.ClientI.GetBlockHeaderIDs(ctx, hash, n)
}

func (c *noBestTipOrderClient) GetBlockIsMined(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	if c.onIsMined != nil {
		c.onIsMined()
	}

	if err := c.failing("GetBlockIsMined"); err != nil {
		return false, err
	}

	return c.ClientI.GetBlockIsMined(ctx, hash)
}

func (c *noBestTipOrderClient) CheckBlockIsAncestorOfBlock(ctx context.Context, ids []uint32, hash *chainhash.Hash) (bool, error) {
	if err := c.failing("CheckBlockIsAncestorOfBlock"); err != nil {
		return false, err
	}

	return c.ClientI.CheckBlockIsAncestorOfBlock(ctx, ids, hash)
}

func (c *noBestTipOrderClient) BlocksBetterThan(context.Context, model.BestTipKey, *model.BestTipKey, uint32, uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, bool, error) {
	return nil, nil, false, errors.NewServiceError("best-tip order not served")
}

func (c *noBestTipOrderClient) SetState(ctx context.Context, key string, data []byte) error {
	if key == SkippedForkMarkStateKey && (c.failMarkSave || c.fail == "SetState" || (c.failListSave && len(data) > 1)) {
		return errors.NewStorageError("state write refused")
	}

	if err := c.ClientI.SetState(ctx, key, data); err != nil {
		return err
	}

	if c.onSave != nil && key == SkippedForkMarkStateKey && len(data) == 1 {
		c.onSave(data[0])
	}

	return nil
}

// startBlockAssembler starts a new block assembler on the test's stores, as a restart does, with
// subtreeStore, the test's blob store when nil. The subtree processor of one whose start failed is
// stopped.
func startBlockAssembler(t *testing.T, items *baTestItems, tSettings *settings.Settings, subtreeStore blob.Store) (*BlockAssembler, error) {
	t.Helper()

	if subtreeStore == nil {
		subtreeStore = items.blobStore
	}

	restarted, err := NewBlockAssembler(t.Context(), ulogger.TestLogger{}, tSettings, gocore.NewStat("test"), items.utxoStore, subtreeStore, items.blockchainClient, make(chan subtreeprocessor.NewSubtreeRequest, 10))
	require.NoError(t, err)
	restarted.SetSkipWaitForPendingBlocks(true)

	if err = restarted.Start(t.Context()); err != nil {
		restarted.subtreeProcessor.Stop(context.Background())
	}

	return restarted, err
}

// TestFollowBestHeader_MarksTxsOfSkippedTip is the case following the best header skips. Block
// assembly is on A1 and holds X. B extends A1 with X, becomes the best tip, and block
// validation marks X mined in it; C overtakes B before block assembly looks. The replay would
// move onto B and back. Without it block assembly follows the best header from A1 to C2, B is
// never moved back, and X would stay marked mined, so a restart, which reloads only unmined
// transactions, would drop it. Both ways the replay falls back are covered, a fork block that
// cannot be read does not keep B's transactions from being marked, and a run that could not
// read B is finished by a later pass even when the replay works again by then, or by the next
// start when block assembly restarts first. B is found when its only child is invalid, so
// the chain tip of its fork is that child, and when its fork is longer than one read of
// headers. A start does not finish while a fork block it owes cannot be read, and block
// assembly does not follow the best header while it cannot record the mark it then owes.
func TestFollowBestHeader_MarksTxsOfSkippedTip(t *testing.T) {
	for _, tc := range []struct {
		name string
		// positionKnown sets a replay position, which the replay then fails to move on from.
		positionKnown bool
		// unreadableFork adds a fork block whose subtree the subtree store cannot be reached
		// for, still at the first restart, which therefore fails.
		unreadableFork bool
		// subtreeLate makes B's subtree unreachable until after the fallback, and the next pass
		// replays. The restart sees it missing until then.
		subtreeLate bool
		// subtreeGone leaves B's subtree missing from the subtree store for good.
		subtreeGone bool
		// restartFirst restarts block assembly before that next pass.
		restartFirst bool
		// startUnreadable makes that restart happen while the subtree store still cannot be
		// reached for B; it fails.
		startUnreadable bool
		// invalidChild gives B a child D that is then invalidated.
		invalidChild bool
		// failSave refuses the first fallback's write of the pending mark.
		failSave bool
		// forkAbove builds that many blocks on B, so its fork is longer.
		forkAbove int
		// headerLimit is blockchain_maxBlockHeadersPerRequest, 0 for the default.
		headerLimit int
		// bestMovesOntoB extends B's fork past C while the mark walks the forks.
		bestMovesOntoB bool
		// resetFirst resets block assembly to the best tip before the passes, as a reset the
		// replay does not follow.
		resetFirst bool
		// refusedReset refuses the reset's write of the pending mark once, so the reset is
		// refused, and lets the next reset through.
		refusedReset bool
	}{
		{name: "best-tip order not served"},
		{name: "replay given up", positionKnown: true},
		{name: "another fork block cannot be read", unreadableFork: true},
		{name: "B read after the replay works again", positionKnown: true, subtreeLate: true},
		{name: "B read by the next start", positionKnown: true, subtreeLate: true, restartFirst: true},
		{name: "B unreadable through a start", positionKnown: true, subtreeLate: true, restartFirst: true, startUnreadable: true},
		{name: "B under an invalid child", invalidChild: true},
		{name: "pending mark cannot be saved", failSave: true},
		{name: "fork and best chain longer than one read", forkAbove: 3, headerLimit: 2},
		{name: "one height per read", forkAbove: 3, headerLimit: 1},
		{name: "B on the best chain by the time it is marked", bestMovesOntoB: true},
		{name: "B skipped by a reset without the best-tip order", resetFirst: true},
		{name: "B skipped by a reset, then the replay given up", positionKnown: true, resetFirst: true},
		{name: "history not given up while the pending mark cannot be saved", positionKnown: true, resetFirst: true, failSave: true},
		{name: "reset refused while its pending mark cannot be saved", resetFirst: true, refusedReset: true},
		{name: "B's subtree gone", subtreeGone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initPrometheusMetrics()
			ctx := t.Context()

			items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
				s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
				if tc.headerLimit > 0 {
					s.BlockChain.MaxBlockHeadersPerRequest = tc.headerLimit
				}

			}, withCoinbaseMaturity(testCoinbaseMaturity))
			ba := items.blockAssembler
			ba.subtreeProcessor.Stop(context.Background())
			stp, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
			require.NoError(t, err)
			ba.subtreeProcessor = stp
			stp.Start(ctx)

			genesis := genesisHeader(t, items)
			a1 := buildChain(genesis, 1, 60000)[0]
			addBlockWithMinedSet(ctx, t, items, a1)

			if tc.positionKnown {
				startReplayAt(t, items, a1, 1)
			} else {
				ba.setBestBlockHeader(a1, 1)
			}

			stp.InitCurrentBlockHeader(a1)
			require.NoError(t, items.utxoStore.SetBlockHeight(2))

			// X spends a confirmed transaction and is in block assembly.
			funding := replayPoolTx(&chainhash.Hash{}, 100)
			x := replayPoolTx(funding.TxIDChainHash(), 99)
			for _, tx := range []*bt.Tx{funding, x} {
				_, err = items.utxoStore.(*utxosql.Store).Create(ctx, tx, 1)
				require.NoError(t, err)
			}
			require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*funding.TxIDChainHash()}, true))

			stp.AddBatch([]subtree.Node{{Hash: *x.TxIDChainHash(), Fee: 1, SizeInBytes: uint64(len(x.Bytes()))}}, []*subtree.TxInpoints{{}})
			require.Eventually(t, func() bool { return stp.GetCurrentTxMap().Exists(*x.TxIDChainHash()) }, 5*time.Second, 10*time.Millisecond, "X is in block assembly")

			// Y spends the same confirmed transaction, and only B has it: block assembly never held it.
			y := replayPoolTx(funding.TxIDChainHash(), 98)
			_, err = items.utxoStore.(*utxosql.Store).Create(ctx, y, 1)
			require.NoError(t, err)

			// B extends A1 with X and Y and is the best tip; block validation marks both mined in it.
			tree, err := subtree.NewTreeByLeafCount(4)
			require.NoError(t, err)
			require.NoError(t, tree.AddCoinbaseNode())
			require.NoError(t, tree.AddNode(*x.TxIDChainHash(), 1, uint64(len(x.Bytes()))))
			require.NoError(t, tree.AddNode(*y.TxIDChainHash(), 1, uint64(len(y.Bytes()))))
			treeBytes, err := tree.Serialize()
			require.NoError(t, err)

			if !tc.subtreeLate && !tc.subtreeGone {
				require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))
			}

			if tc.subtreeLate || tc.unreadableFork {
				ba.subtreeStore = &unreachableSubtreeStore{Store: items.blobStore}
			}

			b := buildChain(a1, 1, 61000)[0]
			coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
			require.NoError(t, err)
			// Block validation sets subtrees_set before it marks a block's transactions mined.
			require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: b, CoinbaseTx: coinbase, TransactionCount: 3, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(true)))

			// Blocks on B, without subtrees, make its fork longer than one read of headers.
			forkTip := b
			for _, h := range buildChain(b, tc.forkAbove, 65000) {
				addBlockWithMinedSet(ctx, t, items, h)
				forkTip = h
			}

			best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
			require.NoError(t, err)
			require.Equal(t, forkTip.Hash().String(), best.Hash().String(), "B's fork was the best chain")

			_, genMeta, err := items.blockchainClient.GetBlockHeader(ctx, genesis.Hash())
			require.NoError(t, err)
			_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0, OnLongestChain: true})
			require.NoError(t, err)

			_, bMeta, err := items.blockchainClient.GetBlockHeader(ctx, b.Hash())
			require.NoError(t, err)
			_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{x.TxIDChainHash(), y.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: bMeta.ID, BlockHeight: 2, OnLongestChain: true})
			require.NoError(t, err)
			require.Zero(t, unminedSince(t, items, x), "precondition: X is marked mined in B")

			if tc.invalidChild {
				// D extends B and is invalidated; block validation processes the invalidation and
				// sets D's mined_set again. B is no longer a chain tip.
				d := buildChain(b, 1, 64000)[0]
				addBlockWithMinedSet(ctx, t, items, d)
				_, err = items.blockchainClient.InvalidateBlock(ctx, d.Hash())
				require.NoError(t, err)
				require.NoError(t, items.blockchainClient.SetBlockMinedSet(ctx, d.Hash()))
			}

			// C overtakes B's fork before block assembly looks.
			c := buildChain(a1, 2+tc.forkAbove, 62000)
			for _, h := range c {
				addBlockWithMinedSet(ctx, t, items, h)
			}
			cTip := c[len(c)-1]

			var eTree *subtree.Subtree
			if tc.unreadableFork {
				// E has as much work as C, so it is read before B, and C, stored first, stays
				// the best tip. E's subtree is not in the subtree store yet.
				eTree, err = subtree.NewTreeByLeafCount(2)
				require.NoError(t, err)
				require.NoError(t, eTree.AddCoinbaseNode())
				require.NoError(t, eTree.AddNode(chainhash.HashH([]byte("E only")), 1, 100))

				for _, e := range buildChain(a1, 2, 63000) {
					require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: e, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{eTree.RootHash()}}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(true)))
				}
			}

			best, _, err = items.blockchainClient.GetBestBlockHeader(ctx)
			require.NoError(t, err)
			require.Equal(t, cTip.Hash().String(), best.Hash().String())

			client := &noBestTipOrderClient{ClientI: items.blockchainClient, failMarkSave: tc.failSave || tc.refusedReset, maxHeaders: tc.headerLimit}
			ba.blockchainClient = client

			if tc.bestMovesOntoB {
				client.afterList = func() {
					for _, h := range buildChain(b, 2, 66000) {
						addBlockWithMinedSet(ctx, t, items, h)
					}
				}
			}

			passes := 1
			if tc.positionKnown {
				passes = maxBestTipReplayFailures
			}

			if tc.resetFirst {
				answer := make(chan error, 1)

				if tc.refusedReset {
					// The reset would persist its tip with nothing recorded to say the fork blocks
					// it skips still need marking, so it does not run.
					ba.handleResetRequests(ctx, resetRequest{ErrCh: answer})
					require.Error(t, <-answer, "the reset is refused")
					requireBlockAssemblyTip(t, items, a1)
					require.Zero(t, unminedSince(t, items, y), "precondition: nothing is marked before the record")

					client.failMarkSave = false
				}

				client.headerIDsCalls = 0
				ba.handleResetRequests(ctx, resetRequest{ErrCh: answer})
				require.NoError(t, <-answer)
				requireBlockAssemblyTip(t, items, cTip)

				if !tc.positionKnown {
					// The reset owed the mark and ran it first, so its one reload took Y in.
					require.False(t, ba.skippedForkMarkPending, "the reset ran the mark it owed")
					require.Equal(t, 1, client.headerIDsCalls, "one load of the unmined transactions, the reset's own")
					require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the reset's reload holds Y")
				}
			}

			recordFailures := testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_record"))

			for range passes {
				ba.processNewBlockAnnouncement(ctx)
			}

			if tc.failSave {
				require.Greater(t, testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_record")), recordFailures, "the refused record is counted")
				if !tc.resetFirst {
					require.Equal(t, skippedForkMarkRetryBase, ba.skippedForkMarkWait, "the refusal is the pass's one attempt")
				}
			}

			if tc.failSave {
				if tc.resetFirst {
					// Giving the history up would persist the replay position past B with nothing
					// recorded to say B's transactions still need marking, so it is kept.
					requireReplayPosition(t, items, a1)
				} else {
					// The move would persist the new tip with nothing recorded to say the fork
					// blocks it skipped still need marking, so block assembly stays where it is.
					requireBlockAssemblyTip(t, items, a1)
				}

				require.Zero(t, unminedSince(t, items, y), "precondition: nothing is marked before the record")
				require.True(t, ba.reconcileDue(time.Now().Add(skippedForkMarkRetryMax)), "the failed fallback is retried without waiting for a block")

				client.failMarkSave = false
				ba.processNewBlockAnnouncement(ctx)
			}

			requireBlockAssemblyTip(t, items, cTip)
			require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*x.TxIDChainHash()), "X is still in block assembly")

			if tc.subtreeGone {
				// Nothing can read B's transactions. B is given up, not retried for ever, and
				// block assembly keeps going: a raised retention can lawfully delete the file.
				require.Zero(t, unminedSince(t, items, y), "nothing can mark Y")
				require.False(t, ba.skippedForkMarkPending, "B is given up, not retried")
				require.Contains(t, ba.skippedForkBlocksMarked, *b.Hash(), "B is recorded as done")

				ba.subtreeProcessor.Stop(context.Background())

				// A start that owes the mark gives B up too.
				require.NoError(t, items.blockchainClient.SetState(ctx, SkippedForkMarkStateKey, []byte{1}))
				_, err = startBlockAssembler(t, items, ba.settings, nil)
				require.NoError(t, err, "a start does not fail on it")

				record, err := items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
				require.NoError(t, err)
				require.Equal(t, []byte{0}, record, "the start finished the mark")

				return
			}

			if tc.bestMovesOntoB {
				// B is on the best chain now, so X and Y are mined on it; moving there is a reorg.
				require.Zero(t, unminedSince(t, items, x), "X, mined in B on the best chain, stays marked mined")
				require.Zero(t, unminedSince(t, items, y), "Y, mined in B on the best chain, stays marked mined")
				require.Nil(t, client.afterList, "the fork blocks were listed")
				require.NotContains(t, ba.skippedForkBlocksMarked, *b.Hash(), "B, whose transactions the best chain took back, is read again on a later run")

				return
			}

			if tc.subtreeLate {
				require.Zero(t, unminedSince(t, items, x), "precondition: B could not be read at the fallback")

				if tc.startUnreadable {
					// The restart cannot reach the store for B either: it must not load the pool
					// without X.
					ba.subtreeProcessor.Stop(context.Background())

					_, err = startBlockAssembler(t, items, ba.settings, &unreachableSubtreeStore{Store: items.blobStore})
					require.Error(t, err, "a start that cannot finish the mark it owes fails")
					require.Zero(t, unminedSince(t, items, x), "precondition: B is still unreadable")
				}

				// The subtree store recovers, and so does the replay.
				require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))
				ba.subtreeStore = items.blobStore

				if !tc.restartFirst {
					ba.blockchainClient = items.blockchainClient
					ba.processNewBlockAnnouncement(ctx)
					requireBlockAssemblyTip(t, items, cTip)
				}
			}

			if !tc.restartFirst {
				require.NotZero(t, unminedSince(t, items, x), "X is marked not on the longest chain")
				require.NotZero(t, unminedSince(t, items, y), "Y is marked not on the longest chain")
				require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the pool is reloaded with Y, which only B had")
				require.False(t, ba.bestTipResetReplayPending.Load(), "the reload on block assembly's own tip leaves no tip history pending, so mining candidates are built")
				require.Equal(t, tc.unreadableFork, ba.skippedForkMarkPending, "only a fork block that could not be read keeps the mark pending")
			}

			if tc.unreadableFork {
				// A failed run is retried after a back-off, not on every idle tick.
				require.False(t, ba.reconcileDue(time.Now()), "the failed run is not retried at once")
				require.True(t, ba.reconcileDue(time.Now().Add(skippedForkMarkRetryMax)), "the failed run is retried after its back-off")

				// The restart cannot reach the store for E: it must not report success with the
				// mark unfinished.
				ba.subtreeProcessor.Stop(context.Background())

				_, err = startBlockAssembler(t, items, ba.settings, &unreachableSubtreeStore{Store: items.blobStore})
				require.Error(t, err, "a start that cannot finish the mark it owes fails")

				eBytes, err := eTree.Serialize()
				require.NoError(t, err)
				require.NoError(t, items.blobStore.Set(ctx, eTree.RootHash()[:], fileformat.FileTypeSubtree, eBytes))
			}

			// A restart reloads the unmined transactions, and X is one of them.
			ba.subtreeProcessor.Stop(context.Background())

			restarted, err := startBlockAssembler(t, items, ba.settings, nil)
			require.NoError(t, err)
			require.NotZero(t, unminedSince(t, items, x), "X is marked not on the longest chain after the restart")
			require.False(t, restarted.skippedForkReloadPending, "the start's load of the unmined transactions is the reload")

			require.Eventually(t, func() bool { return restarted.subtreeProcessor.GetCurrentTxMap().Exists(*x.TxIDChainHash()) }, 10*time.Second, 10*time.Millisecond, "the restart reloads X")
			require.True(t, restarted.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the restart reloads Y")
		})
	}
}

// TestFollowBestHeader_PendingMarkWaitsForTheBestTip: a pending mark runs on a pass only when
// block assembly is on the best tip. Off it, here while the replay is failing, its own blocks
// would read as fork blocks.
func TestFollowBestHeader_PendingMarkWaitsForTheBestTip(t *testing.T) {
	for _, invalidChild := range []bool{false, true} {
		t.Run(map[bool]string{false: "best block is a chain tip", true: "best block has an invalid child"}[invalidChild], func(t *testing.T) {
			pendingMarkWaitsForTheBestTip(t, invalidChild)
		})
	}
}

// pendingMarkWaitsForTheBestTip: with invalidChild the best block has an invalid child, so no
// chain tip reads active.
func pendingMarkWaitsForTheBestTip(t *testing.T, invalidChild bool) {
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 90000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1)
	c := buildChain(a1, 2, 91000)
	addChain(t, items, c)

	if invalidChild {
		// Block validation stores a block it finds invalid with its parent.
		d := buildChain(c[1], 1, 92000)[0]
		coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
		require.NoError(t, err)
		require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: d, CoinbaseTx: coinbase, TransactionCount: 1, Subtrees: []*chainhash.Hash{}}, "", blockchainoptions.WithInvalid(true), blockchainoptions.WithMinedSet(true)))

		tips, err := items.blockchainClient.GetChainTips(ctx)
		require.NoError(t, err)
		for _, tip := range tips {
			require.NotEqual(t, "active", tip.Status, "precondition: no chain tip reads active")
		}
	}

	client := &noBestTipOrderClient{ClientI: items.blockchainClient}
	items.blockAssembler.blockchainClient = client
	items.blockAssembler.skippedForkMarkPending = true

	// The first failed replay pass neither moves block assembly nor follows the best header.
	items.blockAssembler.processNewBlockAnnouncement(ctx)

	requireBlockAssemblyTip(t, items, a1)
	require.True(t, items.blockAssembler.skippedForkMarkPending, "the mark did not run off the best tip and stays pending")
	require.Zero(t, items.blockAssembler.skippedForkMarkWait, "waiting for the best tip is no failure")
}

// TestFollowBestHeader_ReloadsOnlyForTxsItUnmarks: the mark reloads the pool, a full reset, only
// for a transaction it takes off the longest chain that block assembly could hold and does not.
// F is a fork block that was never the best tip, validated during a catch-up: Z, which only F
// has, is still stamped unmined and block assembly never held it. W lost a double spend to the
// best chain after a block marked it mined, so it is conflicting. Neither needs a reload: Z is
// loaded at the next start as it always was, and W is not loaded at all.
func TestFollowBestHeader_ReloadsOnlyForTxsItUnmarks(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	}, withCoinbaseMaturity(testCoinbaseMaturity))
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	stp, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(ctx)

	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 80000)[0]
	addBlockWithMinedSet(ctx, t, items, a1)
	ba.setBestBlockHeader(a1, 1)
	stp.InitCurrentBlockHeader(a1)
	require.NoError(t, items.utxoStore.SetBlockHeight(2))

	funding := replayPoolTx(&chainhash.Hash{}, 100)
	z := replayPoolTx(funding.TxIDChainHash(), 99)
	w := replayPoolTx(funding.TxIDChainHash(), 98)
	for _, tx := range []*bt.Tx{funding, z} {
		_, err = items.utxoStore.(*utxosql.Store).Create(ctx, tx, 1)
		require.NoError(t, err)
	}

	// W lost a double spend to the best chain.
	_, err = items.utxoStore.(*utxosql.Store).Create(ctx, w, 1, utxo.WithConflicting(true))
	require.NoError(t, err)

	_, genMeta, err := items.blockchainClient.GetBlockHeader(ctx, genesis.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0, OnLongestChain: true})
	require.NoError(t, err)

	// C is the best chain before F is stored, so F never is the best tip.
	c := buildChain(a1, 2, 81000)
	for _, h := range c {
		addBlockWithMinedSet(ctx, t, items, h)
	}

	tree, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*z.TxIDChainHash(), 1, uint64(len(z.Bytes()))))
	require.NoError(t, tree.AddNode(*w.TxIDChainHash(), 1, uint64(len(w.Bytes()))))
	treeBytes, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))

	f := buildChain(a1, 1, 82000)[0]
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: f, CoinbaseTx: coinbase, TransactionCount: 3, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(true)))

	_, fMeta, err := items.blockchainClient.GetBlockHeader(ctx, f.Hash())
	require.NoError(t, err)

	// Block validation marks a block off the longest chain without clearing unmined_since.
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{z.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: fMeta.ID, BlockHeight: 2, OnLongestChain: false})
	require.NoError(t, err)
	zUnminedSince := unminedSince(t, items, z)
	require.NotZero(t, zUnminedSince, "precondition: Z is still stamped unmined")

	// A block marked W mined while it was the best tip.
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{w.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: fMeta.ID, BlockHeight: 2, OnLongestChain: true})
	require.NoError(t, err)
	require.Zero(t, unminedSince(t, items, w), "precondition: W is marked mined")
	wMeta, err := items.utxoStore.Get(ctx, w.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, wMeta.Conflicting, "precondition: W is conflicting")

	ba.blockchainClient = &noBestTipOrderClient{ClientI: items.blockchainClient}
	ba.processNewBlockAnnouncement(ctx)

	requireBlockAssemblyTip(t, items, c[1])
	require.False(t, ba.skippedForkMarkPending, "the mark finished")
	require.NotZero(t, unminedSince(t, items, w), "W, which F's block marked mined, is marked not on the longest chain")
	require.Equal(t, zUnminedSince, unminedSince(t, items, z), "Z, never marked mined, is left as it is")
	require.False(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*z.TxIDChainHash()), "no reload ran: a reload would have loaded Z")
}

// TestSkippedForkMarkRetryBackoff: a run of the mark that fails is retried after a back-off that
// doubles with each failure up to skippedForkMarkRetryMax, and a run that finishes clears it.
func TestSkippedForkMarkRetryBackoff(t *testing.T) {
	items := setupBlockAssemblyTest(t)
	ba := items.blockAssembler
	ba.settings.BlockAssembly.ReconcileInterval = 0

	now := time.Now()
	ba.skippedForkMarkPending = true
	require.True(t, ba.reconcileDue(now), "a pending mark that has not failed runs at once")

	// The wait stays at the maximum however many failures follow.
	wait := skippedForkMarkRetryBase
	for range 70 {
		ba.noteSkippedForkMarkFailure(now)
		require.False(t, ba.reconcileDue(now.Add(wait-time.Millisecond)), "not before the back-off of %s", wait)
		require.True(t, ba.reconcileDue(now.Add(wait)), "after the back-off of %s", wait)
		wait = min(2*wait, skippedForkMarkRetryMax)
	}

	ba.noteSkippedForkMarkSuccess()
	require.True(t, ba.reconcileDue(now), "a run that finished clears the back-off")
}

// TestFollowBestHeader_SavesPendingMarkBeforeTheMove: the move to the best header persists block
// assembly's new tip, so a crash right after it would leave nothing to say the fork blocks it
// skipped still need marking. The pending mark is saved before the move.
func TestFollowBestHeader_SavesPendingMarkBeforeTheMove(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 70000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	items.blockAssembler.setBestBlockHeader(a1, 1)

	addChain(t, items, buildChain(a1, 2, 71000))

	var atMove [][]byte

	record := func(mock.Arguments) {
		data, err := items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
		if err != nil {
			data = nil
		}

		atMove = append(atMove, data)
	}

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Reorg", mock.Anything, mock.Anything).Return(nil).Run(record)
	mockStp.On("MoveForwardBlock", mock.Anything).Return(nil).Run(record)
	injectMockStp(t, items, mockStp)

	items.blockAssembler.blockchainClient = &noBestTipOrderClient{ClientI: items.blockchainClient}
	items.blockAssembler.processNewBlockAnnouncement(ctx)

	require.NotEmpty(t, atMove, "block assembly followed the best header")
	require.Equal(t, []byte{1}, atMove[0], "the mark is pending before the first move")
}

// unminedSince reads the unmined_since of tx from the UTXO store.
func unminedSince(t *testing.T, items *baTestItems, tx *bt.Tx) uint32 {
	t.Helper()

	meta, err := items.utxoStore.Get(t.Context(), tx.TxIDChainHash(), fields.UnminedSince)
	require.NoError(t, err)

	return meta.UnminedSince
}

// skippedForkSetup is block assembly on C2, the best tip, with a real subtree processor. F, a
// fork block on A1, has Y, which F's block marked mined and block assembly does not hold. F is
// read by markSkippedForkTxs unless its subtree is withheld; fMinedSet says whether block
// validation has finished F.
func skippedForkSetup(t *testing.T, withSubtree, fMinedSet bool, opts ...func(*settings.Settings)) (*baTestItems, *noBestTipOrderClient, *model.BlockHeader, *bt.Tx) {
	t.Helper()
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, append([]func(*settings.Settings){func(s *settings.Settings) {
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	}, withCoinbaseMaturity(testCoinbaseMaturity)}, opts...)...)
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	stp, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(ctx)

	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 100000)[0]
	addBlockWithMinedSet(ctx, t, items, a1)

	funding := replayPoolTx(&chainhash.Hash{}, 100)
	y := replayPoolTx(funding.TxIDChainHash(), 97)
	for _, tx := range []*bt.Tx{funding, y} {
		_, err = items.utxoStore.(*utxosql.Store).Create(ctx, tx, 1)
		require.NoError(t, err)
	}

	_, genMeta, err := items.blockchainClient.GetBlockHeader(ctx, genesis.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0, OnLongestChain: true})
	require.NoError(t, err)

	tree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*y.TxIDChainHash(), 1, uint64(len(y.Bytes()))))

	if withSubtree {
		treeBytes, err := tree.Serialize()
		require.NoError(t, err)
		require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))
	}

	f := buildChain(a1, 1, 101000)[0]
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: f, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(fMinedSet)))

	_, fMeta, err := items.blockchainClient.GetBlockHeader(ctx, f.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{y.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: fMeta.ID, BlockHeight: 2, OnLongestChain: true})
	require.NoError(t, err)

	c := buildChain(a1, 2, 102000)
	for _, h := range c {
		addBlockWithMinedSet(ctx, t, items, h)
	}

	ba.setBestBlockHeader(c[1], 3)
	stp.InitCurrentBlockHeader(c[1])

	client := &noBestTipOrderClient{ClientI: items.blockchainClient}
	ba.blockchainClient = client

	return items, client, f, y
}

// TestMarkSkippedForkTxs_FailuresKeepItPending: a run that cannot read what it needs, cannot
// reload the pool or cannot record its end returns an error and stays pending, and the next run
// after the store recovers finishes it.
func TestMarkSkippedForkTxs_FailuresKeepItPending(t *testing.T) {
	for _, method := range []string{"GetBestBlockHeader", "GetBlockHeadersFromHeight", "GetBlockHeader", "GetBlockHeaders", "CheckBlockIsAncestorOfBlock", "GetBlockHeaderIDs", "GetBlockIsMined", "SetState", "BatchDecorate", "MarkTransactionsOnLongestChain"} {
		t.Run(method, func(t *testing.T) {
			items, client, _, y := skippedForkSetup(t, true, true)
			ba := items.blockAssembler
			ba.settings.BlockAssembly.ReconcileInterval = 0

			store := &failingUtxoStore{Store: items.utxoStore}
			ba.utxoStore = store

			client.fail, store.fail = method, method
			if method == "GetBlockIsMined" {
				// It is asked only when the read headers say mined_set is not set.
				client.staleMinedSet = true
			}

			require.Error(t, ba.markSkippedForkTxs(t.Context(), false))
			require.True(t, ba.skippedForkMarkPending, "the run stays pending")
			require.False(t, ba.reconcileDue(time.Now()), "and is retried after a back-off")
			// A block is recorded once its own mark is written; the pool reload and the record
			// of the run's end come after it.
			switch method {
			case "SetState":
			case "GetBlockHeaderIDs":
				require.NotEmpty(t, ba.skippedForkBlocksMarked, "a block whose mark was written is recorded although the reload failed")
				require.True(t, ba.skippedForkReloadPending, "the reload is still owed")
			default:
				require.Empty(t, ba.skippedForkBlocksMarked, "a block whose mark failed is not recorded")
			}

			client.fail, store.fail = "", ""
			require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
			require.False(t, ba.skippedForkMarkPending)
			require.False(t, ba.skippedForkReloadPending, "the pool reload ran")
			require.NotZero(t, unminedSince(t, items, y), "Y is marked not on the longest chain")
			require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the pool holds Y")
			require.Zero(t, ba.skippedForkMarkWait, "a run that finished clears the back-off")

			record, err := items.blockchainClient.GetState(t.Context(), SkippedForkMarkStateKey)
			require.NoError(t, err)
			require.Equal(t, []byte{0}, record, "a run that finished records that nothing is pending")
		})
	}
}

// TestMarkSkippedForkTxs_WaitsForMinedSet: a fork block block validation has not finished is
// left for a later run, so its setTxMined cannot clear unmined_since after the mark.
func TestMarkSkippedForkTxs_WaitsForMinedSet(t *testing.T) {
	items, client, f, y := skippedForkSetup(t, true, false)
	ba := items.blockAssembler
	ctx := t.Context()

	require.Error(t, ba.markSkippedForkTxs(ctx, false))
	require.True(t, ba.skippedForkMarkPending, "F is left for a later run")
	require.Zero(t, unminedSince(t, items, y), "nothing of F is marked yet")

	// The header read can still say mined_set is not set, from a cache SetBlockMinedSet does
	// not clear; the mark asks the block itself.
	require.NoError(t, items.blockchainClient.SetBlockMinedSet(ctx, f.Hash()))
	client.staleMinedSet = true
	require.NoError(t, ba.markSkippedForkTxs(ctx, false))
	require.False(t, ba.skippedForkMarkPending)
	require.NotZero(t, unminedSince(t, items, y), "Y is marked once block validation finished F")
}

// TestMarkSkippedForkTxs_ReadsOnlyForksWithRetainedSubtrees: a fork block whose subtree file the
// subtree retention may have expired is not read, so a run, and a start, do not fail on it. F is
// at height 2 with no subtree; with the best tip at 3 and a subtree retention of 2 the mark reads
// from height 3.
func TestMarkSkippedForkTxs_ReadsOnlyForksWithRetainedSubtrees(t *testing.T) {
	items, _, f, y := skippedForkSetup(t, false, true, func(s *settings.Settings) {
		s.GlobalBlockHeightRetention = 10
		s.SubtreeValidation.BlockHeightRetentionAdjustment = -8
	})
	ba := items.blockAssembler

	stale := chainhash.HashH([]byte("recorded below the floor"))
	ba.skippedForkBlocksMarked = map[chainhash.Hash]uint32{stale: 1}

	require.Equal(t, uint32(3), ba.skippedForkMarkFloor(3))
	require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
	require.False(t, ba.skippedForkMarkPending)
	require.Zero(t, unminedSince(t, items, y), "F is below the floor and not read")
	require.NotContains(t, ba.skippedForkBlocksMarked, *f.Hash(), "F was not read, so not given up either")
	require.NotContains(t, ba.skippedForkBlocksMarked, stale, "a record below the floor is dropped")
}

// TestFollowBestHeader_FailedMoveBacksOff: a fallback move that fails leaves the mark it owes
// pending, and the idle wake retries it after a back-off, not on every tick.
//
// A move can fail after it put block assembly on the best tip, as a reset that fails after it set
// the tip does. The failure is the pass's one attempt then too: the owed mark, whose reload is a
// reset as well, waits for the back-off.
func TestFollowBestHeader_FailedMoveBacksOff(t *testing.T) {
	for _, reachesTip := range []bool{false, true} {
		t.Run(map[bool]string{false: "off the tip", true: "on the tip"}[reachesTip], func(t *testing.T) {
			initPrometheusMetrics()
			ctx := t.Context()

			items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
			genesis := genesisHeader(t, items)

			a1 := buildChain(genesis, 1, 110000)[0]
			addChain(t, items, []*model.BlockHeader{a1})
			items.blockAssembler.setBestBlockHeader(a1, 1)
			c := buildChain(a1, 2, 111000)
			addChain(t, items, c)

			fail := func(mock.Arguments) {
				if reachesTip {
					items.blockAssembler.setBestBlockHeader(c[1], 3)
				}
			}

			mockStp := &subtreeprocessor.MockSubtreeProcessor{}
			mockStp.On("Reorg", mock.Anything, mock.Anything).Run(fail).Return(errors.NewProcessingError("move failed"))
			mockStp.On("MoveForwardBlock", mock.Anything).Run(fail).Return(errors.NewProcessingError("move failed"))
			injectMockStp(t, items, mockStp)

			client := &noBestTipOrderClient{ClientI: items.blockchainClient}
			listed := false
			client.afterList = func() { listed = true }

			items.blockAssembler.settings.BlockAssembly.ReconcileInterval = 0
			items.blockAssembler.blockchainClient = client
			items.blockAssembler.processNewBlockAnnouncement(ctx)

			if !reachesTip {
				requireBlockAssemblyTip(t, items, a1)
			}

			require.False(t, listed, "the mark does not run in the pass")
			require.True(t, items.blockAssembler.skippedForkMarkPending, "the move owes a mark")
			require.Equal(t, skippedForkMarkRetryBase, items.blockAssembler.skippedForkMarkWait, "the failed move is the pass's one failure")
			require.False(t, items.blockAssembler.reconcileDue(time.Now()), "not retried at once")
			require.True(t, items.blockAssembler.reconcileDue(time.Now().Add(skippedForkMarkRetryMax)), "retried after the back-off")
		})
	}
}

// TestFollowBestHeader_MoveThatResetsIsNoFailure: a fallback move that ran a reset ends on the
// best tip; it is not a failed move, so it does not back the owed mark off.
func TestFollowBestHeader_MoveThatResetsIsNoFailure(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 120000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	items.blockAssembler.setBestBlockHeader(a1, 1)
	addChain(t, items, buildChain(a1, 2, 121000))

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("Reorg", mock.Anything, mock.Anything).Return(errors.NewBlockAssemblyResetError("reset instead"))
	mockStp.On("MoveForwardBlock", mock.Anything).Return(errors.NewBlockAssemblyResetError("reset instead"))
	injectMockStp(t, items, mockStp)

	items.blockAssembler.settings.BlockAssembly.ReconcileInterval = 0
	items.blockAssembler.blockchainClient = &noBestTipOrderClient{ClientI: items.blockchainClient}
	items.blockAssembler.processNewBlockAnnouncement(ctx)

	require.Zero(t, items.blockAssembler.skippedForkMarkWait, "a move that reset is no failure")
	require.True(t, items.blockAssembler.reconcileDue(time.Now()), "the owed mark is not backed off")
}

// TestMarkSkippedForkTxs_NoReloadForATxInThePool: a marked transaction block assembly holds needs
// no pool reload, which is a full reset.
func TestMarkSkippedForkTxs_NoReloadForATxInThePool(t *testing.T) {
	items, _, _, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler

	ba.subtreeProcessor.AddBatch([]subtree.Node{{Hash: *y.TxIDChainHash(), Fee: 1, SizeInBytes: uint64(len(y.Bytes()))}}, []*subtree.TxInpoints{{}})
	require.Eventually(t, func() bool { return ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()) }, 5*time.Second, 10*time.Millisecond, "Y is in block assembly")

	before := ba.subtreeProcessor.GetCurrentTxMap()

	require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
	require.NotZero(t, unminedSince(t, items, y), "Y is marked not on the longest chain")
	require.False(t, ba.skippedForkReloadPending)
	require.Same(t, before, ba.subtreeProcessor.GetCurrentTxMap(), "no reset rotated the tx map")
}

// TestMarkSkippedForkTxs_SkipsABlockInvalidatedSinceTheListing: a fork block invalidated between
// the listing, which reads only valid blocks, and the read of its meta is block validation's to
// unmark.
func TestMarkSkippedForkTxs_SkipsABlockInvalidatedSinceTheListing(t *testing.T) {
	items, client, f, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler

	client.afterList = func() {
		_, err := items.blockchainClient.InvalidateBlock(t.Context(), f.Hash())
		require.NoError(t, err)
	}

	require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
	require.Nil(t, client.afterList, "the blocks were listed")
	require.Zero(t, unminedSince(t, items, y), "Y of the invalid block is left to block validation")
}

// TestFinishSkippedForkMarkAtStart_ReadsTheRecordStrictly: a record that cannot be read fails
// the start, a single byte other than 0 reads as pending, and a longer record that is not a
// pending mark with whole transaction hashes fails the start.
func TestFinishSkippedForkMarkAtStart_ReadsTheRecordStrictly(t *testing.T) {
	t.Run("unreadable", func(t *testing.T) {
		items, client, _, _ := skippedForkSetup(t, true, true)

		client.fail = "GetState"
		require.Error(t, items.blockAssembler.finishSkippedForkMarkAtStart(t.Context()))
	})

	t.Run("unknown byte", func(t *testing.T) {
		items, _, _, y := skippedForkSetup(t, true, true)

		require.NoError(t, items.blockchainClient.SetState(t.Context(), SkippedForkMarkStateKey, []byte{7}))
		require.NoError(t, items.blockAssembler.finishSkippedForkMarkAtStart(t.Context()))
		require.NotZero(t, unminedSince(t, items, y), "the run it reads as pending marked Y")
	})

	// A record that carries held transactions it cannot read would lose them: the start fails.
	for name, record := range map[string][]byte{
		"cut list":     append([]byte{1}, make([]byte, chainhash.HashSize-1)...),
		"list after 0": append([]byte{0}, make([]byte, chainhash.HashSize)...),
		"unknown data": {7, 7},
	} {
		t.Run(name, func(t *testing.T) {
			items, _, _, y := skippedForkSetup(t, true, true)

			require.NoError(t, items.blockchainClient.SetState(t.Context(), SkippedForkMarkStateKey, record))
			require.ErrorContains(t, items.blockAssembler.finishSkippedForkMarkAtStart(t.Context()), SkippedForkMarkStateKey)
			require.Zero(t, unminedSince(t, items, y), "nothing ran")
		})
	}
}

// TestFinishSkippedForkMarkAtStart_WaitsForBlockValidation: block validation starts alongside
// block assembly, so a fork block it is still finishing at start-up is waited for, not a failed
// start.
func TestFinishSkippedForkMarkAtStart_WaitsForBlockValidation(t *testing.T) {
	items, client, f, y := skippedForkSetup(t, true, false)
	ba := items.blockAssembler
	ctx := t.Context()

	require.NoError(t, items.blockchainClient.SetState(ctx, SkippedForkMarkStateKey, []byte{1}))

	// Block validation finishes F just after the first run asked.
	client.onIsMined = func() {
		client.onIsMined = nil

		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = items.blockchainClient.SetBlockMinedSet(context.Background(), f.Hash())
		}()
	}

	require.NoError(t, ba.finishSkippedForkMarkAtStart(ctx))
	require.NotZero(t, unminedSince(t, items, y), "Y is marked once block validation finished F")
}

// TestMarkSkippedForkTxs_FailedReloadLeavesCandidatesBuilt: a pool reload that fails after the
// reset rotated the tx map leaves no tip history pending, which would keep mining candidates
// empty, and while a replay position makes the replay retry that reset the mark does not run a
// second one beside it. A reset that finishes satisfies the reload the mark owes.
func TestMarkSkippedForkTxs_FailedReloadLeavesCandidatesBuilt(t *testing.T) {
	items, client, _, _ := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ctx := t.Context()

	tip, height := ba.CurrentBlock()
	ba.setBestTipPos(bestTipKeyOf(t, items, tip))
	ba.bestTipPosKnown.Store(true)

	client.fail = "GetBlockHeaderIDs"
	require.Error(t, ba.markSkippedForkTxs(ctx, false))
	require.True(t, ba.bestTipResetIncomplete.Load(), "precondition: the reload failed after the rotation")
	require.False(t, ba.bestTipResetReplayPending.Load(), "a failed reload leaves no history pending")
	require.True(t, ba.skippedForkReloadPending)

	client.headerIDsCalls = 0
	require.Error(t, ba.markSkippedForkTxs(ctx, false))
	require.Zero(t, client.headerIDsCalls, "the reload waits for the replay's retry of the incomplete reset")

	client.fail = ""
	require.NoError(t, ba.resetToBlockAndRecord(ctx, tip, height, true))
	require.False(t, ba.skippedForkReloadPending, "the reset's reload is the one owed")
}

// TestHandleResetRequests_SkippedForkMarkBeforeTheReset: without a replay position a reset runs
// the mark it owes first, so its own reload takes in what the mark unmarks. A reset that fails
// leaves that reload owed, and the idle wake retries it even with the reconcile timer off. A
// fork block whose subtree is gone is given up and the reset runs.
func TestHandleResetRequests_SkippedForkMarkBeforeTheReset(t *testing.T) {
	t.Run("reset fails", func(t *testing.T) {
		items, client, _, y := skippedForkSetup(t, true, true)
		ba := items.blockAssembler
		ba.settings.BlockAssembly.ReconcileInterval = 0
		ctx := t.Context()

		answer := make(chan error, 1)

		client.fail = "GetBlockHeaderIDs"
		client.headerIDsCalls = 0
		ba.handleResetRequests(ctx, resetRequest{ErrCh: answer})
		require.Error(t, <-answer, "the reset's reload fails after the tx map rotation")
		require.Equal(t, 1, client.headerIDsCalls, "the failed reset is not followed by a second in the pass")
		require.False(t, ba.reconcileDue(time.Now()), "the owed reload is backed off")
		require.NotZero(t, unminedSince(t, items, y), "the mark before the reset marked Y")
		require.True(t, ba.skippedForkReloadPending, "the reload the failed reset did not do is owed")
		require.True(t, ba.skippedForkWorkOwed())
		require.True(t, ba.reconcileDue(time.Now().Add(skippedForkMarkRetryMax)), "the owed reload wakes a pass with the timer off, after its back-off")

		client.fail = ""
		ba.processNewBlockAnnouncement(ctx)
		require.False(t, ba.skippedForkWorkOwed())
		require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the owed reload puts Y in the pool")
	})

	t.Run("subtree gone", func(t *testing.T) {
		items, client, f, _ := skippedForkSetup(t, false, true)
		ba := items.blockAssembler

		answer := make(chan error, 1)

		client.headerIDsCalls = 0
		ba.handleResetRequests(t.Context(), resetRequest{ErrCh: answer})
		require.NoError(t, <-answer, "the reset runs")
		require.Equal(t, 1, client.headerIDsCalls, "the reset's one reload")
		require.False(t, ba.skippedForkWorkOwed())
		require.Contains(t, ba.skippedForkBlocksMarked, *f.Hash(), "F is given up")
	})
}

// TestMarkSkippedForkTxs_GivesUpABlockWhoseSubtreeIsGone: a fork block within the floor can have
// lost its subtree lawfully, here because the retention was raised after the file was deleted.
// The run gives the block up and finishes, and so does a start that owes it.
func TestMarkSkippedForkTxs_GivesUpABlockWhoseSubtreeIsGone(t *testing.T) {
	items, _, f, y := skippedForkSetup(t, false, true, func(s *settings.Settings) {
		s.GlobalBlockHeightRetention = 1000
	})
	ba := items.blockAssembler
	ctx := t.Context()

	require.LessOrEqual(t, ba.skippedForkMarkFloor(3), uint32(2), "precondition: F is within the floor")

	before := testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_subtree_lost"))

	require.NoError(t, ba.markSkippedForkTxs(ctx, false))
	require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_subtree_lost")))
	require.Contains(t, ba.skippedForkBlocksMarked, *f.Hash(), "F is given up")
	require.Zero(t, unminedSince(t, items, y))

	ba.skippedForkBlocksMarked = nil
	require.NoError(t, items.blockchainClient.SetState(ctx, SkippedForkMarkStateKey, []byte{1}))
	require.NoError(t, ba.finishSkippedForkMarkAtStart(ctx), "a start does not fail on it")
}

// fallbackFailingSubtreeStore has no subtree file, and fails to answer for the one still to be
// checked, as a store that is down does.
type fallbackFailingSubtreeStore struct {
	blob.Store
	// toCheckExists answers Exists: the subtree file is gone and the one still to be checked
	// is there.
	toCheckExists bool
}

func (s *fallbackFailingSubtreeStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (io.ReadCloser, error) {
	if fileType == fileformat.FileTypeSubtree {
		return nil, errors.ErrNotFound
	}

	return nil, errors.NewStorageError("subtree store unreachable")
}

func (s *fallbackFailingSubtreeStore) Exists(_ context.Context, _ []byte, fileType fileformat.FileType, _ ...blobopts.FileOption) (bool, error) {
	if s.toCheckExists {
		return fileType == fileformat.FileTypeSubtreeToCheck, nil
	}

	return false, errors.NewStorageError("subtree store unreachable")
}

// TestMarkSkippedForkTxs_GivesUpOnlyWhenTheStoreSaysTheFilesAreGone: the subtree read reports a
// failure of its fallback read as not found, so a block is given up only once the store answers
// that neither file exists. A store that does not answer leaves the block for a later run.
func TestMarkSkippedForkTxs_GivesUpOnlyWhenTheStoreSaysTheFilesAreGone(t *testing.T) {
	items, _, f, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ba.subtreeStore = &fallbackFailingSubtreeStore{Store: items.blobStore}

	before := testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_subtree_lost"))

	require.Error(t, ba.markSkippedForkTxs(t.Context(), false))
	require.NotContains(t, ba.skippedForkBlocksMarked, *f.Hash(), "F is not given up")
	require.True(t, ba.skippedForkMarkPending)
	require.Equal(t, before, testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_subtree_lost")))

	// The store answers, and the file still to be checked is there.
	ba.subtreeStore = &fallbackFailingSubtreeStore{Store: items.blobStore, toCheckExists: true}
	require.Error(t, ba.markSkippedForkTxs(t.Context(), false))
	require.NotContains(t, ba.skippedForkBlocksMarked, *f.Hash(), "F, whose file is there, is not given up")

	ba.subtreeStore = items.blobStore
	require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
	require.NotZero(t, unminedSince(t, items, y), "the store answers again and Y is marked")
}

// TestMarkSkippedForkTxs_NoReloadWhileABlockIsUnprocessed: a reload is a reset, which waits for
// every block block validation has not finished, so a run with such a fork block keeps the reload
// owed instead of holding block assembly on it.
func TestMarkSkippedForkTxs_NoReloadWhileABlockIsUnprocessed(t *testing.T) {
	items, client, f, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ctx := t.Context()

	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	_, fMeta, err := items.blockchainClient.GetBlockHeader(ctx, f.Hash())
	require.NoError(t, err)

	parent, _, err := items.blockchainClient.GetBlockHeader(ctx, f.HashPrevBlock)
	require.NoError(t, err)

	g := buildChain(parent, 1, 103000)[0]
	missing := chainhash.HashH([]byte("G's subtree"))
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: g, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{&missing}}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(false)))
	require.Equal(t, fMeta.Height, uint32(2))

	client.headerIDsCalls = 0
	require.Error(t, ba.markSkippedForkTxs(ctx, false), "G is not processed yet")
	require.NotZero(t, unminedSince(t, items, y), "F is marked")
	require.Zero(t, client.headerIDsCalls, "no reload while G waits for block validation")
	require.True(t, ba.skippedForkReloadPending, "the reload stays owed")

	require.NoError(t, items.blockchainClient.SetBlockMinedSet(ctx, g.Hash()))
	require.NoError(t, ba.markSkippedForkTxs(ctx, false), "G, whose subtree is gone, is given up")
	require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the reload ran")
}

// TestMarkSkippedForkTxs_RechecksAfterAnUnfinishedRemark: when the check against the best chain
// after a write fails, the transactions it wrote are checked again on the next run, here after
// F joined the best chain during the write.
func TestMarkSkippedForkTxs_RechecksAfterAnUnfinishedRemark(t *testing.T) {
	for _, failure := range []string{"re-mark", "best read"} {
		t.Run(failure, func(t *testing.T) {
			items, client, f, y := skippedForkSetup(t, true, true)
			ba := items.blockAssembler
			ctx := t.Context()

			store := &failingUtxoStore{Store: items.utxoStore}
			ba.utxoStore = store

			var top *model.BlockHeader

			// The second best read is the check after the write: F's chain is best by then.
			client.onBestCall = func(n int) {
				if n == 2 {
					for _, h := range buildChain(f, 2, 104000) {
						addBlockWithMinedSet(ctx, t, items, h)
						top = h
					}
				}
			}

			if failure == "re-mark" {
				store.fail = "MarkTransactionsOnLongestChain(true)"
			} else {
				client.failBestCall = 2
			}

			require.Error(t, ba.markSkippedForkTxs(ctx, false))
			require.NotZero(t, unminedSince(t, items, y), "precondition: Y was written off the longest chain")
			require.NotEmpty(t, ba.skippedForkRecheck)
			require.True(t, ba.skippedForkWorkOwed())

			store.fail, client.onBestCall, client.failBestCall = "", nil, 0
			ba.processNewBlockAnnouncement(ctx)

			requireBlockAssemblyTip(t, items, top)
			require.Empty(t, ba.skippedForkRecheck)
			require.Zero(t, unminedSince(t, items, y), "Y, mined in F on the best chain, is marked mined again")
		})
	}
}

// TestRestampStaleBranch_RemarksWhatTheBestChainTakes: the replay's restamp checks its write
// against the best chain read after it, as the mark does.
func TestRestampStaleBranch_RemarksWhatTheBestChainTakes(t *testing.T) {
	items, client, f, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ctx := t.Context()

	best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	header, meta, err := items.blockchainClient.GetBlockHeader(ctx, f.Hash())
	require.NoError(t, err)

	client.onBestCall = func(n int) {
		if n == 1 {
			for _, h := range buildChain(f, 2, 105000) {
				addBlockWithMinedSet(ctx, t, items, h)
			}
		}
	}

	require.NoError(t, ba.restampStaleBranch(ctx, []blockHeaderWithMeta{{header: header, meta: meta}}, best))
	require.Zero(t, unminedSince(t, items, y), "Y, mined in F on the best chain by then, stays mined")
}

// TestMarkSkippedForkTxs_ReadsNothingTwice: a fork block recorded as marked is not read again.
func TestMarkSkippedForkTxs_ReadsNothingTwice(t *testing.T) {
	items, client, f, _ := skippedForkSetup(t, true, true)
	ba := items.blockAssembler

	require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
	require.Contains(t, ba.skippedForkBlocksMarked, *f.Hash())

	client.blockHeaderCalls = 0
	ba.skippedForkMarkPending = true
	require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
	require.Zero(t, client.blockHeaderCalls, "F is not read again")
}

// TestMarkSkippedForkTxs_LeavesABlockWithoutSubtreesSet: a block block validation has not set
// subtrees for marked nothing mined, even when a subtree file for it exists.
func TestMarkSkippedForkTxs_LeavesABlockWithoutSubtreesSet(t *testing.T) {
	items, _, f, _ := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ctx := t.Context()

	z := replayPoolTx(&chainhash.Hash{1}, 50)
	_, err := items.utxoStore.(*utxosql.Store).Create(ctx, z, 1)
	require.NoError(t, err)

	tree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*z.TxIDChainHash(), 1, uint64(len(z.Bytes()))))
	treeBytes, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))

	parent, _, err := items.blockchainClient.GetBlockHeader(ctx, f.HashPrevBlock)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	g := buildChain(parent, 1, 106000)[0]
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: g, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithSubtreesSet(false), blockchainoptions.WithMinedSet(true)))

	_, gMeta, err := items.blockchainClient.GetBlockHeader(ctx, g.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{z.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: gMeta.ID, BlockHeight: 2, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, ba.markSkippedForkTxs(ctx, false))
	require.Zero(t, unminedSince(t, items, z), "G is not read")
	require.NotContains(t, ba.skippedForkBlocksMarked, *g.Hash())
}

// TestMarkSkippedForkTxs_ReadErrors: a best-chain read that comes back empty is an error, not a
// panic, and the listing beats the liveness heartbeat.
func TestMarkSkippedForkTxs_ReadErrors(t *testing.T) {
	t.Run("empty best-chain read", func(t *testing.T) {
		items, client, _, _ := skippedForkSetup(t, true, true)

		client.emptyHeaders = true
		require.Error(t, items.blockAssembler.markSkippedForkTxs(t.Context(), false))
		require.True(t, items.blockAssembler.skippedForkMarkPending)
	})

	t.Run("listing beats", func(t *testing.T) {
		items, _, _, _ := skippedForkSetup(t, true, true)
		ba := items.blockAssembler

		best, bestMeta, err := items.blockchainClient.GetBestBlockHeader(t.Context())
		require.NoError(t, err)

		ba.heartbeat.Beat()
		ba.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))

		_, err = ba.skippedForkBlocks(t.Context(), best, bestMeta.Height, 0)
		require.NoError(t, err)
		require.Less(t, ba.heartbeat.Age(), time.Minute)
	})
}

// TestHandleResetRequests_RunsOwedWorkOncePerPass: a reset pass ends with the work a mark of
// skipped fork transactions owes, also with a replay position, and a mark that failed before the
// reset is not run, and counted, a second time in the same pass.
func TestHandleResetRequests_RunsOwedWorkOncePerPass(t *testing.T) {
	t.Run("with a replay position", func(t *testing.T) {
		items, client, _, y := skippedForkSetup(t, true, true)
		ba := items.blockAssembler
		ctx := t.Context()

		tip, _ := ba.CurrentBlock()
		ba.setBestTipPos(bestTipKeyOf(t, items, tip))
		ba.bestTipPosKnown.Store(true)
		ba.skippedForkMarkPending = true
		ba.skippedForkMarkRetryAt = time.Now().Add(time.Hour)

		answer := make(chan error, 1)
		client.headerIDsCalls = 0
		ba.handleResetRequests(ctx, resetRequest{ErrCh: answer})
		require.NoError(t, <-answer)
		require.NotZero(t, unminedSince(t, items, y), "the owed mark ran in the reset's pass")
		require.False(t, ba.skippedForkWorkOwed())
		require.Equal(t, 1, client.headerIDsCalls, "the mark ran before the reset, so the reset's one reload took Y in")
		require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()))
	})

	t.Run("mark fails before the reset", func(t *testing.T) {
		items, _, _, _ := skippedForkSetup(t, false, true)
		ba := items.blockAssembler
		ba.subtreeStore = &unreachableSubtreeStore{Store: items.blobStore}

		before := testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_unfinished"))

		answer := make(chan error, 1)
		ba.handleResetRequests(t.Context(), resetRequest{ErrCh: answer})
		require.NoError(t, <-answer, "an unfinished mark does not hold the reset")
		require.True(t, ba.skippedForkMarkPending)
		require.Equal(t, skippedForkMarkRetryBase, ba.skippedForkMarkWait, "one failure in the pass")
		require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_unfinished")))
	})

	t.Run("refused reset answers every request", func(t *testing.T) {
		items, client, _, _ := skippedForkSetup(t, true, true)
		ba := items.blockAssembler
		client.failMarkSave = true

		before := testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_record"))

		first, queued := make(chan error, 1), make(chan error, 1)
		ba.resetCh <- resetRequest{ErrCh: queued}
		client.headerIDsCalls = 0
		ba.handleResetRequests(t.Context(), resetRequest{ErrCh: first, FullReset: true})
		require.Error(t, <-first)
		require.Error(t, <-queued)
		require.Zero(t, client.headerIDsCalls, "neither the consistency scan nor the reset ran")
		require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_record")))
	})
}

// TestMarkSkippedForkTxs_RechecksAfterAPartialWrite: the stores write a mark off the longest chain
// in chunks and may fail after writing some, so a failed write is checked against the best chain
// again, as a failed check after a full write is. Here D, extending the best tip, mines Y between
// the selection and the write, the write marks Y unmined, the write or the check after it fails,
// and block assembly then moves onto D by a plain move forward. Y is in the pool, so no reload is
// owed: the load of the unmined transactions a reload runs would mark Y mined as well. The next
// run reads F again, but Y, mined on the best chain by then, is no longer among F's transactions
// it marks, so only the recheck marks Y mined again.
func TestMarkSkippedForkTxs_RechecksAfterAPartialWrite(t *testing.T) {
	for _, failure := range []string{"write fails part way", "re-mark fails"} {
		t.Run(failure, func(t *testing.T) {
			items, client, _, y := skippedForkSetup(t, true, true)
			ba := items.blockAssembler
			ctx := t.Context()

			ba.subtreeProcessor.AddBatch([]subtree.Node{{Hash: *y.TxIDChainHash(), Fee: 1, SizeInBytes: uint64(len(y.Bytes()))}}, []*subtree.TxInpoints{{}})
			require.Eventually(t, func() bool { return ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()) }, 5*time.Second, 10*time.Millisecond, "Y is in block assembly")

			best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
			require.NoError(t, err)

			d := buildChain(best, 1, 107000)[0]

			store := &partialWriteUtxoStore{Store: items.utxoStore, failRemark: failure == "re-mark fails", beforeWrite: func() {
				addBlockWithMinedSet(ctx, t, items, d)
				_, dMeta, err := items.blockchainClient.GetBlockHeader(ctx, d.Hash())
				require.NoError(t, err)
				_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{y.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: dMeta.ID, BlockHeight: dMeta.Height, OnLongestChain: true})
				require.NoError(t, err)
			}}
			ba.utxoStore = store

			require.Error(t, ba.markSkippedForkTxs(ctx, false))
			require.NotZero(t, unminedSince(t, items, y), "precondition: the write marked Y, mined in D, unmined")
			require.False(t, ba.skippedForkReloadPending, "precondition: Y is in the pool, so no reload is owed")
			require.Len(t, ba.skippedForkRecheck, 1, "Y is left to be checked again")

			store.beforeWrite, store.failRemark = nil, false
			ba.blockchainClient = client
			client.headerIDsCalls = 0
			ba.processNewBlockAnnouncement(ctx)

			requireBlockAssemblyTip(t, items, d)
			require.Zero(t, client.headerIDsCalls, "no reload ran")
			require.Zero(t, unminedSince(t, items, y), "Y, mined in D on the best chain, is marked mined again")
			require.False(t, ba.skippedForkWorkOwed())
		})
	}
}

// partialWriteUtxoStore writes the first false mark it is asked for, after running beforeWrite,
// and then reports an error, as a store that writes in chunks does when a later chunk fails.
// With failRemark the write succeeds and the mark as on the longest chain after it fails.
type partialWriteUtxoStore struct {
	utxo.Store
	beforeWrite func()
	failRemark  bool
	// succeed makes the write after beforeWrite succeed.
	succeed bool
}

func (s *partialWriteUtxoStore) MarkTransactionsOnLongestChain(ctx context.Context, hashes []chainhash.Hash, onLongestChain bool) error {
	if onLongestChain && s.failRemark {
		return errors.NewStorageError("re-mark failed")
	}

	if onLongestChain || s.beforeWrite == nil {
		return s.Store.MarkTransactionsOnLongestChain(ctx, hashes, onLongestChain)
	}

	s.beforeWrite()

	if err := s.Store.MarkTransactionsOnLongestChain(ctx, hashes, false); err != nil {
		return err
	}

	if s.failRemark || s.succeed {
		return nil
	}

	return errors.NewStorageError("a later chunk failed")
}

// TestHandleResetRequests_MarksABlockThatWasBestBeforeTheReset: without a replay position the
// mark runs before the reset, and the reset reads the best tip again. A block that was the best
// tip in between, here B2, which C3 overtook before the reset read the best tip, is marked by the
// run that ends the pass.
func TestHandleResetRequests_MarksABlockThatWasBestBeforeTheReset(t *testing.T) {
	items, client, _, _ := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ctx := t.Context()

	best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	z := replayPoolTx(&chainhash.Hash{2}, 40)
	_, err = items.utxoStore.(*utxosql.Store).Create(ctx, z, 1)
	require.NoError(t, err)

	tree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*z.TxIDChainHash(), 1, uint64(len(z.Bytes()))))
	treeBytes, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))

	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	b2 := buildChain(best, 1, 108000)[0]
	c3 := buildChain(best, 2, 109000)

	// When the mark before the reset records that it finished, B2 becomes the best tip and block
	// validation marks Z mined in it; then C3 overtakes B2.
	client.onSave = func(value byte) {
		if value != 0 {
			return
		}

		client.onSave = nil

		require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: b2, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(true)))
		_, b2Meta, err := items.blockchainClient.GetBlockHeader(ctx, b2.Hash())
		require.NoError(t, err)
		_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{z.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: b2Meta.ID, BlockHeight: b2Meta.Height, OnLongestChain: true})
		require.NoError(t, err)

		for _, h := range c3 {
			addBlockWithMinedSet(ctx, t, items, h)
		}
	}

	answer := make(chan error, 1)
	ba.handleResetRequests(ctx, resetRequest{ErrCh: answer})
	require.NoError(t, <-answer)
	require.Nil(t, client.onSave, "precondition: the mark before the reset finished")
	requireBlockAssemblyTip(t, items, c3[1])
	require.NotZero(t, unminedSince(t, items, z), "Z, mined only in B2, is marked not on the longest chain")
	require.False(t, ba.skippedForkWorkOwed())
}

// TestFollowBestHeader_CountsOneFailurePerPass: a refused record and a failing mark in the same
// pass advance the back-off once.
func TestFollowBestHeader_CountsOneFailurePerPass(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 110500)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	items.blockAssembler.setBestBlockHeader(a1, 1)
	addChain(t, items, buildChain(a1, 2, 111500))

	// The pass's own best read is the first; the mark's would be the second.
	client := &noBestTipOrderClient{ClientI: items.blockchainClient, failMarkSave: true, failBestCall: 2}
	items.blockAssembler.blockchainClient = client
	items.blockAssembler.processNewBlockAnnouncement(ctx)

	requireBlockAssemblyTip(t, items, a1)
	require.Equal(t, skippedForkMarkRetryBase, items.blockAssembler.skippedForkMarkWait)
}

// TestMarkSkippedForkTxs_RecheckFailureKeepsItPending: a recheck that fails again leaves the run
// pending, keeps the transactions to check, reads no fork block and backs off.
func TestMarkSkippedForkTxs_RecheckFailureKeepsItPending(t *testing.T) {
	items, client, f, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler

	ba.skippedForkRecheck = map[chainhash.Hash]struct{}{*y.TxIDChainHash(): {}}
	client.failBestCall = 2

	require.Error(t, ba.markSkippedForkTxs(t.Context(), false))
	require.True(t, ba.skippedForkMarkPending)
	require.Equal(t, map[chainhash.Hash]struct{}{*y.TxIDChainHash(): {}}, ba.skippedForkRecheck)
	require.NotContains(t, ba.skippedForkBlocksMarked, *f.Hash())
	require.Equal(t, skippedForkMarkRetryBase, ba.skippedForkMarkWait)
}

// TestRunOwedSkippedForkWork_RunsARecheckAlone: transactions the replay's restamp wrote off the
// longest chain without finishing their check are owed work on their own.
func TestRunOwedSkippedForkWork_RunsARecheckAlone(t *testing.T) {
	items, client, _, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler

	ba.utxoStore = &failingUtxoStore{Store: items.utxoStore, fail: "MarkTransactionsOnLongestChain"}
	_, err := ba.markOffLongestChain(t.Context(), []chainhash.Hash{*y.TxIDChainHash()})
	require.Error(t, err)
	_, err = ba.markOffLongestChain(t.Context(), []chainhash.Hash{*y.TxIDChainHash()})
	require.Error(t, err)
	require.Len(t, ba.skippedForkRecheck, 1, "a transaction is held once, however often its write fails")

	ba.utxoStore = items.utxoStore
	require.False(t, ba.skippedForkMarkPending)
	require.False(t, ba.skippedForkReloadPending)

	listed := false
	client.afterList = func() { listed = true }

	// An earlier failure left a back-off, which a recheck that finishes clears.
	ba.noteSkippedForkMarkFailure(time.Now())

	ba.runOwedSkippedForkWork(t.Context())
	require.Empty(t, ba.skippedForkRecheck, "the recheck ran")
	require.False(t, listed, "no mark is owed, so no fork block is listed")
	require.Zero(t, ba.skippedForkMarkWait, "nothing is backed off")

	// A recheck that fails is the pass's one attempt, counted and backed off.
	ba.settings.BlockAssembly.ReconcileInterval = 0
	ba.skippedForkRecheck = map[chainhash.Hash]struct{}{*y.TxIDChainHash(): {}}
	ba.skippedForkMarkRan = false
	before := testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_unfinished"))
	client.bestCalls, client.failBestCall = 0, 1

	ba.runOwedSkippedForkWork(t.Context())
	require.Len(t, ba.skippedForkRecheck, 1, "the recheck stays owed")
	require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockAssemblyProcessingStuck.WithLabelValues("skipped_fork_mark_unfinished")))
	require.Equal(t, skippedForkMarkRetryBase, ba.skippedForkMarkWait)
	require.False(t, ba.reconcileDue(time.Now()), "the idle wake waits for the back-off")
}

// TestMarkSkippedForkTxs_BestChainReads: the best-chain read stays within the header request
// limit, and a page whose headers and metadata disagree is an error.
func TestMarkSkippedForkTxs_BestChainReads(t *testing.T) {
	t.Run("one header per read", func(t *testing.T) {
		items, client, _, y := skippedForkSetup(t, true, true, func(s *settings.Settings) {
			s.BlockChain.MaxBlockHeadersPerRequest = 1
		})

		client.maxHeadersAll = 1
		require.NoError(t, items.blockAssembler.markSkippedForkTxs(t.Context(), false))
		require.NotZero(t, unminedSince(t, items, y))
	})

	t.Run("headers without their metadata", func(t *testing.T) {
		items, client, _, _ := skippedForkSetup(t, true, true)

		best, _, err := items.blockchainClient.GetBestBlockHeader(t.Context())
		require.NoError(t, err)

		// The first page, read from the best tip, is the one refused.
		client.mismatchMetas = true
		require.ErrorContains(t, items.blockAssembler.markSkippedForkTxs(t.Context(), false), "metas from "+best.Hash().String())
	})

	t.Run("each fork block beats", func(t *testing.T) {
		items, client, _, _ := skippedForkSetup(t, true, true)
		ba := items.blockAssembler

		ba.heartbeat.Beat()

		client.afterList = func() {
			ba.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))
		}

		require.NoError(t, ba.markSkippedForkTxs(t.Context(), false))
		require.Less(t, ba.heartbeat.Age(), time.Minute, "the mark beat after the listing")
	})

	t.Run("a fork block block validation has not finished beats", func(t *testing.T) {
		items, client, f, _ := skippedForkSetup(t, true, false)
		ba := items.blockAssembler
		ctx := t.Context()

		coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
		require.NoError(t, err)

		parent, _, err := items.blockchainClient.GetBlockHeader(ctx, f.HashPrevBlock)
		require.NoError(t, err)

		g := buildChain(parent, 1, 103000)[0]
		require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: g, CoinbaseTx: coinbase, TransactionCount: 1}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(false)))

		// Of F and G, both only asked whether block validation finished them, the second is
		// asked after the first made the heartbeat old.
		ba.heartbeat.Beat()

		client.onIsMined = func() {
			client.onIsMined = nil
			ba.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))
		}

		var unprocessed *skippedForkUnprocessedError
		require.ErrorAs(t, ba.markSkippedForkTxs(ctx, false), &unprocessed)
		require.Equal(t, 2, unprocessed.count)
		require.Less(t, ba.heartbeat.Age(), time.Minute, "the mark beat for the second block")
	})
}

// TestRunOwedSkippedForkWork_RunsARecheckOffTheTip: a recheck alone does not wait for block
// assembly to reach the best tip, so it does not keep the idle wake due while block assembly is
// off it. An owed mark or reload does wait, and keeps the wake due to carry block assembly there.
func TestRunOwedSkippedForkWork_RunsARecheckOffTheTip(t *testing.T) {
	items, _, _, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ba.settings.BlockAssembly.ReconcileInterval = 0
	ctx := t.Context()

	best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	parent, parentMeta, err := items.blockchainClient.GetBlockHeader(ctx, best.HashPrevBlock)
	require.NoError(t, err)

	ba.setBestBlockHeader(parent, parentMeta.Height)
	ba.skippedForkRecheck = map[chainhash.Hash]struct{}{*y.TxIDChainHash(): {}}

	ba.runOwedSkippedForkWork(ctx)
	require.Empty(t, ba.skippedForkRecheck, "the recheck ran off the tip")
	require.False(t, ba.reconcileDue(time.Now()), "nothing is owed, so the idle wake is not due")
}

// heldTxReorgSetup builds the case a missing fork subtree leaves to the pool. Block assembly is
// on A2 and holds Y. F2, a sibling of A2 whose subtree files are gone, was the best tip for a
// moment and block validation marked Y mined in it; C2 and C3 overtook it before block assembly
// looked. Following the best header is a reorg from A2 to C3 over a moved-back block, A2, whose
// transactions do not include Y, so the reorg's mark leaves Y marked mined. F2 is returned with
// the block assembler's client, which does not serve the best-tip order.
func heldTxReorgSetup(t *testing.T) (*baTestItems, *noBestTipOrderClient, *model.BlockHeader, *model.BlockHeader, *bt.Tx) {
	t.Helper()
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	stp, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(ctx)

	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 130000)[0]
	addBlockWithMinedSet(ctx, t, items, a1)
	a2 := buildChain(a1, 1, 131000)[0]
	addBlockWithMinedSet(ctx, t, items, a2)

	ba.setBestBlockHeader(a2, 2)
	stp.InitCurrentBlockHeader(a2)

	funding := replayPoolTx(&chainhash.Hash{}, 100)
	y := replayPoolTx(funding.TxIDChainHash(), 97)
	for _, tx := range []*bt.Tx{funding, y} {
		_, err = items.utxoStore.(*utxosql.Store).Create(ctx, tx, 1)
		require.NoError(t, err)
	}

	_, genMeta, err := items.blockchainClient.GetBlockHeader(ctx, genesis.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0, OnLongestChain: true})
	require.NoError(t, err)

	stp.AddBatch([]subtree.Node{{Hash: *y.TxIDChainHash(), Fee: 1, SizeInBytes: uint64(len(y.Bytes()))}}, []*subtree.TxInpoints{{}})
	require.Eventually(t, func() bool { return stp.GetCurrentTxMap().Exists(*y.TxIDChainHash()) }, 5*time.Second, 10*time.Millisecond, "Y is in block assembly")

	// F2's subtree is in neither the subtree store's subtree file nor its file still to be
	// checked.
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	gone := chainhash.HashH([]byte("F2's subtree"))
	f2 := buildChain(a1, 1, 132000)[0]
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: f2, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{&gone}}, "", blockchainoptions.WithSubtreesSet(true), blockchainoptions.WithMinedSet(true)))

	_, f2Meta, err := items.blockchainClient.GetBlockHeader(ctx, f2.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{y.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: f2Meta.ID, BlockHeight: 2, OnLongestChain: true})
	require.NoError(t, err)

	c := buildChain(a1, 2, 133000)
	for _, h := range c {
		addBlockWithMinedSet(ctx, t, items, h)
	}

	best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, c[1].Hash().String(), best.Hash().String(), "precondition: C3 is the best tip")

	for _, fileType := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeToCheck} {
		exists, err := items.blobStore.Exists(ctx, gone[:], fileType)
		require.NoError(t, err)
		require.False(t, exists, "precondition: F2's subtree is gone")
	}

	meta, err := items.utxoStore.Get(ctx, y.TxIDChainHash(), fields.BlockIDs, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, []uint32{f2Meta.ID}, meta.BlockIDs, "precondition: only F2 mined Y")
	require.Zero(t, meta.UnminedSince, "precondition: Y is marked mined")

	client := &noBestTipOrderClient{ClientI: items.blockchainClient}
	ba.blockchainClient = client

	return items, client, f2, c[1], y
}

// TestFollowBestHeader_MissingForkSubtreePreservesHeldTxAfterReorg: a fork block whose subtree
// is gone cannot say which transactions it mined, but block assembly still holds the ones it
// had. Following the best header reorgs from A2 to C3; the mark that follows gives F2 up and
// marks Y, which block assembly holds, off the longest chain, so a restart loads it again.
func TestFollowBestHeader_MissingForkSubtreePreservesHeldTxAfterReorg(t *testing.T) {
	items, _, f2, c3, y := heldTxReorgSetup(t)
	ba := items.blockAssembler
	ctx := t.Context()

	reorgs := testutil.ToFloat64(prometheusBlockAssemblerReorg)

	ba.processNewBlockAnnouncement(ctx)

	requireBlockAssemblyTip(t, items, c3)
	require.Equal(t, reorgs+1, testutil.ToFloat64(prometheusBlockAssemblerReorg), "precondition: block assembly reorged from A2 to C3")
	require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "Y is still held")
	require.NotZero(t, unminedSince(t, items, y), "Y, which no block of the best chain holds, is marked unmined")
	require.Contains(t, ba.skippedForkBlocksMarked, *f2.Hash(), "F2 is given up once Y is repaired")
	require.False(t, ba.skippedForkWorkOwed())
	require.Empty(t, ba.skippedForkHeldRepair, "nothing is left to repair")

	record, err := items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
	require.NoError(t, err)
	require.Equal(t, []byte{0}, record, "the record says the mark finished")

	ba.subtreeProcessor.Stop(context.Background())

	restarted, err := startBlockAssembler(t, items, ba.settings, nil)
	require.NoError(t, err)
	t.Cleanup(func() { restarted.subtreeProcessor.Stop(context.Background()) })

	require.True(t, restarted.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the restarted pool holds Y")
}

// TestMarkSkippedForkTxs_HeldRepairOfAGoneForkBlock covers the repair of the transactions block
// assembly holds that a fork block whose subtree is gone mined.
func TestMarkSkippedForkTxs_HeldRepairOfAGoneForkBlock(t *testing.T) {
	// W, which C3 on the best chain mined as well, stays mined.
	t.Run("a tx the best chain mined too", func(t *testing.T) {
		items, _, f2, c3, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		w := replayPoolTx(y.TxIDChainHash(), 90)
		_, err := items.utxoStore.(*utxosql.Store).Create(ctx, w, 1)
		require.NoError(t, err)

		_, f2Meta, err := items.blockchainClient.GetBlockHeader(ctx, f2.Hash())
		require.NoError(t, err)
		_, c3Meta, err := items.blockchainClient.GetBlockHeader(ctx, c3.Hash())
		require.NoError(t, err)
		for _, id := range []uint32{f2Meta.ID, c3Meta.ID} {
			_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{w.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: id, BlockHeight: 2, OnLongestChain: true})
			require.NoError(t, err)
		}

		ba.subtreeProcessor.AddBatch([]subtree.Node{{Hash: *w.TxIDChainHash(), Fee: 1, SizeInBytes: uint64(len(w.Bytes()))}}, []*subtree.TxInpoints{{}})
		require.Eventually(t, func() bool { return ba.subtreeProcessor.GetCurrentTxMap().Exists(*w.TxIDChainHash()) }, 5*time.Second, 10*time.Millisecond)

		ba.processNewBlockAnnouncement(ctx)

		requireBlockAssemblyTip(t, items, c3)
		require.NotZero(t, unminedSince(t, items, y))
		require.Zero(t, unminedSince(t, items, w), "W, mined on the best chain, stays mined")
	})

	// D, extending C3, mines Y between the repair's selection and its write; the check after
	// the write marks Y mined again.
	t.Run("the best tip moves during the repair", func(t *testing.T) {
		items, client, _, c3, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		d := buildChain(c3, 1, 134000)[0]
		ba.utxoStore = &partialWriteUtxoStore{Store: items.utxoStore, failRemark: true, beforeWrite: func() {
			addBlockWithMinedSet(ctx, t, items, d)
			_, dMeta, err := items.blockchainClient.GetBlockHeader(ctx, d.Hash())
			require.NoError(t, err)
			_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{y.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: dMeta.ID, BlockHeight: dMeta.Height, OnLongestChain: true})
			require.NoError(t, err)
		}}

		ba.processNewBlockAnnouncement(ctx)
		require.True(t, ba.skippedForkWorkOwed(), "the check after the write failed")

		ba.utxoStore = items.utxoStore
		ba.blockchainClient = client
		ba.processNewBlockAnnouncement(ctx)

		requireBlockAssemblyTip(t, items, d)
		require.Zero(t, unminedSince(t, items, y), "Y, mined in D on the best chain, is mined")
		require.False(t, ba.skippedForkWorkOwed())
	})

	// V, which only an invalid fork block mined, is not a transaction of F2 and is left alone.
	t.Run("a tx only another fork block mined", func(t *testing.T) {
		items, _, _, c3, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		v := replayPoolTx(y.TxIDChainHash(), 80)
		_, err := items.utxoStore.(*utxosql.Store).Create(ctx, v, 1)
		require.NoError(t, err)

		a1, _, err := items.blockchainClient.GetBlockHeader(ctx, c3.HashPrevBlock)
		require.NoError(t, err)
		a1, _, err = items.blockchainClient.GetBlockHeader(ctx, a1.HashPrevBlock)
		require.NoError(t, err)

		coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
		require.NoError(t, err)
		x := buildChain(a1, 1, 135000)[0]
		require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: x, CoinbaseTx: coinbase, TransactionCount: 1, Subtrees: []*chainhash.Hash{}}, "", blockchainoptions.WithInvalid(true), blockchainoptions.WithMinedSet(true)))

		_, xMeta, err := items.blockchainClient.GetBlockHeader(ctx, x.Hash())
		require.NoError(t, err)
		_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{v.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: xMeta.ID, BlockHeight: 2, OnLongestChain: true})
		require.NoError(t, err)

		ba.subtreeProcessor.AddBatch([]subtree.Node{{Hash: *v.TxIDChainHash(), Fee: 1, SizeInBytes: uint64(len(v.Bytes()))}}, []*subtree.TxInpoints{{}})
		require.Eventually(t, func() bool { return ba.subtreeProcessor.GetCurrentTxMap().Exists(*v.TxIDChainHash()) }, 5*time.Second, 10*time.Millisecond)

		ba.processNewBlockAnnouncement(ctx)

		requireBlockAssemblyTip(t, items, c3)
		require.NotZero(t, unminedSince(t, items, y))
		require.Zero(t, unminedSince(t, items, v), "V is no transaction of a fork block whose subtree is gone")
	})

	// A read of the pool cut short is not an empty pool.
	t.Run("the pool read is cancelled", func(t *testing.T) {
		items, _, f2, c3, _ := heldTxReorgSetup(t)
		ba := items.blockAssembler

		_, f2Meta, err := items.blockchainClient.GetBlockHeader(t.Context(), f2.Hash())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		require.Error(t, ba.repairHeldTxsOfGoneBlocks(ctx, []blockHeaderWithMeta{{header: f2, meta: f2Meta}}, c3))
	})

	// The record of the held transactions comes before their write: without it nothing is
	// written, and F2 is not given up.
	t.Run("the record of the held txs is refused", func(t *testing.T) {
		items, client, f2, _, y := heldTxReorgSetup(t)
		ba := items.blockAssembler

		client.failListSave = true
		ba.processNewBlockAnnouncement(t.Context())

		require.Zero(t, unminedSince(t, items, y), "Y is not written without its record")
		require.NotContains(t, ba.skippedForkBlocksMarked, *f2.Hash())
		require.True(t, ba.skippedForkMarkPending)
	})

	// Z, which F2 mined but not on the longest chain, is unmined already and not written again;
	// nor is it when a restart applies a recorded list.
	t.Run("an unmined tx is not written again", func(t *testing.T) {
		items, _, f2, c3, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		z := replayPoolTx(y.TxIDChainHash(), 70)
		_, err := items.utxoStore.(*utxosql.Store).Create(ctx, z, 1)
		require.NoError(t, err)

		_, f2Meta, err := items.blockchainClient.GetBlockHeader(ctx, f2.Hash())
		require.NoError(t, err)
		_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{z.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: f2Meta.ID, BlockHeight: 2, OnLongestChain: false})
		require.NoError(t, err)
		require.NotZero(t, unminedSince(t, items, z), "precondition: Z is unmined")

		ba.subtreeProcessor.AddBatch([]subtree.Node{{Hash: *z.TxIDChainHash(), Fee: 1, SizeInBytes: uint64(len(z.Bytes()))}}, []*subtree.TxInpoints{{}})
		require.Eventually(t, func() bool { return ba.subtreeProcessor.GetCurrentTxMap().Exists(*z.TxIDChainHash()) }, 5*time.Second, 10*time.Millisecond)

		// The record carries only Y.
		ba.utxoStore = &failingUtxoStore{Store: items.utxoStore, fail: "MarkTransactionsOnLongestChain"}
		ba.processNewBlockAnnouncement(ctx)

		record, err := items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
		require.NoError(t, err)
		require.Equal(t, append([]byte{1}, y.TxIDChainHash()[:]...), record, "only Y is recorded")

		store := &recordingUtxoStore{Store: items.utxoStore}
		ba.utxoStore = store
		ba.skippedForkMarkRetryAt = time.Time{}
		ba.processNewBlockAnnouncement(ctx)

		requireBlockAssemblyTip(t, items, c3)
		require.Equal(t, [][]chainhash.Hash{{*y.TxIDChainHash()}}, store.markedOff, "only Y is written")

		// The list a restart reads.
		require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*y.TxIDChainHash()}, true))
		store.markedOff = nil
		ba.skippedForkHeldRepair = []chainhash.Hash{*y.TxIDChainHash(), *z.TxIDChainHash()}

		_, err = ba.applyHeldRepair(ctx, c3)
		require.NoError(t, err)
		require.Equal(t, [][]chainhash.Hash{{*y.TxIDChainHash()}}, store.markedOff, "only Y is written")
	})

	// A step of the run that fails before the fork blocks are read does not keep Y from being
	// repaired first: a restart before the next run finds Y unmined. Here an owed recheck fails.
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "an earlier failure in the run", true: "an earlier failure behind a stale header cache"}[stale], func(t *testing.T) {
			items, client, f2, _, y := heldTxReorgSetup(t)
			ba := items.blockAssembler
			ctx := t.Context()

			// The header reads say mined_set is not set, so F2 is asked whether it is finished.
			client.staleMinedSet = stale

			ba.skippedForkRecheck = map[chainhash.Hash]struct{}{chainhash.HashH([]byte("owed recheck")): {}}
			client.onBestCall = func(n int) {
				if calledFrom("recheckSkippedForkMarks") {
					client.failBestCall = n
				}
			}

			ba.processNewBlockAnnouncement(ctx)
			require.True(t, ba.skippedForkMarkPending, "precondition: the run did not finish")
			require.NotZero(t, unminedSince(t, items, y), "Y is repaired before the step that fails")
			require.Contains(t, ba.skippedForkBlocksMarked, *f2.Hash(), "and F2 given up")

			ba.subtreeProcessor.Stop(context.Background())

			restarted, err := startBlockAssembler(t, items, ba.settings, nil)
			require.NoError(t, err)
			t.Cleanup(func() { restarted.subtreeProcessor.Stop(context.Background()) })

			require.True(t, restarted.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the restarted pool holds Y")
		})
	}

	// D, extending C3, mines Y between the repair's selection and its write, and the check after
	// the write marks it mined again: F2 is checked again on a later run.
	t.Run("a held tx joins the best chain during the repair", func(t *testing.T) {
		items, _, f2, c3, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		d := buildChain(c3, 1, 136000)[0]
		ba.utxoStore = &partialWriteUtxoStore{Store: items.utxoStore, succeed: true, beforeWrite: func() {
			addBlockWithMinedSet(ctx, t, items, d)
			_, dMeta, err := items.blockchainClient.GetBlockHeader(ctx, d.Hash())
			require.NoError(t, err)
			_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{y.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: dMeta.ID, BlockHeight: dMeta.Height, OnLongestChain: true})
			require.NoError(t, err)
		}}

		ba.processNewBlockAnnouncement(ctx)

		require.Zero(t, unminedSince(t, items, y), "Y, mined in D on the best chain, is mined")
		require.NotContains(t, ba.skippedForkBlocksMarked, *f2.Hash(), "F2 is checked again on a later run")
	})

	// A list an earlier run left joins the one this run finds: V, which block assembly no longer
	// holds, and Y are both repaired.
	t.Run("an earlier list is kept", func(t *testing.T) {
		items, _, f2, c3, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		v := replayPoolTx(y.TxIDChainHash(), 60)
		_, err := items.utxoStore.(*utxosql.Store).Create(ctx, v, 1)
		require.NoError(t, err)

		_, f2Meta, err := items.blockchainClient.GetBlockHeader(ctx, f2.Hash())
		require.NoError(t, err)
		_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{v.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: f2Meta.ID, BlockHeight: 2, OnLongestChain: true})
		require.NoError(t, err)

		ba.skippedForkHeldRepair = []chainhash.Hash{*v.TxIDChainHash()}
		ba.utxoStore = &failingUtxoStore{Store: items.utxoStore, fail: "MarkTransactionsOnLongestChain"}
		ba.processNewBlockAnnouncement(ctx)

		record, err := items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{1}, v.TxIDChainHash()[:]...), y.TxIDChainHash()[:]...), record, "the record carries V and Y")

		ba.utxoStore = items.utxoStore
		ba.skippedForkMarkRetryAt = time.Time{}
		ba.processNewBlockAnnouncement(ctx)

		requireBlockAssemblyTip(t, items, c3)
		require.NotZero(t, unminedSince(t, items, v))
		require.NotZero(t, unminedSince(t, items, y))
		require.Contains(t, ba.skippedForkBlocksMarked, *f2.Hash(), "F2 is given up once both are repaired")
	})

	// F2's files are still reported there when the run starts and gone when it reads the block.
	t.Run("files gone during the run", func(t *testing.T) {
		items, _, f2, _, y := heldTxReorgSetup(t)
		ba := items.blockAssembler

		ba.subtreeStore = &vanishingSubtreeStore{Store: items.blobStore, existsFor: 1}
		ba.processNewBlockAnnouncement(t.Context())

		require.NotZero(t, unminedSince(t, items, y))
		require.Contains(t, ba.skippedForkBlocksMarked, *f2.Hash())
	})

	// With no fork block left to read, a recorded list that cannot be written keeps the run
	// pending and stays in the record.
	t.Run("a recorded list with no fork block left", func(t *testing.T) {
		items, _, f2, _, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		ba.skippedForkBlocksMarked = map[chainhash.Hash]uint32{*f2.Hash(): 2}
		ba.skippedForkHeldRepair = []chainhash.Hash{*y.TxIDChainHash()}
		ba.utxoStore = &failingUtxoStore{Store: items.utxoStore, fail: "MarkTransactionsOnLongestChain"}

		require.Error(t, ba.markSkippedForkTxs(ctx, true))
		require.True(t, ba.skippedForkMarkPending)

		record, err := items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
		require.NoError(t, err)
		require.Equal(t, append([]byte{1}, y.TxIDChainHash()[:]...), record, "the record keeps Y")
	})

	// The repair's write fails: F2 is not given up, and the next pass repairs Y.
	t.Run("a failed write is retried", func(t *testing.T) {
		items, _, f2, _, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		ba.utxoStore = &failingUtxoStore{Store: items.utxoStore, fail: "MarkTransactionsOnLongestChain"}
		ba.processNewBlockAnnouncement(ctx)

		require.Zero(t, unminedSince(t, items, y))
		require.NotContains(t, ba.skippedForkBlocksMarked, *f2.Hash(), "F2 is not given up before Y is repaired")
		require.True(t, ba.skippedForkMarkPending)

		ba.utxoStore = items.utxoStore
		ba.skippedForkMarkRetryAt = time.Time{}
		ba.processNewBlockAnnouncement(ctx)

		require.NotZero(t, unminedSince(t, items, y))
		require.Contains(t, ba.skippedForkBlocksMarked, *f2.Hash())
		require.False(t, ba.skippedForkWorkOwed())
	})

	// The repair's write fails and block assembly restarts: its pool is gone and F2 still
	// cannot say what it mined, so the record carries Y, and the start repairs it before it
	// loads the unmined transactions.
	t.Run("a failed write survives a restart", func(t *testing.T) {
		items, _, _, _, y := heldTxReorgSetup(t)
		ba := items.blockAssembler
		ctx := t.Context()

		ba.utxoStore = &failingUtxoStore{Store: items.utxoStore, fail: "MarkTransactionsOnLongestChain"}
		ba.processNewBlockAnnouncement(ctx)
		require.Zero(t, unminedSince(t, items, y), "precondition: the repair did not write")

		record, err := items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
		require.NoError(t, err)
		require.Equal(t, append([]byte{1}, y.TxIDChainHash()[:]...), record, "the record carries Y")

		ba.subtreeProcessor.Stop(context.Background())

		// The start must not wait on a pool that is not running yet.
		startCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		restarted, err := NewBlockAssembler(startCtx, ulogger.TestLogger{}, ba.settings, gocore.NewStat("test"), items.utxoStore, items.blobStore, items.blockchainClient, make(chan subtreeprocessor.NewSubtreeRequest, 10))
		require.NoError(t, err)
		restarted.SetSkipWaitForPendingBlocks(true)
		t.Cleanup(func() { restarted.subtreeProcessor.Stop(context.Background()) })
		require.NoError(t, restarted.Start(startCtx), "the start finishes")

		require.NotZero(t, unminedSince(t, items, y))
		require.True(t, restarted.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the restarted pool holds Y")

		record, err = items.blockchainClient.GetState(ctx, SkippedForkMarkStateKey)
		require.NoError(t, err)
		require.Equal(t, []byte{0}, record)
	})
}

// orderServingClient is noBestTipOrderClient with the best-tip order served, so the replay works.
type orderServingClient struct {
	*noBestTipOrderClient
}

func (c *orderServingClient) BlocksBetterThan(ctx context.Context, pos model.BestTipKey, from *model.BestTipKey, floor, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, bool, error) {
	return c.ClientI.BlocksBetterThan(ctx, pos, from, floor, limit)
}

// TestSkippedForkReloadOnly_IdleRetryAfterFailedReset: a pool reload is the only work a finished
// mark owes, and the reset that would do it fails before it rotates the pool. Nothing else is
// owed: no mark, no recheck, no reset recovery, no history, and the reconcile timer is off. The
// owed reload alone wakes the idle tick once its back-off is over, and that pass reloads Y.
func TestSkippedForkReloadOnly_IdleRetryAfterFailedReset(t *testing.T) {
	items, client, _, y := skippedForkSetup(t, true, true)
	ba := items.blockAssembler
	ba.settings.BlockAssembly.ReconcileInterval = 0
	ctx := t.Context()

	best, bestMeta, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	startReplayAt(t, items, best, bestMeta.Height)

	served := &orderServingClient{noBestTipOrderClient: client}
	ba.blockchainClient = served

	// The mark before a reset leaves the reload to the reset.
	require.NoError(t, ba.markSkippedForkTxs(ctx, true))
	require.NotZero(t, unminedSince(t, items, y), "Y is marked")
	require.False(t, ba.skippedForkMarkPending)
	require.True(t, ba.skippedForkReloadPending)
	require.Empty(t, ba.skippedForkRecheck)

	// The reset's own read of the best header fails, before it rotates the pool.
	failed := false
	client.onBestCall = func(n int) {
		if !failed && calledFrom("resetWithResponse") {
			failed = true
			client.failBestCall = n
		}
	}
	client.headerIDsCalls = 0

	answer := make(chan error, 1)
	ba.handleResetRequests(ctx, resetRequest{ErrCh: answer})
	require.ErrorContains(t, <-answer, "error getting best block header", "the reset fails reading the best header")
	require.True(t, failed)
	require.Zero(t, client.headerIDsCalls, "nothing was reloaded")
	require.False(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "Y is not in the pool")

	require.False(t, ba.skippedForkMarkPending, "no mark is owed")
	require.True(t, ba.skippedForkReloadPending, "the reload is owed")
	require.Empty(t, ba.skippedForkRecheck, "no recheck is owed")
	require.False(t, ba.bestTipResetReplayPending.Load(), "no reset recovery is pending")
	require.False(t, ba.bestTipResetIncomplete.Load(), "no reset is incomplete")
	requireBlockAssemblyTip(t, items, best)

	client.onBestCall, client.failBestCall = nil, 0

	require.False(t, ba.reconcileDue(time.Now()), "not woken before the back-off")

	ba.skippedForkMarkRetryAt = time.Now().Add(-time.Millisecond)
	require.True(t, ba.reconcileDue(time.Now()), "the owed reload is due with the timer off")
	require.True(t, ba.idleReconcileNeeded(ctx), "the idle tick wakes a pass for it")

	// The idle tick's dispatch, and the pass it wakes.
	ba.wakeIdleReconcile(ctx)

	select {
	case <-ba.reconcileCh:
	default:
		require.Fail(t, "the idle tick did not wake a pass")
	}

	ba.processNewBlockAnnouncement(ctx)

	require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the reload put Y in the pool")
	require.Equal(t, 1, client.headerIDsCalls, "one reload")
	require.False(t, ba.skippedForkWorkOwed())
	require.False(t, ba.reconcileDue(time.Now()), "no further idle work")
}

// calledFrom reports whether a function named name is on the caller's stack.
func calledFrom(name string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])

	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, "."+name) {
			return true
		}

		if !more {
			return false
		}
	}
}

// recordingUtxoStore records the hashes of each mark as not on the longest chain.
type recordingUtxoStore struct {
	utxo.Store
	markedOff [][]chainhash.Hash
}

func (s *recordingUtxoStore) MarkTransactionsOnLongestChain(ctx context.Context, hashes []chainhash.Hash, onLongestChain bool) error {
	if !onLongestChain {
		s.markedOff = append(s.markedOff, append([]chainhash.Hash(nil), hashes...))
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, hashes, onLongestChain)
}

// vanishingSubtreeStore says every file exists for its first existsFor existence checks, and
// answers the store's truth after that.
type vanishingSubtreeStore struct {
	blob.Store
	existsFor int
}

func (s *vanishingSubtreeStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...blobopts.FileOption) (bool, error) {
	if s.existsFor > 0 {
		s.existsFor--
		return true, nil
	}

	return s.Store.Exists(ctx, key, fileType, opts...)
}

// TestMarkSkippedForkTxs_LeavesAnUnfinishedForkBlockWithoutSubtree: a fork block block validation
// has not finished marked nothing mined, and its subtree may still come; it is neither taken
// for one whose subtree is gone nor given up, but waited for.
func TestMarkSkippedForkTxs_LeavesAnUnfinishedForkBlockWithoutSubtree(t *testing.T) {
	items, _, f, _ := skippedForkSetup(t, false, false)
	ba := items.blockAssembler

	var unprocessed *skippedForkUnprocessedError
	require.ErrorAs(t, ba.markSkippedForkTxs(t.Context(), false), &unprocessed)
	require.NotContains(t, ba.skippedForkBlocksMarked, *f.Hash(), "F is read again once block validation finished it")
}

// TestHandleResetRequests_RefusedResetIsRetried: a reset refused because the mark it would owe
// cannot be recorded is not dropped. The heartbeat took the request that raised it, so nothing
// would ask again: a storage-triggered reset is owed as one that failed before the rotation,
// and any other, after a failed queue drain or a manual one, is queued again by the heartbeat
// once the back-off allows.
func TestHandleResetRequests_RefusedResetIsRetried(t *testing.T) {
	t.Run("storage-triggered", func(t *testing.T) {
		items, client, _, _ := skippedForkSetup(t, true, true)
		ba := items.blockAssembler
		client.failMarkSave = true

		answer := make(chan error, 1)
		ba.handleResetRequests(t.Context(), resetRequest{ErrCh: answer, StorageTriggered: true})
		require.Error(t, <-answer)
		require.True(t, ba.diskTxMapResetPending, "the heartbeat retries the storage-triggered reset")
	})

	t.Run("after a failed drain or a manual one", func(t *testing.T) {
		items, client, _, y := skippedForkSetup(t, true, true)
		ba := items.blockAssembler
		ctx := t.Context()
		client.failMarkSave = true

		answer := make(chan error, 1)
		ba.handleResetRequests(ctx, resetRequest{ErrCh: answer, FullReset: true})
		require.Error(t, <-answer)

		// Not queued again before the back-off is over.
		require.NotNil(t, ba.resetOwed)
		ba.requeueRefusedReset()
		require.NotNil(t, ba.resetOwed, "not retried at once")

		client.failMarkSave = false
		ba.skippedForkMarkRetryAt = time.Now().Add(-time.Millisecond)
		ba.requeueRefusedReset()

		var req resetRequest
		require.Eventually(t, func() bool {
			select {
			case req = <-ba.resetCh:
				return true
			default:
				return false
			}
		}, 5*time.Second, 10*time.Millisecond, "the refused reset is queued again")
		require.True(t, req.FullReset, "with the options it was asked with")

		// The requeue's sender waits for the answer on req.ErrCh.
		ba.handleResetRequests(ctx, req)
		require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the reset ran")

		require.Nil(t, ba.resetOwed, "nothing is owed any more")
	})
}
