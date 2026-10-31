package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
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

	ba := &BlockAssembler{blockchainClient: m, logger: ulogger.TestLogger{}, settings: createTestSettings(t)}
	ba.bestTipPosKnown.Store(true)
	ba.setBestTipPos(model.BestTipKey{ChainWork: []byte{0}})
	ba.setBestBlockHeader(&model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}, 10)

	for i := 1; i < maxBestTipReplayFailures; i++ {
		require.False(t, ba.followBestTipChanges(t.Context()), "pass %d retries", i)
	}

	require.True(t, ba.followBestTipChanges(t.Context()))
	require.Equal(t, model.NewBestTipKey(best, bestMeta), *ba.bestTipPos.Load(), "the position jumps to the best tip, with its id and peer id")
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

func (c *cappedTipClient) BlocksBetterThan(ctx context.Context, after model.BestTipKey, from *model.BestTipKey, floor, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if c.fail {
		return nil, nil, errors.NewStorageError("temporary replay read failure")
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
	require.Error(t, <-answer)
	require.True(t, ba.bestTipResetReplayPending.Load())
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
