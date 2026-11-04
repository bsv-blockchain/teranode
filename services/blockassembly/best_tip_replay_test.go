package blockassembly

import (
	"context"
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
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
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
// failMarkSave it refuses writes of SkippedForkMarkStateKey. With forkBranchLen it reports that
// branch length for every chain tip but the active one, as a store that caps its walk does.
// chainTipsCalls counts GetChainTips calls, and beforeBestAfterTips runs once, before the first
// read of the best header that follows one.
type noBestTipOrderClient struct {
	blockchain.ClientI
	failMarkSave        bool
	forkBranchLen       *uint32
	chainTipsCalls      int
	beforeBestAfterTips func()
}

func (c *noBestTipOrderClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if c.chainTipsCalls > 0 && c.beforeBestAfterTips != nil {
		hook := c.beforeBestAfterTips
		c.beforeBestAfterTips = nil
		hook()
	}

	return c.ClientI.GetBestBlockHeader(ctx)
}

func (c *noBestTipOrderClient) BlocksBetterThan(context.Context, model.BestTipKey, *model.BestTipKey, uint32, uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, bool, error) {
	return nil, nil, false, errors.NewServiceError("best-tip order not served")
}

func (c *noBestTipOrderClient) SetState(ctx context.Context, key string, data []byte) error {
	if c.failMarkSave && key == SkippedForkMarkStateKey {
		return errors.NewStorageError("state write refused")
	}

	return c.ClientI.SetState(ctx, key, data)
}

func (c *noBestTipOrderClient) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	c.chainTipsCalls++

	tips, err := c.ClientI.GetChainTips(ctx)
	if err != nil || c.forkBranchLen == nil {
		return tips, err
	}

	for _, tip := range tips {
		if tip.Status != "active" {
			tip.Branchlen = *c.forkBranchLen
		}
	}

	return tips, nil
}

// startBlockAssembler starts a new block assembler on the test's stores, as a restart does. The
// subtree processor of one whose start failed is stopped.
func startBlockAssembler(t *testing.T, items *baTestItems, tSettings *settings.Settings) (*BlockAssembler, error) {
	t.Helper()

	restarted, err := NewBlockAssembler(t.Context(), ulogger.TestLogger{}, tSettings, gocore.NewStat("test"), items.utxoStore, items.blobStore, items.blockchainClient, make(chan subtreeprocessor.NewSubtreeRequest, 10))
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
	capped := uint32(2)
	unknown := uint32(0)

	for _, tc := range []struct {
		name string
		// positionKnown sets a replay position, which the replay then fails to move on from.
		positionKnown bool
		// unreadableFork adds a fork block whose subtree the subtree store cannot read until
		// after the first restart, which therefore fails.
		unreadableFork bool
		// subtreeLate writes B's subtree only after the fallback, and the next pass replays.
		subtreeLate bool
		// restartFirst restarts block assembly before that next pass.
		restartFirst bool
		// startUnreadable makes that restart happen while B still cannot be read; it fails.
		startUnreadable bool
		// invalidChild gives B a child D that is then invalidated.
		invalidChild bool
		// failSave refuses the first fallback's write of the pending mark.
		failSave bool
		// forkAbove builds that many blocks on B, so its fork is longer.
		forkAbove int
		// branchLen is the branch length reported for B's fork, nil for the store's own.
		branchLen *uint32
		// headerLimit is blockchain_maxBlockHeadersPerRequest, 0 for the default.
		headerLimit int
		// bestMovesOntoB extends B's fork past C while the mark waits for pending blocks, after
		// it read the fork blocks.
		bestMovesOntoB bool
	}{
		{name: "best-tip order not served"},
		{name: "replay given up", positionKnown: true},
		{name: "another fork block cannot be read", unreadableFork: true},
		{name: "B read after the replay works again", positionKnown: true, subtreeLate: true},
		{name: "B read by the next start", positionKnown: true, subtreeLate: true, restartFirst: true},
		{name: "B unreadable through a start", positionKnown: true, subtreeLate: true, restartFirst: true, startUnreadable: true},
		{name: "B under an invalid child", invalidChild: true},
		{name: "pending mark cannot be saved", failSave: true},
		{name: "fork longer than its capped branch length", forkAbove: 3, branchLen: &capped, headerLimit: 2},
		{name: "fork without a branch length longer than one read", forkAbove: 3, branchLen: &unknown, headerLimit: 2},
		{name: "B on the best chain by the time it is marked", bestMovesOntoB: true},
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

			if !tc.subtreeLate {
				require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))
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

			client := &noBestTipOrderClient{ClientI: items.blockchainClient, failMarkSave: tc.failSave, forkBranchLen: tc.branchLen}
			ba.blockchainClient = client

			if tc.bestMovesOntoB {
				client.beforeBestAfterTips = func() {
					for _, h := range buildChain(b, 2, 66000) {
						addBlockWithMinedSet(ctx, t, items, h)
					}
				}
			}

			passes := 1
			if tc.positionKnown {
				passes = maxBestTipReplayFailures
			}

			for range passes {
				ba.processNewBlockAnnouncement(ctx)
			}

			if tc.failSave {
				// The move would persist the new tip with nothing recorded to say the fork blocks
				// it skipped still need marking, so block assembly stays where it is.
				requireBlockAssemblyTip(t, items, a1)
				require.Zero(t, unminedSince(t, items, x), "precondition: nothing is marked before the move")
				require.True(t, ba.reconcileDue(time.Now().Add(skippedForkMarkRetryMax)), "the failed fallback is retried without waiting for a block")

				client.failMarkSave = false
				ba.processNewBlockAnnouncement(ctx)
			}

			requireBlockAssemblyTip(t, items, cTip)
			require.True(t, ba.subtreeProcessor.GetCurrentTxMap().Exists(*x.TxIDChainHash()), "X is still in block assembly")

			if tc.bestMovesOntoB {
				// B is on the best chain now, so X and Y are mined on it; moving there is a reorg.
				require.Zero(t, unminedSince(t, items, x), "X, mined in B on the best chain, stays marked mined")
				require.Zero(t, unminedSince(t, items, y), "Y, mined in B on the best chain, stays marked mined")

				return
			}

			if tc.subtreeLate {
				require.Zero(t, unminedSince(t, items, x), "precondition: B could not be read at the fallback")

				if tc.startUnreadable {
					// The restart cannot read B either: it must not load the pool without X.
					ba.subtreeProcessor.Stop(context.Background())

					_, err = startBlockAssembler(t, items, ba.settings)
					require.Error(t, err, "a start that cannot finish the mark it owes fails")
					require.Zero(t, unminedSince(t, items, x), "precondition: B is still unreadable")
				}

				// The subtree store recovers, and so does the replay.
				require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))

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

				// The restart cannot read E: it must not report success with the mark unfinished.
				ba.subtreeProcessor.Stop(context.Background())

				_, err = startBlockAssembler(t, items, ba.settings)
				require.Error(t, err, "a start that cannot finish the mark it owes fails")

				eBytes, err := eTree.Serialize()
				require.NoError(t, err)
				require.NoError(t, items.blobStore.Set(ctx, eTree.RootHash()[:], fileformat.FileTypeSubtree, eBytes))
			}

			// A restart reloads the unmined transactions, and X is one of them.
			ba.subtreeProcessor.Stop(context.Background())

			restarted, err := startBlockAssembler(t, items, ba.settings)
			require.NoError(t, err)
			require.NotZero(t, unminedSince(t, items, x), "X is marked not on the longest chain after the restart")

			require.Eventually(t, func() bool { return restarted.subtreeProcessor.GetCurrentTxMap().Exists(*x.TxIDChainHash()) }, 10*time.Second, 10*time.Millisecond, "the restart reloads X")
			require.True(t, restarted.subtreeProcessor.GetCurrentTxMap().Exists(*y.TxIDChainHash()), "the restart reloads Y")
		})
	}
}

// TestFollowBestHeader_PendingMarkWaitsForTheBestTip: a pending mark runs on a pass only when
// block assembly is on the best tip. Off it, here while the replay is failing, its own blocks
// would read as fork blocks.
func TestFollowBestHeader_PendingMarkWaitsForTheBestTip(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()

	items := setupBlockAssemblyTest(t, withCoinbaseMaturity(testCoinbaseMaturity))
	genesis := genesisHeader(t, items)

	a1 := buildChain(genesis, 1, 90000)[0]
	addChain(t, items, []*model.BlockHeader{a1})
	startReplayAt(t, items, a1, 1)
	addChain(t, items, buildChain(a1, 2, 91000))

	client := &noBestTipOrderClient{ClientI: items.blockchainClient}
	items.blockAssembler.blockchainClient = client
	items.blockAssembler.skippedForkMarkPending = true

	// The first failed replay pass neither moves block assembly nor follows the best header.
	items.blockAssembler.processNewBlockAnnouncement(ctx)

	requireBlockAssemblyTip(t, items, a1)
	require.Zero(t, client.chainTipsCalls, "the pending mark did not run off the best tip")
	require.True(t, items.blockAssembler.skippedForkMarkPending, "the mark stays pending")
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

	wait := skippedForkMarkRetryBase
	for range 10 {
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
