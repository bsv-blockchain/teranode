package subtreeprocessor

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// progressRecorder is a progress hook that records the processor's current
// block header at every call, so a test can tell WHEN the beats happened
// relative to the blocks being applied, not only how many there were.
type progressRecorder struct {
	mu      sync.Mutex
	stp     *SubtreeProcessor
	headers []chainhash.Hash
}

func (r *progressRecorder) hook() {
	var h chainhash.Hash
	if header := r.stp.currentBlockHeader.Load(); header != nil {
		h = *header.Hash()
	}

	r.mu.Lock()
	r.headers = append(r.headers, h)
	r.mu.Unlock()
}

func (r *progressRecorder) seen() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]chainhash.Hash(nil), r.headers...)
}

// newProgressTestProcessor builds a started processor on a sqlitememory UTXO
// store, with a blockchain mock the caller configures.
func newProgressTestProcessor(t *testing.T, blockchainClient *blockchain.Mock) (*SubtreeProcessor, *sql.Store) {
	t.Helper()

	ctx := t.Context()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), blob_memory.New(), blockchainClient, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.Start(ctx)
	t.Cleanup(func() {
		stp.Stop(context.Background())
		close(newSubtreeChan)
	})

	return stp, utxoStore
}

func childHeader(parent *model.BlockHeader, nonce uint32) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parent.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890 + nonce,
		Bits:           model.NBit{},
		Nonce:          nonce,
	}
}

// TestProgressHookBeatsBetweenCatchUpBlocks pins the property the hook exists
// for: a multi-block catch-up reports progress as each block lands, so the
// owner's heartbeat ages by one block's work at most rather than by the whole
// call (issue 1447).
//
// The assertion is on the header the processor held at each beat, not on a
// count. A beat while the tip was block1 proves a beat happened after block1
// was applied and before block2; the same for block2. Beating only once at the
// start of Reorg would leave only the parent in the list and fail this test.
func TestProgressHookBeatsBetweenCatchUpBlocks(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	recorder := &progressRecorder{stp: stp}
	stp.SetProgressHook(recorder.hook)

	parent := prevBlockHeader
	h1 := childHeader(parent, 11)
	h2 := childHeader(h1, 12)
	h3 := childHeader(h2, 13)

	blocks := []*model.Block{
		{Height: 1, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Header: h1},
		{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{}, Header: h2},
		{Height: 3, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{}, Header: h3},
	}

	stp.InitCurrentBlockHeader(parent)

	require.NoError(t, stp.Reorg([]*model.Block{}, blocks))

	// End state first: the catch-up must actually have been applied, or the
	// beats below prove nothing about a working catch-up.
	require.Equal(t, h3.Hash(), stp.GetCurrentBlockHeader().Hash(), "catch-up must advance the processor to the last block")

	seen := recorder.seen()
	require.Contains(t, seen, *h1.Hash(), "no beat between block1 and block2: a long catch-up would age the heartbeat by the whole call")
	require.Contains(t, seen, *h2.Hash(), "no beat between block2 and block3: a long catch-up would age the heartbeat by the whole call")

	// One beat for the waitForBlockBeingMined poll, one per block.
	require.Len(t, seen, 1+len(blocks))
}

// TestProgressHookBeatsThroughAFullReorg covers the other branch of
// reorgBlocks, taken whenever blocks are moved back: every step that runs once
// per block must report progress, moving back, moving forward and marking
// processed, as must the wait for pending blocks that precedes them.
func TestProgressHookBeatsThroughAFullReorg(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, &model.BlockHeaderMeta{}, nil)
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, utxoStore := newProgressTestProcessor(t, blockchainClient)

	var calls int

	var mu sync.Mutex

	stp.SetProgressHook(func() {
		mu.Lock()
		calls++
		mu.Unlock()
	})

	// block2 is moved back, block3 replaces it. After the move back the tip is
	// the parent the blockchain mock returns, prevBlockHeader, so block3 builds
	// on that.
	block2Header := childHeader(blockHeader, 2)
	block3Header := childHeader(prevBlockHeader, 3)

	moveBack := &model.Block{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{}, Header: block2Header}
	moveForward := &model.Block{Height: 2, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{}, Header: block3Header}

	_, err := utxoStore.Create(t.Context(), coinbaseTx2, 2)
	require.NoError(t, err)

	stp.InitCurrentBlockHeader(block2Header)

	require.NoError(t, stp.Reorg([]*model.Block{moveBack}, []*model.Block{moveForward}))
	require.Equal(t, block3Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "reorg must land on block3")

	mu.Lock()
	defer mu.Unlock()

	// WaitForPendingBlocks attempt, move back block2, move forward block3, mark
	// block3 processed.
	require.Equal(t, 4, calls)
}

// TestProgressHookBeatsWhileWaitingForBlockValidation pins that each poll of
// waitForBlockBeingMined reports progress. The single-block advance runs this
// wait, up to 300s, whenever a block carries conflicting transactions; unbeaten
// it would age the heartbeat by the whole wait on a routine new block.
func TestProgressHookBeatsWhileWaitingForBlockValidation(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(false, nil).Twice()
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil).Once()

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	var calls int

	stp.SetProgressHook(func() { calls++ })

	mined, err := stp.waitForBlockBeingMined(t.Context(), blockHeader.Hash())
	require.NoError(t, err)
	require.True(t, mined)

	require.Equal(t, 3, calls, "one beat per poll")
}

// TestProgressHookBeatsWhileWaitingForPendingBlocks pins that each attempt of
// WaitForPendingBlocks reports progress. That wait has no ceiling; it runs
// before a full reorg and from the reset path.
func TestProgressHookBeatsWhileWaitingForPendingBlocks(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{{Header: blockHeader}}, nil).Once()
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil).Once()

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	var calls int

	stp.SetProgressHook(func() { calls++ })

	require.NoError(t, stp.WaitForPendingBlocks(t.Context()))

	require.Equal(t, 2, calls, "one beat per attempt")
}

// TestSetProgressHookNilRemovesTheHook pins that clearing the hook is safe:
// block movement must not call a removed hook, nor panic without one.
func TestSetProgressHookNilRemovesTheHook(t *testing.T) {
	stp := &SubtreeProcessor{}

	var calls int

	stp.SetProgressHook(func() { calls++ })
	stp.reportProgress()
	require.Equal(t, 1, calls)

	stp.SetProgressHook(nil)
	stp.reportProgress()
	require.Equal(t, 1, calls, "a removed hook must not be called")
}

// countingHook is a concurrency-safe progress hook. Reset runs some of its
// per-block steps on errgroup goroutines, so the hook is called concurrently.
type countingHook struct {
	mu    sync.Mutex
	calls int
}

func (c *countingHook) hook() {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
}

func (c *countingHook) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// resetFixture returns two blocks to move back and two to move forward, on the
// coinbases the rest of this package's tests already use.
func resetFixture() (moveBack, moveForward []*model.Block) {
	back1 := childHeader(prevBlockHeader, 201)
	back2 := childHeader(back1, 202)
	fwd1 := childHeader(prevBlockHeader, 301)
	fwd2 := childHeader(fwd1, 302)

	moveBack = []*model.Block{
		{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{}, Header: back2},
		{Height: 1, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{}, Header: back1},
	}
	moveForward = []*model.Block{
		{Height: 1, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Header: fwd1},
		{Height: 2, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{}, Header: fwd2},
	}

	return moveBack, moveForward
}

func resetBlockchainMock() *blockchain.Mock {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, &model.BlockHeaderMeta{}, nil)

	return blockchainClient
}

// TestProgressHookBeatsThroughAReset pins that Reset reports progress per block,
// like Reorg. The owner's main loop is blocked on Reset for its whole length,
// and a reset can move hundreds of blocks and only persists state at the end,
// so an unbeaten reset lets the probe kill the node part-way through and the
// restart goes straight back into the same reset (issue 1447).
//
// The count is exact so removing any one beat fails it: two blocks each through
// the moveBack loop, the concurrent processed_at clear, the moveForward loop
// and the processed_at loop.
func TestProgressHookBeatsThroughAReset(t *testing.T) {
	stp, _ := newProgressTestProcessor(t, resetBlockchainMock())

	counter := &countingHook{}
	stp.SetProgressHook(counter.hook)

	moveBack, moveForward := resetFixture()
	stp.InitCurrentBlockHeader(moveBack[0].Header)

	postProcessed := false

	response := stp.Reset(moveBack[0].Header, moveBack, moveForward, false, func() error {
		postProcessed = true
		return nil
	})
	require.NoError(t, response.Err)

	// End state first: the reset must have landed on the new tip, or the beats
	// below prove nothing about a working reset.
	require.True(t, postProcessed, "the reset must run to its post-process step")
	require.Equal(t, moveForward[1].Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "reset must land on the last moveForward block")

	require.Equal(t, 4*len(moveBack), counter.count(),
		"one beat per block in each of: moveBack, processed_at clear, moveForward, processed_at")
}

// TestProgressHookBeatsThroughAFastForwardReset covers the checkpoint-trusted
// branch of Reset, which replaces the per-block moveForward loop with
// concurrent coinbase processing. Two blocks each through the moveBack loop,
// the processed_at clear, the coinbase goroutines and the processed_at loop.
func TestProgressHookBeatsThroughAFastForwardReset(t *testing.T) {
	stp, _ := newProgressTestProcessor(t, resetBlockchainMock())

	counter := &countingHook{}
	stp.SetProgressHook(counter.hook)

	moveBack, moveForward := resetFixture()
	stp.InitCurrentBlockHeader(moveBack[0].Header)

	response := stp.Reset(moveBack[0].Header, moveBack, moveForward, true, func() error { return nil })
	require.NoError(t, response.Err)
	require.Equal(t, moveForward[1].Header.Hash(), stp.GetCurrentBlockHeader().Hash(), "reset must land on the last moveForward block")

	require.Equal(t, 4*len(moveBack), counter.count(),
		"one beat per block in each of: moveBack, processed_at clear, coinbase processing, processed_at")
}

// TestProgressHookSkipsReorgBeatsOnceCancelled pins the shutdown guard on the
// reorg loops: once the context is done, a step that still runs must not beat.
// The owner disables its heartbeat on the way out, and a beat landing after
// that would be progress the probe should never hear about.
//
// The context is cancelled from inside the first beat, the catch-up's
// mined-status poll, so cancellation lands mid-reorg. A context cancelled
// before the call never reaches the loops at all, because that poll returns
// on ctx.Done first.
func TestProgressHookSkipsReorgBeatsOnceCancelled(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	counter := &countingHook{}
	stp.SetProgressHook(func() {
		counter.hook()
		cancel()
	})

	parent := prevBlockHeader
	h1 := childHeader(parent, 41)
	h2 := childHeader(h1, 42)

	blocks := []*model.Block{
		{Height: 1, CoinbaseTx: coinbaseTx, Subtrees: []*chainhash.Hash{}, Header: h1},
		{Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{}, Header: h2},
	}

	stp.InitCurrentBlockHeader(parent)

	// Whether the reorg completes on a context cancelled part-way is not what
	// this pins, only that its per-block loops stay silent once it is done.
	_ = stp.reorgBlocks(ctx, []*model.Block{}, blocks)

	require.Equal(t, 1, counter.count(),
		"only the poll before cancellation may beat; a per-block step after it must not")
}

// TestWaitForPendingBlocksDoesNotBeatWhenTheCallFails pins the other half of
// counting a wait on block validation as progress: only an answer counts. The
// wait retries forever, so a call that keeps failing must leave the heartbeat
// to go stale, or liveness reports 200 on a node that can make no progress.
func TestWaitForPendingBlocksDoesNotBeatWhenTheCallFails(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlocksMinedNotSet", mock.Anything).Return(nil, errors.NewServiceError("block validation unreachable"))

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	counter := &countingHook{}
	stp.SetProgressHook(counter.hook)

	// Attempts land at 0s and 1s; the deadline ends the infinite retry after both.
	ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
	defer cancel()

	require.Error(t, stp.WaitForPendingBlocks(ctx))

	blockchainClient.AssertNumberOfCalls(t, "GetBlocksMinedNotSet", 2)
	require.Zero(t, counter.count(), "a failed check for pending blocks must not beat")
}

// TestWaitForBlockBeingMinedDoesNotBeatWhenTheCallFails is the same rule for
// the per-block wait inside a block move.
func TestWaitForBlockBeingMinedDoesNotBeatWhenTheCallFails(t *testing.T) {
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(false, errors.NewServiceError("block validation unreachable"))

	stp, _ := newProgressTestProcessor(t, blockchainClient)

	counter := &countingHook{}
	stp.SetProgressHook(counter.hook)

	_, err := stp.waitForBlockBeingMined(t.Context(), blockHeader.Hash())
	require.Error(t, err)

	blockchainClient.AssertCalled(t, "GetBlockIsMined", mock.Anything, mock.Anything)
	require.Zero(t, counter.count(), "a failed mined-status poll must not beat")
}
