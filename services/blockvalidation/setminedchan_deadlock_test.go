// Package blockvalidation — regression tests for the setMinedChan deadlock (PR #828 review P0).
package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newSetMinedTestBV builds the minimal BlockValidation needed by the setMined
// enqueue/drain machinery: the channel plus the overflow set the worker falls
// back to when the channel is full.
func newSetMinedTestBV(buffer int) *BlockValidation {
	return &BlockValidation{
		logger:                 ulogger.TestLogger{},
		setMinedChan:           make(chan *chainhash.Hash, buffer),
		setMinedOverflow:       make(map[chainhash.Hash]struct{}),
		setMinedOverflowSignal: make(chan struct{}, 1),
	}
}

// nextHashWithTimeout drives the worker's nextSetMinedHash with a bounded wait so a
// regression fails the test instead of hanging the suite.
func nextHashWithTimeout(t *testing.T, ctx context.Context, bv *BlockValidation) *chainhash.Hash {
	t.Helper()

	res := make(chan *chainhash.Hash, 1)
	go func() { res <- bv.nextSetMinedHash(ctx) }()

	select {
	case h := <-res:
		return h
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for next setMined hash")
		return nil
	}
}

// TestProcessBlockMinedNotSet_DoesNotBlockWhenBacklogExceedsBuffer reproduces the
// startup deadlock. processBlockMinedNotSet enqueues the entire GetBlocksMinedNotSet
// backlog onto setMinedChan, and that query has no SQL LIMIT. In start() this runs
// BEFORE the consumer worker is launched, so a backlog larger than the channel buffer
// blocks the send forever and start() never completes (the node never finishes booting
// block validation). The function must never block its caller.
func TestProcessBlockMinedNotSet_DoesNotBlockWhenBacklogExceedsBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nBits, _ := model.NewNBitFromString("2000ffff")
	// A single shared header is fine: distinctness is irrelevant here (no consumer,
	// no dedup), we only care that more blocks than the buffer get enqueued.
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1,
		Bits:           *nBits,
		Nonce:          0,
	}

	const backlog = 5
	blocks := make([]*model.Block, backlog)
	for i := range blocks {
		blocks[i] = &model.Block{Header: header}
	}

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlocksMinedNotSet", mock.Anything).Return(blocks, nil)

	// Buffer smaller than the backlog, with NO consumer draining — exactly the startup
	// window before the setMinedChan worker is launched.
	bv := &BlockValidation{
		logger:           ulogger.TestLogger{},
		blockchainClient: mockBC,
		setMinedChan:     make(chan *chainhash.Hash, 2),
	}

	done := make(chan struct{})
	go func() {
		bv.processBlockMinedNotSet(ctx)
		close(done)
	}()

	select {
	case <-done:
		// returned promptly — good
	case <-time.After(2 * time.Second):
		t.Fatal("processBlockMinedNotSet blocked when the backlog exceeded the setMinedChan buffer (startup deadlock)")
	}
}

// TestProcessBlockMinedNotSet_SingleFeederInFlight locks in the guard against feeder
// pile-up: the periodic ticker calls processBlockMinedNotSet every minute, and when the
// worker drains slowly the feeder from the previous tick is still blocked on the channel.
// Without a single-flight guard each tick would re-query the same backlog and stack
// another feeder goroutine enqueuing duplicates.
func TestProcessBlockMinedNotSet_SingleFeederInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nBits, _ := model.NewNBitFromString("2000ffff")
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1,
		Bits:           *nBits,
		Nonce:          0,
	}

	const backlog = 5
	blocks := make([]*model.Block, backlog)
	for i := range blocks {
		blocks[i] = &model.Block{Header: header}
	}

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlocksMinedNotSet", mock.Anything).Return(blocks, nil)

	// Buffer smaller than the backlog and no consumer: the first feeder parks on the
	// channel send and stays in flight.
	bv := &BlockValidation{
		logger:           ulogger.TestLogger{},
		blockchainClient: mockBC,
		setMinedChan:     make(chan *chainhash.Hash, 2),
	}

	bv.processBlockMinedNotSet(ctx)
	bv.processBlockMinedNotSet(ctx) // second tick while the first feeder is parked

	// The second call must skip both the query and the feeder launch.
	mockBC.AssertNumberOfCalls(t, "GetBlocksMinedNotSet", 1)

	// Once the parked feeder exits (ctx cancel), the guard releases and the next
	// tick queries again.
	cancel()
	require.Eventually(t, func() bool {
		return !bv.minedNotSetFeederActive.Load()
	}, 2*time.Second, 10*time.Millisecond, "feeder guard not released after feeder exit")

	bv.processBlockMinedNotSet(context.Background())
	mockBC.AssertNumberOfCalls(t, "GetBlocksMinedNotSet", 2)
}

// TestEnqueueSetMined_DoesNotBlockWhenChannelFull locks in the property the fix relies
// on: enqueuing a block for the setMined worker must never block the caller, even when
// the channel is full. The setMined worker is the sole drainer of setMinedChan, so a
// blocking send from the worker's own retry path — or from any producer while the worker
// is busy — would wedge mined finalization. An overflow hash is parked in the pending
// set (no goroutine spawned) and delivered once the worker drains the channel.
func TestEnqueueSetMined_DoesNotBlockWhenChannelFull(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bv := newSetMinedTestBV(1)

	h1 := &chainhash.Hash{1}
	h2 := &chainhash.Hash{2}

	// Fill the single buffer slot so the channel is full.
	bv.setMinedChan <- h1

	done := make(chan struct{})
	go func() {
		bv.enqueueSetMined(h2)
		close(done)
	}()

	select {
	case <-done:
		// returned without blocking — good
	case <-time.After(2 * time.Second):
		t.Fatal("enqueueSetMined blocked on a full setMinedChan")
	}

	// Both hashes are delivered through the worker's intake: the buffered one first,
	// then the overflow one once the channel has drained.
	require.Equal(t, h1, nextHashWithTimeout(t, ctx, bv))
	require.Equal(t, h2, nextHashWithTimeout(t, ctx, bv))

	// With everything drained, a cancelled context unblocks the worker intake.
	cancel()
	require.Nil(t, nextHashWithTimeout(t, ctx, bv))
}

// TestEnqueueSetMined_OverflowDedups locks in the bound freemans13 asked for: overflow
// is a set keyed by block hash, so repeated enqueues of the same block while the channel
// is full cost one entry, not one parked goroutine each.
func TestEnqueueSetMined_OverflowDedups(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bv := newSetMinedTestBV(1)

	h1 := &chainhash.Hash{1}
	h2 := &chainhash.Hash{2}

	bv.setMinedChan <- h1

	for i := 0; i < 3; i++ {
		bv.enqueueSetMined(h2)
	}

	bv.setMinedOverflowMu.Lock()
	overflowLen := len(bv.setMinedOverflow)
	bv.setMinedOverflowMu.Unlock()
	require.Equal(t, 1, overflowLen, "overflow must dedup repeated enqueues of the same hash")

	// The deduped hash is still delivered exactly once after the buffered one.
	require.Equal(t, h1, nextHashWithTimeout(t, ctx, bv))
	require.Equal(t, h2, nextHashWithTimeout(t, ctx, bv))
}
