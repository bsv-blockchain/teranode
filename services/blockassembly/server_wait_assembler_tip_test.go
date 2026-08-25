package blockassembly

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

// newTipWaitServer builds a block assembly server whose assembler is level with
// the genesis tip its blockchain client reports, which is the state a healthy
// node is in between blocks.
func newTipWaitServer(t *testing.T) *BlockAssembly {
	t.Helper()

	common := testutil.NewCommonTestSetup(t)
	subtreeStore := testutil.NewMemoryBlobStore()
	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(common.Ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(0)

	server := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
	server.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, server.Init(common.Ctx))

	return server
}

// syncAssemblerToChainTip points the assembler at whatever tip the blockchain
// client currently reports, so only the running state is left varying.
func syncAssemblerToChainTip(t *testing.T, server *BlockAssembly) {
	t.Helper()

	header, meta, err := server.blockchainClient.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	server.blockAssembler.setBestBlockHeader(header, meta.Height)
}

// flakyTipClient fails its first failures calls to GetBestBlockHeader and then
// behaves normally, so the retry branch can be exercised deterministically.
type flakyTipClient struct {
	blockchain.ClientI

	mu       sync.Mutex
	failures int
	calls    int
}

func (c *flakyTipClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.mu.Lock()
	c.calls++
	shouldFail := c.calls <= c.failures
	c.mu.Unlock()

	if shouldFail {
		return nil, nil, errors.NewServiceError("blockchain service unavailable")
	}

	return c.ClientI.GetBestBlockHeader(ctx)
}

func (c *flakyTipClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

// TestWaitForAssemblerTip covers the branches the sequential end-to-end test
// cannot reach deterministically. The interesting reasoning lives in exactly
// these branches: a nil current header is a wait condition rather than a
// failure, a transient tip-read error must not abandon the guard, and being
// level with the chain is not on its own enough to proceed.
func TestWaitForAssemblerTip(t *testing.T) {
	t.Run("returns immediately when level and running", func(t *testing.T) {
		server := newTipWaitServer(t)
		syncAssemblerToChainTip(t, server)
		server.blockAssembler.setCurrentRunningState(StateRunning)

		start := time.Now()
		require.NoError(t, server.waitForAssemblerTip(context.Background()))
		require.Less(t, time.Since(start), 25*time.Millisecond,
			"an already-ready assembler must not pay a tick")
	})

	t.Run("waits while the assembler is still reorging, then proceeds", func(t *testing.T) {
		// This is the ChiR1 case: the assembler is already level with the chain
		// because reset() publishes the tip before SubtreeProcessor.Reset runs,
		// so tip equality alone would return here — mid-rebuild.
		server := newTipWaitServer(t)
		syncAssemblerToChainTip(t, server)
		server.blockAssembler.setCurrentRunningState(StateReorging)

		go func() {
			time.Sleep(80 * time.Millisecond)
			server.blockAssembler.setCurrentRunningState(StateRunning)
		}()

		start := time.Now()
		require.NoError(t, server.waitForAssemblerTip(context.Background()))
		require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond,
			"being level with the chain is not enough while a transition is still running")
	})

	t.Run("waits through MovingUp, when the candidate would be transaction-less", func(t *testing.T) {
		server := newTipWaitServer(t)
		syncAssemblerToChainTip(t, server)
		server.blockAssembler.setCurrentRunningState(StateMovingUp)

		go func() {
			time.Sleep(60 * time.Millisecond)
			server.blockAssembler.setCurrentRunningState(StateRunning)
		}()

		require.NoError(t, server.waitForAssemblerTip(context.Background()))
		require.Equal(t, StateRunning, server.blockAssembler.GetCurrentRunningState())
	})

	t.Run("times out with an actionable error rather than proceeding", func(t *testing.T) {
		server := newTipWaitServer(t)
		syncAssemblerToChainTip(t, server)
		server.blockAssembler.setCurrentRunningState(StateResetting)
		server.blockAssembler.settings.BlockAssembly.GenerateTipWaitTimeout = 100 * time.Millisecond

		err := server.waitForAssemblerTip(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "did not reach the chain tip",
			"the operator is told what to retry, not handed a stale-candidate error")
		require.Contains(t, err.Error(), StateStrings[StateResetting],
			"the state it was stuck in is the useful part of the message")
	})

	t.Run("a nil current header keeps waiting rather than failing", func(t *testing.T) {
		server := newTipWaitServer(t)
		server.blockAssembler.bestBlock.Store((*BestBlockInfo)(nil))
		server.blockAssembler.setCurrentRunningState(StateRunning)
		server.blockAssembler.settings.BlockAssembly.GenerateTipWaitTimeout = 100 * time.Millisecond

		start := time.Now()
		err := server.waitForAssemblerTip(context.Background())
		require.Error(t, err, "an assembler that never loads its best block times out")
		require.Contains(t, err.Error(), "did not reach the chain tip")
		// Without this the subtest would pass unchanged if a nil header started
		// failing immediately, which is the opposite of what its name claims.
		require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond,
			"a nil current header must be waited out, not failed on immediately")
	})

	t.Run("a transient tip-read failure costs a poll, not the guard", func(t *testing.T) {
		server := newTipWaitServer(t)
		syncAssemblerToChainTip(t, server)
		server.blockAssembler.setCurrentRunningState(StateRunning)

		// Fail the first three reads. The wait must keep polling and still
		// succeed: abandoning the guard on a blockchain-service blip was the
		// whole of the ChiR4 defect.
		flaky := &flakyTipClient{ClientI: server.blockchainClient, failures: 3}
		server.blockchainClient = flaky

		require.NoError(t, server.waitForAssemblerTip(context.Background()))
		require.Greater(t, flaky.callCount(), 3, "the wait must have retried past the failures")
	})

	t.Run("waits while the assembler is behind the chain, then proceeds", func(t *testing.T) {
		// The issue 764 shape, and the one the running-state check alone cannot
		// see. The subscription loop enters StateBlockchainSubscription only once
		// it dequeues the notification, so between the store committing a new tip
		// and that dequeue the assembler sits in Running holding a stale
		// CurrentBlock. Every other waiting subtest here syncs the assembler to
		// the chain first and varies only the state, which left the tip
		// comparison in assemblerReady covered by nothing.
		server := newTipWaitServer(t)

		tip, meta, err := server.blockchainClient.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		stale := staleHeader(t, tip)
		server.blockAssembler.setBestBlockHeader(stale, meta.Height)
		server.blockAssembler.setCurrentRunningState(StateRunning)

		go func() {
			time.Sleep(80 * time.Millisecond)
			server.blockAssembler.setBestBlockHeader(tip, meta.Height)
		}()

		start := time.Now()
		require.NoError(t, server.waitForAssemblerTip(context.Background()))
		require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond,
			"a Running assembler on a stale tip must not be treated as ready")
	})

	t.Run("a running assembler that never catches up times out", func(t *testing.T) {
		// The same divergence, never resolved. Running alone would return
		// immediately and generate would build on the pre-reorg parent.
		server := newTipWaitServer(t)

		tip, meta, err := server.blockchainClient.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		server.blockAssembler.setBestBlockHeader(staleHeader(t, tip), meta.Height)
		server.blockAssembler.setCurrentRunningState(StateRunning)
		server.settings.BlockAssembly.GenerateTipWaitTimeout = 100 * time.Millisecond

		start := time.Now()
		err = server.waitForAssemblerTip(context.Background())
		require.Error(t, err, "a stale tip must not be reported as ready just because the state is Running")
		require.Contains(t, err.Error(), "did not reach the chain tip")
		require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond,
			"the divergence must be waited out, not failed on immediately")
	})

	t.Run("a cancelled parent context ends the wait", func(t *testing.T) {
		server := newTipWaitServer(t)
		syncAssemblerToChainTip(t, server)
		server.blockAssembler.setCurrentRunningState(StateReorging)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()

		start := time.Now()
		require.Error(t, server.waitForAssemblerTip(ctx))
		require.Less(t, time.Since(start), 2*time.Second,
			"the parent deadline must bound the wait, not just the configured timeout")
	})
}

// staleHeader returns a header that is valid but is not the one passed in, so a
// tip comparison against it must fail. Hash() recomputes from the bytes, so
// changing any field of a copy is enough.
func staleHeader(t *testing.T, tip *model.BlockHeader) *model.BlockHeader {
	t.Helper()

	stale := *tip
	stale.Nonce = tip.Nonce + 1

	require.False(t, stale.Hash().IsEqual(tip.Hash()),
		"the fixture must actually differ from the chain tip or the subtest proves nothing")

	return &stale
}
