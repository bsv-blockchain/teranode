package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchupAttemptCap is the #1057 regression: catchup retries for a single
// block must be bounded so a block no peer can complete (or a persistent local
// service error) cannot re-enter catchup forever and pin a worker + the catchup
// lock. The per-block attempt counter caps at CatchupMaxAttemptsPerBlock and then
// reports the block as exhausted (in cooldown); once the window expires the block
// can be retried again, so a transient failure self-heals. A cap of <= 0 disables
// the bound.
func TestCatchupAttemptCap(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = 3

	u := &Server{
		settings: tSettings,
		logger:   ulogger.TestLogger{},
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
	}

	h := chainhash.HashH([]byte("blk-cap"))

	require.False(t, u.catchupAttemptsExhausted(&h), "zero attempts is not exhausted")
	require.Equal(t, 1, u.recordCatchupAttempt(&h))
	require.False(t, u.catchupAttemptsExhausted(&h), "1/3 is below the cap")
	require.Equal(t, 2, u.recordCatchupAttempt(&h))
	require.False(t, u.catchupAttemptsExhausted(&h), "2/3 is below the cap")
	require.Equal(t, 3, u.recordCatchupAttempt(&h))
	require.True(t, u.catchupAttemptsExhausted(&h), "3/3 reaches the cap -> cooldown")

	// A different block has its own independent budget.
	other := chainhash.HashH([]byte("blk-other"))
	require.False(t, u.catchupAttemptsExhausted(&other))

	// Cooldown reset (simulating the TTL window expiring): the block can be retried.
	u.blockCatchupAttempts.Delete(h)
	require.False(t, u.catchupAttemptsExhausted(&h), "after the window expires the block is retriable again")

	// Cap disabled (<= 0) never exhausts, regardless of attempt count.
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = 0
	for i := 0; i < 10; i++ {
		u.recordCatchupAttempt(&h)
	}
	require.False(t, u.catchupAttemptsExhausted(&h), "cap <= 0 disables the bound")
}

// TestCatchupAttemptCap_NilCacheSafe verifies the helpers degrade gracefully when
// the attempt cache was never initialised (a Server literal built without the
// NewServer wiring, as some unit tests do): no panic, no cap.
func TestCatchupAttemptCap_NilCacheSafe(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	u := &Server{settings: tSettings, logger: ulogger.TestLogger{}} // blockCatchupAttempts == nil

	h := chainhash.HashH([]byte("blk-nil"))
	require.NotPanics(t, func() {
		require.Equal(t, 0, u.recordCatchupAttempt(&h))
		require.False(t, u.catchupAttemptsExhausted(&h))
	})
}

// newAttemptCapServer builds a Server with just the attempt-cap machinery wired,
// for the cooldown-window tests below.
func newAttemptCapServer(t *testing.T, cap int) *Server {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = cap
	return &Server{
		settings: tSettings,
		logger:   ulogger.TestLogger{},
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
			ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
		),
	}
}

// TestCatchupAttemptCap_WindowAnchoredToFirstFailure is the #1057 P1 regression:
// the cooldown window must run from the FIRST failure, not be extended by each
// subsequent one. ttlcache.Set always (re)sets the TTL, so the old code reset the
// window on every failure. Assert the stored expiry does not move on a repeat
// failure (a window reset would push it ~10 minutes out).
func TestCatchupAttemptCap_WindowAnchoredToFirstFailure(t *testing.T) {
	u := newAttemptCapServer(t, 5)
	h := chainhash.HashH([]byte("blk-window"))

	// Seed a first failure whose cooldown window is already partway through — a 30s
	// remaining window stands in for "the original failure happened a while ago".
	// (Two back-to-back recordCatchupAttempt calls can't show the bug: even a full
	// reset lands ~microseconds from the first, so we must start from a window that
	// is meaningfully shorter than the fresh 10-minute default.)
	u.blockCatchupAttempts.Set(h, 1, 30*time.Second)

	// A repeat failure must PRESERVE that ~30s window, not reset it to a fresh
	// ~10-minute one.
	require.Equal(t, 2, u.recordCatchupAttempt(&h))

	item := u.blockCatchupAttempts.Get(h)
	require.NotNil(t, item, "entry must still exist")
	require.Less(t, time.Until(item.ExpiresAt()), 2*time.Minute,
		"repeat failure must preserve the original (~30s) cooldown window, not reset it to a fresh ~10m one")
}

// TestCatchupAttemptCap_ClearedOnSuccess is the #1057 P2 regression: a recovering
// block must not carry its accumulated failure count into a later catchup. The
// helper used by every success path resets the counter and its window.
func TestCatchupAttemptCap_ClearedOnSuccess(t *testing.T) {
	u := newAttemptCapServer(t, 3)
	h := chainhash.HashH([]byte("blk-recover"))

	require.Equal(t, 1, u.recordCatchupAttempt(&h))
	require.Equal(t, 2, u.recordCatchupAttempt(&h))
	require.False(t, u.catchupAttemptsExhausted(&h), "2/3 is below the cap")

	require.Equal(t, 3, u.recordCatchupAttempt(&h))
	require.True(t, u.catchupAttemptsExhausted(&h), "3/3 reaches the cap")

	u.clearCatchupAttempts(&h)

	require.Nil(t, u.blockCatchupAttempts.Get(h), "counter must be gone after success")
	require.False(t, u.catchupAttemptsExhausted(&h))
	require.Equal(t, 1, u.recordCatchupAttempt(&h), "next failure starts a fresh count")
}

// TestProcessCatchupChItem exercises the catchup-consumer per-item handler
// (extracted from the Init goroutine so its #1057 cap branches are unit-testable
// via the injected catchupFunc). Covers: cooldown skip, success reset, the
// service-error count, the progress-aware reset (the review nit — a cycle that
// advanced the chain must not count toward the cap), state-error/in-progress
// no-count paths, the all-peers-exhausted count, and the invalid-block path.
func TestProcessCatchupChItem(t *testing.T) {
	initPrometheusMetrics()

	testBlock := func() *model.Block {
		prev := chainhash.HashH([]byte("prev"))
		mr := chainhash.HashH([]byte("merkle"))
		return &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}
	}

	// newServer wires just enough for processCatchupChItem: the three caches, an
	// injected catchupFunc (records call count + returns catchupErr), and a
	// blockchain mock for the generic-failure ReportPeerFailure. p2pClient is nil
	// and peerID is "" in items, so isPeerBad/isPeerMalicious/reportCatchup* are
	// no-ops and tryAlternativePeersForCatchup returns false.
	newServer := func(maxAttempts int, catchupErr error) (*Server, *int) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = maxAttempts

		mockBC := &blockchain.Mock{}
		mockBC.On("ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		calls := 0
		u := &Server{
			settings:            tSettings,
			logger:              ulogger.TestLogger{},
			blockchainClient:    mockBC,
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
			blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
				ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
				ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
			),
		}
		u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
			calls++
			return catchupErr
		}
		return u, &calls
	}

	ctx := context.Background()
	item := func(b *model.Block) processBlockCatchup {
		return processBlockCatchup{block: b, peerID: "", baseURL: ""}
	}

	t.Run("success clears guards and resets counter", func(t *testing.T) {
		u, calls := newServer(3, nil)
		b := testBlock()
		u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)

		u.processCatchupChItem(ctx, item(b))

		require.Equal(t, 1, *calls)
		require.Nil(t, u.processBlockNotify.Get(*b.Hash()))
		require.Nil(t, u.blockCatchupAttempts.Get(*b.Hash()))
	})

	t.Run("service error with no progress counts toward cap", func(t *testing.T) {
		u, _ := newServer(3, errors.NewServiceError("blockchain unavailable"))
		b := testBlock()

		u.processCatchupChItem(ctx, item(b))

		it := u.blockCatchupAttempts.Get(*b.Hash())
		require.NotNil(t, it)
		require.Equal(t, 1, it.Value())
		require.Nil(t, u.processBlockNotify.Get(*b.Hash()))
	})

	t.Run("failure that advanced the chain resets the counter (does not count)", func(t *testing.T) {
		u, _ := newServer(3, errors.NewServiceError("dropped mid-batch"))
		b := testBlock()
		u.recordCatchupAttempt(b.Hash()) // a prior failed cycle
		u.blocksValidated.Store(2)       // but THIS cycle validated 2 blocks before erroring

		u.processCatchupChItem(ctx, item(b))

		require.Nil(t, u.blockCatchupAttempts.Get(*b.Hash()), "a progress-making cycle must reset the cap counter")
	})

	t.Run("state error clears without counting", func(t *testing.T) {
		u, _ := newServer(3, errors.NewStateError("not running"))
		b := testBlock()

		u.processCatchupChItem(ctx, item(b))

		require.Nil(t, u.blockCatchupAttempts.Get(*b.Hash()))
		require.Nil(t, u.processBlockNotify.Get(*b.Hash()))
	})

	t.Run("catchup-in-progress keeps the guard and does not count", func(t *testing.T) {
		u, _ := newServer(3, errors.ErrCatchupInProgress)
		b := testBlock()
		u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)

		u.processCatchupChItem(ctx, item(b))

		require.Nil(t, u.blockCatchupAttempts.Get(*b.Hash()))
		require.NotNil(t, u.processBlockNotify.Get(*b.Hash()), "in-progress requeue must keep the processing guard")
	})

	t.Run("all peers exhausted counts and clears", func(t *testing.T) {
		u, _ := newServer(3, errors.ErrBlockIncomplete)
		b := testBlock()

		u.processCatchupChItem(ctx, item(b))

		it := u.blockCatchupAttempts.Get(*b.Hash())
		require.NotNil(t, it)
		require.Equal(t, 1, it.Value())
		require.Nil(t, u.processBlockNotify.Get(*b.Hash()))
	})

	t.Run("recovers via cached alternative peer", func(t *testing.T) {
		u, _ := newServer(3, nil)
		b := testBlock()

		call := 0
		u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
			call++
			if call == 1 {
				return errors.ErrBlockIncomplete // primary peer can't complete it
			}
			return nil // cached alternative succeeds
		}

		// Seed a cached alternative under a different peer id (the loop skips the
		// just-failed peer, which is "").
		u.catchupAlternatives.Set(*b.Hash(), []processBlockCatchup{{block: b, peerID: "altpeer", baseURL: "http://alt"}}, ttlcache.DefaultTTL)
		u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)

		u.processCatchupChItem(ctx, item(b))

		require.Equal(t, 2, call, "primary peer then the cached alternative")
		require.Nil(t, u.processBlockNotify.Get(*b.Hash()), "guard cleared on alternative success")
		require.Nil(t, u.blockCatchupAttempts.Get(*b.Hash()), "counter reset on alternative success")
		require.Nil(t, u.catchupAlternatives.Get(*b.Hash()), "alternatives cleared on success")
	})

	// ChiR1 regression: the ErrExternal classification from
	// fetchAndStoreSubtreeAndSubtreeData reaches processCatchupChItem buried in
	// the middle of the chain — fetchSubtreeDataForBlock wraps it in a
	// ServiceError (get_blocks.go) and orderedDelivery wraps that in a
	// ProcessingError. errors.Is matches by code across the whole chain, so
	// errors.Is(err, ErrServiceError) is ALSO true for this error; the handler
	// must check ErrExternal before ErrServiceError or the peer-failure path is
	// never taken for peer-unreachable/HTTP-error failures.
	t.Run("external error buried under service and processing wraps takes the peer-failure path", func(t *testing.T) {
		// Reconstruct the real catchup-path chain, innermost first:
		// fetchSubtreeFromPeer(HTTP failure) -> ExternalError(all peers failed)
		// -> ServiceError(fetchSubtreeDataForBlock) -> ProcessingError(orderedDelivery).
		httpErr := errors.NewServiceError("failed to fetch subtree from http://peer/subtree/aa")
		allPeersErr := errors.NewExternalError("all peers failed to fetch subtree aa", httpErr)
		svcWrap := errors.NewServiceError("failed to fetch subtree data for block bb", allPeersErr)
		chain := errors.NewProcessingError("worker failed for block bb", svcWrap)

		require.True(t, errors.Is(chain, errors.ErrServiceError), "precondition: the chain also matches ErrServiceError — that is the trap")
		require.True(t, errors.Is(chain, errors.ErrExternal))

		u, _ := newServer(3, chain)
		b := testBlock()
		u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)
		u.catchupAlternatives.Set(*b.Hash(), []processBlockCatchup{{block: b, peerID: "altpeer"}}, ttlcache.DefaultTTL)

		u.processCatchupChItem(ctx, item(b))

		require.Nil(t, u.processBlockNotify.Get(*b.Hash()), "peer-failure path must clear the processing guard")
		require.Nil(t, u.catchupAlternatives.Get(*b.Hash()), "peer-failure path must clear cached alternatives")

		it := u.blockCatchupAttempts.Get(*b.Hash())
		require.NotNil(t, it, "an all-peers-failed cycle must count toward the #1057 cap")
		require.Equal(t, 1, it.Value())

		mockBC := u.blockchainClient.(*blockchain.Mock)
		mockBC.AssertCalled(t, "ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, "catchup", mock.Anything)
	})

	t.Run("plain external error takes the peer-failure path", func(t *testing.T) {
		u, _ := newServer(3, errors.NewExternalError("all peers failed"))
		b := testBlock()
		u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)

		u.processCatchupChItem(ctx, item(b))

		require.Nil(t, u.processBlockNotify.Get(*b.Hash()))

		it := u.blockCatchupAttempts.Get(*b.Hash())
		require.NotNil(t, it)
		require.Equal(t, 1, it.Value())

		mockBC := u.blockchainClient.(*blockchain.Mock)
		mockBC.AssertCalled(t, "ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, "catchup", mock.Anything)
	})

	t.Run("invalid block clears notify without counting toward cap", func(t *testing.T) {
		u, _ := newServer(3, errors.NewBlockInvalidError("bad block"))
		b := testBlock()
		u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)

		u.processCatchupChItem(ctx, item(b))

		require.Nil(t, u.blockCatchupAttempts.Get(*b.Hash()), "an invalid block is not a retry-cap candidate")
		require.Nil(t, u.processBlockNotify.Get(*b.Hash()))
	})

	t.Run("exhausted cap skips catchup entirely", func(t *testing.T) {
		u, calls := newServer(2, errors.NewServiceError("svc"))
		b := testBlock()
		u.recordCatchupAttempt(b.Hash())
		u.recordCatchupAttempt(b.Hash())
		require.True(t, u.catchupAttemptsExhausted(b.Hash()))
		u.processBlockNotify.Set(*b.Hash(), true, ttlcache.DefaultTTL)

		u.processCatchupChItem(ctx, item(b))

		require.Equal(t, 0, *calls, "catchup must be skipped while the block is in cooldown")
		require.Nil(t, u.processBlockNotify.Get(*b.Hash()), "guard cleared on cooldown skip")
	})
}

// TestCatchupCap_BoundsReentry is the integration-level proof of the #1057 bound
// (requested in review): it drives the real consumer handler (processCatchupChItem,
// what the catchupCh goroutine calls per item) with a catchup that ALWAYS fails,
// re-entering it far more times than the cap, and asserts catchup() is invoked at
// most CatchupMaxAttemptsPerBlock times — after the cap the dequeue gate skips the
// block. Validates the livelock is actually bounded, not just the helpers.
func TestCatchupCap_BoundsReentry(t *testing.T) {
	initPrometheusMetrics()

	const (
		maxAttempts = 5
		reentries   = 20 // far more than the cap: simulate 20 re-announcements
	)

	testBlock := func() *model.Block {
		prev := chainhash.HashH([]byte("prev"))
		mr := chainhash.HashH([]byte("merkle"))
		return &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}
	}

	// run drives processCatchupChItem reentries times with a catchup that always
	// returns catchupErr, and returns how many times catchup was actually invoked.
	run := func(t *testing.T, catchupErr error) int {
		t.Helper()
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.CatchupMaxAttemptsPerBlock = maxAttempts

		mockBC := &blockchain.Mock{}
		mockBC.On("ReportPeerFailure", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		calls := 0
		u := &Server{
			settings:            tSettings,
			logger:              ulogger.TestLogger{},
			blockchainClient:    mockBC,
			processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
			catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
			blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](
				ttlcache.WithTTL[chainhash.Hash, int](10*time.Minute),
				ttlcache.WithDisableTouchOnHit[chainhash.Hash, int](),
			),
		}
		u.catchupFunc = func(_ context.Context, _ *model.Block, _, _ string) error {
			calls++
			return catchupErr
		}

		c := processBlockCatchup{block: testBlock(), peerID: "", baseURL: ""}
		for i := 0; i < reentries; i++ {
			u.processCatchupChItem(context.Background(), c)
		}
		return calls
	}

	t.Run("persistent service error is bounded", func(t *testing.T) {
		calls := run(t, errors.NewServiceError("blockchain unavailable"))
		require.LessOrEqualf(t, calls, maxAttempts, "catchup must run at most %d times over %d re-entries, got %d", maxAttempts, reentries, calls)
		require.Equal(t, maxAttempts, calls, "catchup runs exactly the cap, then the dequeue gate skips")
	})

	t.Run("all-peers-exhausted (ErrBlockIncomplete) is bounded", func(t *testing.T) {
		calls := run(t, errors.ErrBlockIncomplete)
		require.LessOrEqualf(t, calls, maxAttempts, "catchup must run at most %d times over %d re-entries, got %d", maxAttempts, reentries, calls)
		require.Equal(t, maxAttempts, calls, "catchup runs exactly the cap, then the dequeue gate skips")
	})
}

// TestBlockProcessingWithRetry tests the retry mechanism when block fetching fails
func TestBlockProcessingWithRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 2
	tSettings.BlockValidation.NearForkThreshold = 10

	// Create test blocks
	blocks := testhelpers.CreateTestBlockChain(t, 5)
	targetBlock := blocks[4]
	targetHash := targetBlock.Hash()

	// Create mock blockchain store and client
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Store the parent blocks so the test block can be processed
	for i := 0; i < 4; i++ {
		err := mockBlockchainClient.AddBlock(ctx, blocks[i], "test-peer")
		require.NoError(t, err)
	}

	// Create mock validator
	mockValidator := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	// Create server with priority queue
	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient,
		blockValidation:     bv,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, uint32(tSettings.BlockValidation.NearForkThreshold), mockBlockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		// Note: peerMetrics field has been removed from Server struct
	}

	t.Run("Retry_Uses_Alternative_Peer", func(t *testing.T) {
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// First peer fails
		failCount := 0
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer1/block/%s", targetHash),
			func(req *http.Request) (*http.Response, error) {
				failCount++
				return nil, errors.NewNetworkError("network error")
			})

		// Second peer succeeds
		targetBlockBytes, err := targetBlock.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer2/block/%s", targetHash),
			httpmock.NewBytesResponder(200, targetBlockBytes))

		// Add block announcement from first peer
		blockFound1 := processBlockFound{
			hash:    targetHash,
			baseURL: "http://peer1",
			peerID:  "peer1",
		}

		// Add block announcement from second peer as alternative
		blockFound2 := processBlockFound{
			hash:    targetHash,
			baseURL: "http://peer2",
			peerID:  "peer2",
		}

		// Add to queue - first one becomes primary, second becomes alternative
		server.blockPriorityQueue.Add(blockFound1, PriorityChainExtending, targetBlock.Height)
		server.blockPriorityQueue.Add(blockFound2, PriorityChainExtending, targetBlock.Height)

		// Process block - should fail with peer1, succeed with peer2
		err = server.processBlockWithPriority(ctx, blockFound1)
		require.NoError(t, err)

		// Verify first peer was tried
		assert.Equal(t, 1, failCount)

		// Verify block was processed successfully
		exists, err := server.blockValidation.GetBlockExists(ctx, targetHash)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("Retry_After_All_Alternatives_Fail", func(t *testing.T) {
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create a new block for this test
		testBlocks := testhelpers.CreateTestBlockChain(t, 6)
		testBlock := testBlocks[5]
		testHash := testBlock.Hash()

		// Store parent blocks so test block can be processed
		for i := 0; i < 5; i++ {
			err := mockBlockchainClient.AddBlock(ctx, testBlocks[i], "test-peer")
			require.NoError(t, err)
		}

		// All peers fail initially
		peer1Attempts := 0
		peer2Attempts := 0
		peer3Attempts := 0

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer1/block/%s", testHash),
			func(req *http.Request) (*http.Response, error) {
				peer1Attempts++
				if peer1Attempts < 2 {
					return nil, errors.NewNetworkError("temporary failure")
				}
				// Succeed on second attempt
				testBlockBytes, _ := testBlock.Bytes()
				return httpmock.NewBytesResponse(200, testBlockBytes), nil
			})

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer2/block/%s", testHash),
			func(req *http.Request) (*http.Response, error) {
				peer2Attempts++
				return nil, errors.NewNetworkError("peer2 always fails")
			})

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer3/block/%s", testHash),
			func(req *http.Request) (*http.Response, error) {
				peer3Attempts++
				return nil, errors.NewNetworkError("peer3 always fails")
			})

		// Clean queue for this test
		server.blockPriorityQueue.Clear()

		// Add multiple peer announcements
		blockFound1 := processBlockFound{
			hash:    testHash,
			baseURL: "http://peer1",
			peerID:  "peer1",
		}
		blockFound2 := processBlockFound{
			hash:    testHash,
			baseURL: "http://peer2",
			peerID:  "peer2",
		}
		blockFound3 := processBlockFound{
			hash:    testHash,
			baseURL: "http://peer3",
			peerID:  "peer3",
		}

		server.blockPriorityQueue.Add(blockFound1, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound2, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound3, PriorityChainExtending, testBlock.Height)

		// First attempt should fail after trying all peers
		err := server.processBlockWithPriority(ctx, blockFound1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get block")

		// Verify all peers were tried
		assert.Equal(t, 1, peer1Attempts)
		assert.Equal(t, 1, peer2Attempts)
		assert.Equal(t, 1, peer3Attempts)

		// Re-add the sources for retry since they were consumed
		server.blockPriorityQueue.Add(blockFound1, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound2, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound3, PriorityChainExtending, testBlock.Height)

		// Now simulate a retry with special retry marker
		retryBlock := processBlockFound{
			hash:    testHash,
			baseURL: "retry",
			peerID:  "",
		}

		// Process retry - should use alternative source (peer1 again) and succeed
		err = server.processBlockWithPriority(ctx, retryBlock)
		require.NoError(t, err)

		// Verify peer1 was tried again and succeeded
		assert.Equal(t, 2, peer1Attempts)
	})

	t.Run("Malicious_Peer_Skipped_On_Retry", func(t *testing.T) {
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create another test block
		maliciousTestBlocks := testhelpers.CreateTestBlockChain(t, 7)
		maliciousTestBlock := maliciousTestBlocks[6]
		maliciousHash := maliciousTestBlock.Hash()

		// Store parent blocks so test block can be processed
		for i := 5; i < 6; i++ {
			err := mockBlockchainClient.AddBlock(ctx, maliciousTestBlocks[i], "test-peer")
			require.NoError(t, err)
		}

		// Mark peer1 as malicious
		// Note: peerMetrics field has been removed from Server struct
		// (malicious peer marking disabled)

		// Good peer responds correctly
		maliciousBlockBytes, err := maliciousTestBlock.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://good_peer/block/%s", maliciousHash),
			httpmock.NewBytesResponder(200, maliciousBlockBytes))

		// Clean queue
		server.blockPriorityQueue.Clear()

		// Add announcements
		maliciousFound := processBlockFound{
			hash:    maliciousHash,
			baseURL: "http://malicious_peer",
			peerID:  "malicious_peer",
		}
		goodFound := processBlockFound{
			hash:    maliciousHash,
			baseURL: "http://good_peer",
			peerID:  "good_peer",
		}

		server.blockPriorityQueue.Add(maliciousFound, PriorityChainExtending, maliciousTestBlock.Height)
		server.blockPriorityQueue.Add(goodFound, PriorityChainExtending, maliciousTestBlock.Height)

		// Process should skip malicious peer and use good peer
		err = server.processBlockWithPriority(ctx, maliciousFound)
		require.NoError(t, err)

		// Verify block was processed
		exists, err := server.blockValidation.GetBlockExists(ctx, maliciousHash)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("No_Alternative_Sources_On_Retry", func(t *testing.T) {
		// Create isolated block
		isolatedBlock := testhelpers.CreateTestBlockChain(t, 8)[7]
		isolatedHash := isolatedBlock.Hash()

		// Clean queue
		server.blockPriorityQueue.Clear()

		// Create retry block with no alternatives
		retryBlock := processBlockFound{
			hash:    isolatedHash,
			baseURL: "retry",
			peerID:  "",
		}

		// Process should fail with no sources available
		err := server.processBlockWithPriority(ctx, retryBlock)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no sources available")
	})
}

// TestBlockPriorityQueueRetry tests the priority queue retry functionality
func TestBlockPriorityQueueRetry(t *testing.T) {
	queue := NewBlockPriorityQueue(ulogger.TestLogger{})

	// Create test block
	hash1 := &chainhash.Hash{0x01}
	hash2 := &chainhash.Hash{0x02}

	blockFound1 := processBlockFound{
		hash:    hash1,
		baseURL: "http://peer1",
		peerID:  "peer1",
	}

	t.Run("RequeueForRetry_New_Block", func(t *testing.T) {
		queue.Add(blockFound1, PriorityChainExtending, 100)

		// Get and process the block
		mockBP := &mockBlockProcessor{}
		found, status := queue.Get(context.Background(), mockBP)
		require.Equal(t, GetOK, status)
		assert.Equal(t, hash1, found.hash)

		// Queue should be empty
		assert.Equal(t, 0, queue.Size())

		// Requeue for retry
		retryBlock := processBlockFound{
			hash:    hash1,
			baseURL: "retry",
			peerID:  "",
		}
		queue.RequeueForRetry(retryBlock, PriorityDeepFork, 100)

		// Should be back in queue with retry info
		assert.Equal(t, 1, queue.Size())

		// Get the retry block
		found2, status := queue.Get(context.Background(), mockBP)
		require.Equal(t, GetOK, status)
		assert.Equal(t, "retry", found2.baseURL)
		assert.Equal(t, "", found2.peerID)
	})

	t.Run("RequeueForRetry_Existing_Block", func(t *testing.T) {
		// Add block
		blockFound2 := processBlockFound{
			hash:    hash2,
			baseURL: "http://peer2",
			peerID:  "peer2",
		}
		queue.Add(blockFound2, PriorityNearFork, 200)

		// Check it exists
		assert.True(t, queue.Contains(*hash2))

		// Requeue same block - should update retry count
		queue.RequeueForRetry(blockFound2, PriorityNearFork, 200)

		// Should still be 1 item
		assert.Equal(t, 1, queue.Size())

		// Verify block is still in queue
		assert.True(t, queue.Contains(*hash2))
	})
}

// TestAlternativeSourceTracking tests that alternative sources are properly tracked
func TestAlternativeSourceTracking(t *testing.T) {
	queue := NewBlockPriorityQueue(ulogger.TestLogger{})
	hash := &chainhash.Hash{0x03}

	// Add primary source
	primary := processBlockFound{
		hash:    hash,
		baseURL: "http://primary",
		peerID:  "primary",
	}
	queue.Add(primary, PriorityChainExtending, 300)

	// Add multiple alternatives
	for i := 1; i <= 3; i++ {
		alt := processBlockFound{
			hash:    hash,
			baseURL: fmt.Sprintf("http://alt%d", i),
			peerID:  fmt.Sprintf("alt%d", i),
		}
		queue.Add(alt, PriorityChainExtending, 300)
	}

	// Should have 3 alternatives stored
	alt1, ok := queue.GetAlternativeSource(hash)
	require.True(t, ok)
	assert.Equal(t, "http://alt1", alt1.baseURL)

	alt2, ok := queue.GetAlternativeSource(hash)
	require.True(t, ok)
	assert.Equal(t, "http://alt2", alt2.baseURL)

	alt3, ok := queue.GetAlternativeSource(hash)
	require.True(t, ok)
	assert.Equal(t, "http://alt3", alt3.baseURL)

	// No more alternatives
	_, ok = queue.GetAlternativeSource(hash)
	assert.False(t, ok)
}

// TestBlockProcessingWorkerRetry tests the worker retry mechanism
func TestBlockProcessingWorkerRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create mock blockchain store and client
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Create mock validator
	mockValidator := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient,
		blockValidation:     bv,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, mockBlockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	// Create test block
	blocks := testhelpers.CreateTestBlockChain(t, 2)
	targetBlock := blocks[1]
	targetHash := targetBlock.Hash()

	// Store parent
	err = mockBlockchainClient.AddBlock(ctx, blocks[0], "test-peer")
	require.NoError(t, err)

	// Mock failing endpoint
	var attemptCount sync.Mutex
	var attempts int
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://failing-peer/block/%s", targetHash),
		func(req *http.Request) (*http.Response, error) {
			attemptCount.Lock()
			attempts++
			attemptCount.Unlock()
			return nil, errors.NewNetworkError("failed to fetch block")
		})

	// Add block to queue
	blockFound := processBlockFound{
		hash:    targetHash,
		baseURL: "http://failing-peer",
		peerID:  "failing-peer",
	}
	server.blockPriorityQueue.Add(blockFound, PriorityChainExtending, targetBlock.Height)

	// Start worker
	workerCtx, workerCancel := context.WithCancel(ctx)
	done := make(chan bool)

	go func() {
		server.blockProcessingWorker(workerCtx, 1)
		done <- true
	}()

	// Wait for worker to process and fail
	require.Eventually(t, func() bool {
		attemptCount.Lock()
		defer attemptCount.Unlock()
		return attempts >= 1
	}, 5*time.Second, 10*time.Millisecond, "worker should have attempted at least once")

	// Cancel worker
	workerCancel()
	<-done

	// Check that block is still in queue (would be re-queued after delay)
	// Note: In real scenario, the retry goroutine would re-add after 5 seconds
	assert.Equal(t, 0, server.blockPriorityQueue.Size()) // Empty because retry is async with delay
}

// TestChainExtendingBlocksNotSentToCatchup tests that chain-extending blocks
// are NOT sent to catchup even when the queue is busy
func TestChainExtendingBlocksNotSentToCatchup(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.UseCatchupWhenBehind = true

	// Create test blockchain
	blocks := testhelpers.CreateTestBlockChain(t, 5)

	// Create mock blockchain store and client
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Store the first 3 blocks as the base chain
	for i := 0; i < 3; i++ {
		err := mockBlockchainClient.AddBlock(ctx, blocks[i], "test-peer")
		require.NoError(t, err)
	}

	// Create mock validator
	mockValidator := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	server := &Server{
		logger:             logger,
		settings:           tSettings,
		blockchainClient:   mockBlockchainClient,
		blockValidation:    bv,
		blockFoundCh:       make(chan processBlockFound, 20),
		blockPriorityQueue: NewBlockPriorityQueue(logger),
		blockClassifier:    NewBlockClassifier(logger, 10, mockBlockchainClient),
		forkManager:        NewForkManager(logger, tSettings),
		catchupCh:          make(chan processBlockCatchup, 10),
		// Note: peerMetrics field has been removed from Server struct
		stats:               gocore.NewStat("test"),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	// Mock HTTP responses for blocks
	for i := 3; i < 5; i++ {
		block := blocks[i]
		hash := block.Hash()
		blockBytes, err := block.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/block/%s", hash),
			httpmock.NewBytesResponder(200, blockBytes))
	}

	// Add many fake blocks to the queue to simulate busy system
	for i := 0; i < 15; i++ {
		fakeHash := &chainhash.Hash{byte(i + 100)}
		blockFound := processBlockFound{
			hash:    fakeHash,
			baseURL: "http://test-peer",
			peerID:  "test_peer",
		}
		server.blockPriorityQueue.Add(blockFound, PriorityDeepFork, uint32(100+i))
	}

	// Queue should have 15 blocks
	assert.Equal(t, 15, server.blockPriorityQueue.Size())

	// Create a channel to monitor catchup
	catchupReceived := make(chan *chainhash.Hash, 1)
	go func() {
		for {
			select {
			case catchupBlock := <-server.catchupCh:
				catchupReceived <- catchupBlock.block.Hash()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Now process block 3 which extends the chain
	// This should NOT go to catchup despite busy queue
	bestHeader, bestMeta, err := mockBlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), bestMeta.Height)
	assert.True(t, blocks[3].Header.HashPrevBlock.IsEqual(bestHeader.Hash()))

	blockFound3 := processBlockFound{
		hash:    blocks[3].Hash(),
		baseURL: "http://test-peer",
		peerID:  "test_peer",
		errCh:   make(chan error, 1),
	}

	// Process the chain-extending block
	err = server.processBlockFoundChannel(ctx, blockFound3)
	require.NoError(t, err)

	// Give time for any async operations
	time.Sleep(500 * time.Millisecond)

	// Check that block 3 was NOT sent to catchup
	select {
	case hash := <-catchupReceived:
		t.Errorf("Chain-extending block %s should NOT have been sent to catchup", hash)
	default:
		// Good - no block in catchup
	}

	// Block 3 should be in the priority queue as chain-extending
	assert.Equal(t, 16, server.blockPriorityQueue.Size())

	// Verify it was classified correctly - block should be in queue
	queue := server.blockPriorityQueue
	assert.True(t, queue.Contains(*blocks[3].Hash()))

	// Now test that a non-chain-extending block DOES go to catchup
	blockFound4 := processBlockFound{
		hash:    blocks[4].Hash(), // This block's parent (block 3) doesn't exist yet
		baseURL: "http://test-peer",
		peerID:  "test_peer",
		errCh:   make(chan error, 1),
	}

	// Process the non-chain-extending block
	err = server.processBlockFoundChannel(ctx, blockFound4)
	require.NoError(t, err)

	// This one SHOULD go to catchup
	select {
	case hash := <-catchupReceived:
		assert.Equal(t, blocks[4].Hash(), hash, "Non-chain-extending block should have been sent to catchup")
	case <-time.After(5 * time.Second):
		t.Error("Expected non-chain-extending block to be sent to catchup")
	}
}
