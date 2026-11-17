package utxopersister

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestReadWriteHeight(t *testing.T) {
	ctx := context.Background()

	store := memory.New()

	// Create a new UTXO persister
	tSettings := test.CreateBaseTestSettings(t)
	s := New(ctx, ulogger.TestLogger{}, tSettings, store, nil)

	oldHeight, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), oldHeight)

	// Write the height
	err = s.writeLastHeight(ctx, 100_000)
	require.NoError(t, err)

	height, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(100_000), height)

	// Write the old height back
	err = s.writeLastHeight(ctx, oldHeight)
	require.NoError(t, err)
}

// Helper function to create a test hash
func createTestHash(s string) chainhash.Hash {
	hash := chainhash.DoubleHashH([]byte(s))
	return hash
}

// Helper function to create test block header
func createTestBlockHeader() *model.BlockHeader {
	prevHash := createTestHash("prev-block")
	merkleRoot := createTestHash("merkle-root")

	// Create NBit from bytes
	var nBit model.NBit
	copy(nBit[:], []byte{0xff, 0xff, 0x00, 0x1d}) // Little endian representation of 0x1d00ffff

	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &prevHash,
		HashMerkleRoot: &merkleRoot,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           nBit,
		Nonce:          12345,
	}
}

// Helper function to create test block header meta
func createTestBlockHeaderMeta(height uint32) *model.BlockHeaderMeta {
	return &model.BlockHeaderMeta{
		Height:      height,
		ChainWork:   []byte("chainwork"),
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
		TxCount:     1,
		SizeInBytes: 1000,
		MinedSet:    true,
		SubtreesSet: true,
	}
}

// Test NewDirect constructor
func TestNewDirect(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	blockchainStore := &blockchain_store.MockStore{}

	server, err := NewDirect(ctx, logger, tSettings, blockStore, blockchainStore)

	require.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, logger, server.logger)
	assert.Equal(t, tSettings, server.settings)
	assert.Equal(t, blockStore, server.blockStore)
	assert.Equal(t, blockchainStore, server.blockchainStore)
	assert.NotNil(t, server.stats)
	assert.NotNil(t, server.triggerCh)
}

// Test New constructor with nil block store
func TestNew_NilBlockStore(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	mockBlockchainClient := &blockchain.Mock{}

	server := New(ctx, logger, tSettings, nil, mockBlockchainClient)

	assert.NotNil(t, server)
	assert.Equal(t, logger, server.logger)
	assert.Equal(t, tSettings, server.settings)
	assert.Nil(t, server.blockStore)
	assert.Equal(t, mockBlockchainClient, server.blockchainClient)
}

// Test Health - liveness check
func TestHealth_LivenessCheck(t *testing.T) {
	ctx := context.Background()
	server := &Server{}

	status, message, err := server.Health(ctx, true)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", message)
}

// Test Health - readiness check with no dependencies (simpler test)
func TestHealth_ReadinessCheck_NoDependencies(t *testing.T) {
	ctx := context.Background()

	server := &Server{
		// No dependencies set
	}

	status, message, err := server.Health(ctx, false)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "200")
}

// Test Init - successful initialization
func TestInit_Success(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	// Write a test height first
	err := server.writeLastHeight(ctx, 12345)
	require.NoError(t, err)

	err = server.Init(ctx)

	assert.NoError(t, err)
	assert.Equal(t, uint32(12345), server.lastHeight)
}

// Test Init - no existing height file
func TestInit_NoExistingHeight(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	err := server.Init(ctx)

	assert.NoError(t, err)
	assert.Equal(t, uint32(0), server.lastHeight)
}

// Test Stop
func TestStop(t *testing.T) {
	ctx := context.Background()
	server := &Server{}

	err := server.Stop(ctx)

	assert.NoError(t, err)
}

// Test trigger - not running
func TestTrigger_NotRunning(t *testing.T) {
	ctx := context.Background()

	// Create server with minimal setup to avoid nil pointer errors
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	mockBlockchainClient := &blockchain.Mock{}

	// Mock the blockchain client methods that will be called during processNextBlock
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		createTestBlockHeader(),
		createTestBlockHeaderMeta(150),
		nil,
	)

	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)
	server.lastHeight = 100

	err := server.trigger(ctx, "test")

	assert.NoError(t, err)
	assert.False(t, server.running)

	mockBlockchainClient.AssertExpectations(t)
}

// Test trigger - already running
func TestTrigger_AlreadyRunning(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	server := &Server{
		running:  true,
		logger:   ulogger.TestLogger{},
		settings: tSettings,
		mu:       sync.Mutex{},
	}

	err := server.trigger(ctx, "test")

	assert.NoError(t, err)
	assert.True(t, server.running) // Should remain true
}

// Test processNextBlock - waiting for confirmations
func TestProcessNextBlock_WaitingForConfirmations(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		createTestBlockHeader(),
		createTestBlockHeaderMeta(150), // Best block is at height 150
		nil,
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)
	server.lastHeight = 100 // Current height 100, need 100 confirmations, so can process up to height 50

	duration, err := server.processNextBlock(ctx)

	assert.NoError(t, err)
	assert.Equal(t, 1*time.Minute, duration)

	mockBlockchainClient.AssertExpectations(t)
}

// Test processNextBlock - error getting best block header
func TestProcessNextBlock_GetBestBlockHeaderError(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		(*model.BlockHeader)(nil),
		(*model.BlockHeaderMeta)(nil),
		terrors.NewServiceError("blockchain error"),
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)

	duration, err := server.processNextBlock(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "blockchain error")
	assert.Equal(t, time.Duration(0), duration)

	mockBlockchainClient.AssertExpectations(t)
}

// Test readLastHeight - file parsing error
func TestReadLastHeight_ParseError(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	// Write invalid height data
	err := blockStore.Set(ctx, nil, "dat", []byte("invalid-height"), options.WithFilename("lastProcessed"))
	require.NoError(t, err)

	height, err := server.readLastHeight(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse height")
	assert.Equal(t, uint32(0), height)
}

// Test verifyLastSet - error case (no UTXO set exists)
func TestVerifyLastSet_NoUTXOSetExists(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	testHash := createTestHash("test-block")

	// This should fail because no UTXO set exists
	err := server.verifyLastSet(ctx, &testHash)

	assert.Error(t, err)
}

// TestVerifyLastSet_ExistingValidSet pins that verifyLastSet does NOT
// double-read the fileformat magic. The store layer (validateFileHeader
// inside file.go and the equivalent in memory.go) already validates the
// 8-byte magic on GetIoReader; consuming another 8 bytes here would
// read the first bytes of the block hash field that follows the header
// in the UTXO set file layout, fail with "unknown magic: [<random
// bytes from hash>]", and bring the whole core sidecar down via
// ServiceManager.
//
// The body bytes used here are intentionally not a valid magic: they
// match the actual UTXO set layout (32-byte block hash followed by
// 4-byte height), so without the fix the test fails with exactly the
// production failure mode.
func TestVerifyLastSet_ExistingValidSet(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	testHash := createTestHash("test-block")

	// Write a minimal UTXO set body. memory.Set prepends the fileformat
	// magic for us, so we only supply the post-header bytes.
	body := make([]byte, 0, 36)
	body = append(body, testHash[:]...)         // 32 bytes block hash
	body = append(body, 0x01, 0x00, 0x00, 0x00) // 4 bytes height
	require.NoError(t, blockStore.Set(ctx, testHash[:], fileformat.FileTypeUtxoSet, body))

	err := server.verifyLastSet(ctx, &testHash)
	require.NoError(t, err, "verifyLastSet must succeed when a valid UTXO set file exists; double-read of the fileformat magic would surface here as \"unknown magic: [...]\"")
	// Explicit regression intent: future divergent failures should not be
	// confused with the original double-read bug.
	if err != nil {
		require.NotContains(t, err.Error(), "unknown magic", "regression: double-read of fileformat magic must not return")
	}
}

// Test trigger - processNextBlock returns not found error
func TestTrigger_ProcessNextBlockNotFound(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	// Mock to return not found error
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		(*model.BlockHeader)(nil),
		(*model.BlockHeaderMeta)(nil),
		terrors.ErrNotFound,
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)

	err := server.trigger(ctx, "test")

	assert.NoError(t, err) // ErrNotFound is handled gracefully
	assert.False(t, server.running)

	mockBlockchainClient.AssertExpectations(t)
}

// Test trigger - processNextBlock returns other error
func TestTrigger_ProcessNextBlockError(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	// Mock to return generic error
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		(*model.BlockHeader)(nil),
		(*model.BlockHeaderMeta)(nil),
		terrors.NewServiceError("service error"),
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)

	err := server.trigger(ctx, "test")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service error")
	assert.False(t, server.running)

	mockBlockchainClient.AssertExpectations(t)
}

// Test processNextBlock - wrong number of headers
func TestProcessNextBlock_WrongNumberOfHeaders(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		createTestBlockHeader(),
		createTestBlockHeaderMeta(200), // Best block height 200
		nil,
	)
	// Mock GetBlockHeadersByHeight to return empty slice (wrong number)
	mockBlockchainClient.On("GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything).Return(
		[]*model.BlockHeader{}, // Empty slice instead of 1 header
		[]*model.BlockHeaderMeta{},
		nil,
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)
	server.lastHeight = 50

	duration, err := server.processNextBlock(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "1 headers should have been returned, got 0")
	assert.Equal(t, time.Duration(0), duration)

	mockBlockchainClient.AssertExpectations(t)
}

// Test writeLastHeight - success (already covered but explicit test)
func TestWriteLastHeight_Success(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	err := server.writeLastHeight(ctx, 54321)

	assert.NoError(t, err)

	// Verify it was written correctly
	height, err := server.readLastHeight(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint32(54321), height)
}

// Test trigger - already running (simplified)
func TestTrigger_AlreadyRunning_Simple(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	server := New(ctx, ulogger.TestLogger{}, tSettings, memory.New(), nil)
	server.running = true

	err := server.trigger(ctx, "test")

	assert.NoError(t, err)
	assert.True(t, server.running) // Should remain true
}

// TestStart_FSMContextCancellation verifies graceful shutdown handling when
// the context is cancelled during the FSM wait. The error must be returned
// (not swallowed) and must be a context error so the service manager can
// distinguish it from a real failure.
func TestStart_FSMContextCancellation(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(context.Canceled)

	server := New(ctx, logger, tSettings, memory.New(), mockBlockchainClient)

	readyCh := make(chan struct{})
	err := server.Start(ctx, readyCh)

	require.Error(t, err)
	require.True(t, terrors.IsContextError(err), "expected context error, got %v", err)
	mockBlockchainClient.AssertExpectations(t)
}

// A loop that stopped beating longer ago than the timeout is reported as wedged,
// and only then: no timeout, or a heartbeat no loop has claimed, stays healthy
// (issue 1447).
func TestHealth_LivenessReportsStalledLoop(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		stallTimeout time.Duration
		lastBeatAgo  time.Duration // 0 = never beaten
		wantStatus   int
	}{
		{name: "stale beat past the timeout", stallTimeout: time.Minute, lastBeatAgo: 2 * time.Minute, wantStatus: http.StatusServiceUnavailable},
		{name: "fresh beat", stallTimeout: time.Minute, lastBeatAgo: time.Second, wantStatus: http.StatusOK},
		{name: "timeout off", stallTimeout: 0, lastBeatAgo: time.Hour, wantStatus: http.StatusOK},
		{name: "loop not started yet", stallTimeout: time.Minute, lastBeatAgo: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Block.UTXOPersisterLivenessStallTimeout = tt.stallTimeout

			server := New(ctx, ulogger.TestLogger{}, tSettings, memory.New(), nil)
			if tt.lastBeatAgo > 0 {
				server.heartbeat.SetLastBeatForTest(time.Now().Add(-tt.lastBeatAgo))
			}

			status, msg, err := server.Health(ctx, true)
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, status, msg)
		})
	}
}

// newLivenessTestServer starts a Server's main loop with a short idle tick and a
// liveness timeout, its best-block lookup answered by bestBlockHeader. It
// returns the server and a stop function that ends the loop and waits for Start
// to return.
func newLivenessTestServer(t *testing.T, stallTimeout time.Duration, bestBlockHeader func(mock.Arguments)) (*Server, func()) {
	t.Helper()

	return startLivenessTestServer(t, stallTimeout, 0, bestBlockHeader, nil)
}

// startLivenessTestServer is newLivenessTestServer with a poll interval (0 =
// the production default) and a best-block lookup that fails with
// bestBlockErr when it is not nil, so a pass returns at once and the loop
// goes back to idling in its select.
func startLivenessTestServer(t *testing.T, stallTimeout, pollInterval time.Duration, bestBlockHeader func(mock.Arguments), bestBlockErr error) (*Server, func()) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Block.UTXOPersisterLivenessStallTimeout = stallTimeout

	mockClient := &blockchain.Mock{}
	mockClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(nil)
	mockClient.On("Subscribe", mock.Anything, mock.Anything).Return(make(chan *blockchain.Notification), nil)
	mockClient.On("GetBestBlockHeader", mock.Anything).Run(bestBlockHeader).Return(createTestBlockHeader(), createTestBlockHeaderMeta(150), bestBlockErr)

	ctx, cancel := context.WithCancel(context.Background())

	server := New(ctx, ulogger.TestLogger{}, tSettings, memory.New(), mockClient)
	server.lastHeight = 100
	server.heartbeatInterval = 10 * time.Millisecond
	server.pollInterval = pollInterval

	done := make(chan error, 1)

	go func() { done <- server.Start(ctx, make(chan struct{})) }()

	stop := func() {
		cancel()

		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Start did not return after its context was cancelled")
		}
	}

	return server, stop
}

// An idle, synced persister is healthy: the startup pass finds the next block
// still waiting for 100 confirmations and waits a minute inside the loop's select
// case. That wait beats, so the probe never fires on a node with nothing to do.
// The stop then disables the heartbeat, so a graceful drain is not read as a stall.
func TestStart_IdleLoopStaysLiveAndStopDisablesHeartbeat(t *testing.T) {
	waiting := make(chan struct{}, 1)

	server, stop := newLivenessTestServer(t, 100*time.Millisecond, func(mock.Arguments) {
		select {
		case waiting <- struct{}{}:
		default:
		}
	})

	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("the startup pass never asked for the best block")
	}

	require.Never(t, func() bool {
		status, _, _ := server.Health(context.Background(), true)
		return status != http.StatusOK
	}, 500*time.Millisecond, 10*time.Millisecond, "a persister waiting for confirmations must stay live")

	require.NotZero(t, server.heartbeat.Age(), "the loop must have claimed the heartbeat")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusUTXOPersisterLivenessHeartbeatAge) > 0
	}, 5*time.Second, 10*time.Millisecond, "a running loop publishes its heartbeat age")

	stop()

	require.Zero(t, server.heartbeat.Age(), "a stopped loop must disable its heartbeat")

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusUTXOPersisterLivenessHeartbeatAge) == 0
	}, 5*time.Second, 10*time.Millisecond, "a stopped loop's gauge reads 0, not its last age")
}

// A pass that returns at once leaves the loop idling in its select, where only
// the idle tick beats. With nothing else to do for longer than the timeout, the
// probe must still answer OK.
func TestStart_IdleTickKeepsAnIdleLoopLive(t *testing.T) {
	asked := make(chan struct{}, 1)

	server, stop := startLivenessTestServer(t, 100*time.Millisecond, 0, func(mock.Arguments) {
		select {
		case asked <- struct{}{}:
		default:
		}
	}, terrors.NewServiceError("best block unavailable"))

	defer stop()

	select {
	case <-asked:
	case <-time.After(5 * time.Second):
		t.Fatal("the startup pass never asked for the best block")
	}

	require.Never(t, func() bool {
		status, _, _ := server.Health(context.Background(), true)
		return status != http.StatusOK
	}, 500*time.Millisecond, 10*time.Millisecond, "an idle loop must stay live on the idle tick alone")
}

// The poll timer fires even though the idle tick keeps waking the select. A
// time.After created on every pass would be restarted by each tick and never
// fire, so a persister without notifications would never poll again.
func TestStart_PollTimerFiresBetweenIdleTicks(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)

	_, stop := startLivenessTestServer(t, 0, 50*time.Millisecond, func(mock.Arguments) {
		mu.Lock()
		calls++
		mu.Unlock()
	}, terrors.NewServiceError("best block unavailable"))

	defer stop()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return calls >= 3
	}, 5*time.Second, 10*time.Millisecond, "the poll timer must run passes with no notification or trigger")
}

// The reproduction from issue 1447: a loop stuck inside a pass (here a
// best-block lookup that never returns) stops beating, and the probe reports
// 503 instead of answering OK forever.
func TestStart_WedgedLoopFailsLiveness(t *testing.T) {
	release := make(chan struct{})

	server, stop := newLivenessTestServer(t, 100*time.Millisecond, func(mock.Arguments) {
		<-release
	})

	defer stop()
	defer close(release)

	require.Eventually(t, func() bool {
		status, _, _ := server.Health(context.Background(), true)
		return status == http.StatusServiceUnavailable
	}, 5*time.Second, 10*time.Millisecond, "a wedged loop must fail liveness")
}

// restartTestChain stages three blocks of deltas and the set for block 1, the
// state a persister is in once it has recorded height 1. It returns a Server
// at lastHeight 1 whose best block is 3, so its next pass rolls blocks 2 and 3
// up into the set for block 3.
//
// block3Txs replaces block 3's single new output when given.
func restartTestChain(t *testing.T, store blob.Store, block3Txs ...*bt.Tx) (*Server, map[uint32]*chainhash.Hash) {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	headers, metas, byH := buildChainHeaders(t, tSettings.ChainCfgParams.GenesisHash, 3)

	stageBlockDeltas(t, ctx, tSettings, store, byH[1], 1, p2pkhTx(t, 0x11, 1000))
	stageBlockDeltas(t, ctx, tSettings, store, byH[2], 2, p2pkhTx(t, 0x22, 2000))
	if len(block3Txs) == 0 {
		block3Txs = []*bt.Tx{p2pkhTx(t, 0x33, 3000)}
	}

	stageBlockDeltas(t, ctx, tSettings, store, byH[3], 3, block3Txs...)

	mockClient := &blockchain.Mock{}
	mockClient.On("GetBestBlockHeader", mock.Anything).Return(headers[2], metas[2], nil)

	for _, r := range [][2]uint32{{1, 1}, {1, 3}, {2, 3}} {
		mockClient.On("GetBlockHeadersByHeight", mock.Anything, r[0], r[1]).Return(headers[r[0]-1:r[1]], metas[r[0]-1:r[1]], nil)
	}

	server := New(ctx, ulogger.TestLogger{}, tSettings, store, mockClient)

	require.NoError(t, server.BuildUTXOSetToHeight(ctx, 0, 1, true))
	require.NoError(t, server.Init(ctx))
	require.Equal(t, uint32(1), server.lastHeight)

	return server, byH
}

// A run that published the set for block 3 and then stopped before recording
// it (a crash, a deploy, a liveness restart) must not wedge the next run on
// "already exists": the next pass records the finished set and moves on.
func TestProcessNextBlock_RecordsASetAnEarlierRunPublished(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	server, byH := restartTestChain(t, store)

	// The earlier run: the set for block 3 is published, lastProcessed still says 1.
	require.NoError(t, server.BuildUTXOSetToHeight(ctx, 1, 3, false))

	_, err := server.processNextBlock(ctx)
	require.NoError(t, err)

	height, err := server.readLastHeight(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(3), height)

	exists, err := store.Exists(ctx, byH[1][:], fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.False(t, exists, "the previous set is deleted once the new one is recorded")

	require.Len(t, readSetUTXOs(t, ctx, server.settings, store, byH[3]), 3)
}

// delFailingStore fails every delete of a UTXO set, standing in for a run that
// stops after writing the new set and before the previous one is gone.
type delFailingStore struct {
	blob.Store
}

func (s *delFailingStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	if fileType == fileformat.FileTypeUtxoSet {
		return terrors.NewStorageError("injected delete failure")
	}

	return s.Store.Del(ctx, key, fileType, opts...)
}

// The new height is recorded before the previous set is deleted. Otherwise a
// stop between the two leaves lastProcessed pointing at a base set that is gone,
// and every later pass fails to verify it.
func TestProcessNextBlock_RecordsHeightBeforeDeletingPreviousSet(t *testing.T) {
	ctx := context.Background()
	store := &delFailingStore{Store: memory.New()}

	server, byH := restartTestChain(t, store)

	_, err := server.processNextBlock(ctx)
	require.Error(t, err, "the injected delete failure is still reported")

	height, err := server.readLastHeight(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(3), height, "the new set must be recorded even though the old one was not deleted")

	exists, err := store.Exists(ctx, byH[3][:], fileformat.FileTypeUtxoSet)
	require.NoError(t, err)
	require.True(t, exists)
}

// processNextBlock and BuildUTXOSetToHeight both hand the heartbeat to the
// consolidator, so a long roll-up renews a heartbeat the main loop owns. The
// heartbeat is armed an hour stale; the per-block beats must bring it back.
func TestRollUp_RenewsAnArmedHeartbeat(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		rollUp func(*Server) error
	}{
		{name: "processNextBlock", rollUp: func(s *Server) error {
			_, err := s.processNextBlock(ctx)
			return err
		}},
		{name: "BuildUTXOSetToHeight", rollUp: func(s *Server) error {
			return s.BuildUTXOSetToHeight(ctx, 1, 3, false)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := restartTestChain(t, memory.New())
			server.heartbeat.SetLastBeatForTest(time.Now().Add(-time.Hour))

			require.NoError(t, tt.rollUp(server))
			require.Less(t, server.heartbeat.Age(), time.Minute, "the roll-up must renew the heartbeat")
		})
	}
}

// Every record a roll-up and a set write handle counts toward a beat. With a
// beat on every record, the count is the sum of each counting step, so removing
// any one of them changes it.
func TestRollUpAndSetWrite_BeatPerRecord(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	// Block 3 spends block 1's output, so the roll-up reads a deletion and the
	// set write filters a record copied from the previous set.
	spend := p2pkhTx(t, 0x33, 3000)

	in := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x51}),
		SequenceNumber:     bt.DefaultSequenceNumber,
	}
	require.NoError(t, in.PreviousTxIDAdd(p2pkhTx(t, 0x11, 1000).TxIDChainHash()))
	spend.Inputs = append(spend.Inputs, in)

	server, byH := restartTestChain(t, store, spend)

	c := NewConsolidator(server.logger, server.settings, nil, server.blockchainClient, store, byH[1])
	c.beatEvery = 1

	beats := 0
	c.progress = func() { beats++ }

	require.NoError(t, c.ConsolidateBlockRange(ctx, 2, 3))

	// Two blocks, one deletion read, and one additions record per block.
	require.Equal(t, 2+1+2, beats, "roll-up beats")

	us, err := GetUTXOSet(ctx, server.logger, server.settings, store, byH[1], 1)
	require.NoError(t, err)

	beats = 0

	require.NoError(t, us.CreateUTXOSet(ctx, c))

	// One record copied from block 1's set, and the two additions.
	require.Equal(t, 1+2, beats, "set write beats")
}
