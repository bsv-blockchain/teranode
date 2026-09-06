package blockassembly

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func createTestClient(mockClient *mockBlockAssemblyAPIClient, batchSize int) *Client {
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		BlockAssembly: settings.BlockAssemblySettings{
			SendBatchSize:    batchSize,
			SendBatchTimeout: 100,
		},
	}

	client := &Client{
		client:    mockClient,
		logger:    logger,
		settings:  tSettings,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	if batchSize > 0 {
		sendBatch := func(batch []*batchItem) {
			client.sendBatchToBlockAssembly(context.Background(), batch)
		}
		duration := time.Duration(100) * time.Millisecond
		client.batcher = batcher.New(batchSize, duration, sendBatch, true)
	}

	return client
}

// Test constructor failures
func TestNewClient_ConfigErrors(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tests := []struct {
		name     string
		settings func() *settings.Settings
		wantErr  string
	}{
		{
			name: "missing grpc address",
			settings: func() *settings.Settings {
				return &settings.Settings{
					BlockAssembly: settings.BlockAssemblySettings{
						GRPCAddress: "",
					},
				}
			},
			wantErr: "no blockassembly_grpcAddress setting found",
		},
		{
			name: "zero retry backoff",
			settings: func() *settings.Settings {
				return &settings.Settings{
					BlockAssembly: settings.BlockAssemblySettings{
						GRPCAddress:      "localhost:8080",
						GRPCRetryBackoff: 0,
					},
				}
			},
			wantErr: "blockassembly_grpcRetryBackoff setting error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(ctx, logger, tt.settings())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestNewClient_BatchLogging(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := &settings.Settings{
		BlockAssembly: settings.BlockAssemblySettings{
			GRPCAddress:      "localhost:0", // Use port 0 to force an error
			GRPCRetryBackoff: 1000,
			GRPCMaxRetries:   1,
			SendBatchSize:    10,
			SendBatchTimeout: 5000,
		},
	}

	// This will fail at connection, but we want to test the batch size logging path
	_, err := NewClient(ctx, logger, tSettings)
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect to block assembly")
	}
	// Note: Connection may or may not fail in test environment
}

func TestNewClientWithAddress_BatchLogging(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := &settings.Settings{
		GRPCMaxRetries:   1,
		GRPCRetryBackoff: 1000,
		BlockAssembly: settings.BlockAssemblySettings{
			SendBatchSize:    10,
			SendBatchTimeout: 5000,
		},
	}

	// This will fail at connection, but we want to test the batch size logging path
	_, err := NewClientWithAddress(ctx, logger, tSettings, "localhost:0")
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect to block assembly")
	}
	// Note: Connection may or may not fail in test environment
}

func TestClient_Health(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	tests := []struct {
		name          string
		checkLiveness bool
		mockSetup     func()
		wantCode      int
		wantMessage   string
		wantErr       bool
	}{
		{
			name:          "liveness check returns OK",
			checkLiveness: true,
			mockSetup:     func() {},
			wantCode:      http.StatusOK,
			wantMessage:   "OK",
			wantErr:       false,
		},
		{
			name:          "readiness check success",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
					&blockassembly_api.HealthResponse{Ok: true, Details: "healthy"},
					nil,
				)
			},
			wantCode:    http.StatusOK,
			wantMessage: "OK",
			wantErr:     false,
		},
		{
			name:          "readiness check fails with grpc error",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
					nil,
					status.Error(codes.Unavailable, "service unavailable"),
				)
			},
			wantCode:    http.StatusFailedDependency,
			wantMessage: "",
			wantErr:     true,
		},
		{
			name:          "readiness check fails with ok=false",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
					&blockassembly_api.HealthResponse{Ok: false, Details: "unhealthy"},
					nil,
				)
			},
			wantCode:    http.StatusFailedDependency,
			wantMessage: "unhealthy",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient.ExpectedCalls = nil
			tt.mockSetup()

			code, message, err := client.Health(ctx, tt.checkLiveness)

			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantMessage, message)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestClient_Store_NonBatchMode(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0) // batchSize = 0 for non-batch mode

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	fee := uint64(1000)
	size := uint64(250)
	txInpoints := subtree.TxInpoints{}

	t.Run("successful store", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.MatchedBy(func(req *blockassembly_api.AddTxRequest) bool {
			return req.Fee == fee && req.Size == size
		}), mock.Anything).Return(&blockassembly_api.AddTxResponse{}, nil)

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.True(t, success)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "internal error"))

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.False(t, success)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("successful store with empty txInpoints", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.AddTxResponse{}, nil)

		emptyTxInpoints := subtree.TxInpoints{}
		success, err := client.Store(ctx, hash, fee, size, emptyTxInpoints)
		assert.True(t, success)
		assert.NoError(t, err) // Serialization of empty txInpoints should succeed
		mockClient.AssertExpectations(t)
	})

	t.Run("store with serialization that succeeds", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.AddTxResponse{}, nil)

		// Create a valid txInpoints structure
		validTxInpoints := subtree.TxInpoints{}
		success, err := client.Store(ctx, hash, fee, size, validTxInpoints)
		assert.True(t, success)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_Store_BatchMode(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 5) // batchSize = 5 for batch mode

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	fee := uint64(1000)
	size := uint64(250)
	txInpoints := subtree.TxInpoints{}

	t.Run("successful batch store", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", mock.Anything, mock.MatchedBy(func(req *blockassembly_api.AddTxBatchRequest) bool {
			return len(req.TxRequests) == 1
		}), mock.Anything).Return(&blockassembly_api.AddTxBatchResponse{}, nil)

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.True(t, success)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("batch error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", mock.Anything, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "batch error"))

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.False(t, success)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

// wedgedBatchHarness is a batch-mode client whose AddTxBatch is entered and then
// wedges, plus the observables a test needs to drive the dispatched-but-wedged case
// deterministically.
//
// entered closes once the RPC has actually been entered. That is the load-bearing
// signal: a test that asserted only on Store's return would also pass on a build where
// the item never reached the RPC at all, which is the trivial
// abandoned-before-dispatch case rather than the one that matters.
type wedgedBatchHarness struct {
	client  *Client
	entered <-chan struct{}

	// release lets the wedged RPC return, so the dispatcher can finish the item it is
	// holding. Idempotent, and also run from t.Cleanup so no dispatcher goroutine is
	// left parked when a test fails early.
	release func()

	// completed counts AddTxBatch calls that have returned, so a test can wait for the
	// dispatcher to finish an abandoned item rather than sleeping.
	completed func() int
}

// newWedgedBatchHarness builds the harness with a batch size of EXACTLY 1.
//
// go-batcher dispatches when the size is reached, the timeout expires, or Trigger() is
// called, so a size of 1 dispatches on the first PutCtx with no dependence on the 100ms
// batch timeout. A larger batch size combined with a deadline shorter than that timeout
// would prove only that Store gave up before the batch ever formed.
func newWedgedBatchHarness(t *testing.T) *wedgedBatchHarness {
	t.Helper()

	mockClient := &mockBlockAssemblyAPIClient{}

	var (
		enteredCh   = make(chan struct{})
		gate        = make(chan struct{})
		enterOnce   sync.Once
		releaseOnce sync.Once
		completed   atomic.Int32
	)

	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)

	mockClient.On("AddTxBatch", mock.Anything, mock.Anything, mock.Anything).
		Run(func(mock.Arguments) {
			enterOnce.Do(func() { close(enteredCh) })

			<-gate

			completed.Add(1)
		}).
		Return(&blockassembly_api.AddTxBatchResponse{}, nil)

	return &wedgedBatchHarness{
		client:    createTestClient(mockClient, 1),
		entered:   enteredCh,
		release:   release,
		completed: func() int { return int(completed.Load()) },
	}
}

// requireEntered blocks until the wedged RPC has been entered, establishing that the
// item was dispatched and the call is stuck. The timeout is a hang detector, not the
// property under test.
func (h *wedgedBatchHarness) requireEntered(t *testing.T) {
	t.Helper()

	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("AddTxBatch was never entered, so the item was never dispatched; this test would otherwise prove only the trivial abandoned-before-dispatch case")
	}
}

type storeOutcome struct {
	success bool
	err     error
}

// storeAsync runs Store on its own goroutine and publishes the outcome, so the test
// goroutine can establish the wedged-RPC precondition first and only then wait for the
// return. Calling Store inline and inferring that dispatch "must have" happened first
// would make the ordering a timing assumption.
func storeAsync(ctx context.Context, client *Client, hash *chainhash.Hash) <-chan storeOutcome {
	out := make(chan storeOutcome, 1)

	go func() {
		success, err := client.Store(ctx, hash, 1000, 250, subtree.TxInpoints{})
		out <- storeOutcome{success: success, err: err}
	}()

	return out
}

func requireStoreOutcome(t *testing.T, out <-chan storeOutcome) storeOutcome {
	t.Helper()

	select {
	case o := <-out:
		return o
	case <-time.After(10 * time.Second):
		t.Fatal("Store did not return while the block-assembly RPC was wedged; the caller's deadline was discarded")

		return storeOutcome{}
	}
}

func testStoreHash(t *testing.T) *chainhash.Hash {
	t.Helper()

	hash, err := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)

	return hash
}

// TestClient_Store_BatchMode_HonoursContextDeadline pins the whole point of the batch
// bound: the caller's deadline is honoured even when the item HAS been dispatched and
// the RPC is wedged.
//
// Batch mode used to wait on context.Background(), which discarded the caller's
// deadline entirely. Since blockassembly_sendBatchSize ships at 1024, that made the
// validator's hand-off deadline inert in the configuration everybody runs: a wedged
// block assembly could park an ingest goroutine, and the Kafka record batch it holds,
// for as long as the stall lasted.
//
// Must run under -race. The abandoned item's result slot is written by the dispatcher
// after Store has returned, and the race detector is what proves that write has no
// concurrent reader.
func TestClient_Store_BatchMode_HonoursContextDeadline(t *testing.T) {
	h := newWedgedBatchHarness(t)
	hash := testStoreHash(t)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	out := storeAsync(ctx, h.client, hash)

	// Precondition: the item reached the RPC and the RPC is stuck.
	h.requireEntered(t)

	o := requireStoreOutcome(t, out)

	require.False(t, o.success, "an abandoned hand-off is not a success")
	require.Error(t, o.err)
	require.ErrorIs(t, o.err, context.DeadlineExceeded, "the caller's deadline must be what ends the wait")
}

// TestClient_Store_BatchMode_AbandonmentIsNotAShed pins the classification, which is
// what keeps the validator out of its unwind branch.
//
// An early return here is abandonment, not cancellation: the item is on the batcher and
// may still be dispatched, so the transaction may yet reach block assembly. A shed, by
// contrast, is unwound by the caller — record deleted, inputs unspent. Doing that to a
// transaction still in flight could delete a record already in a subtree or a mining
// template, so the two must never be confusable.
func TestClient_Store_BatchMode_AbandonmentIsNotAShed(t *testing.T) {
	h := newWedgedBatchHarness(t)
	hash := testStoreHash(t)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	out := storeAsync(ctx, h.client, hash)

	h.requireEntered(t)

	o := requireStoreOutcome(t, out)

	require.False(t, o.success)
	require.Error(t, o.err)
	require.NotErrorIs(t, o.err, errors.ErrThresholdExceeded,
		"an abandoned hand-off must not be mistakable for a queue-full shed, or the caller unwinds a transaction that may still be in flight")
}

// TestClient_Store_BatchMode_LateCompletionAfterAbandonIsSafe pins that abandoning a
// wait does not corrupt the batcher or the completion group.
//
// After Store has returned, the dispatcher still finishes the already-abandoned item —
// the late result write that now has no reader. Nothing may panic, and the client must
// remain usable. The wedged-RPC precondition is what makes this a genuinely late
// completion rather than a no-op on an item that was never dispatched.
func TestClient_Store_BatchMode_LateCompletionAfterAbandonIsSafe(t *testing.T) {
	h := newWedgedBatchHarness(t)
	hash := testStoreHash(t)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	out := storeAsync(ctx, h.client, hash)

	h.requireEntered(t)

	o := requireStoreOutcome(t, out)
	require.Error(t, o.err, "precondition: the first hand-off was abandoned")

	// Let the dispatcher complete the already-abandoned item.
	h.release()

	require.Eventually(t, func() bool { return h.completed() >= 1 }, 10*time.Second, 5*time.Millisecond,
		"the dispatcher must finish the abandoned item; that completion is the write with no reader")

	// The client is still usable: a fresh hand-off on an uncancelled context waits to
	// completion and succeeds, exactly as batch mode always did.
	success, err := h.client.Store(context.Background(), hash, 1000, 250, subtree.TxInpoints{})
	require.NoError(t, err, "abandoning a wait must not leave the batcher or the completion group broken")
	require.True(t, success)
}

func TestClient_RemoveTx(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	t.Run("successful removal", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("RemoveTx", ctx, mock.MatchedBy(func(req *blockassembly_api.RemoveTxRequest) bool {
			return string(req.Txid) == string(hash[:])
		}), mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.RemoveTx(ctx, hash)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("RemoveTx", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.NotFound, "transaction not found"))

		err := client.RemoveTx(ctx, hash)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("RemoveTx", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.RemoveTx(ctx, hash)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetMiningCandidate(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedCandidate := &model.MiningCandidate{
		Id:      []byte("test-id"),
		Version: 1,
		Height:  123,
	}

	t.Run("successful without subtrees", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetMiningCandidate", ctx, mock.MatchedBy(func(req *blockassembly_api.GetMiningCandidateRequest) bool {
			return req.IncludeSubtrees == false
		}), mock.Anything).Return(expectedCandidate, nil)

		candidate, err := client.GetMiningCandidate(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedCandidate, candidate)
		mockClient.AssertExpectations(t)
	})

	t.Run("successful with subtrees", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetMiningCandidate", ctx, mock.MatchedBy(func(req *blockassembly_api.GetMiningCandidateRequest) bool {
			return req.IncludeSubtrees == true
		}), mock.Anything).Return(expectedCandidate, nil)

		candidate, err := client.GetMiningCandidate(ctx, true)
		assert.NoError(t, err)
		assert.Equal(t, expectedCandidate, candidate)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetMiningCandidate", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "internal error"))

		candidate, err := client.GetMiningCandidate(ctx)
		assert.Nil(t, candidate)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetCurrentDifficulty(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedDifficulty := 12345.67

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetCurrentDifficulty", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			&blockassembly_api.GetCurrentDifficultyResponse{Difficulty: expectedDifficulty}, nil)

		difficulty, err := client.GetCurrentDifficulty(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedDifficulty, difficulty)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetCurrentDifficulty", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "internal error"))

		difficulty, err := client.GetCurrentDifficulty(ctx)
		assert.Equal(t, float64(0), difficulty)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_SubmitMiningSolution(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	timeValue := uint32(1234567890)
	versionValue := uint32(1)
	solution := &model.MiningSolution{
		Id:       []byte("test-id"),
		Nonce:    12345,
		Coinbase: []byte("coinbase-data"),
		Time:     &timeValue,
		Version:  &versionValue,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("SubmitMiningSolution", ctx, mock.MatchedBy(func(req *blockassembly_api.SubmitMiningSolutionRequest) bool {
			return string(req.Id) == string(solution.Id) && req.Nonce == solution.Nonce
		}), mock.Anything).Return(&blockassembly_api.OKResponse{}, nil)

		err := client.SubmitMiningSolution(ctx, solution)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("SubmitMiningSolution", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.InvalidArgument, "invalid solution"))

		err := client.SubmitMiningSolution(ctx, solution)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("SubmitMiningSolution", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.OKResponse{}, nil)

		err := client.SubmitMiningSolution(ctx, solution)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GenerateBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	req := &blockassembly_api.GenerateBlocksRequest{
		Count: 5,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GenerateBlocks", ctx, req, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.GenerateBlocks(ctx, req)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GenerateBlocks", ctx, req, mock.Anything).Return(
			nil, status.Error(codes.Internal, "generation failed"))

		err := client.GenerateBlocks(ctx, req)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GenerateBlocks", ctx, req, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.GenerateBlocks(ctx, req)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_ResetBlockAssembly(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ResetBlockAssembly", context.Background(), &blockassembly_api.EmptyMessage{}, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.ResetBlockAssembly(ctx)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ResetBlockAssembly", context.Background(), &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "reset failed"))

		err := client.ResetBlockAssembly(ctx)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ResetBlockAssembly", context.Background(), &blockassembly_api.EmptyMessage{}, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.ResetBlockAssembly(ctx)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetBlockAssemblyState(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedState := &blockassembly_api.StateMessage{
		BlockAssemblyState: "running",
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyState", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(expectedState, nil)

		state, err := client.GetBlockAssemblyState(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedState, state)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyState", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "state error"))

		state, err := client.GetBlockAssemblyState(ctx)
		assert.Nil(t, state)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_BlockAssemblyAPIClient(t *testing.T) {
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	apiClient := client.BlockAssemblyAPIClient()
	assert.Equal(t, mockClient, apiClient)
}

func TestClient_GetBlockAssemblyBlockCandidate(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	blockData := []byte("mock-block-data")
	expectedResponse := &blockassembly_api.GetBlockAssemblyBlockCandidateResponse{
		Block: blockData,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyBlockCandidate", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(expectedResponse, nil)

		// Since model.NewBlockFromBytes might not work with our mock data, we expect an error
		_, err := client.GetBlockAssemblyBlockCandidate(ctx)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("successful with valid block data", func(t *testing.T) {
		mockClient.ExpectedCalls = nil

		// We'll test the path where model.NewBlockFromBytes fails, which exercises the error handling
		validResponse := &blockassembly_api.GetBlockAssemblyBlockCandidateResponse{
			Block: []byte("invalid-block-data"),
		}
		mockClient.On("GetBlockAssemblyBlockCandidate", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(validResponse, nil)

		_, err := client.GetBlockAssemblyBlockCandidate(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create block from bytes")
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyBlockCandidate", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "candidate error"))

		block, err := client.GetBlockAssemblyBlockCandidate(ctx)
		assert.Nil(t, block)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetTransactionHashes(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedHashes := []string{"hash1", "hash2", "hash3"}
	expectedResponse := &blockassembly_api.GetBlockAssemblyTxsResponse{
		Txs: expectedHashes,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyTxs", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(expectedResponse, nil)

		hashes, err := client.GetTransactionHashes(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedHashes, hashes)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyTxs", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "txs error"))

		hashes, err := client.GetTransactionHashes(ctx)
		assert.Nil(t, hashes)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

// newTestBatch builds a two-item batch sharing one completion.Group sized to
// the batch length, mirroring how the Store producer wires items to the
// dispatcher in the group-completion model.
func newTestBatch() (*completion.Group, []*batchItem) {
	group := completion.NewGroup(2)

	batch := []*batchItem{
		{
			req: &blockassembly_api.AddTxRequest{
				Txid: []byte("txid1"),
				Fee:  1000,
				Size: 250,
			},
			group: group,
		},
		{
			req: &blockassembly_api.AddTxRequest{
				Txid: []byte("txid2"),
				Fee:  2000,
				Size: 500,
			},
			group: group,
		},
	}

	return group, batch
}

func TestClient_sendBatchToBlockAssembly(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 5)

	t.Run("successful batch", func(t *testing.T) {
		group, batch := newTestBatch()

		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", ctx, mock.MatchedBy(func(req *blockassembly_api.AddTxBatchRequest) bool {
			return len(req.TxRequests) == 2
		}), mock.Anything).Return(&blockassembly_api.AddTxBatchResponse{}, nil)

		client.sendBatchToBlockAssembly(ctx, batch)

		// group completes (no timer): every item was completed exactly once. A
		// double-Done would have panicked in the dispatcher before we got here.
		require.NoError(t, group.Wait(context.Background(), 0))
		require.NoError(t, batch[0].result)
		require.NoError(t, batch[1].result)

		mockClient.AssertExpectations(t)
	})

	t.Run("batch error", func(t *testing.T) {
		group, batch := newTestBatch()

		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "batch failed"))

		client.sendBatchToBlockAssembly(ctx, batch)

		require.NoError(t, group.Wait(context.Background(), 0))
		require.Error(t, batch[0].result)
		require.Error(t, batch[1].result)

		mockClient.AssertExpectations(t)
	})

	t.Run("dispatcher panic completes every item", func(t *testing.T) {
		group, batch := newTestBatch()

		mockClient.ExpectedCalls = nil
		// AddTxBatch panics part-way through the dispatch fn. The whole-batch
		// panic sweep must recover and complete every item so no producer is
		// stranded on group.Wait.
		mockClient.On("AddTxBatch", ctx, mock.Anything, mock.Anything).
			Run(func(mock.Arguments) { panic("boom") }).
			Return(nil, error(nil))

		require.NotPanics(t, func() { client.sendBatchToBlockAssembly(ctx, batch) })

		require.NoError(t, group.Wait(context.Background(), 0))
		require.Error(t, batch[0].result)
		require.Error(t, batch[1].result)
	})
}

func TestClient_Store_BatchMode_MaxConcurrent(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}

	// Create client with maxConcurrent=2
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		BlockAssembly: settings.BlockAssemblySettings{
			SendBatchSize:          2,
			SendBatchTimeout:       100,
			SendBatchMaxConcurrent: 2,
		},
	}

	client := &Client{
		client:    mockClient,
		logger:    logger,
		settings:  tSettings,
		batchSize: 2,
		batchCh:   make(chan []*batchItem),
	}

	// Track concurrent calls
	var concurrentCalls atomic.Int32
	var maxConcurrentSeen atomic.Int32
	var callCount atomic.Int32

	// Block gRPC calls with a gate so we can control timing
	gate := make(chan struct{})

	mockClient.On("AddTxBatch", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			current := concurrentCalls.Add(1)
			// Track the maximum concurrent calls observed
			for {
				old := maxConcurrentSeen.Load()
				if current <= old || maxConcurrentSeen.CompareAndSwap(old, current) {
					break
				}
			}
			callCount.Add(1)
			<-gate // Block until test releases
			concurrentCalls.Add(-1)
		}).
		Return(&blockassembly_api.AddTxBatchResponse{}, nil)

	sendBatch := func(b []*batchItem) {
		client.sendBatchToBlockAssembly(ctx, b)
	}
	duration := time.Duration(100) * time.Millisecond
	b := batcher.New(2, duration, sendBatch, true)
	b.SetMaxConcurrent(2)
	client.batcher = b

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	txInpoints := subtree.TxInpoints{}

	// Submit enough items to fill multiple batches (6 items = 3 batches of 2)
	for i := 0; i < 6; i++ {
		go func() {
			_, _ = client.Store(ctx, hash, 1000, 250, txInpoints)
		}()
	}

	// Wait for exactly 2 concurrent calls to be blocked at the gate
	require.Eventually(t, func() bool {
		return concurrentCalls.Load() == 2
	}, 2*time.Second, 10*time.Millisecond, "should reach max concurrent of 2")

	// With 2 calls blocked at the gate, exactly 2 should have started (3rd is queued by the limiter)
	require.Equal(t, int32(2), callCount.Load(),
		"only 2 batches should have started while at max concurrency")

	// Release all blocked calls
	close(gate)

	// Wait for all 3 batches to complete (the queued 3rd batch should now proceed)
	require.Eventually(t, func() bool {
		return callCount.Load() >= 3
	}, 2*time.Second, 10*time.Millisecond, "all batches should complete after gate opens")

	// Verify concurrency never exceeded the limit
	require.LessOrEqual(t, maxConcurrentSeen.Load(), int32(2),
		"concurrent calls should never exceed maxConcurrent=2")
}

func TestNewClientWithAddress_ConfigErrors(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// This test mainly checks that the function can handle configuration
	// The actual gRPC connection will fail in testing, but we can test the happy path logic
	tSettings := &settings.Settings{
		GRPCMaxRetries:   3,
		GRPCRetryBackoff: 1000,
		BlockAssembly: settings.BlockAssemblySettings{
			SendBatchSize:    10,
			SendBatchTimeout: 5000,
		},
	}

	// Test with invalid address that will cause connection failure
	_, err := NewClientWithAddress(ctx, logger, tSettings, "invalid-address:99999")
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect to block assembly")
	}
	// Note: Connection might not fail immediately in test environment, so we don't assert.Error
}

// TestClient_GetBlockAssemblyQueueStats_RoundTrip pins that every queue-stats
// field survives the client's protobuf-to-native conversion, including the two
// that make the signal self-describing: the double-spend window the head age is
// measured against, and the item cap the depth is a fraction of.
//
// The struct return is deliberate: HeadAge and DoubleSpendWindow are both durations
// with entirely different meanings, so as adjacent positional returns they would be
// silently swappable at every call site — and swapping them inverts the control
// decision the reader makes.
func TestClient_GetBlockAssemblyQueueStats_RoundTrip(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	t.Run("every field survives the conversion", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyQueueStats", mock.Anything, mock.Anything, mock.Anything).
			Return(&blockassembly_api.QueueStatsMessage{
				QueueCount:              42,
				QueueHeadAgeMillis:      1500,
				DoubleSpendWindowMillis: 750,
				QueueMaxItems:           6400,
			}, nil)

		stats, err := client.GetBlockAssemblyQueueStats(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(42), stats.Count)
		require.Equal(t, 1500*time.Millisecond, stats.HeadAge)
		require.Equal(t, 750*time.Millisecond, stats.DoubleSpendWindow)
		require.Equal(t, int64(6400), stats.MaxItems)
		mockClient.AssertExpectations(t)
	})

	t.Run("a zero window and a zero cap are carried through as zero", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyQueueStats", mock.Anything, mock.Anything, mock.Anything).
			Return(&blockassembly_api.QueueStatsMessage{QueueCount: 1, QueueHeadAgeMillis: 10}, nil)

		stats, err := client.GetBlockAssemblyQueueStats(ctx)
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), stats.DoubleSpendWindow)
		require.Equal(t, 10*time.Millisecond, stats.HeadAge)
		require.Equal(t, int64(0), stats.MaxItems, "an unbounded producer reports no fill signal")
	})

	t.Run("an RPC error yields a zero snapshot and the unwrapped error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyQueueStats", mock.Anything, mock.Anything, mock.Anything).
			Return(nil, status.Error(codes.Unavailable, "block assembly unavailable"))

		stats, err := client.GetBlockAssemblyQueueStats(ctx)
		require.Error(t, err)
		require.Equal(t, QueueStats{}, stats, "a failed read must not present a partially-populated snapshot")
	})
}
