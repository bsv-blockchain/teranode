package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/stretchr/testify/require"
)

// newServerForChanTest builds a BlockAssembly with a non-nil (zero-value)
// block assembler so SubmitMiningSolution passes its readiness guards, but
// without starting the real Init listener — letting each test control the
// blockSubmissionChan consumer (or lack of one) deterministically.
func newServerForChanTest(t *testing.T) (*BlockAssembly, *testutil.CommonTestSetup) {
	t.Helper()

	common := testutil.NewCommonTestSetup(t)
	s := New(common.Logger, common.Settings, nil, nil, nil, nil)
	s.blockAssembler = &BlockAssembler{} // zero value: not loading, non-nil

	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	return s, common
}

// TestSubmitMiningSolution_BeforeInit_FailsFast verifies that calling
// SubmitMiningSolution before Init (no block assembler) returns immediately
// rather than blocking on a channel that has no receiver.
func TestSubmitMiningSolution_BeforeInit_FailsFast(t *testing.T) {
	common := testutil.NewCommonTestSetup(t)
	s := New(common.Logger, common.Settings, nil, nil, nil, nil)
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	done := make(chan error, 1)
	go func() {
		_, err := s.SubmitMiningSolution(context.Background(), &blockassembly_api.SubmitMiningSolutionRequest{Id: make([]byte, 32)})
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitMiningSolution blocked before Init instead of failing fast")
	}
}

// TestSubmitMiningSolution_Send_ContextCancelled verifies that, with no
// listener ready to receive, a cancelled caller context aborts the send to
// blockSubmissionChan instead of blocking the gRPC handler forever.
func TestSubmitMiningSolution_Send_ContextCancelled(t *testing.T) {
	s, _ := newServerForChanTest(t)

	// No consumer of blockSubmissionChan and listenerDone is open, so the only
	// ready select case is the cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{Id: make([]byte, 32)})
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitMiningSolution did not abort the send on context cancellation")
	}
}

// TestSubmitMiningSolution_ListenerStopped_FailsFast verifies that once the
// real submission listener has exited (service context cancelled), a new
// submission fails fast even with a healthy caller context.
func TestSubmitMiningSolution_ListenerStopped_FailsFast(t *testing.T) {
	common := testutil.NewCommonTestSetup(t)
	subtreeStore := testutil.NewMemoryBlobStore()

	ctx, cancel := context.WithCancel(common.Ctx)

	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	s := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
	s.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, s.Init(ctx))
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// Stop the listener by cancelling the service context and wait for it to exit.
	cancel()
	select {
	case <-s.blockSubmissionListenerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("block submission listener did not stop after context cancellation")
	}

	// Caller context is healthy; the failure must come from the stopped listener.
	done := make(chan error, 1)
	go func() {
		_, err := s.SubmitMiningSolution(context.Background(), &blockassembly_api.SubmitMiningSolutionRequest{Id: make([]byte, 32)})
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitMiningSolution blocked after listener stopped instead of failing fast")
	}
}

// TestSubmitMiningSolution_ResponseWait_ContextCancelled verifies that, when
// waiting for a response is enabled and the submission is stuck (listener
// received it but does not respond), a cancelled caller context unblocks the
// handler instead of leaving it waiting on responseChan forever.
func TestSubmitMiningSolution_ResponseWait_ContextCancelled(t *testing.T) {
	s, common := newServerForChanTest(t)
	common.Settings.BlockAssembly.SubmitMiningSolutionWaitForResponse = true

	// Fake listener: receive the request but deliberately never respond,
	// simulating a stuck/slow submission.
	received := make(chan struct{})
	go func() {
		<-s.blockSubmissionChan
		close(received)
		// intentionally never send on responseChan
	}()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{Id: make([]byte, 32)})
		done <- err
	}()

	// Ensure the send succeeded and the handler is now waiting on responseChan.
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not receive the submission")
	}

	cancel() // cancel while waiting for the response

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitMiningSolution did not return after context cancellation while waiting for response")
	}
}

// TestSubmitMiningSolution_ResponseWait_Success verifies the normal path: a
// submission flows through to the listener and a successful response is
// returned to the caller.
func TestSubmitMiningSolution_ResponseWait_Success(t *testing.T) {
	s, common := newServerForChanTest(t)
	common.Settings.BlockAssembly.SubmitMiningSolutionWaitForResponse = true

	// Fake listener mirroring runBlockSubmissionListener's response handling.
	go func() {
		req := <-s.blockSubmissionChan
		if req.responseChan != nil {
			req.responseChan <- nil
		}
	}()

	done := make(chan struct {
		resp *blockassembly_api.OKResponse
		err  error
	}, 1)
	go func() {
		resp, err := s.SubmitMiningSolution(context.Background(), &blockassembly_api.SubmitMiningSolutionRequest{Id: make([]byte, 32)})
		done <- struct {
			resp *blockassembly_api.OKResponse
			err  error
		}{resp, err}
	}()

	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.NotNil(t, got.resp)
		require.True(t, got.resp.Ok)
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitMiningSolution did not complete a successful submission")
	}
}
