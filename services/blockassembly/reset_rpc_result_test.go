package blockassembly

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

func resetRPCs(ba *BlockAssembly) map[string]func(context.Context, *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	return map[string]func(context.Context, *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error){
		"normal":          ba.ResetBlockAssembly,
		"full":            ba.ResetBlockAssemblyFully,
		"validate_inputs": ba.ResetBlockAssemblyValidateInputs,
	}
}

func resetRPCServer(assembler *BlockAssembler) *BlockAssembly {
	return &BlockAssembly{blockAssembler: assembler, logger: ulogger.TestLogger{}, stats: gocore.NewStat("reset-rpc-result")}
}

type failingResetWaitProcessor struct {
	subtreeprocessor.Interface
	calls atomic.Int32
}

func (p *failingResetWaitProcessor) WaitForPendingBlocks(context.Context) error {
	return terrors.NewProcessingError("reset wait failure %d", p.calls.Add(1))
}

func TestResetRPCsReturnOperationError(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	failing := &failingResetWaitProcessor{Interface: assembler.subtreeProcessor}
	assembler.subtreeProcessor = failing
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	for name, rpc := range resetRPCs(resetRPCServer(assembler)) {
		t.Run(name, func(t *testing.T) {
			callCtx, cancelCall := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelCall()
			response, err := rpc(callCtx, &blockassembly_api.EmptyMessage{})
			require.ErrorContains(t, err, "[Reset] error waiting for pending blocks")
			require.Nil(t, response)
			require.Equal(t, terrors.NewProcessingError("expected").Code(), terrors.UnwrapGRPC(status.Convert(err).Err()).Code())
		})
	}
	require.EqualValues(t, 3, failing.calls.Load())
}

func TestResetRPCsReturnCallerCancellation(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	for name, rpc := range resetRPCs(resetRPCServer(assembler)) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			response, err := rpc(ctx, &blockassembly_api.EmptyMessage{})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, response)
		})
	}
	require.Empty(t, assembler.resetCh, "cancelled callers must not queue work")
}

func TestResetRPCQueuedCancellationSkipsReset(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := resetRPCServer(assembler).ResetBlockAssembly(ctx, &blockassembly_api.EmptyMessage{})
		result <- err
	}()
	require.Eventually(t, func() bool { return len(assembler.resetCh) == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued RPC did not return on cancellation")
	}
	request := <-assembler.resetCh
	err := request.run(t.Context())
	require.ErrorIs(t, err, context.Canceled, "cancelled queued reset must not execute")
	request.ErrCh <- err // The abandoned caller cannot block the listener's result send.
}

func TestResetRPCFullQueueCancellation(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	assembler.resetCh <- resetRequest{}
	assembler.resetCh <- resetRequest{}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := resetRPCServer(assembler).ResetBlockAssembly(ctx, &blockassembly_api.EmptyMessage{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, assembler.resetCh, 2)
}

func TestResetListenerRejectsMalformedQueuedRequest(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	reply := make(chan error, 1)
	assembler.resetCh <- resetRequest{ErrCh: reply}
	select {
	case err := <-reply:
		require.ErrorContains(t, err, "no operation")
	case <-time.After(time.Second):
		t.Fatal("malformed reset request stopped listener without a result")
	}
}

func TestResetRPCQueuedOptionsReceiveTheirOwnErrors(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	failing := &failingResetWaitProcessor{Interface: assembler.subtreeProcessor}
	assembler.subtreeProcessor = failing
	assembler.resetCh = make(chan resetRequest, 3)
	results := make(chan error, 3)
	for _, rpc := range resetRPCs(resetRPCServer(assembler)) {
		go func() {
			_, rpcErr := rpc(t.Context(), &blockassembly_api.EmptyMessage{})
			results <- rpcErr
		}()
	}
	require.Eventually(t, func() bool { return len(assembler.resetCh) == 3 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	for range 3 {
		select {
		case err := <-results:
			require.ErrorContains(t, err, "[Reset] error waiting for pending blocks", "no queued reset option may be acknowledged without execution")
		case <-time.After(time.Second):
			t.Fatal("queued reset did not receive a result")
		}
	}
	require.EqualValues(t, 3, failing.calls.Load())
}

func TestResetRPCListenerShutdownReleasesQueuedCaller(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	entered := make(chan struct{})
	assembler.resetCh <- resetRequest{run: func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}, ErrCh: make(chan error, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("listener did not start reset command")
	}
	result := make(chan error, 1)
	go func() {
		_, err := resetRPCServer(assembler).ResetBlockAssembly(t.Context(), &blockassembly_api.EmptyMessage{})
		result <- err
	}()
	require.Eventually(t, func() bool { return len(assembler.resetCh) == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued RPC did not return on listener shutdown")
	}
	assembler.wg.Wait()
	_, err := resetRPCServer(assembler).ResetBlockAssembly(t.Context(), &blockassembly_api.EmptyMessage{})
	require.ErrorIs(t, err, context.Canceled, "stopped listeners cannot accept a new reset")
}

func TestResetRPCsReturnCompletedSuccess(t *testing.T) {
	for _, name := range []string{"normal", "full", "validate_inputs"} {
		t.Run(name, func(t *testing.T) {
			assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(func() { cancel(); assembler.wg.Wait() })
			require.NoError(t, assembler.startChannelListeners(ctx))
			callCtx, cancelCall := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelCall()
			response, err := resetRPCs(resetRPCServer(assembler))[name](callCtx, &blockassembly_api.EmptyMessage{})
			require.NoError(t, err)
			require.NotNil(t, response)
			require.False(t, assembler.subtreeProcessor.RecoveryPending())
		})
	}
}

// Existing reset RPC tests also need a live dispatcher now that replies report completion.
func newResetRPCListeningServer(t *testing.T) *BlockAssembly {
	t.Helper()
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	return resetRPCServer(assembler)
}

func TestResetRPCAcceptedWorkSurvivesCallerCancellation(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	callCtx, cancelCall := context.WithCancel(t.Context())
	defer cancelCall()
	entered := make(chan struct{})
	release := make(chan struct{})
	request := newResetRequest(callCtx, func(workCtx context.Context) error {
		close(entered)
		select {
		case <-release:
		case <-workCtx.Done():
			return workCtx.Err()
		}
		return assembler.executeResetRequest(workCtx, false, false)
	})
	assembler.resetCh <- request
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); assembler.wg.Wait() })
	require.NoError(t, assembler.startChannelListeners(ctx))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reset was not accepted")
	}
	cancelCall()
	close(release)
	select {
	case err := <-request.ErrCh:
		require.NoError(t, err, "accepted reset must finish against real stores after RPC cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("accepted reset did not complete")
	}
}

// A failed consistency scan must not prevent the independent template reset.
type failedConsistencyScanStore struct{ utxo.Store }

func (s failedConsistencyScanStore) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	return nil, terrors.NewStorageError("temporary consistency scan failure")
}
func TestFullResetContinuesAfterConsistencyScanFailure(t *testing.T) {
	assembler, _ := newUnminedRecoveryTestAssembler(t, blockchain.FSMStateRUNNING)
	hash := storeSuppressedRecoveryTransaction(t, assembler)
	assembler.utxoStore = failedConsistencyScanStore{assembler.utxoStore}
	require.NoError(t, assembler.executeResetRequest(t.Context(), true, false))
	require.Contains(t, recoveryCandidateHashes(t, assembler), hash)
}
