package blockassembly

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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

// p2shLockingScript is the exact 23-byte pay-to-script-hash shape bitcoin-sv's
// IsP2SH tests: OP_HASH160 (0xa9) <20-byte push (0x14)> OP_EQUAL (0x87).
const p2shLockingScriptHex = "a914000000000000000000000000000000000000000087"

// TestCoinbaseHasP2SHOutput pins the helper to the exact svnode IsP2SH shape:
// only a 23-byte OP_HASH160 <20-byte push> OP_EQUAL script matches; near-miss
// shapes (wrong length, wrong leading/trailing opcode) must not.
func TestCoinbaseHasP2SHOutput(t *testing.T) {
	mkTx := func(scripts ...[]byte) *bt.Tx {
		tx := bt.NewTx()
		for _, s := range scripts {
			ls := bscript.Script(s)
			tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 0, LockingScript: &ls})
		}

		return tx
	}

	p2sh, err := hex.DecodeString(p2shLockingScriptHex)
	require.NoError(t, err)

	p2pkh, err := hex.DecodeString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	wrongLeadingOp := append([]byte{}, p2sh...)
	wrongLeadingOp[0] = 0xa8 // OP_SHA256 instead of OP_HASH160

	wrongTrailingOp := append([]byte{}, p2sh...)
	wrongTrailingOp[22] = 0x88 // OP_EQUALVERIFY instead of OP_EQUAL

	wrongPushLen := append([]byte{}, p2sh...)
	wrongPushLen[1] = 0x13 // 19-byte push marker (still 23 bytes long)

	tests := []struct {
		name string
		tx   *bt.Tx
		want bool
	}{
		{"exact P2SH shape", mkTx(p2sh), true},
		{"P2SH among other outputs", mkTx(p2pkh, p2sh), true},
		{"P2PKH only", mkTx(p2pkh), false},
		{"truncated (22 bytes)", mkTx(p2sh[:22]), false},
		{"extended (24 bytes)", mkTx(append(append([]byte{}, p2sh...), 0x00)), false},
		{"wrong leading opcode", mkTx(wrongLeadingOp), false},
		{"wrong trailing opcode", mkTx(wrongTrailingOp), false},
		{"wrong push length marker", mkTx(wrongPushLen), false},
		{"empty script", mkTx([]byte{}), false},
		{"no outputs", bt.NewTx(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, coinbaseHasP2SHOutput(tt.tx))
		})
	}

	t.Run("nil locking script", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 0, LockingScript: nil})
		require.False(t, coinbaseHasP2SHOutput(tx))
	})

	t.Run("nil tx", func(t *testing.T) {
		require.False(t, coinbaseHasP2SHOutput(nil))
	})
}

// TestValidateCoinbaseForSubmission unit-tests the convergence point both
// submitMiningSolution branches (pool-supplied and recreated coinbase) must go
// through. The recreated branch cannot produce a P2SH output through public
// configuration (payout addresses derive from wallet public keys, always
// P2PKH), so this direct test is what pins the guard for that branch: any
// refactor that stops routing a branch through validateCoinbaseForSubmission
// must consciously touch this seam.
func TestValidateCoinbaseForSubmission(t *testing.T) {
	p2sh, err := hex.DecodeString(p2shLockingScriptHex)
	require.NoError(t, err)

	p2pkh, err := hex.DecodeString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)

	mkCoinbase := func(script []byte) *bt.Tx {
		tx := bt.NewTx()
		tx.Inputs = append(tx.Inputs, &bt.Input{})
		ls := bscript.Script(script)
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 0, LockingScript: &ls})

		return tx
	}

	t.Run("P2SH output rejected", func(t *testing.T) {
		err := validateCoinbaseForSubmission("job", mkCoinbase(p2sh))
		require.Error(t, err)
		require.Contains(t, err.Error(), "bad-txns-vout-p2sh")
	})

	t.Run("P2PKH output accepted", func(t *testing.T) {
		require.NoError(t, validateCoinbaseForSubmission("job", mkCoinbase(p2pkh)))
	})

	t.Run("nil coinbase rejected", func(t *testing.T) {
		require.Error(t, validateCoinbaseForSubmission("job", nil))
	})

	t.Run("wrong input count rejected", func(t *testing.T) {
		tx := mkCoinbase(p2pkh)
		tx.Inputs = append(tx.Inputs, &bt.Input{})
		err := validateCoinbaseForSubmission("job", tx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exactly one input")
	})
}

// TestSubmitMiningSolution_RejectsP2SHCoinbaseOutput verifies the local mining
// guard: a pool-supplied coinbase paying to a P2SH output is refused with
// bad-txns-vout-p2sh (parity with bitcoin-sv's mining RPCs), while an
// unmodified candidate coinbase is not rejected by that guard.
func TestSubmitMiningSolution_RejectsP2SHCoinbaseOutput(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	candidate, err := server.GetMiningCandidate(t.Context(), &blockassembly_api.GetMiningCandidateRequest{})
	require.NoError(t, err)

	t.Run("P2SH coinbase output rejected", func(t *testing.T) {
		coinbaseTx, err := candidate.CreateCoinbaseTxCandidate(server.settings)
		require.NoError(t, err)

		p2shScript, err := bscript.NewFromHexString(p2shLockingScriptHex)
		require.NoError(t, err)
		coinbaseTx.Outputs[0].LockingScript = p2shScript

		resp, err := server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
			SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
				Id:         candidate.Id,
				CoinbaseTx: coinbaseTx.Bytes(),
			},
		})
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "bad-txns-vout-p2sh")
	})

	t.Run("non-P2SH coinbase not rejected by the guard", func(t *testing.T) {
		coinbaseTx, err := candidate.CreateCoinbaseTxCandidate(server.settings)
		require.NoError(t, err)

		_, err = server.submitMiningSolution(t.Context(), &BlockSubmissionRequest{
			SubmitMiningSolutionRequest: &blockassembly_api.SubmitMiningSolutionRequest{
				Id:         candidate.Id,
				CoinbaseTx: coinbaseTx.Bytes(),
			},
		})
		// The submission may still fail deeper in the pipeline (e.g. proof of
		// work); the guard specifically must not fire.
		if err != nil {
			require.NotContains(t, err.Error(), "bad-txns-vout-p2sh")
		}
	})
}
