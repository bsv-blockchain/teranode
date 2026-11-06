//go:build bagaps

package blockassembly

// Known-defect proofs for the block assembly spec (services/blockassembly/spec.md).
//
// Each test asserts the spec-correct behaviour and FAILS against the current
// implementation. They sit behind the bagaps build tag so they don't turn the
// default test run red; run them with:
//
//	go test -tags bagaps -run '^TestKnownDefect_' ./services/blockassembly/
//
// When a defect is fixed, drop the TestKnownDefect_ prefix, move the test next
// to the other spec tests, and remove its Conformance Gaps entry from spec.md.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
)

// txStoreThatCannotPersist wraps a blob.Store and fails every Set, so a test can
// drive the coinbase-persistence failure branch of submitMiningSolution.
type txStoreThatCannotPersist struct {
	blob.Store
}

func (f *txStoreThatCannotPersist) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return errors.NewStorageError("injected tx-store Set failure")
}

// TestKnownDefect_CoinbaseStoreFailureStillSubmitsBlock: when the coinbase
// transaction cannot be persisted to the TX store, the submission MUST fail and
// the block MUST NOT stay on the chain (abort before submitting it, or invalidate
// it afterwards).
//
// Today submitMiningSolution only logs the txStore.Set failure, submits the block
// to the Blockchain service anyway and returns Ok, so the chain advances on a
// block whose coinbase was never persisted.
//
// Spec: BA-SOLUTION-003, BA-SOLUTION-007 (tracked as GAP-BA-004).
func TestKnownDefect_CoinbaseStoreFailureStillSubmitsBlock(t *testing.T) {
	s, ctx := newRunningServer(t, &txStoreThatCannotPersist{Store: memory.New()})

	// Given a mined solution for a real candidate, and a TX store that rejects writes
	candidate, solution := mineRealCandidate(t, s, ctx, "coinbase-store-failure")

	tipBefore, _, err := s.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	// When the solution is submitted
	resp, err := s.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         candidate.Id,
		Nonce:      solution.Nonce,
		Time:       solution.Time,
		Version:    solution.Version,
		CoinbaseTx: solution.Coinbase,
	})

	// Then the submission fails ...
	require.Error(t, err, "a coinbase that could not be persisted must fail the submission")
	require.False(t, resp != nil && resp.Ok, "the submission must not return Ok")

	// ... and the chain tip is unchanged (SubmitMiningSolutionWaitForResponse is on,
	// so any AddBlock has completed by the time the call returns)
	tipAfter, _, err := s.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, tipBefore.Hash(), tipAfter.Hash(),
		"a block whose coinbase was not persisted must not stay on the chain")
}

// TestKnownDefect_GetMiningCandidateServesWhileUnminedTxsLoading: while block
// assembly is still replaying unmined transactions at startup, GetMiningCandidate
// MUST reject as not ready, like SubmitMiningSolution and the reset calls do.
//
// Today GetMiningCandidate only checks that the blockchain FSM is RUNNING. During
// startup recovery the FSM is usually already RUNNING while block assembly is
// still loading, so a miner can be handed a candidate built from a partly
// recovered pipeline that omits unmined transactions not yet reloaded.
//
// Spec: BA-STATE-001 matrix row "GetMiningCandidate | LoadingUnmined | reject
// (not ready)", BA-CANDIDATE-001 (tracked as GAP-BA-005).
func TestKnownDefect_GetMiningCandidateServesWhileUnminedTxsLoading(t *testing.T) {
	s, ctx := newRunningServer(t, nil)

	// Given the blockchain FSM is RUNNING but block assembly is still loading
	// unmined transactions
	s.blockAssembler.unminedTransactionsLoading.Store(true)
	t.Cleanup(func() { s.blockAssembler.unminedTransactionsLoading.Store(false) })

	// When a miner asks for a candidate
	_, err := s.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{})

	// Then it is refused as not ready
	require.Error(t, err, "GetMiningCandidate must reject while unmined transactions are still loading")
	require.True(t, errors.Is(err, errors.ErrServiceNotReady), "expected ServiceNotReady, got %v", err)
}
