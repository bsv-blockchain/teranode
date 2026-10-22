package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/stretchr/testify/require"
)

// A legacy block whose parent is not stored must never come back from ProcessBlock as success
// on the default (non-unified) route either (bitcoin-sv/teranode#4973, BV-INGRESS-017). The
// catch-up divert returns nil, legacy netsync reads nil as accepted, and nothing was stored.
// The error must be a ServiceError so legacy's park keeps the block and retries it
// (parkCommitFailure reads it as IsTransientLocalError), rather than judging it.
func TestProcessBlock_LegacyBlockWithMissingParentReturnsServiceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	require.False(t, s.settings.BlockValidation.LegacyUnifiedBelowCheckpoint, "test assumes the default (non-unified) route")
	s.catchupCh = make(chan processBlockCatchup, 1)

	// The harness stores the parent; remove it so the parent is genuinely missing.
	delete(store.BlockExists, *block.Header.HashPrevBlock)
	delete(store.Blocks, *block.Header.HashPrevBlock)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = s.ProcessBlock(ctx, &blockvalidation_api.ProcessBlockRequest{
		Block:   blockBytes,
		Height:  block.Height,
		BaseUrl: "legacy",
	})
	require.Error(t, err, "ProcessBlock must not report success for a legacy block whose parent is missing")

	unwrapped := errors.UnwrapGRPC(err)
	require.NotNil(t, unwrapped)
	require.True(t, errors.Is(unwrapped, errors.ErrServiceError), "expected ERR_SERVICE_ERROR, got: %v", unwrapped)
	require.True(t, errors.IsTransientLocalError(unwrapped), "legacy's park must read this as retry-later: %v", unwrapped)

	_, stored := store.Blocks[*block.Hash()]
	require.False(t, stored, "the block must not be stored")
	require.Empty(t, s.catchupCh, "a legacy block never takes the catch-up divert")
}

// The refusal is for legacy only. A peer-announced block with a missing parent still diverts
// to catch-up and returns nil: the block-found worker (processBlockWithPriority) relies on that,
// and catch-up is how a p2p node fetches the missing ancestry from the announcing peer.
func TestProcessBlockFound_PeerBlockWithMissingParentStillDivertsToCatchup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	s.catchupCh = make(chan processBlockCatchup, 1)

	delete(store.BlockExists, *block.Header.HashPrevBlock)
	delete(store.Blocks, *block.Header.HashPrevBlock)

	require.NoError(t, s.processBlockFound(ctx, block.Hash(), "peer-1", "http://peer-1", false, block))

	select {
	case c := <-s.catchupCh:
		require.Equal(t, block.Hash(), c.block.Hash())
		require.Equal(t, "http://peer-1", c.baseURL)
	case <-time.After(5 * time.Second):
		t.Fatal("a peer block with a missing parent was not handed to catch-up")
	}
}
