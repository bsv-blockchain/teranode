//go:build bvgaps

package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/stretchr/testify/require"
)

// TestGapBV005_ProcessBlockMissingParentReturnsServiceError proves GAP-BV-005.
//
// Spec: BV-INGRESS-017 (AC-BV-INGRESS-017.1): with blockvalidation_legacy_unified_below_checkpoint
// = false (the default), ProcessBlock called from "legacy" for a block whose parent is not
// stored MUST return a retryable ERR_SERVICE_ERROR, never success, and nothing is stored.
//
// Fails today because Server.go processBlockFound (the `!parentExists` branch) hands the
// block to catchupCh in a goroutine and returns nil, which ProcessBlock turns into an
// EmptyMessage success. Legacy treats nil as "accepted".
func TestGapBV005_ProcessBlockMissingParentReturnsServiceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, store, block := newProcessBlockFoundHarness(ctx, t)
	require.False(t, s.settings.BlockValidation.LegacyUnifiedBelowCheckpoint, "test assumes the default (non-unified) route")

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

	require.Error(t, err, "BV-INGRESS-017: ProcessBlock must not report success for a block whose parent is missing (it was only diverted to catchup)")

	unwrapped := errors.UnwrapGRPC(err)
	require.NotNil(t, unwrapped)
	require.True(t, errors.Is(unwrapped, errors.ErrServiceError), "BV-INGRESS-017: expected ERR_SERVICE_ERROR, got: %v", unwrapped)

	// Nothing was committed.
	_, stored := store.Blocks[*block.Hash()]
	require.False(t, stored, "the block must not be stored")
}
