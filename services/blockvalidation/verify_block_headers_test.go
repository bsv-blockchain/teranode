package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/require"
)

// TestVerifyBlockHeaders guards the hash comparison in verifyBlockHeaders, which uses
// the zero-alloc chainhash.Hash.IsEqual instead of string conversion.
func TestVerifyBlockHeaders(t *testing.T) {
	blocks := testhelpers.CreateTestBlockChain(t, 5)
	blockUpTo := blocks[len(blocks)-1]

	headers := make([]*model.BlockHeader, len(blocks))
	for i, b := range blocks {
		headers[i] = b.Header
	}

	t.Run("all hashes match", func(t *testing.T) {
		require.NoError(t, verifyBlockHeaders(blocks, headers, blockUpTo))
	})

	t.Run("mismatch is detected", func(t *testing.T) {
		mismatched := make([]*model.BlockHeader, len(headers))
		copy(mismatched, headers)
		// Swap two distinct headers so positions 0 and 1 no longer match their blocks.
		mismatched[0], mismatched[1] = mismatched[1], mismatched[0]

		err := verifyBlockHeaders(blocks, mismatched, blockUpTo)
		require.Error(t, err)
		require.Contains(t, err.Error(), "block hash mismatch at index 0")
	})
}
