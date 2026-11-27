package subtreeprocessor

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// A request ends stored, failed for good, or cancelled; only the first adds the root and only the
// second notes a failure the processor sends again from memory.
func TestStoredSet_FinishByHowTheRequestEnded(t *testing.T) {
	boom := errors.NewStorageError("boom")

	for name, tc := range map[string]struct {
		res        StoreResult
		wantStored bool
		wantFailed bool
	}{
		"stored":    {res: StoreResult{SubtreeStored: true, MetaStored: true}, wantStored: true},
		"failed":    {res: StoreResult{Err: boom}, wantFailed: true},
		"cancelled": {res: StoreResult{Err: boom, Cancelled: true}},
	} {
		t.Run(name, func(t *testing.T) {
			set := NewStoredSet()
			root := chainhash.HashH([]byte(name))

			set.Finish(root, tc.res, false)

			require.Equal(t, tc.wantStored, set.Has(root))
			require.Equal(t, tc.wantFailed, set.HasFailed(root))
		})
	}
}
